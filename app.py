# app.py  ← FINAL FREE VERSION (NO REDIS, NO WORKER, HANDLES 5000+ ROWS)
from flask import Flask, request, render_template, jsonify, redirect
from pathlib import Path
import os
import logging
from io import StringIO
import pandas as pd
import time
import tempfile
from decimal import Decimal, ROUND_HALF_UP
from werkzeug.middleware.proxy_fix import ProxyFix

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

BASE_DIR = Path(__file__).resolve().parent
import sys
sys.path.append(str(BASE_DIR / "src"))

from src.csv_parser import CSVParser
from src.mapper import TransactionMapper
from src.customer_service import CustomerService
from src.product_service import ProductService
from src.invoice_service import InvoiceService
from src.receipt_service import ReceiptService
from src.qb_auth import QuickBooksAuth
from src.qb_client import QuickBooksClient
from src.logger import setup_logger, log_processing_result

app = Flask(__name__, template_folder=str(BASE_DIR / "templates"), static_folder=str(BASE_DIR / "static"))
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

logger = setup_logger(__name__)
log_stream = StringIO()
handler = logging.StreamHandler(log_stream)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# ←←← CHUNKED VERSION (FREE FOREVER) ←←←
def process_csv_file(file_path):
    try:
        qb_auth = QuickBooksAuth()
        qb_client = QuickBooksClient(qb_auth)
        customer_service = CustomerService(qb_client)
        product_service = ProductService(qb_client)
        invoice_service = InvoiceService(qb_client)
        receipt_service = ReceiptService(qb_client)

        parser = CSVParser()
        df = parser.parse_file(file_path)
        logger.info(f"Successfully parsed CSV with {len(df)} rows")

        required_columns = ['Invoice No.', 'Patient Name', 'Patient ID', 'Product / Service',
                            'Description', 'Total Amount', 'Quantity', 'Unit Cost',
                            'Service Date', 'Mode of Payment']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False, log_stream.getvalue()

        results = []
        grouped = df.groupby('Invoice No.')
        invoice_groups = list(grouped)
        total_invoices = len(invoice_groups)
        logger.info(f"Found {total_invoices} unique invoices – starting chunked processing")

        chunk_size = 50
        for chunk_start in range(0, total_invoices, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_invoices)
            current_chunk = invoice_groups[chunk_start:chunk_end]
            logger.info(f"Processing chunk {(chunk_start//chunk_size)+1}: invoices {chunk_start+1}–{chunk_end}")

            def parse_money(value):
                if pd.isna(value): return Decimal('0.00')
                s = ''.join(c for c in str(value).strip() if c in '0123456789.-')
                return Decimal(s) if s and s not in {'.', '-'} else Decimal('0.00')

            def calculate_markup_factor(row):
                try:
                    qty = Decimal(str(row['Quantity']))
                    unit_cost = parse_money(row['Unit Cost'])
                    total_amount = parse_money(row['Total Amount'])
                    if qty <= 0 or unit_cost <= 0 or total_amount <= 0: return Decimal('1.0')
                    factor = total_amount / (unit_cost * qty)
                    return factor.quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
                except: return Decimal('1.0')

            def build_lines(group, invoice_num, for_invoice=False):
                lines = []
                for _, row in group.iterrows():
                    item_id = product_service.find_or_create_product(row, invoice_num)
                    total_amount_csv = parse_money(row['Total Amount'])
                    if total_amount_csv <= 0: continue

                    qty_csv = Decimal(str(row['Quantity'] or '1'))
                    description = str(row.get('Description', '') or '').strip()

                    sales_item_detail = {'ItemRef': {'value': str(item_id)}}

                    if for_invoice:
                        unit_price = float(total_amount_csv.quantize(Decimal('0.01'), ROUND_HALF_UP))
                        desc_parts = [description] if description else []
                        if qty_csv != 1: desc_parts.append(f"Qty: {qty_csv}")
                        full_desc = " | ".join(filter(None, desc_parts))
                        sales_item_detail.update({'Qty': 1.0, 'UnitPrice': unit_price, "TaxCodeRef": {"value": "6"}})
                        line = {'DetailType': 'SalesItemLineDetail', 'Amount': unit_price, 'Description': full_desc, 'SalesItemLineDetail': sales_item_detail}
                    else:
                        qty_to_send = float(qty_csv) if qty_csv > 0 else 1.0
                        unit_price = float(parse_money(row['Unit Cost']).quantize(Decimal('0.01'), ROUND_HALF_UP))
                        amount = float((Decimal(str(qty_to_send)) * parse_money(row['Unit Cost'])).quantize(Decimal('0.01'), ROUND_HALF_UP))
                        sales_item_detail.update({'Qty': qty_to_send, 'UnitPrice': unit_price, "TaxCodeRef": {"value": "6"}})
                        line = {'DetailType': 'SalesItemLineDetail', 'Amount': amount, 'Description': description, 'SalesItemLineDetail': sales_item_detail}
                    lines.append(line)
                return lines

            for invoice_num, group in current_chunk:
                try:
                    mapper = TransactionMapper()
                    is_insurance = mapper.is_insurance_transaction(group)   # True if "Is Insurance?" = Yes

                    # ———— FIXED INSURANCE LOGIC ————
                    if is_insurance:
                        insurance_name = mapper.extract_insurance_name(group)   # pulls from "Mode of Payment"
                        if insurance_name and insurance_name.strip():
                            # Bill to insurance company → create INVOICE
                            customer_id = customer_service.find_or_create_customer(
                                group,
                                mapper,
                                customer_type="insurance",
                                insurance_name=insurance_name.strip()
                            )
                            transaction_type = "invoice"          # ← force invoice
                            logger.info(f"INSURANCE → INVOICE for '{insurance_name.strip()}' (Invoice #{invoice_num})")
                        else:
                            # Insurance flag but no name → fallback to patient as cash patient
                            customer_id = customer_service.find_or_create_customer(group, mapper, customer_type="patient")
                            transaction_type = "sales_receipt"
                            logger.info(f"Insurance flag but no name → Sales Receipt for patient (Invoice #{invoice_num})")
                    else:
                        # Normal cash / MPESA / etc.
                        customer_id = customer_service.find_or_create_customer(group, mapper, customer_type="patient")
                        transaction_type = "sales_receipt"
                        logger.info(f"Cash patient → Sales Receipt (Invoice #{invoice_num})")
                    # ———————————————

                    delay = min(2.0, max(0.6, 6600.0 / total_invoices))
                    time.sleep(delay)

                    # ←←← DELETE OR COMMENT THE NEXT LINE — it was overriding everything!
                    # transaction_type = mapper.determine_transaction_type(group)

                    lines = build_lines(group, invoice_num, for_invoice=(transaction_type == "invoice"))
                    if not lines:
                        logger.warning(f"No lines for invoice {invoice_num}")
                        continue

                    if transaction_type == "invoice":
                        result = invoice_service.create_or_update_invoice(group, customer_id, lines)
                        logger.info(f"Invoice created → QB ID: {result.get('Invoice', {}).get('Id')}")
                    else:
                        result = receipt_service.create_sales_receipt(group, customer_id, lines)
                        logger.info(f"Sales Receipt created → QB ID: {result.get('SalesReceipt', {}).get('Id')}")

                    results.append({
                        "invoice": invoice_num,
                        "status": "success",
                        "transaction_id": result.get('Invoice', result.get('SalesReceipt', {})).get('Id'),
                        "type": transaction_type
                    })

                except Exception as e:
                    logger.error(f"Error on invoice {invoice_num}: {str(e)}", exc_info=True)
                    results.append({"invoice": invoice_num, "status": "error", "error": str(e)})

            logger.info(f"Chunk finished – {chunk_end}/{total_invoices} done")

        log_processing_result(file_path, results)
        return True, log_stream.getvalue()

    except Exception as e:
        logger.error(f"Failed to process CSV: {str(e)}", exc_info=True)
        return False, log_stream.getvalue()


@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file'})

    file = request.files['file']
    if not file or not file.filename.lower().endswith('.csv'):
        return jsonify({'success': False, 'error': 'CSV only'})

    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
        file.save(tmp.name)
        success, logs = process_csv_file(tmp.name)
        os.unlink(tmp.name)

    return jsonify({
        'success': success,
        'message': 'Done!' if success else 'Failed',
        'logs': logs
    })

@app.route('/')
def index(): return render_template('index.html')

@app.route('/login')
def login():
    auth = QuickBooksAuth()
    url, _ = auth.get_authorization_url()
    return redirect(url)

@app.route('/callback')
def callback():
    auth_response_url = request.url  # Full URL QuickBooks redirected to

    if 'error' in request.args:
        error = request.args.get('error')
        description = request.args.get('error_description', '')
        return f"QuickBooks authorization failed: {error} – {description}", 400

    if 'code' not in request.args and 'state' not in request.args:
        return "Missing authorization code from QuickBooks", 400

    auth = QuickBooksAuth()

    try:
        tokens = auth.fetch_tokens(auth_response_url)

        realm_id = tokens.get("realmId")
        logger.info("OAuth2 flow completed successfully!")
        logger.info(f"Company ID (realmId): {realm_id}")
        logger.info("A new refresh token has been printed in the logs above.")
        logger.info("Copy the new QB_REFRESH_TOKEN and update it in your environment variables.")
        logger.info("Also set QB_REALM_ID if it's not already set.")

        return '''
        <h2>Connected to QuickBooks successfully!</h2>
        <p>Check the server logs – your <strong>new QB_REFRESH_TOKEN</strong> is printed there.</p>
        <p>Copy it and update your environment variable immediately (it only shows once).</p>
        <p>Optionally set <code>QB_REALM_ID={realm_id}</code> too.</p>
        <hr>
        <a href="/">← Back to upload page</a>
        '''.replace("{realm_id}", str(realm_id))

    except Exception as e:
        logger.error("Callback failed", exc_info=True)
        return f"Authentication failed: {str(e)}", 500

@app.errorhandler(404)
def not_found(e):
    if request.path.startswith('/static/'): return "Not found", 404
    return render_template('index.html'), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 3000)))