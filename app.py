# app.py  (FINAL WORKING VERSION – NOV 2025)
from flask import Flask, request, render_template, jsonify, redirect, send_from_directory
import sys
from pathlib import Path
import os
import logging
from io import StringIO
import pandas as pd
import tempfile
import time
from decimal import Decimal, ROUND_HALF_UP
from werkzeug.middleware.proxy_fix import ProxyFix

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

BASE_DIR = Path(__file__).resolve().parent
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

        required_columns = [
            'Invoice No.', 'Patient Name', 'Patient ID', 'Product / Service',
            'Description', 'Total Amount', 'Quantity', 'Unit Cost',
            'Service Date', 'Mode of Payment'
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required CSV columns: {missing_columns}")
            return False, log_stream.getvalue()

        results = []
        grouped = df.groupby('Invoice No.')

        def parse_money(value):
            if pd.isna(value):
                return Decimal('0.00')
            s = str(value).strip()
            s = ''.join(c for c in s if c in '0123456789.-')
            if not s or s in {'.', '-'}:
                return Decimal('0.00')
            try:
                return Decimal(s)
            except Exception:
                return Decimal('0.00')

        def calculate_markup_factor(row):
            try:
                qty = Decimal(str(row['Quantity']))
                unit_cost = parse_money(row['Unit Cost'])
                total_amount = parse_money(row['Total Amount'])
                if qty <= 0 or unit_cost <= 0 or total_amount <= 0:
                    return Decimal('1.0')
                expected = unit_cost * qty
                if expected == 0:
                    return Decimal('1.0')
                factor = total_amount / expected
                return factor.quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
            except Exception:
                return Decimal('1.0')

        def build_lines(group, invoice_num, for_invoice=False):
            lines = []
            for _, row in group.iterrows():
                item_id = product_service.find_or_create_product(row, invoice_num)
                total_amount_csv = parse_money(row['Total Amount'])
                qty_csv = Decimal(str(row['Quantity'] or '1'))
                unit_cost_csv = parse_money(row['Unit Cost'])

                if total_amount_csv <= 0:
                    continue

                markup_factor = calculate_markup_factor(row)
                description = str(row.get('Description', '') or '').strip()

                sales_item_detail = {
                    'ItemRef': {'value': str(item_id)},
                }

                if for_invoice:
                    qty_to_send = Decimal('1')
                    unit_price = total_amount_csv.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    amount = unit_price
                    desc_parts = [description] if description else []
                    if qty_csv != 1:
                        desc_parts.append(f"Qty: {qty_csv}")
                    full_desc = " | ".join(filter(None, desc_parts))

                    sales_item_detail.update({
                        'Qty': 1.0,
                        'UnitPrice': float(unit_price),
                        "TaxCodeRef": {"value": "6"}   # ← ZERO VAT
                    })

                    line = {
                        'DetailType': 'SalesItemLineDetail',
                        'Amount': float(unit_price),
                        'Description': full_desc,
                        'SalesItemLineDetail': sales_item_detail,
                    }

                else:
                    qty_to_send = float(qty_csv) if qty_csv > 0 else 1.0
                    unit_price = float(unit_cost_csv.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
                    amount = float((Decimal(str(qty_to_send)) * unit_cost_csv).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))

                    sales_item_detail.update({
                        'Qty': qty_to_send,
                        'UnitPrice': unit_price,
                        "TaxCodeRef": {"value": "6"}   # ← ZERO VAT
                    })

                    line = {
                        'DetailType': 'SalesItemLineDetail',
                        'Amount': amount,
                        'Description': description,
                        'SalesItemLineDetail': sales_item_detail,
                    }

                lines.append(line)
            return lines

        # ================================
        # MAIN LOOP – FIXED VERSION
        # ================================
        for invoice_num, group in grouped:
            try:
                mapper = TransactionMapper()
                is_insurance = mapper.is_insurance_transaction(group)

                if is_insurance:
                    insurance_name = mapper.extract_insurance_name(group)
                    customer_id = customer_service.find_or_create_customer(
                        group, mapper,
                        customer_type="insurance" if insurance_name else "patient",
                        insurance_name=insurance_name
                    )
                else:
                    customer_id = customer_service.find_or_create_customer(group, mapper, customer_type="patient")

                # CRITICAL FIX: ENSURE WE HAVE A VALID CUSTOMER ID BEFORE PROCEEDING
                if not customer_id or not str(customer_id).isdigit():
                    logger.error(f"Invalid or missing Customer ID for invoice {invoice_num}. Got: {customer_id}")
                    results.append({"invoice": invoice_num, "status": "error", "error": "Customer ID missing or invalid"})
                    continue

                logger.info(f"Using Customer ID {customer_id} for invoice {invoice_num}")

                # Small delay to reduce rate limiting (optional but helpful)
                time.sleep(0.8)

                transaction_type = mapper.determine_transaction_type(group)

                if transaction_type == "invoice":
                    lines = build_lines(group, invoice_num, for_invoice=True)
                    if not lines:
                        logger.warning(f"No valid lines for invoice {invoice_num}")
                        continue
                    result = invoice_service.create_invoice(group, customer_id, lines)
                    logger.info(f"Invoice created → QB ID: {result.get('Id')}")

                elif transaction_type == "sales_receipt":
                    lines = build_lines(group, invoice_num, for_invoice=False)
                    if not lines:
                        continue
                    result = receipt_service.create_sales_receipt(group, customer_id, lines)
                    logger.info(f"Sales Receipt created → QB ID: {result.get('Id')}")

                else:
                    logger.warning(f"Unknown transaction type '{transaction_type}' for {invoice_num}")
                    results.append({"invoice": invoice_num, "status": "error", "error": f"Unknown type: {transaction_type}"})
                    continue

                results.append({
                    "invoice": invoice_num,
                    "status": "success",
                    "transaction_id": result.get("Id"),
                    "type": transaction_type
                })

            except Exception as e:
                logger.error(f"Error processing invoice {invoice_num}: {str(e)}", exc_info=True)
                results.append({"invoice": invoice_num, "status": "error", "error": str(e)})

        log_processing_result(file_path, results)
        return True, log_stream.getvalue()

    except Exception as e:
        logger.error(f"Failed to process CSV: {str(e)}", exc_info=True)
        return False, log_stream.getvalue()


# ----------------------------------------------------------------------
# 6. Routes
# ----------------------------------------------------------------------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        logger.error("No file uploaded")
        return jsonify({'success': False, 'logs': log_stream.getvalue()})

    file = request.files['file']
    if not file.filename or not file.filename.lower().endswith('.csv'):
        logger.error("Invalid file")
        return jsonify({'success': False, 'logs': log_stream.getvalue()})

    log_stream.truncate(0)
    log_stream.seek(0)

    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
        file.save(tmp.name)
        tmp_path = tmp.name

    try:
        success, logs = process_csv_file(tmp_path)
        return jsonify({'success': success, 'logs': logs})
    except Exception as e:
        logger.error(f"Processing failed: {e}", exc_info=True)
        return jsonify({'success': False, 'logs': log_stream.getvalue()}), 500
    finally:
        # SAFE delete – ignore if file gone
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except Exception as e:
            logger.warning(f"Could not delete temp file {tmp_path}: {e}")


@app.route('/login')
def login():
    auth = QuickBooksAuth()
    url, _ = auth.get_authorization_url()
    return redirect(url)

@app.route('/callback')
def callback():
    auth = QuickBooksAuth()
    try:
        tokens = auth.fetch_tokens(request.url)
        new_refresh_token = tokens['refresh_token']
        realm_id = auth.get_realm_id() or "UNKNOWN"

        html = f"""
        <div style="font-family: system-ui, sans-serif; padding: 50px; text-align: center; background: #f0fdf4; min-height: 100vh;">
            <h1 style="color: #16a34a;">QuickBooks Connected Successfully Connected!</h1>
            <h2>COPY THESE TWO LINES IMMEDIATELY</h2>
            <pre style="background:#000;color:#0f0;padding:40px;font-size:24px;border-radius:12px;display:inline-block;">
QB_REFRESH_TOKEN={new_refresh_token}

QB_REALM_ID={realm_id}
            </pre>
            <p style="font-size:20px;margin-top:30px;">
                → Go to Vercel Dashboard → Your Project → Settings → Environment Variables<br>
                → Paste the two lines above (Production environment)<br>
                → Save → Wait 10 seconds → Upload any CSV → IT WILL WORK
            </p>
            <script>
                navigator.clipboard.writeText("QB_REFRESH_TOKEN={new_refresh_token}\\nQB_REALM_ID={realm_id}");
                alert("Copied to clipboard!");
            </script>
        </div>
        """
        return html

    except Exception as e:
        return f"<h2 style='color:red;'>Connection failed: {str(e)}</h2><p>Try again or contact support.</p>", 500

# ----------------------------------------------------------------------
# 7. SPA fallback (optional)
# ----------------------------------------------------------------------
@app.errorhandler(404)
def not_found(e):
    if request.path.startswith('/static/'):
        return "Not found", 404
    return render_template('index.html'), 200

# ----------------------------------------------------------------------
# 8. Local dev
# ----------------------------------------------------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 3000)), debug=True)