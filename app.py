# app.py  (place in project root)
from flask import Flask, request, render_template, jsonify, redirect
import sys
from pathlib import Path
import os
import logging
from io import StringIO
import pandas as pd
import tempfile
from werkzeug.middleware.proxy_fix import ProxyFix

# ----------------------------------------------------------------------
# 1. Environment & path setup
# ----------------------------------------------------------------------
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

# Add src/ to Python path (relative to root app.py)
sys.path.append(str(Path(__file__).parent / "src"))

# ----------------------------------------------------------------------
# 2. Import your own modules
# ----------------------------------------------------------------------
from src.csv_parser import CSVParser
from src.mapper import TransactionMapper
from src.customer_service import CustomerService
from src.product_service import ProductService
from src.invoice_service import InvoiceService
from src.receipt_service import ReceiptService
from src.qb_auth import QuickBooksAuth
from src.qb_client import QuickBooksClient
from src.logger import setup_logger, log_processing_result

# ----------------------------------------------------------------------
# 3. Flask app + ProxyFix
# ----------------------------------------------------------------------
app = Flask(__name__, template_folder="templates", static_folder="static")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# ----------------------------------------------------------------------
# 4. Logger that captures output for the UI
# ----------------------------------------------------------------------
logger = setup_logger(__name__)
log_stream = StringIO()
handler = logging.StreamHandler(log_stream)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# ----------------------------------------------------------------------
# 5. Core CSV processing (exact copy of your original function)
# ----------------------------------------------------------------------
def process_csv_file(file_path):
    """Main processing workflow for CSV files from file path"""
    try:
        qb_auth = QuickBooksAuth()
        qb_client = QuickBooksClient(qb_auth)
        customer_service = CustomerService(qb_client)
        product_service = ProductService(qb_client)
        invoice_service = InvoiceService(qb_client)
        receipt_service = ReceiptService(qb_client)
        mapper = TransactionMapper()

        parser = CSVParser()
        df = parser.parse_file(file_path)  # Parse from file path
        logger.info(f"Successfully parsed CSV with {len(df)} rows")

        # Validate required columns
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

        for invoice_num, group in grouped:
            try:
                # ---- Customer -------------------------------------------------
                mapper = TransactionMapper()  # fresh instance per invoice
                is_insurance = mapper.is_insurance_transaction(group)
                if is_insurance:
                    insurance_name = mapper.extract_insurance_name(group)
                    if insurance_name:
                        customer_id = customer_service.find_or_create_customer(
                            group, mapper, customer_type="insurance",
                            insurance_name=insurance_name
                        )
                    else:
                        customer_id = customer_service.find_or_create_customer(
                            group, mapper, customer_type="patient"
                        )
                else:
                    customer_id = customer_service.find_or_create_customer(
                        group, mapper, customer_type="patient"
                    )

                # ---- Line items ---------------------------------------------
                lines = []
                for _, row in group.iterrows():
                    item_id = product_service.find_or_create_product(row, invoice_num)
                    qty = float(row['Quantity'])
                    unit_price = float(row['Unit Cost'])
                    calculated_amount = qty * unit_price
                    if abs(calculated_amount - float(row['Total Amount'])) > 0.01:
                        logger.warning(
                            f"Mismatched amount for invoice {invoice_num}, "
                            f"item {row['Product / Service']}: "
                            f"CSV Total {row['Total Amount']} != {qty} * {unit_price}. "
                            f"Using calculated {calculated_amount}."
                        )
                    if calculated_amount == 0:
                        logger.warning(
                            f"Skipping zero-amount line for invoice {invoice_num}, "
                            f"item {row['Product / Service']}."
                        )
                        continue

                    # Insurance markup
                    is_insurance_row = str(row.get('Is Insurance?', '')).strip().lower() == 'yes'
                    category = str(row.get('Product / Service', '')).strip()
                    markup_map = {
                        'Pharmacy': 1.35,
                        'Laboratory': 1.20,
                        'Radiology': 1.25
                    }
                    adjusted_unit_price = (
                        round(unit_price * markup_map[category], 2)
                        if is_insurance_row and category in markup_map else unit_price
                    )
                    calculated_amount = round(qty * adjusted_unit_price, 2)

                    line = {
                        'DetailType': 'SalesItemLineDetail',
                        'Amount': calculated_amount,
                        'SalesItemLineDetail': {
                            'ItemRef': {'value': item_id},
                            'Qty': qty,
                            'UnitPrice': adjusted_unit_price
                        },
                        'Description': row['Description']
                    }
                    lines.append(line)

                # ---- Transaction type -----------------------------------------
                transaction_type = mapper.determine_transaction_type(group)
                if transaction_type == "sales_receipt":
                    result = receipt_service.create_sales_receipt(group, customer_id, lines)
                    logger.info(f"Created sales receipt for invoice {invoice_num}")
                elif transaction_type == "invoice":
                    result = invoice_service.create_invoice(group, customer_id, lines)
                    logger.info(f"Created invoice for invoice {invoice_num}")
                else:
                    logger.warning(f"Unknown transaction type '{transaction_type}' for invoice {invoice_num}")
                    results.append({
                        "invoice": invoice_num,
                        "status": "error",
                        "error": f"Unknown transaction type: {transaction_type}"
                    })
                    continue

                results.append({
                    "invoice": invoice_num,
                    "status": "success",
                    "transaction_id": result.get("Id"),
                    "type": transaction_type
                })

            except Exception as e:
                logger.error(f"Error processing invoice {invoice_num}: {str(e)}")
                results.append({
                    "invoice": invoice_num,
                    "status": "error",
                    "error": str(e)
                })

        # Move processed file (your log_processing_result does this)
        log_processing_result(file_path, results)
        return True, log_stream.getvalue()

    except Exception as e:
        logger.error(f"Failed to process CSV: {str(e)}")
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
    if file.filename == '' or not file.filename.lower().endswith('.csv'):
        logger.error("Invalid file")
        return jsonify({'success': False, 'logs': log_stream.getvalue()})

    # Reset log stream for this request
    log_stream.truncate(0)
    log_stream.seek(0)

    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
        file.save(tmp.name)
        tmp_path = tmp.name

    success, logs = process_csv_file(tmp_path)
    return jsonify({'success': success, 'logs': logs})

@app.route('/login')
def login():
    auth = QuickBooksAuth()
    url, _ = auth.get_authorization_url()
    return redirect(url)

@app.route('/callback')
def callback():
    auth = QuickBooksAuth()
    tokens = auth.fetch_tokens(request.url)
    realm_id = request.args.get("realmId")
    return jsonify({
        "status": "connected",
        "realmId": realm_id,
        "tokens": tokens
    })

# ----------------------------------------------------------------------
# 7. Vercel entry-point (required for serverless)
# ----------------------------------------------------------------------
def handler(event, context=None):
    """Vercel serverless function wrapper."""
    from werkzeug.serving import run_simple
    return run_simple('0.0.0.0', int(os.environ.get('PORT', 3000)), app, use_reloader=False, use_debugger=False)

# ----------------------------------------------------------------------
# 8. Local dev entry-point
# ----------------------------------------------------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 3000)), debug=True)