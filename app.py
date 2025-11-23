# app.py  ← NEW BACKGROUND-JOB VERSION (SAFE FOR 3000+ INVOICES)
from flask import Flask, request, render_template, jsonify, redirect
from pathlib import Path
import os
import logging
from io import StringIO
import pandas as pd
import tempfile
import time
from decimal import Decimal, ROUND_HALF_UP
from werkzeug.middleware.proxy_fix import ProxyFix
from rq import Queue
from redis import Redis
import uuid

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

# ←←← REDIS QUEUE SETUP (NEW)
redis_conn = Redis.from_url(os.getenv('REDIS_URL'))
q = Queue('default', connection=redis_conn)

# Your existing process_csv_file stays EXACTLY THE SAME
# (copy-paste it unchanged — I'm keeping it here for completeness)
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
                        "TaxCodeRef": {"value": "6"}
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
                        "TaxCodeRef": {"value": "6"}
                    })

                    line = {
                        'DetailType': 'SalesItemLineDetail',
                        'Amount': amount,
                        'Description': description,
                        'SalesItemLineDetail': sales_item_detail,
                    }

                lines.append(line)
            return lines

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

                logger.info(f"Using Customer ID {customer_id} for invoice {invoice_num}")

                total_invoices = len(grouped)
                delay = min(2.0, max(0.6, 6600.0 / total_invoices))
                time.sleep(delay)

                transaction_type = mapper.determine_transaction_type(group)

                if transaction_type == "invoice":
                    lines = build_lines(group, invoice_num, for_invoice=True)
                    if not lines:
                        logger.warning(f"No valid lines for invoice {invoice_num}")
                        continue
                    result = invoice_service.create_or_update_invoice(group, customer_id, lines)
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


# ==================== NEW UPLOAD ROUTE (INSTANT RESPONSE) ====================
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file part'})

    file = request.files['file']
    if not file or not file.filename.lower().endswith('.csv'):
        return jsonify({'success': False, 'error': 'Please upload a CSV file'})

    # Save file so worker can access it
    upload_dir = Path("/tmp/uploads")
    upload_dir.mkdir(exist_ok=True)
    job_id = str(uuid.uuid4())
    file_path = upload_dir / f"{job_id}.csv"
    file.save(str(file_path))

    # ENQUEUE — returns in <1 second
    job = q.enqueue(process_csv_file, str(file_path), job_id=job_id, job_timeout=10800)  # 3 hours max

    return jsonify({
        'success': True,
        'job_id': job.id,
        'message': 'Upload successful! Processing started in background...'
    })


# ==================== NEW STATUS ENDPOINT ====================
@app.route('/status/<job_id>')
def job_status(job_id):
    job = q.fetch_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404

    response = {
        'job_id': job_id,
        'status': job.get_status(),
        'is_finished': job.is_finished,
        'is_failed': job.is_failed,
    }

    if job.is_finished or job.is_failed:
        result = job.result
        response['success'] = result[0] if result else False
        response['logs'] = result[1] if result else "No logs"
    else:
        response['logs'] = "Processing in background... (this can take 10–60 minutes for large files)"

    return jsonify(response)


# Your existing routes (/, /login, /callback) stay exactly the same
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/login')
def login():
    auth = QuickBooksAuth()
    url, _ = auth.get_authorization_url()
    return redirect(url)

@app.route('/callback')
def callback():
    # ... your existing callback code unchanged ...
    # (keep it exactly as you had it)

    @app.errorhandler(404)
    def not_found(e):
        if request.path.startswith('/static/'):
            return "Not found", 404
        return render_template('index.html'), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 3000)), debug=True)