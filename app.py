# app.py
"""
Flask entrypoint. Sync processing, DB-backed auth & audit.
"""
from flask import Flask, request, render_template, jsonify, redirect, session
from pathlib import Path
import os
import sys
import logging
import tempfile
from io import StringIO
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd
from werkzeug.middleware.proxy_fix import ProxyFix

os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")

BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR / "src"))

import db
db.init_db()  # idempotent; safe every boot

from src.csv_parser import CSVParser
from src.mapper import TransactionMapper
from src.customer_service import CustomerService
from src.product_service import ProductService
from src.invoice_service import InvoiceService
from src.receipt_service import ReceiptService
from src.qb_auth import QuickBooksAuth, QuickBooksAuthError
from src.qb_client import QuickBooksClient
from src.logger import setup_logger, log_processing_result

app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "templates"),
    static_folder=str(BASE_DIR / "static"),
)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-key-change-me")

logger = setup_logger(__name__)
log_stream = StringIO()
_handler = logging.StreamHandler(log_stream)
_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(_handler)


# ---------- auth gate ----------
def require_connection():
    try:
        QuickBooksAuth().get_valid_access_token()
        return True
    except QuickBooksAuthError as e:
        logger.error(f"Connection validation failed: {e}")
        session.clear()
        return False
    except Exception as e:
        logger.error(f"Connection validation failed: {e}", exc_info=True)
        session.clear()
        return False


# ---------- chunked processing ----------
def process_csv_file(file_path, upload_id):
    """
    Reads the CSV, groups by Invoice No., and pushes each invoice to QB.
    Records every invoice outcome to the DB (idempotent), no file moves.
    """
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
            'Service Date', 'Mode of Payment', 'Is Insurance?',
        ]
        missing = [c for c in required_columns if c not in df.columns]
        if missing:
            logger.error(f"Missing required columns: {missing}")
            return False, log_stream.getvalue()

        results = []
        invoice_groups = list(df.groupby('Invoice No.'))
        total_invoices = len(invoice_groups)
        logger.info(f"Found {total_invoices} unique invoices – starting chunked processing")

        # helpers -----------------------------------------------------------
        def parse_money(value):
            if pd.isna(value):
                return Decimal('0.00')
            s = ''.join(c for c in str(value).strip() if c in '0123456789.-')
            return Decimal(s) if s and s not in {'.', '-'} else Decimal('0.00')

        def build_lines(group, invoice_num, for_invoice=False):
            lines = []
            inventory_adjustments = []
            total_value = Decimal('0.00')

            for _, row in group.iterrows():
                qty_csv = Decimal(str(row['Quantity'] or '1'))
                total_amount_csv = parse_money(row['Total Amount'])
                description = str(row.get('Description', '') or '').strip()

                total_value += total_amount_csv
                if total_amount_csv <= 0:
                    continue

                prod_svc_raw = str(row.get('Product / Service', '') or '').strip()
                items = [i.strip() for i in prod_svc_raw.split(',') if i.strip()]
                if not items:
                    items = [prod_svc_raw]

                for item in items:
                    try:
                        item_row = row.copy()
                        item_row['Product / Service'] = item
                        item_id = product_service.find_or_create_product(item, invoice_num)
                        line = {
                            "DetailType": "SalesItemLineDetail",
                            "Amount": 0.0,
                            "SalesItemLineDetail": {
                                "ItemRef": {"value": str(item_id)},
                                "Qty": 1.0,
                                "UnitPrice": 0.0,
                                "TaxCodeRef": {"value": "6"},
                            },
                        }
                        lines.append(line)
                        if for_invoice and product_service.is_pharmacy_item(item_row) and qty_csv > 1:
                            inventory_adjustments.append({
                                "item_id": item_id,
                                "real_qty": int(qty_csv),
                                "description": description or item,
                            })
                    except Exception as e:
                        logger.warning(f"Item {item!r} failed on {invoice_num}: {e} — skipping")

            if inventory_adjustments and for_invoice:
                group._inventory_adjustments = inventory_adjustments

            if lines and total_value > 0:
                total_item_id = product_service.find_or_create_product(
                    "Total Visit Charges", invoice_num)
                lines.append({
                    "DetailType": "SalesItemLineDetail",
                    "Amount": float(total_value.quantize(Decimal('0.01'))),
                    "Description": "Total Visit Charges",
                    "SalesItemLineDetail": {
                        "ItemRef": {"value": str(total_item_id)},
                        "Qty": 1.0,
                        "UnitPrice": float(total_value.quantize(Decimal('0.01'))),
                        "TaxCodeRef": {"value": "6"},
                    },
                })
            return lines

        # main loop ---------------------------------------------------------
        chunk_size = 50
        for start in range(0, total_invoices, chunk_size):
            end = min(start + chunk_size, total_invoices)
            chunk = invoice_groups[start:end]
            logger.info(f"Processing chunk {(start//chunk_size)+1}: invoices {start+1}–{end}")

            for invoice_num, group in chunk:
                try:
                    mapper = TransactionMapper()
                    is_insurance = mapper.is_insurance_transaction(group)

                    if is_insurance:
                        ins_name = mapper.extract_insurance_name(group)
                        if ins_name and ins_name.strip():
                            customer_id = customer_service.find_or_create_customer(
                                group, mapper, customer_type="insurance",
                                insurance_name=ins_name.strip())
                            transaction_type = "invoice"
                        else:
                            customer_id = customer_service.find_or_create_customer(
                                group, mapper, customer_type="patient")
                            transaction_type = "sales_receipt"
                    else:
                        customer_id = customer_service.find_or_create_customer(
                            group, mapper, customer_type="patient")
                        transaction_type = "sales_receipt"

                    # Idempotency
                    if db.is_already_processed(str(invoice_num), transaction_type):
                        logger.info(f"Skip {invoice_num} ({transaction_type}): already processed")
                        results.append({"invoice": invoice_num, "status": "success",
                                        "transaction_id": "EXISTS", "type": transaction_type})
                        continue

                    delay = min(0.9, max(0.2, 900.0 / max(total_invoices, 1)))
                    import time; time.sleep(delay)

                    lines = build_lines(group, invoice_num,
                                        for_invoice=(transaction_type == "invoice"))
                    if not lines:
                        logger.warning(f"No lines for invoice {invoice_num}")
                        continue

                    if transaction_type == "invoice":
                        result = invoice_service.create_or_update_invoice(
                            group, customer_id, lines)
                        qb_id = (result.get("Invoice") or {}).get("Id", "EXISTS")
                    else:
                        result = receipt_service.create_sales_receipt(
                            group, customer_id, lines)
                        qb_id = (result.get("SalesReceipt") or {}).get("Id", "EXISTS")

                    db.record_invoice(upload_id, str(invoice_num), transaction_type,
                                      "success", qb_id=qb_id)
                    results.append({"invoice": invoice_num, "status": "success",
                                    "transaction_id": qb_id, "type": transaction_type})

                except Exception as e:
                    logger.error(f"Error on invoice {invoice_num}: {e}", exc_info=True)
                    db.record_invoice(upload_id, str(invoice_num), "unknown",
                                      "error", error_message=str(e))
                    results.append({"invoice": invoice_num, "status": "error",
                                    "error": str(e)})

        # UI log (concise)
        has_real_error = any(
            "Failed to refresh QuickBooks token" in l or "400 Client Error" in l
            for l in log_stream.getvalue().splitlines()
        )
        ui_log = "\n".join(log_stream.getvalue().splitlines()[-500:])

        log_processing_result(upload_id, results, archive_files=False)
        return (not has_real_error), ui_log

    except QuickBooksAuthError as e:
        logger.error(f"QuickBooks auth failed: {e}", exc_info=True)
        return False, f"QuickBooks authentication failed: {e}\nReconnect at /login"
    except Exception as e:
        logger.error(f"Failed to process CSV: {e}", exc_info=True)
        return False, log_stream.getvalue()


# ---------- routes ----------
@app.route('/upload', methods=['POST'])
def upload_file():
    if not require_connection():
        return jsonify({'success': False,
                        'error': 'QuickBooks connection lost. Please reconnect at /login.'}), 401
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file'})
    f = request.files['file']
    if not f or not f.filename.lower().endswith('.csv'):
        return jsonify({'success': False, 'error': 'CSV only'})

    upload_id = db.create_upload(f.filename)
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
        f.save(tmp.name)
        try:
            ok, logs = process_csv_file(tmp.name, upload_id)
        finally:
            try: os.unlink(tmp.name)
            except FileNotFoundError: pass

    return jsonify({
        'success': ok,
        'message': 'Done!' if ok else 'Failed',
        'upload_id': upload_id,
        'logs': logs,
    })


@app.route('/status')
def status():
    if not require_connection():
        return jsonify({'connected': False, 'reason': 'invalid_refresh'})
    return jsonify({'connected': True})


@app.route('/')
def index():
    if not require_connection():
        return redirect('/login')
    return render_template('index.html')


@app.route('/login')
def login():
    url, _ = QuickBooksAuth().get_authorization_url()
    return redirect(url)


@app.route('/callback')
def callback():
    if 'error' in request.args:
        return (f"QuickBooks authorization failed: "
                f"{request.args.get('error')} – {request.args.get('error_description','')}", 400)
    if 'code' not in request.args and 'state' not in request.args:
        return "Missing authorization code from QuickBooks", 400

    try:
        tokens = QuickBooksAuth().fetch_tokens(request.url)
        session['connected'] = True
        session['realm_id'] = tokens.get("realmId")
        logger.info("OAuth2 flow completed successfully (realmId=%s)", tokens.get("realmId"))
        return redirect('/')
    except Exception as e:
        logger.error("Callback failed", exc_info=True)
        return f"Authentication failed: {e}", 500


@app.route('/disconnect')
def disconnect():
    return render_template('disconnect.html')


@app.route('/eula')
def eula():
    return render_template('eula.html')


@app.route('/privacy')
def privacy():
    return render_template('privacy.html')


@app.errorhandler(404)
def not_found(e):
    if request.path.startswith('/static/'):
        return "Not found", 404
    return render_template('index.html'), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 3000)))