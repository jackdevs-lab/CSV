# app.py — SYNCHRONOUS PROCESSOR + DB-TOKEN AUTH
import os
import sys
import time
import logging
import tempfile
from io import StringIO
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import json
import queue
from flask import Response
import threading

import pandas as pd
from flask import Flask, request, render_template, jsonify, redirect, session
from werkzeug.middleware.proxy_fix import ProxyFix

os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")

BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR / "src"))

import db
db.init_db()

from src.csv_parser import CSVParser
from src.mapper import TransactionMapper
from src.customer_service import CustomerService
from src.product_service import ProductService
from src.invoice_service import InvoiceService
from src.receipt_service import ReceiptService
from src.qb_auth import QuickBooksAuth, QuickBooksAuthError
from src.qb_client import QuickBooksClient
from src.logger import setup_logger

app = Flask(__name__, template_folder=str(BASE_DIR / "templates"), static_folder=str(BASE_DIR / "static"))
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-key-change-me")

logger = setup_logger(__name__)

# --- STREAMING LOG SETUP ---
class StreamingLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.queue = queue.Queue()
    def emit(self, record):
        self.queue.put(self.format(record))

stream_handler = StreamingLogHandler()
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(stream_handler)
logging.getLogger().setLevel(logging.INFO)


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
                            'Service Date', 'Mode of Payment', 'Is Insurance?']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False, log_stream.getvalue()

        results = []
        grouped = df.groupby('Invoice No.')
        invoice_groups = list(grouped)
        total_invoices = len(invoice_groups)
        logger.info(f"Found {total_invoices} unique invoices – starting sync")

        def parse_money(value):
            if pd.isna(value): return Decimal('0.00')
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
                        
                        # Resolves to the exact item ID in QuickBooks 
                        # (If it exists as Inventory, it uses it directly)
                        item_id = product_service.find_or_create_product(item, invoice_num)

                        sales_item_detail = {
                            "ItemRef": {"value": str(item_id)},
                            "Qty": 1.0,
                            "UnitPrice": 0.0,
                            "TaxCodeRef": {"value": "6"}
                        }

                        line = {
                            "DetailType": "SalesItemLineDetail",
                            "Amount": 0.0,
                            "SalesItemLineDetail": sales_item_detail
                        }
                        lines.append(line)

                        if for_invoice and product_service.is_pharmacy_item(item_row) and qty_csv > 1:
                            inventory_adjustments.append({
                                "item_id": item_id,
                                "real_qty": int(qty_csv),
                                "description": description or item
                            })
                    except Exception as e:
                        logger.warning(f"Item {item!r} failed on {invoice_num}: {e} — skipping")

            if inventory_adjustments and for_invoice:
                group._inventory_adjustments = inventory_adjustments

            if lines and total_value > 0:
                total_item_id = product_service.find_or_create_product("Total Visit Charges", invoice_num)
                lines.append({
                    "DetailType": "SalesItemLineDetail",
                    "Amount": float(total_value.quantize(Decimal('0.01'))),
                    "Description": "Total Visit Charges",
                    "SalesItemLineDetail": {
                        "ItemRef": {"value": str(total_item_id)},
                        "Qty": 1.0,
                        "UnitPrice": float(total_value.quantize(Decimal('0.01'))),
                        "TaxCodeRef": {"value": "6"}
                    }
                })
            return lines

        for idx, (invoice_num, group) in enumerate(invoice_groups, start=1):
            transaction_type = "unknown"
            try:
                mapper = TransactionMapper()
                is_insurance = mapper.is_insurance_transaction(group)

                if is_insurance:
                    insurance_name = mapper.extract_insurance_name(group)
                    if insurance_name and insurance_name.strip():
                        try:
                            customer_id = customer_service.find_or_create_customer(
                                group, mapper, customer_type="insurance", insurance_name=insurance_name.strip())
                        except RuntimeError as e:
                            if "Duplicate" in str(e) or "6240" in str(e):
                                from src.customer_service import INSURANCE_FULL_NAMES
                                cleaned = str(insurance_name).strip().upper()
                                full_name = INSURANCE_FULL_NAMES.get(cleaned, str(insurance_name).strip())
                                customer_id = customer_service.get_customer_id_by_name(full_name)
                                if not customer_id: raise
                            else:
                                raise
                        transaction_type = "invoice"
                    else:
                        try:
                            customer_id = customer_service.find_or_create_customer(
                                group, mapper, customer_type="patient")
                        except RuntimeError as e:
                            if "Duplicate" in str(e) or "6240" in str(e):
                                p_name = (' '.join(str(group['Patient Name'].iloc[0]).strip().split()).title())
                                p_id = str(group['Patient ID'].iloc[0]).strip()
                                customer_id = customer_service.get_customer_id_by_name(f"{p_name} ID {p_id}")
                                if not customer_id: raise
                            else:
                                raise
                        transaction_type = "sales_receipt"
                else:
                    try:
                        customer_id = customer_service.find_or_create_customer(
                            group, mapper, customer_type="patient")
                    except RuntimeError as e:
                        if "Duplicate" in str(e) or "6240" in str(e):
                            p_name = (' '.join(str(group['Patient Name'].iloc[0]).strip().split()).title())
                            p_id = str(group['Patient ID'].iloc[0]).strip()
                            customer_id = customer_service.get_customer_id_by_name(f"{p_name} ID {p_id}")
                            if not customer_id: raise
                        else:
                            raise
                    transaction_type = "sales_receipt"

                # ---------------------------------------------------------
                # IDEMPOTENCY GUARD: Skip if already in QB
                # ---------------------------------------------------------
                table = "Invoice" if transaction_type == "invoice" else "SalesReceipt"
                existing = qb_client.query(
                    f"SELECT Id FROM {table} WHERE DocNumber = '{invoice_num}' MAXRESULTS 1")
                if (existing.get("QueryResponse", {}).get(table) or existing.get(table)):
                    logger.info(f"[{idx}/{total_invoices}] SKIP {invoice_num}: already in QB")
                    results.append({"invoice": invoice_num, "status": "success", "transaction_id": "EXISTS"})
                    continue
                # ---------------------------------------------------------

                # Rate limiting (matching the old script)
                delay = min(2.0, max(0.6, 6600.0 / total_invoices))
                time.sleep(delay)

                lines = build_lines(group, invoice_num, for_invoice=(transaction_type == "invoice"))
                if not lines:
                    logger.warning(f"No lines for invoice {invoice_num}")
                    continue

                if transaction_type == "invoice":
                    result = invoice_service.create_or_update_invoice(group, customer_id, lines)
                    qb_id = (result.get('Invoice') or {}).get('Id', 'EXISTS')
                else:
                    result = receipt_service.create_sales_receipt(group, customer_id, lines)
                    qb_id = (result.get('SalesReceipt') or {}).get('Id', 'EXISTS')

                logger.info(f"[{idx}/{total_invoices}] OK   {invoice_num} -> {qb_id}")
                results.append({"invoice": invoice_num, "status": "success", "transaction_id": qb_id, "type": transaction_type})

            except Exception as e:
                err_msg = str(e)
                logger.error(f"[{idx}/{total_invoices}] FAIL {invoice_num}: {err_msg}")
                results.append({"invoice": invoice_num, "status": "error", "error": err_msg})

        # Filter logs for UI
        success_count = sum(1 for r in results if r['status'] == 'success')
        fail_count = sum(1 for r in results if r['status'] == 'error')
        
        summary = f"\nProcessing Complete.\nSucceeded: {success_count}\nFailed: {fail_count}\n\n"
        if fail_count > 0:
            summary += "Failed Invoices:\n"
            for r in results:
                if r['status'] == 'error':
                    summary += f"  {r['invoice']}: {r.get('error', 'Unknown')[:150]}\n"
        
        ui_log = summary + "\n(System logs written to server output)"
        
        return fail_count == 0, ui_log

    except QuickBooksAuthError as e:
        logger.error(f"QuickBooks auth failed during processing: {e}", exc_info=True)
        return False, f"QuickBooks authentication failed: {e}\nPlease reconnect your account at /login"
    except Exception as e:
        logger.error(f"Failed to process CSV: {str(e)}", exc_info=True)
        return False, log_stream.getvalue()


@app.route('/upload', methods=['POST'])
def upload_file():
    # We keep this endpoint so the old JS doesn't break if someone has it cached, 
    # but the new JS will use /upload_stream
    return upload_stream()

@app.route('/upload_stream', methods=['POST'])
def upload_stream():
    if not require_connection():
        return "Unauthorized", 401
    if 'file' not in request.files:
        return "No file", 400
    file = request.files['file']
    if not file or not file.filename.lower().endswith('.csv'):
        return "CSV only", 400

    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    file.save(temp_file.name)
    temp_file.close()

    def generate():
        # Clear any old logs from previous runs
        with stream_handler.queue.mutex:
            stream_handler.queue.queue.clear()

        result_holder = {}
        
        def run():
            try:
                ok, summary = process_csv_file(temp_file.name)
                result_holder['ok'] = ok
                result_holder['summary'] = summary
            except Exception as e:
                logger.error(f"Thread error: {e}", exc_info=True)
                result_holder['ok'] = False
                result_holder['summary'] = str(e)
            finally:
                try: os.unlink(temp_file.name)
                except: pass
                stream_handler.queue.put("__DONE__")

        thread = threading.Thread(target=run)
        thread.start()

        while True:
            try:
                msg = stream_handler.queue.get(timeout=10)
                if msg == "__DONE__":
                    yield f"data: {json.dumps({'type': 'complete', 'success': result_holder.get('ok', False), 'summary': result_holder.get('summary', '')})}\n\n"
                    break
                yield f"data: {json.dumps({'type': 'log', 'message': msg})}\n\n"
            except queue.Empty:
                # Keep-alive to prevent Railway proxy from killing the connection
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"

    return Response(generate(), mimetype='text/event-stream')

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
    auth_response_url = request.url

    if 'error' in request.args:
        return f"QuickBooks authorization failed: {request.args.get('error')}", 400

    if 'code' not in request.args and 'state' not in request.args:
        return "Missing authorization code from QuickBooks", 400

    try:
        tokens = QuickBooksAuth().fetch_tokens(auth_response_url)
        session['connected'] = True
        session['realm_id'] = tokens.get("realmId")
        logger.info(f"OAuth2 flow completed successfully! Realm: {tokens.get('realmId')}")
        return redirect('/')
    except Exception as e:
        logger.error("Callback failed", exc_info=True)
        return f"Authentication failed: {str(e)}", 500

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
    if request.path.startswith('/static/'): return "Not found", 404
    return render_template('index.html'), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 3000)))