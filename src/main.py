from flask import Flask, request, render_template, jsonify
import os
import sys
from pathlib import Path
import logging
from io import StringIO
import pandas as pd
import tempfile
from flask import redirect
from werkzeug.middleware.proxy_fix import ProxyFix  # ✅ add this
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"


# Add project root to sys.path
sys.path.append(str(Path(__file__).parent))

from src.csv_parser import CSVParser
from src.mapper import TransactionMapper
from src.customer_service import CustomerService
from src.product_service import ProductService
from src.invoice_service import InvoiceService
from src.receipt_service import ReceiptService
from src.qb_auth import QuickBooksAuth
from src.qb_client import QuickBooksClient
from src.logger import setup_logger, log_processing_result

# Set up Flask app
app = Flask(__name__, template_folder="templates", static_folder="static")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
# Set up logger and capture logs for UI
logger = setup_logger(__name__)
log_stream = StringIO()
handler = logging.StreamHandler(log_stream)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

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
        required_columns = ['Invoice No.', 'Patient Name', 'Patient ID', 'Product / Service', 'Description', 'Total Amount', 'Quantity', 'Unit Cost', 'Service Date', 'Mode of Payment']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required CSV columns: {missing_columns}")
            return False, log_stream.getvalue()

        results = []
        grouped = df.groupby('Invoice No.')

        for invoice_num, group in grouped:
            try:
                # Step 1: Find or create customer
                mapper = TransactionMapper()  # Ensure mapper is initialized here
                is_insurance = mapper.is_insurance_transaction(group)
                if is_insurance:
                    insurance_name = mapper.extract_insurance_name(group)
                    if insurance_name:
                        customer_id = customer_service.find_or_create_customer(group, mapper, customer_type="insurance", insurance_name=insurance_name)
                    else:
                        # Fallback to patient if no insurance name found despite Is Insurance? = Yes
                        customer_id = customer_service.find_or_create_customer(group, mapper, customer_type="patient")
                else:
                    customer_id = customer_service.find_or_create_customer(group, mapper, customer_type="patient")

                # Step 2: Process items
                lines = []
                for _, row in group.iterrows():
                    item_id = product_service.find_or_create_product(row, invoice_num)
                    qty = float(row['Quantity'])
                    unit_price = float(row['Unit Cost'])
                    calculated_amount = qty * unit_price
                    if abs(calculated_amount - float(row['Total Amount'])) > 0.01:
                        logger.warning(f"Mismatched amount for invoice {invoice_num}, item {row['Product / Service']}: CSV Total {row['Total Amount']} != {qty} * {unit_price}. Using calculated {calculated_amount}.")
                    
                    if calculated_amount == 0:
                        logger.warning(f"Skipping zero-amount line for invoice {invoice_num}, item {row['Product / Service']}.")
                        continue
                    # Apply insurance markup rules before line creation
                    is_insurance_row = str(row.get('Is Insurance?', '')).strip().lower() == 'yes'
                    category = str(row.get('Product / Service', '')).strip()

                    # Define markup rules
                    markup_map = {
                        'Pharmacy': 1.35,
                        'Laboratory': 1.20,
                        'Radiology': 1.25
                    }

                    # Compute effective unit price
                    if is_insurance_row and category in markup_map:
                        adjusted_unit_price = round(unit_price * markup_map[category], 2)
                    else:
                        adjusted_unit_price = unit_price

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

                # Step 3: Create sales receipt or invoice
                transaction_type = mapper.determine_transaction_type(group)
                if transaction_type == "sales_receipt":
                    result = receipt_service.create_sales_receipt(group, customer_id, lines)
                    logger.info(f"Created sales receipt for patient payment on invoice {invoice_num}")
                elif transaction_type == "invoice":
                    result = invoice_service.create_invoice(group, customer_id, lines)
                    logger.info(f"Created invoice for insurance payment on invoice {invoice_num}")
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

        log_processing_result(file_path, results)  # Use file_path for moving
        return True, log_stream.getvalue()

    except Exception as e:
        logger.error(f"Failed to process CSV: {str(e)}")
        return False, log_stream.getvalue()



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

@app.route('/')
def index():
    """Render the main page with file upload and log display"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload and process CSV"""
    if 'file' not in request.files:
        logger.error("No file uploaded")
        return jsonify({'success': False, 'logs': log_stream.getvalue()})
    
    file = request.files['file']
    if file.filename == '':
        logger.error("No file selected")
        return jsonify({'success': False, 'logs': log_stream.getvalue()})

    if file and file.filename.endswith('.csv'):
        # Reset log stream for new processing
        log_stream.truncate(0)
        log_stream.seek(0)
        
        # Save uploaded file to a temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
            file.save(temp_file.name)
            temp_file_path = temp_file.name

        # Process the temporary file
        success, logs = process_csv_file(temp_file_path)
        
        # File is moved by log_processing_result, so no need to delete here
        return jsonify({'success': success, 'logs': logs})
    
    logger.error("Invalid file format, CSV required")
    return jsonify({'success': False, 'logs': log_stream.getvalue()})
application = app

if __name__ == '__main__':
    app.run(debug=True)