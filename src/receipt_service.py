from src.logger import setup_logger
import pandas as pd
from src.qb_client import QuickBooksClient
from datetime import datetime
import json

logger = setup_logger(__name__)

class ReceiptService:
    """Handles sales receipt creation and appending in QuickBooks"""
    
    def __init__(self, qb_client: QuickBooksClient):
        self.qb_client = qb_client
        self.payment_method_ids = {}

    def create_sales_receipt(self, group, customer_id, lines):
        doc_number = str(group['Invoice No.'].iloc[0]).strip()
        
        service_date = group['Service Date'].iloc[0] if 'Service Date' in group else datetime.now().strftime('%Y-%m-%d')
        if pd.isna(service_date):
            service_date = datetime.now().strftime('%Y-%m-%d')
        elif not isinstance(service_date, str):
            service_date = pd.Timestamp(service_date).strftime('%Y-%m-%d')

        # 1. CHECK IF SALES RECEIPT ALREADY EXISTS (Inpatient check)
        query = f"SELECT * FROM SalesReceipt WHERE DocNumber = '{doc_number}' MAXRESULTS 1"
        existing = self.qb_client.query(query)

        if existing and existing.get("QueryResponse", {}).get("SalesReceipt"):
            # 2. APPEND TO EXISTING SALES RECEIPT
            receipt = existing["QueryResponse"]["SalesReceipt"][0]
            
            # Keep all existing lines
            current_lines = receipt.get("Line", [])
            
            # Prepare new lines (Let QB auto-assign IDs)
            new_lines = []
            for line in lines:
                new_line = line.copy()
                new_line.pop("Id", None)
                new_lines.append(new_line)

            update_payload = {
                "Id": receipt["Id"],
                "SyncToken": receipt["SyncToken"],
                "sparse": True,
                "Line": current_lines + new_lines
            }

            response = self.qb_client._make_request("POST", "salesreceipt", data=update_payload)
            logger.info(f"Appended new items to existing sales receipt #{doc_number}")

        else:
            # 3. CREATE BRAND NEW SALES RECEIPT
            receipt_data = {
                "CustomerRef": {"value": str(customer_id)},
                "TxnDate": service_date,
                "DocNumber": doc_number,
                "Line": lines,
                "TxnTaxDetail": {
                    "TxnTaxCodeRef": {"value": "6"},   
                    "TotalTax": 0
                }
            }
            logger.debug(f"Creating sales receipt with data: {json.dumps(receipt_data, indent=2)}")
            response = self.qb_client.create_sales_receipt(receipt_data)
            logger.info(f"Created new sales receipt #{doc_number} for customer {customer_id}")

        return response

    def _get_payment_method_ref(self, payment_method):
        """Get or create QuickBooks payment method ID."""
        pm_lower = payment_method.lower().strip()
        if pm_lower in self.payment_method_ids:
            return self.payment_method_ids[pm_lower]
       
        name_map = {
            'cash': 'Cash',
            'cheque': 'Cheque',
            'credit card': 'Credit Card',
            'debit card': 'Debit Card',
            'mpesa': 'MPESA',
            'visa': 'Visa',
        }
        qb_name = name_map.get(pm_lower, payment_method.title())

        method_id = self.qb_client.find_payment_method_by_name(qb_name)
        if not method_id:
            method_id = self.qb_client.create_payment_method(qb_name)

        if not method_id:
            raise ValueError(f"Payment method '{qb_name}' not found or created in QuickBooks")

        self.payment_method_ids[pm_lower] = method_id
        return str(method_id)