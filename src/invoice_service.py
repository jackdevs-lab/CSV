from src.logger import setup_logger
import pandas as pd
from datetime import datetime
import json

logger = setup_logger(__name__)

class InvoiceService:
    """Handles invoice creation and appending in QuickBooks"""
    
    def __init__(self, qb_client):
        self.qb_client = qb_client
    
    def create_or_update_invoice(self, group, customer_id, lines):
        doc_number = str(group['Invoice No.'].iloc[0]).strip()
        
        service_date = group['Service Date'].iloc[0] if 'Service Date' in group else datetime.now().strftime('%Y-%m-%d')
        if pd.isna(service_date):
            service_date = datetime.now().strftime('%Y-%m-%d')
        elif not isinstance(service_date, str):
            service_date = pd.Timestamp(service_date).strftime('%Y-%m-%d')

        patient_name = group['Patient Name'].iloc[0]

        # 1. CHECK IF INVOICE ALREADY EXISTS (Inpatient check)
        query = f"SELECT * FROM Invoice WHERE DocNumber = '{doc_number}' MAXRESULTS 1"
        existing = self.qb_client.query(query)

        if existing and existing.get("QueryResponse", {}).get("Invoice"):
            # 2. APPEND TO EXISTING INVOICE
            invoice = existing["QueryResponse"]["Invoice"][0]
            
            # Keep all existing lines (these already have QuickBooks IDs)
            current_lines = invoice.get("Line", [])
            
            # Prepare new lines (Ensuring no 'Id' exists so QB assigns it automatically)
            new_lines = []
            for line in lines:
                new_line = line.copy()
                new_line.pop("Id", None)
                new_lines.append(new_line)

            update_payload = {
                "Id": invoice["Id"],
                "SyncToken": invoice["SyncToken"],
                "sparse": True,
                "Line": current_lines + new_lines,
                "CustomerMemo": {"value": f"Medical service for {patient_name}"}
            }

            # Send update to QBO
            response = self.qb_client._make_request("POST", "invoice", data=update_payload)
            logger.info(f"Appended new items to existing invoice #{doc_number}")

        else:
            # 3. CREATE BRAND NEW INVOICE
            invoice_data = {
                "CustomerRef": {"value": str(customer_id)},
                "TxnDate": service_date,
                "DocNumber": doc_number,
                "Line": lines,
                "CustomerMemo": {"value": f"Medical service for {patient_name}"},
                "TxnTaxDetail": {"TxnTaxCodeRef": {"value": "6"}, "TotalTax": 0}
            }
            response = self.qb_client.create_invoice(invoice_data)
            logger.info(f"Created new invoice #{doc_number}")

        # PHARMACY REAL STOCK DEDUCTION (ONLY FOR INSURANCE INVOICES)
        if hasattr(group, '_inventory_adjustments') and group._inventory_adjustments:
            for adj in group._inventory_adjustments:
                payload = {
                    "AdjustQty": {
                        "Line": [{
                            "DetailType": "ItemAdjustmentLineDetail",
                            "ItemAdjustmentLineDetail": {
                                "ItemRef": {"value": adj["item_id"]},
                                "QtyDiff": -adj["real_qty"]
                            }
                        }]
                    }
                }
                try:
                    self.qb_client._make_request('POST', 'inventoryadjustment', data=payload)
                    logger.info(f"Stock deducted: {adj['description']} × {adj['real_qty']}")
                except Exception as e:
                    logger.warning(f"Stock adjust failed: {e}")

        return response