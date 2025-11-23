from src.logger import setup_logger
import pandas as pd
from datetime import datetime
import json

logger = setup_logger(__name__)

class InvoiceService:
    """Handles invoice creation in QuickBooks"""
    
    def __init__(self, qb_client):
        self.qb_client = qb_client
    
    def create_or_update_invoice(self, group, customer_id, lines):
        """
        If invoice with same DocNumber exists → append lines
        If not → create new invoice
        """
        doc_number = str(group['Invoice No.'].iloc[0]).strip()
        
        # Extract shared data
        service_date = group['Service Date'].iloc[0] if 'Service Date' in group else datetime.now().strftime('%Y-%m-%d')
        if pd.isna(service_date):
            service_date = datetime.now().strftime('%Y-%m-%d')
        elif not isinstance(service_date, str):
            service_date = pd.Timestamp(service_date).strftime('%Y-%m-%d')

        patient_name = group['Patient Name'].iloc[0]

        # THE ONLY CHANGE YOU NEED — NO QUOTES AROUND doc_number
        query = f"SELECT Id, SyncToken, Line FROM Invoice WHERE DocNumber = {doc_number} MAXRESULTS 1"
        existing = self.qb_client.query(query)
        
        if existing and existing.get("QueryResponse", {}).get("Invoice"):
            invoice = existing["QueryResponse"]["Invoice"][0]
            invoice_id = invoice["Id"]
            sync_token = invoice["SyncToken"]
            
            logger.info(f"Found existing invoice #{doc_number} (ID: {invoice_id}) → appending {len(lines)} line(s)")

            current_lines = invoice.get("Line", [])
            max_id = max([int(l.get("Id", 0)) for l in current_lines], default=-1)
            new_lines = []
            for i, line in enumerate(lines):
                new_line = line.copy()
                new_line["Id"] = max_id + 1 + i
                new_line["DetailType"] = "SalesItemLineDetail"
                new_lines.append(new_line)

            update_payload = {
                "Id": invoice_id,
                "SyncToken": sync_token,
                "sparse": True,
                "Line": current_lines + new_lines,
                "CustomerMemo": {"value": f"Medical service for {patient_name}"},
            }

            response = self.qb_client._make_request(
                "POST",
                "invoice",
                json={"Invoice": update_payload}
            )
            logger.info(f"Successfully updated invoice #{doc_number} with {len(new_lines)} new lines")
            return response

        else:
            logger.info(f"No existing invoice #{doc_number} → creating new one")

            invoice_data = {
                "CustomerRef": {"value": str(customer_id)},
                "TxnDate": service_date,
                "DocNumber": doc_number,
                "Line": lines,
                "CustomerMemo": {"value": f"Medical service for {patient_name}"},
                "TxnTaxDetail": {
                    "TxnTaxCodeRef": {"value": "6"},
                    "TotalTax": 0
                }
            }

            response = self.qb_client.create_invoice(invoice_data)
            logger.info(f"Created new invoice #{doc_number} (ID: {response['Invoice']['Id']})")
            return response