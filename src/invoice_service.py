# src/invoice_service.py
import pandas as pd
from datetime import datetime
import requests
from src.logger import setup_logger

logger = setup_logger(__name__)


class InvoiceService:
    def __init__(self, qb_client):
        self.qb_client = qb_client

    def create_or_update_invoice(self, group, customer_id, lines):
        doc_number = str(group['Invoice No.'].iloc[0]).strip()
        service_date = group['Service Date'].iloc[0] if 'Service Date' in group else None
        if pd.isna(service_date) or not service_date:
            service_date = datetime.now().strftime('%Y-%m-%d')
        elif not isinstance(service_date, str):
            service_date = pd.Timestamp(service_date).strftime('%Y-%m-%d')

        patient_name = group['Patient Name'].iloc[0]

        try:
            existing = self.qb_client.query(
                f"SELECT * FROM Invoice WHERE DocNumber = '{doc_number}' MAXRESULTS 1")
            inv = (existing.get("QueryResponse", {}).get("Invoice", [])
                   or existing.get("Invoice", []))
            if inv:
                invoice = inv[0]
                new_lines = []
                for line in lines:
                    l = line.copy()
                    l.pop("Id", None)
                    new_lines.append(l)
                update_payload = {
                    "Id": invoice["Id"],
                    "SyncToken": invoice["SyncToken"],
                    "sparse": True,
                    "Line": invoice.get("Line", []) + new_lines,
                    "CustomerMemo": {"value": f"Medical service for {patient_name}"},
                }
                response = self.qb_client._make_request("POST", "invoice", data=update_payload)
                logger.info("Appended to existing invoice #%s", doc_number)
            else:
                invoice_data = {
                    "CustomerRef": {"value": str(customer_id)},
                    "TxnDate": service_date,
                    "DocNumber": doc_number,
                    "Line": lines,
                    "CustomerMemo": {"value": f"Medical service for {patient_name}"},
                    "TxnTaxDetail": {"TxnTaxCodeRef": {"value": "6"}, "TotalTax": 0},
                }
                response = self.qb_client.create_invoice(invoice_data)
                logger.info("Created invoice #%s", doc_number)
        except requests.exceptions.HTTPError as e:
            text = getattr(e.response, "text", "")
            if "6140" in text or "Duplicate Document Number Error" in text:
                logger.warning("Invoice %s already exists (6140)", doc_number)
                return {"Id": "EXISTS", "DocNumber": doc_number}
            raise

        if hasattr(group, '_inventory_adjustments') and group._inventory_adjustments:
            for adj in group._inventory_adjustments:
                payload = {
                    "AdjustQty": {"Line": [{
                        "DetailType": "ItemAdjustmentLineDetail",
                        "ItemAdjustmentLineDetail": {
                            "ItemRef": {"value": adj["item_id"]},
                            "QtyDiff": -adj["real_qty"],
                        },
                    }]},
                }
                try:
                    self.qb_client._make_request('POST', 'inventoryadjustment', data=payload)
                except Exception as e:
                    logger.warning("Stock adjust failed: %s", e)

        return response