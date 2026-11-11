# src/invoice_service.py
import pandas as pd
from datetime import datetime
import requests
from src.logger import setup_logger
from src.qb_client import _extract_fault_code

logger = setup_logger(__name__)


class InvoiceService:
    def __init__(self, qb_client):
        self.qb_client = qb_client

    @staticmethod
    def _line_key(line):
        d = line.get("SalesItemLineDetail") or {}
        return (
            str(d.get("ItemRef", {}).get("value", "")),
            float(d.get("Qty", 0) or 0),
            float(d.get("UnitPrice", 0) or 0),
        )

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
                f"SELECT * FROM Invoice WHERE DocNumber = '{doc_number}' "
                "MAXRESULTS 1")
            inv = (existing.get("QueryResponse", {}).get("Invoice", [])
                   or existing.get("Invoice", []))

            if inv:
                invoice = inv[0]
                try:
                    import db
                    if db.is_already_processed(doc_number, "invoice"):
                        logger.info("Skip append %s: already processed",
                                    doc_number)
                        return {"Id": "EXISTS", "DocNumber": doc_number}
                except Exception:
                    pass

                existing_keys = {
                    self._line_key(l)
                    for l in invoice.get("Line", [])
                    if l.get("DetailType") == "SalesItemLineDetail"
                }
                new_lines = []
                for line in lines:
                    l = line.copy()
                    l.pop("Id", None)
                    if (l.get("DetailType") == "SalesItemLineDetail"
                            and self._line_key(l) in existing_keys):
                        continue
                    new_lines.append(l)

                if not new_lines:
                    logger.info("Skip append %s: all lines present",
                                doc_number)
                    return {"Id": invoice["Id"], "DocNumber": doc_number}

                update_payload = {
                    "Id": invoice["Id"],
                    "SyncToken": invoice["SyncToken"],
                    "sparse": True,
                    "Line": invoice.get("Line", []) + new_lines,
                    "CustomerMemo": {
                        "value": f"Medical service for {patient_name}"},
                }
                response = self.qb_client._make_request(
                    "POST", "invoice", data=update_payload)
                logger.info("Appended to existing invoice #%s", doc_number)
            else:
                invoice_data = {
                    "CustomerRef": {"value": str(customer_id)},
                    "TxnDate": service_date,
                    "DocNumber": doc_number,
                    "Line": lines,
                    "CustomerMemo": {
                        "value": f"Medical service for {patient_name}"},
                    "TxnTaxDetail": {
                        "TxnTaxCodeRef": {"value": "6"},
                        "TotalTax": 0,
                    },
                }
                response = self.qb_client.create_invoice(invoice_data)
                logger.info("Created invoice #%s", doc_number)

        except requests.exceptions.HTTPError as e:
            body = getattr(e, "qb_body", "") or getattr(e.response, "text", "")
            code = _extract_fault_code(body)
            if code == "6140" or "Duplicate Document Number Error" in body:
                logger.warning("Invoice %s exists (6140)", doc_number)
                return {"Id": "EXISTS", "DocNumber": doc_number}
            logger.error("Invoice failure on %s | code=%s body=%s",
                         doc_number, code, body[:1200])
            raise

        return response