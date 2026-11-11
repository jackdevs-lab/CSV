# src/receipt_service.py
import pandas as pd
from datetime import datetime
import requests
from src.logger import setup_logger
from src.qb_client import QuickBooksClient, _extract_fault_code

logger = setup_logger(__name__)


class ReceiptService:
    def __init__(self, qb_client: QuickBooksClient):
        self.qb_client = qb_client
        self.payment_method_ids = {}

    @staticmethod
    def _line_key(line):
        d = line.get("SalesItemLineDetail") or {}
        return (
            str(d.get("ItemRef", {}).get("value", "")),
            float(d.get("Qty", 0) or 0),
            float(d.get("UnitPrice", 0) or 0),
        )

    def create_sales_receipt(self, group, customer_id, lines):
        doc_number = str(group['Invoice No.'].iloc[0]).strip()
        service_date = group['Service Date'].iloc[0] if 'Service Date' in group else None
        if pd.isna(service_date) or not service_date:
            service_date = datetime.now().strftime('%Y-%m-%d')
        elif not isinstance(service_date, str):
            service_date = pd.Timestamp(service_date).strftime('%Y-%m-%d')

        try:
            existing = self.qb_client.query(
                f"SELECT * FROM SalesReceipt WHERE DocNumber = '{doc_number}' "
                "MAXRESULTS 1")
            rs = (existing.get("QueryResponse", {}).get("SalesReceipt", [])
                  or existing.get("SalesReceipt", []))

            if rs:
                receipt = rs[0]

                # Skip entirely if DB says we already wrote this
                try:
                    import db
                    if db.is_already_processed(doc_number, "sales_receipt"):
                        logger.info("Skip append %s: already processed",
                                    doc_number)
                        return {"Id": "EXISTS", "DocNumber": doc_number}
                except Exception:
                    pass

                existing_keys = {
                    self._line_key(l)
                    for l in receipt.get("Line", [])
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
                    return {"Id": receipt["Id"], "DocNumber": doc_number}

                update_payload = {
                    "Id": receipt["Id"],
                    "SyncToken": receipt["SyncToken"],
                    "sparse": True,
                    "Line": receipt.get("Line", []) + new_lines,
                }
                response = self.qb_client._make_request(
                    "POST", "salesreceipt", data=update_payload)
                logger.info("Appended to existing sales receipt #%s",
                            doc_number)
            else:
                receipt_data = {
                    "CustomerRef": {"value": str(customer_id)},
                    "TxnDate": service_date,
                    "DocNumber": doc_number,
                    "Line": lines,
                    "TxnTaxDetail": {
                        "TxnTaxCodeRef": {"value": "6"},
                        "TotalTax": 0,
                    },
                }
                response = self.qb_client.create_sales_receipt(receipt_data)
                logger.info("Created sales receipt #%s", doc_number)

        except requests.exceptions.HTTPError as e:
            body = getattr(e, "qb_body", "") or getattr(e.response, "text", "")
            code = _extract_fault_code(body)
            if code == "6140" or "Duplicate Document Number Error" in body:
                logger.warning("Sales receipt %s exists (6140)", doc_number)
                return {"Id": "EXISTS", "DocNumber": doc_number}
            logger.error("Sales receipt failure on %s | code=%s body=%s",
                         doc_number, code, body[:1200])
            raise

        return response

    def _get_payment_method_ref(self, payment_method):
        pm_lower = payment_method.lower().strip()
        if pm_lower in self.payment_method_ids:
            return self.payment_method_ids[pm_lower]
        name_map = {
            'cash': 'Cash', 'cheque': 'Cheque', 'credit card': 'Credit Card',
            'debit card': 'Debit Card', 'mpesa': 'MPESA', 'visa': 'Visa',
        }
        qb_name = name_map.get(pm_lower, payment_method.title())
        method_id = (self.qb_client.find_payment_method_by_name(qb_name)
                     or self.qb_client.create_payment_method(qb_name))
        if not method_id:
            raise ValueError(f"Payment method '{qb_name}' not available")
        self.payment_method_ids[pm_lower] = method_id
        return str(method_id)