# src/qb_client.py
import os
import json
import time
import requests
from src.logger import setup_logger

logger = setup_logger(__name__)


class SafeQBDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if not isinstance(obj, dict):
            return obj
        numeric_fields = {'Id', 'SyncToken', 'Balance', 'BalanceWithJobs',
                          'TotalAmt', 'LineNum', 'Amount', 'Qty', 'UnitPrice',
                          'TxnDate'}
        for k in list(obj.keys()):
            v = obj[k]
            if v is None and k in numeric_fields:
                obj[k] = 0
            elif isinstance(v, dict):
                obj[k] = self.object_hook(v)
            elif isinstance(v, list):
                obj[k] = [self.object_hook(i) if isinstance(i, dict) else i
                          for i in v]
        return obj


class QuickBooksClient:
    def __init__(self, auth):
        self.auth = auth
        self.base_url = {
            "sandbox": "https://sandbox-quickbooks.api.intuit.com",
            "production": "https://quickbooks.api.intuit.com",
        }.get(os.getenv("QB_ENVIRONMENT", "production").lower())
        self.realm_id = self.auth.get_realm_id() or os.getenv("QB_REALM_ID")
        if not self.realm_id:
            raise ValueError("Missing QuickBooks realm ID")
        logger.info("QuickBooksClient initialized | Realm %s | %s",
                    self.realm_id, os.getenv('QB_ENVIRONMENT'))

    def _get_headers(self):
        return {
            'Authorization': f'Bearer {self.auth.get_valid_access_token()}',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }

    def _make_request(self, method, endpoint, data=None, params=None,
                      raise_on_error=True, _retry=True):
        url = f"{self.base_url}/v3/company/{self.realm_id}/{endpoint}"
        try:
            resp = requests.request(method, url, headers=self._get_headers(),
                                    json=data, params=params, timeout=30)
        except Exception as e:
            logger.error(f"Request failed: {e}", exc_info=True)
            raise

        if resp.status_code == 401 and _retry:
            try:
                self.auth._refresh_token_if_needed()
            except Exception:
                pass
            return self._make_request(method, endpoint, data, params,
                                      raise_on_error, _retry=False)

        try:
            parsed = json.loads(resp.content or b"{}", cls=SafeQBDecoder)
        except json.JSONDecodeError:
            parsed = {"raw": resp.text}

        if raise_on_error:
            try:
                resp.raise_for_status()
            except requests.exceptions.HTTPError as e:
                logger.error(f"QB HTTP {resp.status_code}: {resp.text[:400]}")
                raise
        return parsed

    def find_customer_by_name(self, name: str):
        raise RuntimeError(
            "find_customer_by_name() is banned. Use CustomerService.find_or_create_customer().")

    def create_customer(self, customer_data):
        resp = self._make_request('POST', 'customer', data=customer_data,
                                  raise_on_error=False)
        if isinstance(resp, dict) and "Fault" in resp:
            err = resp["Fault"]["Error"][0]
            detail = err.get("Detail", "")
            code = err.get("code", "")
            msg = err.get("Message", "") or detail
            if "Duplicate Name Exists" in msg or str(code) == "6240" or "Duplicate" in detail:
                raise RuntimeError(f"Duplicate customer: {msg} (Code: {code})")
            raise RuntimeError(f"QuickBooks rejected customer creation: {msg} (Code: {code})")
        if "Customer" not in resp:
            raise RuntimeError(f"No Customer object returned: {resp}")
        return resp

    def find_item_by_name(self, name: str):
        if not name:
            return None
        escaped = name.replace("'", "''")
        data = self._query_safe(f"SELECT * FROM Item WHERE Name = '{escaped}' AND Active = true")
        items = data.get('Item', []) or data.get('QueryResponse', {}).get('Item', [])
        return items[0] if items else None

    def find_payment_method_by_name(self, name: str):
        escaped = name.replace("'", "''")
        data = self._query_safe(f"SELECT * FROM PaymentMethod WHERE Name = '{escaped}'")
        methods = data.get('PaymentMethod', []) or \
                  data.get('QueryResponse', {}).get('PaymentMethod', [])
        return methods[0]['Id'] if methods else None

    def create_payment_method(self, name: str):
        sanitized = ' '.join(name.split())[:31]
        try:
            resp = self._make_request('POST', 'paymentmethod',
                                      {"Name": sanitized, "Type": "NON_CREDIT_CARD"})
            return resp["PaymentMethod"]["Id"]
        except requests.exceptions.HTTPError as e:
            if "Duplicate" in getattr(e.response, "text", ""):
                return self.find_payment_method_by_name(sanitized)
            raise

    def create_item(self, item_data: dict):
        payload = {
            "Name": item_data["Name"][:100],
            "Type": item_data.get("Type", "Service"),
            "UnitPrice": item_data.get("UnitPrice", 0),
            "IncomeAccountRef": item_data["IncomeAccountRef"],
            "Description": item_data.get("Description", "")[:4000],
            "Active": True,
            "TrackQtyOnHand": item_data.get("TrackQtyOnHand", False),
        }
        if item_data.get("Type") == "Inventory":
            payload.update({
                "QtyOnHand": item_data.get("QtyOnHand", 0),
                "InvStartDate": item_data.get("InvStartDate", "2025-01-01"),
                "AssetAccountRef": item_data.get("AssetAccountRef", {"value": "11"}),
                "ExpenseAccountRef": item_data.get("ExpenseAccountRef", {"value": "51"}),
            })
        resp = self._make_request('POST', 'item', payload)
        if "Item" not in resp:
            raise RuntimeError(f"Item created but no 'Item' object: {resp}")
        return resp

    def create_invoice(self, invoice_data):
        return self._make_request('POST', 'invoice', invoice_data)

    def create_sales_receipt(self, receipt_data):
        return self._make_request('POST', 'salesreceipt', receipt_data)

    def _query_safe(self, sql: str):
        try:
            resp = self._make_request('GET', 'query', params={'query': sql})
            return resp.get('QueryResponse', {}) or {}
        except Exception as e:
            logger.error(f"QB Query failed: {sql} | {e}", exc_info=True)
            return {'QueryResponse': {}}

    def query(self, sql: str):
        return self._query_safe(sql)

    def verify_customer_exists(self, customer_id: str, max_retries: int = 10) -> bool:
        for attempt in range(max_retries):
            data = self._query_safe(f"SELECT Id FROM Customer WHERE Id = {customer_id}")
            if data.get('QueryResponse', {}).get('Customer') or data.get('Customer'):
                return True
            time.sleep(1.0)
        logger.error(f"Customer {customer_id} never became available")
        return False