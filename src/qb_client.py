import os
import requests
import json
from urllib.parse import quote
from src.logger import setup_logger

logger = setup_logger(__name__)


class SafeQBDecoder(json.JSONDecoder):
    """Custom JSON decoder that converts null → 0 for known numeric fields Intuit sometimes returns as null"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if not isinstance(obj, dict):
            return obj

        # Fix known numeric fields that Intuit sometimes returns as null
        numeric_fields = {
            'Id', 'SyncToken', 'Balance', 'BalanceWithJobs', 'TotalAmt',
            'LineNum', 'Amount', 'Qty', 'UnitPrice', 'Taxable', 'TxnDate'
        }

        for key in obj:
            value = obj[key]
            if value is None and key in numeric_fields:
                obj[key] = 0
            elif isinstance(value, dict):
                obj[key] = self.object_hook(value)
            elif isinstance(value, list):
                obj[key] = [self.object_hook(item) if isinstance(item, dict) else item for item in value]
        return obj


class QuickBooksClient:
    """Safe & robust wrapper for QuickBooks Online v3 API — immune to None > float errors"""

    def __init__(self, auth):
        self.auth = auth
        self.base_url = {
            "sandbox": "https://sandbox-quickbooks.api.intuit.com",
            "production": "https://quickbooks.api.intuit.com"
        }.get(os.getenv("QB_ENVIRONMENT", "sandbox").lower())

        self.realm_id = self.auth.get_realm_id() or os.getenv("QB_REALM_ID")
        if not self.realm_id:
            raise ValueError("Missing QuickBooks realm ID")

        logger.info(f"QuickBooksClient initialized | Realm ID: {self.realm_id} | Env: {os.getenv('QB_ENVIRONMENT')}")

    def _get_headers(self):
        access_token = self.auth.get_valid_access_token()
        return {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def _make_request(self, method, endpoint, data=None, params=None):
        url = f"{self.base_url}/v3/company/{self.realm_id}/{endpoint}"
        headers = self._get_headers()

        try:
            response = requests.request(
                method, url, headers=headers, json=data, params=params, timeout=30
            )
            logger.debug(f"QB → {method} {url}")
            response.raise_for_status()

            if not response.content:
                return {}

            # THIS IS THE CRITICAL FIX: use our safe decoder
            return json.loads(response.content, cls=SafeQBDecoder)

        except requests.exceptions.HTTPError as e:
            error_body = e.response.text
            logger.error(f"QuickBooks HTTP {e.response.status_code}: {error_body}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e} | Response: {response.text[:500]}")
            return {}
        except Exception as e:
            logger.error(f"Request failed: {str(e)}")
            raise

    # ———————— CUSTOMER METHODS ———————— #
    def find_customer_by_name(self, name: str):
            raise RuntimeError(
        "find_customer_by_name() is banned.\n"
        "Use CustomerService.find_or_create_customer() instead.\n"
        "This method caused 'Customer not found' errors for days."
    )

    def create_customer(self, customer_data):
        resp = self._make_request('POST', 'customer', customer_data)
        customer = resp.get('Customer', {})
        if not customer.get('Id'):
            raise ValueError("Customer created but no Id returned")
        return customer

    # ———————— ITEM METHODS ———————— #
    def find_item_by_name(self, name: str):
        if not name:
            return None
        escaped = name.replace("'", "''")
        query = f"SELECT * FROM Item WHERE Name = '{escaped}' AND Active = true"
        data = self._query_safe(query)
        items = data.get('QueryResponse', {}).get('Item', [])
        return items[0] if items else None

    # ———————— PAYMENT METHOD ———————— #
    def find_payment_method_by_name(self, name: str):
        escaped = name.replace("'", "''")
        query = f"SELECT * FROM PaymentMethod WHERE Name = '{escaped}'"
        data = self._query_safe(query)
        methods = data.get('QueryResponse', {}).get('PaymentMethod', [])
        return methods[0]['Id'] if methods else None

    def create_payment_method(self, name: str):
        sanitized = ' '.join(name.split())[:31]
        data = {"Name": sanitized, "Type": "NON_CREDIT_CARD"}
        try:
            resp = self._make_request('POST', 'paymentmethod', data)
            return resp["PaymentMethod"]["Id"]
        except requests.exceptions.HTTPError as e:
            if "Duplicate" in e.response.text:
                return self.find_payment_method_by_name(sanitized)
            raise

    # ———————— INVOICE / SALES RECEIPT ———————— #
    def create_invoice(self, invoice_data):
        return self._make_request('POST', 'invoice', invoice_data)

    def create_sales_receipt(self, receipt_data):
        return self._make_request('POST', 'salesreceipt', receipt_data)

    # ———————— SAFE QUERY HELPER ———————— #
    def _query_safe(self, sql: str):
        try:
            resp = self._make_request('GET', 'query', params={'query': sql})
            return resp.get('QueryResponse', {}) or {}
        except Exception as e:
            logger.error(f"QB Query failed: {sql} | Error: {e}", exc_info=True)
            return {'QueryResponse': {}}

    def query(self, sql: str):
        return self._query_safe(sql)