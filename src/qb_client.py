import os
import requests
import json
from urllib.parse import quote
from src.logger import setup_logger

logger = setup_logger(__name__)

class QuickBooksClient:
    """Safe & robust wrapper for QuickBooks Online v3 API"""

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
            response = requests.request(method, url, headers=headers, json=data, params=params, timeout=30)
            logger.debug(f"QB → {method} {url} | Body: {json.dumps(data) if data else 'None'}")
            response.raise_for_status()
            return response.json() if response.content else {}
        except requests.exceptions.HTTPError as e:
            error_body = e.response.text
            logger.error(f"QuickBooks HTTP {e.response.status_code}: {error_body}")
            raise
        except Exception as e:
            logger.error(f"Request failed: {str(e)}")
            raise

    # ———————— CUSTOMER METHODS ———————— #
    def find_customer_by_name(self, name: str):
        """Safely find customer by DisplayName — NEVER returns malformed objects"""
        if not name or not name.strip():
            return None

        name = name.strip()
        escaped = name.replace("'", "''")

        # 1. Exact match (fast + safe)
        query = f"SELECT * FROM Customer WHERE DisplayName = '{escaped}' AND Active IN (true, false)"
        try:
            data = self._query_safe(query)
            customers = data.get('QueryResponse', {}).get('Customer', [])

            for cust in customers:
                if cust.get('DisplayName') == name and cust.get('Id'):
                    return {"Id": str(cust['Id']), "DisplayName": cust['DisplayName']}
        except Exception as e:
            logger.warning(f"Exact customer search failed for '{name}': {e}")

        # 2. Fallback: partial match (only if exact failed)
        query = f"SELECT * FROM Customer WHERE DisplayName LIKE '%{escaped}%' AND Active IN (true, false) MAXRESULTS 5"
        try:
            data = self._query_safe(query)
            candidates = data.get('QueryResponse', {}).get('Customer', [])
            for cust in candidates:
                if name.lower() in (cust.get('DisplayName') or '').lower() and cust.get('Id'):
                    logger.info(f"Customer partial match: '{name}' → '{cust['DisplayName']}' (ID: {cust['Id']})")
                    return {"Id": str(cust['Id']), "DisplayName": cust['DisplayName']}
        except Exception as e:
            logger.warning(f"Partial customer search failed: {e}")

        logger.info(f"Customer not found: '{name}'")
        return None

    def create_customer(self, customer_data):
        """Create customer with safe response handling"""
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

    # ———————— SAFE QUERY HELPER (THIS IS THE KEY FIX) ———————— #
    def _query_safe(self, sql: str):
        """Execute SQL query with full error handling and malformed object protection"""
        encoded = quote(sql, safe='')
        try:
            resp = self._make_request('GET', f'query', params={'query': sql})
            # Intuit sometimes returns empty body → {}
            return resp.get('QueryResponse', {}) if resp else {}
        except Exception as e:
            logger.error(f"Query failed: {sql}\nError: {e}")
            return {'QueryResponse': {}}

    # Optional: expose raw query for advanced use
    def query(self, sql: str):
        return self._query_safe(sql)