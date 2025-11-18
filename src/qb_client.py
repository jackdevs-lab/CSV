import os
import requests
import json
from urllib.parse import quote
from src.logger import setup_logger

logger = setup_logger(__name__)

class QuickBooksClient:
    """Wrapper for QuickBooks Online REST API"""

    def __init__(self, auth):
        self.auth = auth
        # Use .env values directly (already loaded by qb_auth)
        self.base_url = {
            "sandbox": "https://sandbox-quickbooks.api.intuit.com",
            "production": "https://quickbooks.api.intuit.com"
        }.get(os.getenv("QB_ENVIRONMENT", "sandbox"))

        # Prefer realm_id from auth (from .env tokens or blob)
        self.realm_id = self.auth.get_realm_id() or os.getenv("QB_REALM_ID")

        if not self.realm_id:
            logger.error("❌ Missing realm ID. Add QB_REALM_ID to .env or ensure tokens contain it.")
            raise ValueError("Missing QuickBooks realm ID")

    # In src/qb_client.py → _get_headers()
    def _get_headers(self):
        access_token = self.auth.get_valid_access_token()  # ← This is the key change
        return {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
    def _make_request(self, method, endpoint, data=None):
        """Make API request to QuickBooks"""
        url = f"{self.base_url}/v3/company/{self.realm_id}/{endpoint}"
        headers = self._get_headers()

        try:
            response = requests.request(method, url, headers=headers, json=data)
            logger.debug(f"QB REQUEST → {method} {url}\nData: {json.dumps(data, indent=2) if data else 'No body'}")
            response.raise_for_status()

            if response.headers.get('Content-Type', '').startswith('application/json'):
                return response.json()
            else:
                logger.warning(f"⚠️ Non-JSON response: {response.text}")
                return {"response_text": response.text}

        except requests.exceptions.HTTPError as e:
            logger.error(f"❌ HTTP Error: {e.response.status_code} - {e.response.text}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Network/Request error: {e}")
            raise

    # --- Business Object Methods --- #

    def set_realm_id(self, realm_id):
        """Manually override company realm ID"""
        self.realm_id = str(realm_id).strip()
        logger.info(f"✅ Realm ID set to {self.realm_id}")

    def create_customer(self, customer_data):
        return self._make_request('POST', 'customer', customer_data)

    def query_customers(self, query):
        encoded_query = quote(query, safe='')
        return self._make_request('GET', f'query?query={encoded_query}')

    def find_customer_by_name(self, name):
        """Find customer by display name"""
        escaped = name.replace("'", "''").strip()
        query = f"select * from Customer where DisplayName = '{escaped}'"
        try:
            resp = self.query_customers(query)
            customers = resp.get('QueryResponse', {}).get('Customer', [])
            if customers:
                logger.debug(f"Found exact match for {name}: {customers[0]['Id']}")
                return customers[0]

            # fallback: partial match
            query_like = f"select * from Customer where DisplayName LIKE '%{escaped}%'"
            resp = self.query_customers(query_like)
            customers = resp.get('QueryResponse', {}).get('Customer', [])
            if customers:
                logger.info(f"Found partial match for {name}: {customers[0]['Id']}")
                return customers[0]

            return None
        except Exception as e:
            logger.error(f"Error finding customer '{name}': {e}")
            return None

    def create_item(self, item_data):
        return self._make_request('POST', 'item', item_data)

    def query_items(self, query):
        encoded_query = quote(query, safe='')
        return self._make_request('GET', f'query?query={encoded_query}')

    def find_item_by_name(self, name):
        sanitized = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in name)
        sanitized = ' '.join(sanitized.split()).title()[:100]
        escaped = sanitized.replace("'", "''")

        query = f"select * from Item where Name = '{escaped}'"
        try:
            resp = self.query_items(query)
            items = resp.get('QueryResponse', {}).get('Item', [])
            if items:
                return items[0]
            # fallback
            query_like = f"select * from Item where Name LIKE '%{escaped}%'"
            resp = self.query_items(query_like)
            items = resp.get('QueryResponse', {}).get('Item', [])
            return items[0] if items else None
        except Exception as e:
            logger.error(f"Error finding item '{name}': {e}")
            return None

    def create_invoice(self, invoice_data):
        return self._make_request('POST', 'invoice', invoice_data)

    def create_sales_receipt(self, receipt_data):
        return self._make_request('POST', 'salesreceipt', receipt_data)

    def find_payment_method_by_name(self, name):
        sanitized = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in name)
        sanitized = ' '.join(sanitized.split()).title()[:31]
        escaped = sanitized.replace("'", "''")
        query = f"select * from PaymentMethod where Name = '{escaped}'"

        try:
            resp = self._make_request('GET', f'query?query={query}')
            methods = resp.get('QueryResponse', {}).get('PaymentMethod', [])
            if methods:
                return methods[0]['Id']
            return None
        except Exception as e:
            logger.error(f"Error finding payment method '{name}': {e}")
            return None

    def create_payment_method(self, name):
        sanitized = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in name)
        sanitized = ' '.join(sanitized.split()).title()[:31]
        type_map = {
            'cash': 'NON_CREDIT_CARD',
            'check': 'NON_CREDIT_CARD',
            'credit card': 'CREDIT_CARD',
            'debit card': 'NON_CREDIT_CARD',
            'mpesa': 'NON_CREDIT_CARD'
        }
        method_type = type_map.get(name.lower(), 'NON_CREDIT_CARD')
        data = {"Name": sanitized, "Type": method_type}

        try:
            resp = self._make_request('POST', 'paymentmethod', data)
            return resp["PaymentMethod"]["Id"]
        except requests.exceptions.HTTPError as e:
            if '"code":"6240"' in str(e):  # Duplicate
                return self.find_payment_method_by_name(sanitized)
            raise
