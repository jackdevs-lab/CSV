import os
import requests
import json
import time
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
            'LineNum', 'Amount', 'Qty', 'UnitPrice',  'TxnDate'
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
        }.get(os.getenv("QB_ENVIRONMENT", "production").lower())

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

    def _make_request(self, method, endpoint, data=None, params=None, raise_on_error=True):
        """
        Central HTTP. If raise_on_error=False we do NOT call response.raise_for_status()
        and we always attempt to parse the response body (useful to read QuickBooks Fault payloads).
        """
        url = f"{self.base_url}/v3/company/{self.realm_id}/{endpoint}"
        headers = self._get_headers()

        try:
            response = requests.request(
                method, url, headers=headers, json=data, params=params, timeout=30
            )
            logger.debug(f"QB → {method} {url} | status={response.status_code}")

            # Try parse body to dict regardless of HTTP status (use SafeQBDecoder)
            content = response.content or b"{}"
            try:
                parsed = json.loads(content, cls=SafeQBDecoder)
            except json.JSONDecodeError:
                parsed = {"raw": response.text}

            if raise_on_error:
                # Preserve existing behavior for callers that expect an exception
                response.raise_for_status()
                return parsed

            # When raise_on_error is False, return parsed dict even if status >= 400
            if response.status_code >= 400:
                logger.debug(f"QB non-2xx response (raise_on_error=False): {response.status_code} | body: {parsed}")
            return parsed

        except requests.exceptions.HTTPError as e:
            # If raise_on_error was True, we will be here — log and re-raise so higher layer sees requests exc
            logger.error(f"QuickBooks HTTP {e.response.status_code}: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Request failed: {str(e)}", exc_info=True)
            raise

    # ———————— CUSTOMER METHODS ———————— #
    def find_customer_by_name(self, name: str):
            raise RuntimeError(
        "find_customer_by_name() is banned.\n"
        "Use CustomerService.find_or_create_customer() instead.\n"
        "This method caused 'Customer not found' errors for days."
    )

    def create_customer(self, customer_data):
        """
        Create a customer in QuickBooks.
        Behavior:
          - We call _make_request(..., raise_on_error=False) so we can inspect Fault bodies on 400.
          - If QuickBooks returns a Fault with Duplicate Name, raise RuntimeError containing 'Duplicate' (existing code expects that).
          - Otherwise, if successful, return the response dict with "Customer".
        """
        resp = self._make_request('POST', 'customer', data=customer_data, raise_on_error=False)

        # If QuickBooks returned a Fault => handle specially
        if isinstance(resp, dict) and "Fault" in resp:
            err = resp["Fault"]["Error"][0]
            detail = err.get("Detail", "")
            code = err.get("code", "")
            msg = err.get("Message", "") or detail
            if "Duplicate Name Exists" in msg or str(code) == "6240" or "Duplicate" in detail:
                # Raise a RuntimeError with 'Duplicate' so CustomerService's existing handler catches it
                raise RuntimeError(f"Duplicate customer: {msg} (Code: {code})")
            # Other validation faults should be raised as RuntimeError for caller to log/handle
            raise RuntimeError(f"QuickBooks rejected customer creation: {msg} (Code: {code})")

        # If response looks fine, check Customer object
        if "Customer" not in resp:
            raise RuntimeError(f"Customer creation succeeded but no Customer object returned: {resp}")

        return resp


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
    def create_item(self, item_data: dict):
        """
        Create a Service item in QuickBooks Online.
        Tested and working on Kenyan + global companies (2025).
        """
        payload = {
            "Name": str(item_data["Name"])[:100],
            "Type": "Service",
            "UnitPrice": 0,
            "IncomeAccountRef": item_data["IncomeAccountRef"],  # expects {"value": "79"}
            "Description": str(item_data.get("Description", ""))[:4000],
            "Active": True,

        }

        resp = self._make_request('POST', 'item', payload)

        if "Fault" in resp:
            err = resp["Fault"]["Error"][0]
            raise RuntimeError(f"Item creation failed ({err.get('code')}): {err.get('Detail')}")

        if "Item" not in resp:
            raise RuntimeError(f"Item created but no 'Item' object returned: {resp}")

        return resp
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
    def list_tax_codes(self):
        """
        Debug method – prints all TaxCode IDs and their rates in your company.
        Run once, check logs, then remove or comment out.
        """
        query = "SELECT Id, Name, TaxRateDetail FROM TaxCode WHERE Active = true"
        logger.info("Fetching all TaxCode entries from QuickBooks...")
        
        try:
            response = self._query_safe(query)
            tax_codes = response.get('QueryResponse', {}).get('TaxCode', [])
            
            if not tax_codes:
                logger.warning("No TaxCode entries found – this is unusual for a Kenyan VAT company.")
                return []

            logger.info("=== ACTIVE TAX CODES IN YOUR COMPANY ===")
            results = []
            for tc in tax_codes:
                tax_id = tc.get('Id')
                name = tc.get('Name', 'No Name')
                rates = tc.get('TaxRateDetail', {}).get('TaxRateRef', [])
                
                # Handle single rate or multiple
                if isinstance(rates, dict):
                    rates = [rates]
                
                rate_info = []
                for r in rates:
                    rate_id = r.get('value')
                    # We don't have rate % here, but we know 2 = 16% usually
                    rate_info.append(f"RateRef:{rate_id}")
                
                info = f"ID: {tax_id} | Name: {name} | Rates: {', '.join(rate_info)}"
                logger.info(info)
                results.append({"Id": tax_id, "Name": name, "RateRefs": rate_info})
            
            logger.info("=== END TAX CODES ===")
            return results
            
        except Exception as e:
            logger.error(f"Failed to query TaxCodes: {e}", exc_info=True)
            return []
    def query(self, sql: str):
        return self._query_safe(sql)
    def verify_customer_exists(self, customer_id: str, max_retries: int = 10) -> bool:
        """Wait until QuickBooks indexes a newly created customer"""
        for attempt in range(max_retries):
            query = f"SELECT Id FROM Customer WHERE Id = {customer_id}"
            try:
                data = self._query_safe(query)
                if data.get('QueryResponse', {}).get('Customer'):
                    logger.info(f"Customer {customer_id} is now indexed (attempt {attempt + 1})")
                    return True
            except:
                pass
            time.sleep(1.5)
        logger.error(f"Customer {customer_id} never became available after {max_retries} retries")
        return False