import requests
import json
from urllib.parse import quote
from src.logger import setup_logger
from config.settings import QB_BASE_URL, QB_ENVIRONMENT, QB_REALM_ID

logger = setup_logger(__name__)

class QuickBooksClient:
    """Wrapper for QuickBooks Online REST API"""
    
    def __init__(self, auth):
        self.auth = auth
        self.base_url = QB_BASE_URL[QB_ENVIRONMENT]
        self.realm_id = QB_REALM_ID  # Will be set after company selection
    
    def _get_headers(self):
        """Get headers for API requests"""
        return {
            'Authorization': f'Bearer {self.auth.get_access_token()}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
    
    def _make_request(self, method, endpoint, data=None):
        """Make API request to QuickBooks"""
        url = f"{self.base_url}/v3/company/{self.realm_id}/{endpoint}"
        headers = self._get_headers()
        
        try:
            response = requests.request(method, url, headers=headers, json=data)
            logger.debug(f"QB REQUEST to {url}: {json.dumps(data, indent=2)}")
            response.raise_for_status()
            if response.headers.get('Content-Type', '').startswith('application/json'):
                return response.json()
            else:
                logger.warning(f"Non-JSON response received: {response.text}")
                return {"response_text": response.text}
        except requests.exceptions.HTTPError as e:
            logger.error(f"API request failed: {str(e)}")
            if e.response is not None:
                logger.error(f"Response body: {e.response.text}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            logger.error("No response body available (non-HTTP error)")
            raise
    
    def set_realm_id(self, realm_id):
        """Set the company realm ID safely"""
        if isinstance(realm_id, (list, tuple)):
            realm_id = realm_id[0]
        self.realm_id = str(realm_id).strip()
    
    def create_customer(self, customer_data):
        """Create a new customer"""
        return self._make_request('POST', 'customer', customer_data)
    
    def query_customers(self, query):
        encoded_query = quote(query, safe='')
        return self._make_request('GET', f'query?query={encoded_query}')
    
    def find_customer_by_name(self, name):
        """Find customer by display name"""
        # Escape single quotes for QuickBooks query syntax
        escaped_name = name.replace("'", "''")
        # Sanitize name to remove problematic characters (optional, adjust as needed)
        sanitized_name = escaped_name.strip()

        query = f"select * from Customer where DisplayName = '{sanitized_name}'"
        try:
            response = self.query_customers(query)
            customers = response.get('QueryResponse', {}).get('Customer', [])
            if customers:
                logger.debug(f"Found customer '{name}' (sanitized: '{sanitized_name}', ID: {customers[0]['Id']})")
                return customers[0]
            
            logger.debug(f"No customer found for name: '{name}' (sanitized: '{sanitized_name}')")
            # Fallback: Try partial match
            query_like = f"select * from Customer where DisplayName LIKE '%{sanitized_name}%'"
            response = self.query_customers(query_like)
            customers = response.get('QueryResponse', {}).get('Customer', [])
            if customers:
                logger.info(f"Found customer '{name}' via partial match (ID: {customers[0]['Id']})")
                return customers[0]
            
            return None
        except Exception as e:
            logger.error(f"Error searching for customer '{name}' (sanitized: '{sanitized_name}'): {e}")
            return None
    
    def create_item(self, item_data):
        """Create a new item (product/service)"""
        return self._make_request('POST', 'item', item_data)
    
    def query_items(self, query):
        encoded_query = quote(query, safe='')
        return self._make_request('GET', f'query?query={encoded_query}')
    
    def find_item_by_name(self, name):
        """Find item in QuickBooks by exact name (handles quotes and special characters)."""
        # Sanitize name to remove problematic characters
        sanitized_name = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in name)
        sanitized_name = ' '.join(sanitized_name.split()).title()[:100]  # Truncate to 100 chars
        # Escape single quotes for QuickBooks query syntax (after sanitization, should be rare)
        escaped_name = sanitized_name.replace("'", "''")

        # Build query string using Name field
        query = f"select * from Item where Name = '{escaped_name}'"

        try:
            response = self.query_items(query)
            items = response.get('QueryResponse', {}).get('Item', [])
            if items:
                item = items[0]
                logger.debug(f"Found item '{name}' (sanitized: '{sanitized_name}', ID: {item['Id']})")
                return item

            logger.debug(f"No item found for name: '{name}' (sanitized: '{sanitized_name}')")
            # Fallback: Try partial match
            query_like = f"select * from Item where Name LIKE '%{escaped_name}%'"
            response = self.query_items(query_like)
            items = response.get('QueryResponse', {}).get('Item', [])
            if items:
                logger.info(f"Found item '{name}' via partial match (ID: {items[0]['Id']})")
                return items[0]

            return None

        except Exception as e:
            logger.error(f"Error searching for item '{name}' (sanitized: '{sanitized_name}'): {e}")
            return None


    
    def create_invoice(self, invoice_data):
        """Create a new invoice"""
        return self._make_request('POST', 'invoice', invoice_data)
    
    def create_sales_receipt(self, receipt_data):
        """Create a new sales receipt"""
        return self._make_request('POST', 'salesreceipt', receipt_data)
    
    def find_payment_method_by_name(self, name):
        """Find payment method by name, case-insensitive."""
        # Sanitize and truncate name to match create_payment_method
        sanitized_name = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in name)
        sanitized_name = ' '.join(sanitized_name.split()).title()[:31]
        escaped_name = sanitized_name.replace("'", "''")
        query = f"select * from PaymentMethod where Name = '{escaped_name}'"

        try:
            response = self._make_request('GET', f'query?query={query}')
            methods = response.get('QueryResponse', {}).get('PaymentMethod', [])
            if methods:
                logger.debug(f"Found payment method: {methods[0]['Name']} (original: {name}, ID: {methods[0]['Id']})")
                return methods[0]['Id']
            logger.debug(f"No payment method found for name: {name} (sanitized: {sanitized_name})")
            return None
        except Exception as e:
            logger.error(f"Error searching for payment method '{name}' (sanitized: {sanitized_name}): {e}")
            return None
    
    def create_payment_method(self, name):
        """Create a new payment method."""
        # Truncate name to QuickBooks' 31-character limit
        sanitized_name = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in name)
        sanitized_name = ' '.join(sanitized_name.split()).title()[:31]
        
        # Map to valid QuickBooks Type
        type_map = {
            'cash': 'NON_CREDIT_CARD',
            'check': 'NON_CREDIT_CARD',
            'credit card': 'CREDIT_CARD',
            'debit card': 'NON_CREDIT_CARD',
            'mpesa': 'NON_CREDIT_CARD'  # Treat as non-credit
        }
        method_type = type_map.get(name.lower(), 'NON_CREDIT_CARD')  # Default to non-credit
        data = {
            "Name": sanitized_name,
            "Type": method_type
        }
        try:
            response = self._make_request('POST', 'paymentmethod', data)
            new_id = response["PaymentMethod"]["Id"]
            logger.info(f"Created payment method: {sanitized_name} (original: {name}, ID: {new_id})")
            return new_id
        except requests.exceptions.HTTPError as e:
            if '"code":"6240"' in str(e):  # Duplicate Name Exists Error
                logger.warning(f"Duplicate detected for payment method '{sanitized_name}'. Fetching existing record.")
                existing_id = self.find_payment_method_by_name(sanitized_name)
                if existing_id:
                    return existing_id
                logger.error(f"Failed to find existing payment method '{sanitized_name}' after duplicate error")
                raise
            logger.error(f"HTTP error while creating payment method '{sanitized_name}': {str(e)}")
            raise