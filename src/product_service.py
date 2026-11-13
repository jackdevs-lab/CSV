# src/product_service.py
import time
import re
import requests
from src.logger import setup_logger
from src.mapper import TransactionMapper

logger = setup_logger(__name__)

class ProductService:
    def __init__(self, qb_client):
        self.qb_client = qb_client
        self.item_cache = {}
        self.mapper = TransactionMapper()
        self._preload_products()

    def _preload_products(self):
        try:
            response = self.qb_client.query("SELECT Id, Name FROM Item MAXRESULTS 1000")
            items = (response.get('QueryResponse', {}).get('Item', []) or response.get('Item', []) or [])
            for it in items:
                name = (it.get('Name') or '').strip().lower()
                iid = it.get('Id')
                if name and iid:
                    self.item_cache[name] = iid
            logger.info(f"Preloaded {len(self.item_cache)} items")
        except Exception as e:
            logger.warning(f"Preload failed: {e}")

    def find_or_create_product(self, item_name, invoice_id=None):
        product = str(item_name or '').strip() or "Uncategorized"
        sanitized = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in product)
        sanitized = ' '.join(sanitized.split()).title()[:100] or "Uncategorized"
        key = sanitized.lower()

        if key in self.item_cache:
            return self.item_cache[key]

        try:
            existing_item = self.qb_client.find_item_by_name(sanitized)
        except Exception as e:
            logger.warning(f"Lookup failed for '{sanitized}': {e}")
            existing_item = None

        if existing_item:
            item_id = str(existing_item["Id"])
            self.item_cache[key] = item_id
            return item_id

        income_ref = self.mapper.map_income_account(product)
        item_data = {
            "Name": sanitized,
            "Type": "Service",
            "IncomeAccountRef": income_ref,
            "Description": product[:4000],
            "TrackQtyOnHand": False,
        }

        try:
            response = self.qb_client.create_item(item_data)
            item_id = str(response["Item"]["Id"])
        except requests.exceptions.HTTPError as e:
            text = getattr(e.response, "text", "")
            match = re.search(r'Id=(\d+)', text)
            if match:
                item_id = match.group(1)
            else:
                item_data["Name"] = f"{sanitized}_{int(time.time())}"[:100]
                response = self.qb_client.create_item(item_data)
                item_id = str(response["Item"]["Id"])
        except Exception as e:
            logger.error(f"Failed to create '{sanitized}': {e}", exc_info=True)
            raise

        self.item_cache[key] = item_id
        return item_id

    def is_pharmacy_item(self, row):
        product = str(row.get('Product / Service') or '').strip().lower()
        description = str(row.get('Description') or '').strip().lower()
        return product == "pharmacy" or "pharmacy" in description