import pandas as pd
from src.logger import setup_logger
import requests
import time
from src.mapper import TransactionMapper

logger = setup_logger(__name__)

class ProductService:
    """Handles product resolution and creation"""

    def __init__(self, qb_client):
        self.qb_client = qb_client
        self.item_cache = {}  # Cache for item IDs
        self.mapper = TransactionMapper()  
        self._preload_products()  # ✅ Preload everything on startup!

    def _preload_products(self):
        """Fetch all items from QuickBooks once on startup to eliminate N+1 query bottlenecks."""
        try:
            query = "SELECT Id, Name FROM Item MAXRESULTS 1000"
            response = self.qb_client.query(query)
            if response and 'QueryResponse' in response and 'Item' in response['QueryResponse']:
                for item in response['QueryResponse']['Item']:
                    name = item.get('Name', '').strip().lower()
                    item_id = item.get('Id')
                    if name and item_id:
                        self.item_cache[name] = item_id
            logger.info(f"Successfully preloaded {len(self.item_cache)} products into local memory cache.")
        except Exception as e:
            logger.warning(f"Failed to preload products cache: {e} — falling back to lazy loading.")

    def find_or_create_product(self, item_name, invoice_id=None):
        """
        Resolve a split product/service item by its ACTUAL name.
        """
        product = str(item_name or '').strip() or "Uncategorized"

        # Sanitize the name so it is a valid QuickBooks Item Name.
        sanitized_name = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in product)
        sanitized_name = ' '.join(sanitized_name.split()).title()[:100]

        if not sanitized_name:
            sanitized_name = "Uncategorized"

        cache_key = sanitized_name.lower()

        # 1. Instant local cache lookup (Zero network overhead!)
        if cache_key in self.item_cache:
            return self.item_cache[cache_key]

        # 2. Fallback check via query if somehow missed during preload
        try:
            existing_item = self.qb_client.find_item_by_name(sanitized_name)
        except Exception as e:
            logger.warning(
                f"find_or_create_product lookup failed for '{sanitized_name}' "
                f"(invoice {invoice_id}): {e} — treating as not found"
            )
            existing_item = None

        if existing_item:
            item_id = existing_item["Id"]
            self.item_cache[cache_key] = item_id
            return item_id

        # 3. Not found anywhere → create as a Service item
        income_account_ref = self.mapper.map_income_account(product)

        item_data = {
            "Name": sanitized_name,
            "Type": "Service",
            "IncomeAccountRef": income_account_ref,
            "Description": product[:4000],
            "TrackQtyOnHand": False
        }

        try:
            response = self.qb_client.create_item(item_data)
            item_id = response["Item"]["Id"]
        except requests.exceptions.HTTPError as e:
            text = getattr(e.response, "text", "")
            import re
            match = re.search(r'Id=(\d+)', text)
            if match:
                item_id = match.group(1)
            else:
                item_data["Name"] = f"{sanitized_name}_{int(time.time())}"[:100]
                response = self.qb_client.create_item(item_data)
                item_id = response["Item"]["Id"]
        except Exception as e:
            logger.error(
                f"find_or_create_product failed to create '{sanitized_name}' "
                f"(invoice {invoice_id}): {e}", exc_info=True
            )
            raise

        # Cache it forever
        self.item_cache[cache_key] = item_id
        return item_id

    def _robust_find_item(self, name, max_retries=8, delay=2):
        """Search with exponential backoff — handles eventual consistency perfectly."""
        for attempt in range(1, max_retries + 1):
            item = self.qb_client.find_item_by_name(name)
            if item:
                return item

            if attempt < max_retries:
                wait = delay * (1.5 ** (attempt - 1))
                logger.debug(f"Item '{name}' not found yet (attempt {attempt}), waiting {wait:.1f}s...")
                time.sleep(wait)

        return None

    def is_pharmacy_item(self, row):
        product = str(row.get('Product / Service') or '').strip().lower()
        description = str(row.get('Description') or '').strip().lower()
        return product == "pharmacy" or "pharmacy" in description