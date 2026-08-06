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
        self.mapper = TransactionMapper()  # ✅ Add this line

    def find_or_create_product(self, item_name, invoice_id=None):
        """
        Resolve a split product/service item by its ACTUAL name.

        - Lookup QuickBooks by name (SELECT * FROM Item WHERE Name = '...').
        - If it exists, return its unique item ID.
        - If it does not exist, create it as a new Service item and return the
          newly generated ID.

        This prevents the generic 'Service' fallback that made every split
        line render as 'Service' in QuickBooks.

        Error isolation: a failure to look up or create one item is logged and
        contained so it never aborts processing of the remaining items.
        """
        product = str(item_name or '').strip() or "Uncategorized"

        # Sanitize the name so it is a valid QuickBooks Item Name.
        sanitized_name = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in product)
        sanitized_name = ' '.join(sanitized_name.split()).title()[:100]

        if not sanitized_name:
            sanitized_name = "Uncategorized"

        # Cache = speed king
        if sanitized_name in self.item_cache:
            return self.item_cache[sanitized_name]

        # ONE SINGLE LOOKUP by the actual item name.
        # find_item_by_name already escapes single quotes ('') so special
        # characters in names (parentheses, slashes, ampersands, quotes) do
        # not break the SQL. We still guard against any unexpected exception so
        # a bad name never aborts the whole invoice.
        try:
            existing_item = self.qb_client.find_item_by_name(sanitized_name)
        except Exception as e:
            logger.warning(
                f"find_or_create_product lookup failed for '{sanitized_name}' "
                f"(invoice {invoice_id}): {e} — treating as not found"
            )
            existing_item = None

        if existing_item:
            # Found it → cache and return.
            item_id = existing_item["Id"]
            self.item_cache[sanitized_name] = item_id
            return item_id

        # Not found → create as a Service item with the correct income account.
        income_account_ref = self.mapper.map_income_account(product)

        item_data = {
            "Name": sanitized_name,
            "Type": "Service",
            "IncomeAccountRef": income_account_ref,
            "Description": product[:4000],
            "TrackQtyOnHand": False
        }

        # One create attempt. If it fails due to duplicate → extract ID and move on.
        # Any other exception is isolated and re-raised so the caller can decide
        # whether to skip this item and continue with the rest.
        try:
            response = self.qb_client.create_item(item_data)
            item_id = response["Item"]["Id"]
        except requests.exceptions.HTTPError as e:
            text = getattr(e.response, "text", "")
            # QuickBooks sometimes returns the real ID in the error body.
            import re
            match = re.search(r'Id=(\d+)', text)
            if match:
                item_id = match.group(1)
            else:
                # Worst case: name collision we didn't expect → append suffix and go.
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
        self.item_cache[sanitized_name] = item_id
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
