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


    def find_or_create_product(self, row, invoice_id):
        """One lookup. Miss → create. That's it. No retries. No waiting. Done."""
        
        # Fast input cleanup
        description = str(row.get('Description') or '').strip() or "Service"
        product = str(row.get('Product / Service') or '').strip() or "Uncategorized"

        original_product = product.lower()

        # Use description as QB Item Name (your original logic)
        service_name = ' '.join(description.split())
        sanitized_name = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in service_name)
        sanitized_name = ' '.join(sanitized_name.split()).title()[:100]

        # Cache = speed king
        if sanitized_name in self.item_cache:
            return self.item_cache[sanitized_name]

        # ONE SINGLE LOOKUP — that's all you're willing to pay for
        existing_item = self.qb_client.find_item_by_name(service_name)

        if existing_item:
            # Found it → cache and return (even if account is wrong — you said speed > perfection)
            item_id = existing_item["Id"]
            self.item_cache[sanitized_name] = item_id
            return item_id

        # Not found → create with correct income account
        income_account_ref = self.mapper.map_income_account(original_product)

        item_data = {
            "Name": sanitized_name,
            "Type": "Service",
            "IncomeAccountRef": income_account_ref,
            "Description": product[:4000],
            "TrackQtyOnHand": False
        }

        # One create attempt. If it fails due to duplicate → extract ID and move on
        try:
            response = self.qb_client.create_item(item_data)
            item_id = response["Item"]["Id"]
        except requests.exceptions.HTTPError as e:
            text = getattr(e.response, "text", "")
            # Magic: QuickBooks tells us the real ID in the error
            import re
            match = re.search(r'Id=(\d+)', text)
            if match:
                item_id = match.group(1)
            else:
                # Worst case: name collision we didn't expect → append suffix and go
                item_data["Name"] = f"{sanitized_name}_{int(time.time())}"[:100]
                response = self.qb_client.create_item(item_data)
                item_id = response["Item"]["Id"]

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