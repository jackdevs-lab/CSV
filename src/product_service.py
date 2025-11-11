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
            """Find or create product in QuickBooks. Returns item ID (guaranteed)."""
            
            # Extract and normalize fields
            product = (row.get('Product / Service') or '').strip()
            description = (row.get('Description') or '').strip()
            original_product = (row.get('Product / Service') or '').strip().lower()

            # Handle NaN or invalid values
            if pd.isna(product) or not product:
                product = "Default Product"
                logger.warning(f"Missing or invalid Product / Service for invoice {invoice_id}, defaulting to '{product}'")
            if pd.isna(description) or not description:
                description = "No Description"
                logger.warning(f"Missing or invalid Description for invoice {invoice_id}, defaulting to '{description}'")

            # Swap Product / Service and Description for QuickBooks
            service_name = description  # Use original description as new product/service
            new_description = product   # Use original product/service as new description

            # Build service name (normalize spaces)
            service_name = ' '.join(service_name.split())
            # Sanitize name to match QuickBooksClient.find_item_by_name
            sanitized_name = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in service_name)
            sanitized_name = ' '.join(sanitized_name.split()).title()[:100]  # Truncate to 100 chars

            # Check cache
            if sanitized_name in self.item_cache:
                logger.info(f"Using cached item ID for '{service_name}': {self.item_cache[sanitized_name]}")
                return self.item_cache[sanitized_name]

            # Step 1: Try finding it first
            existing_item = self.qb_client.find_item_by_name(service_name)
            if existing_item:
                logger.info(f"Item '{service_name}' already exists with ID {existing_item['Id']} for invoice {invoice_id}")
                self.item_cache[sanitized_name] = existing_item["Id"]
                return existing_item["Id"]

            # Step 2: DO NOT USE UNIT COST — markup is in Total Amount
            income_account_ref = self.mapper.map_income_account(original_product)

            # Step 3: Prepare item payload — DO NOT SET UnitPrice
            item_data = {
                "Name": sanitized_name,
                "Type": "Service",
                "IncomeAccountRef": income_account_ref,
                # "UnitPrice": 0,  # DO NOT SET — let line override
                "Description": new_description[:4000]
            }

            # Step 4: Try creating the item
            try:
                response = self.qb_client.create_item(item_data)
                new_item_id = response["Item"]["Id"]
                logger.info(f"Created new item: {service_name} (sanitized: {sanitized_name}, ID: {new_item_id}) for invoice {invoice_id}")
                self.item_cache[sanitized_name] = new_item_id
                return new_item_id

            except requests.exceptions.HTTPError as e:
                error_text = str(getattr(e.response, "text", str(e)))

                if '"code":"6240"' in error_text:
                    logger.warning(f"Duplicate detected for '{service_name}' (sanitized: {sanitized_name}). Retrying lookup...")
                    item_id = self._retry_find_existing_item(service_name, invoice_id, max_retries=15, delay=3)
                    self.item_cache[sanitized_name] = item_id
                    return item_id

                logger.error(f"HTTP error while creating item '{service_name}': {error_text}")
                raise

            except Exception as e:
                logger.error(f"Unexpected error creating item '{service_name}': {e}")
                raise

    def _retry_find_existing_item(self, service_name, invoice_id, max_retries=15, delay=3):
        """Re-query QuickBooks multiple times until the item becomes available."""
        for attempt in range(1, max_retries + 1):
            item = self.qb_client.find_item_by_name(service_name)
            if item:
                logger.info(f"Item '{service_name}' confirmed in QuickBooks after {attempt} attempt(s) for invoice {invoice_id}")
                sanitized_name = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in service_name)
                sanitized_name = ' '.join(sanitized_name.split()).title()[:100]
                self.item_cache[sanitized_name] = item["Id"]
                return item["Id"]

            wait_time = delay * attempt  # Exponential backoff
            logger.debug(f"Item '{service_name}' not yet indexed (attempt {attempt}/{max_retries}), waiting {wait_time}s...")
            time.sleep(wait_time)

        logger.error(f"Failed to confirm existence of '{service_name}' after {max_retries} retries.")
        raise RuntimeError(f"Item '{service_name}' creation could not be confirmed.")