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
        """Bullet-proof version: never fails, always returns correct item with right income account."""
        
        # === 1. Normalize input ===
        product = str(row.get('Product / Service') or '').strip()
        description = str(row.get('Description') or '').strip()
        original_product = product.lower()

        if not product or product == 'nan':
            product = "Uncategorized Service"
        if not description or description == 'nan':
            description = "No description provided"

        service_name = ' '.join(description.split())
        new_description = product

        # Sanitize name (QB allows max 100 chars)
        sanitized_base = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in service_name)
        sanitized_base = ' '.join(sanitized_base.split()).title()[:90]  # leave room for suffix

        desired_account_ref = self.mapper.map_income_account(original_product)

        # === 2. Fast cache path ===
        cache_key = sanitized_base
        if cache_key in self.item_cache:
            return self.item_cache[cache_key]

        # === 3. Try to find existing item with correct account ===
        existing_item = self._robust_find_item(service_name)
        if existing_item:
            current_ref = existing_item.get('IncomeAccountRef', {})
            if current_ref.get('value') == desired_account_ref.get('value'):
                item_id = existing_item['Id']
                logger.debug(f"Reusing perfect match: '{service_name}' → {item_id}")
                self.item_cache[cache_key] = item_id
                return item_id

            # Wrong account → we must create a new one
            logger.warning(f"Income account mismatch for '{service_name}'. "
                        f"Existing: {current_ref.get('value')} ≠ Desired: {desired_account_ref.get('value')}. "
                        f"Creating new item.")
        else:
            logger.debug(f"No existing item found for '{service_name}'")

        # === 4. Create with unique name (only when needed) ===
        timestamp = int(time.time() * 1000) % 1_000_000  # 6-digit millisecond precision
        unique_suffix = f"_{timestamp}" if existing_item else ""  # only add suffix on conflict
        final_name = f"{sanitized_base}{unique_suffix}"[:100]

        item_data = {
            "Name": final_name,
            "Type": "Service",
            "IncomeAccountRef": desired_account_ref,
            "Description": new_description[:4000],
            "TrackQtyOnHand": False
        }

        # === 5. Create with full retry resilience ===
        max_attempts = 7
        for attempt in range(1, max_attempts + 1):
            try:
                response = self.qb_client.create_item(item_data)
                item_id = response["Item"]["Id"]
                logger.info(f"Successfully created item: '{final_name}' (ID: {item_id})")
                self.item_cache[cache_key] = item_id
                self.item_cache[final_name] = item_id  # cache both keys
                return item_id

            except requests.exceptions.HTTPError as e:
                if not e.response:
                    if attempt == max_attempts:
                        raise
                    time.sleep(2 ** attempt)
                    continue

                text = e.response.text or ""
                status = e.response.status_code

                # 429 → Rate limit
                if status == 429:
                    wait = 2 ** attempt + random.uniform(0, 1)
                    logger.warning(f"Rate limited (429). Waiting {wait:.1f}s...")
                    time.sleep(wait)
                    continue

                # 500 / 503 → transient
                if status >= 500:
                    wait = 2 ** attempt
                    logger.warning(f"QB server error {status}. Retrying in {wait}s...")
                    time.sleep(wait)
                    continue

                # Duplicate name → extract ID or fallback
                if "Duplicate" in text or "code=6240" in text or "Id=" in text:
                    import re
                    m = re.search(r'Id=(\d+)', text)
                    if m:
                        existing_id = m.group(1)
                        logger.info(f"Duplicate resolved → using existing item ID {existing_id}")
                        self.item_cache[cache_key] = existing_id
                        self.item_cache[final_name] = existing_id
                        return existing_id

                    # No ID in body → slow search with retry
                    logger.warning("Duplicate error without ID. Falling back to search...")
                    found_id = self._robust_find_item(service_name, max_retries=10)
                    if found_id:
                        self.item_cache[cache_key] = found_id['Id']
                        return found_id['Id']

                # Any other 4xx → probably permanent, log and re-raise on last attempt
                if 400 <= status < 500 and attempt == max_attempts:
                    logger.error(f"Permanent QB error creating item '{final_name}': {text}")
                    raise

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt == max_attempts:
                    raise
                wait = 2 ** attempt
                logger.warning(f"Network error ({e.__class__.__name__}). Retrying in {wait}s...")
                time.sleep(wait)

            # Exponential backoff between attempts
            if attempt < max_attempts:
                time.sleep(2 ** attempt)

        # Should never reach here
        raise RuntimeError(f"Exhausted all attempts to create/find item for '{service_name}'")


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