# src/product_service.py
import time
import re
import requests
from src.logger import setup_logger
from src.mapper import TransactionMapper

logger = setup_logger(__name__)


class ProductService:
    """
    Service-only product resolver.

    Every CSV product resolves to a QB item with Type == 'Service'.
    If a QB item with the same name exists but is Type == 'Inventory',
    the Inventory item is never referenced — a Service sibling with
    a ' (Svc)' suffix is created (or reused) instead.
    """

    SERVICE_SUFFIX = " (Svc)"

    def __init__(self, qb_client):
        self.qb_client = qb_client
        self.item_cache = {}   # lowered_name -> item_id (Service items only)
        self.mapper = TransactionMapper()
        self._preload_products()

    # ---------- preload ----------
    def _preload_products(self):
        """Preload all Service/NonInventory items so we never reference
        an Inventory item."""
        try:
            query = ("SELECT Id, Name, Type FROM Item "
                     "WHERE Active = true MAXRESULTS 5000")
            response = self.qb_client.query(query)
            items = (response.get('QueryResponse', {}).get('Item', [])
                     or response.get('Item', []) or [])
            svc_count = 0
            for it in items:
                itype = it.get('Type') or 'Service'
                if itype not in ('Service', 'NonInventory', 'Category'):
                    continue
                name = (it.get('Name') or '').strip().lower()
                iid = it.get('Id')
                if name and iid:
                    self.item_cache[name] = str(iid)
                    svc_count += 1
            logger.info("Preloaded %d Service/NonInventory items",
                        svc_count)
        except Exception as e:
            logger.warning("Preload failed: %s", e)

    # ---------- helpers ----------
    @staticmethod
    def _sanitize(raw: str) -> str:
        s = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in raw)
        return ' '.join(s.split()).title()[:100] or "Uncategorized"

    def _lookup_service_only(self, name: str):
        """Return the QB Service item with this exact name, or None.
        Never returns an Inventory item."""
        try:
            data = self.qb_client._query_safe(
                "SELECT Id, Name, Type FROM Item "
                f"WHERE Name = '{name.replace(chr(39), chr(39) * 2)}' "
                "AND Active = true MAXRESULTS 1")
            items = (data.get('Item', [])
                     or data.get('QueryResponse', {}).get('Item', []))
            if items:
                it = items[0]
                if (it.get('Type') or 'Service') in ('Service', 'NonInventory'):
                    return it
            return None
        except Exception as e:
            logger.warning("Lookup failed for %r: %s", name, e)
            return None

    # ---------- main ----------
    def find_or_create_product(self, item_name, invoice_id=None):
        product = str(item_name or '').strip() or "Uncategorized"
        sanitized = self._sanitize(product)
        key = sanitized.lower()

        # 1. Cache hit
        if key in self.item_cache:
            return self.item_cache[key]

        # 2. Live lookup of the base name — only accept Service items
        existing = self._lookup_service_only(sanitized)
        if existing:
            self.item_cache[key] = str(existing["Id"])
            return self.item_cache[key]

        # 3. Try the (Svc) sibling name (from a previous run)
        svc_name = f"{sanitized}{self.SERVICE_SUFFIX}"[:100]
        svc_key = svc_name.lower()
        if svc_key in self.item_cache:
            return self.item_cache[svc_key]
        existing_svc = self._lookup_service_only(svc_name)
        if existing_svc:
            self.item_cache[svc_key] = str(existing_svc["Id"])
            return self.item_cache[svc_key]

        # 4. Determine which name to create under.
        #    If an Inventory item with the base name exists, we can't
        #    reuse the name (QB enforces unique names per company).
        #    Create as (Svc). Otherwise, create under the base name.
        base_name_taken = self._name_exists_anywhere(sanitized)
        create_name = svc_name if base_name_taken else sanitized
        create_key = create_name.lower()

        income_ref = self.mapper.map_income_account(product)
        item_data = {
            "Name": create_name,
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
            m = re.search(r'Id=(\d+)', text)
            if m:
                item_id = m.group(1)
            else:
                # Final fallback: unique timestamp suffix
                stamped = f"{create_name}_{int(time.time())}"[:100]
                item_data["Name"] = stamped
                response = self.qb_client.create_item(item_data)
                item_id = str(response["Item"]["Id"])
                create_key = stamped.lower()
        except Exception as e:
            logger.error("Failed to create service item %r: %s",
                         create_name, e, exc_info=True)
            raise

        self.item_cache[create_key] = item_id
        logger.info("Created service item %r → %s", create_name, item_id)
        return item_id

    def _name_exists_anywhere(self, name: str) -> bool:
        """True if ANY item (any type) already uses this exact name."""
        try:
            data = self.qb_client._query_safe(
                "SELECT Id FROM Item "
                f"WHERE Name = '{name.replace(chr(39), chr(39) * 2)}' "
                "MAXRESULTS 1")
            items = (data.get('Item', [])
                     or data.get('QueryResponse', {}).get('Item', []))
            return bool(items)
        except Exception:
            return False

    # ---------- pharmacy detection (kept for compatibility) ----------
    def is_pharmacy_item(self, row):
        product = str(row.get('Product / Service') or '').strip().lower()
        description = str(row.get('Description') or '').strip().lower()
        return product == "pharmacy" or "pharmacy" in description

    def is_inventory_tracked(self, item_id: str) -> bool:
        # In service-only mode, nothing we create is inventory-tracked.
        return False