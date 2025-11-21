# src/customer_service.py

import time
import re
from src.logger import setup_logger
import pandas as pd

logger = setup_logger(__name__)

class CustomerService:
    def __init__(self, qb_client):
        self.qb_client = qb_client

    def find_or_create_customer(self, group, mapper, customer_type="patient", insurance_name=None):
        patient_name_raw = group['Patient Name'].iloc[0]
        patient_id_raw = group['Patient ID'].iloc[0]

        patient_name = ' '.join(str(patient_name_raw).strip().split()).title() if pd.notna(patient_name_raw) else "Unknown Patient"
        patient_id = str(patient_id_raw).strip() if pd.notna(patient_id_raw) else "UnknownID"

        if customer_type == "insurance" and insurance_name:
            full_display_name = str(insurance_name).strip().title()
        else:
            full_display_name = f"{patient_name} ID {patient_id}"

        # Normalize display name used for search & creation
        full_display_name = ' '.join(full_display_name.split())

        # Try to find existing
        existing_id = self.get_customer_id_by_name(full_display_name)
        if existing_id:
            return existing_id

        logger.info(f"Customer not found: '{full_display_name}' → creating new one")

        # Create a deterministic safe email from display name
        safe_local = re.sub(r'[^a-z0-9\.]', '.', full_display_name.lower())
        safe_local = re.sub(r'\.+', '.', safe_local).strip('.')
        safe_email = (safe_local[:60] or "user") + "@example.com"

        payload = {
            "DisplayName": full_display_name,
            "PrimaryEmailAddr": {"Address": safe_email},
            "PrimaryPhone": {"FreeFormNumber": "0712345678"},
            "BillAddr": {
                "Line1": "N/A",
                "City": "Nairobi",
                "Country": "Kenya",
                "CountrySubDivisionCode": "KE-110",
                "PostalCode": "00100"
            },
            "Taxable": False
        }

        if customer_type == "insurance":
            payload["CompanyName"] = full_display_name
        else:
            # GivenName: first token of patient_name
            payload["GivenName"] = patient_name.split()[0] if ' ' in patient_name else patient_name

        # Try up to 3 times (handles transient issues + duplicate recovery)
        for attempt in range(3):
            try:
                resp = self.qb_client.create_customer(payload)
                new_id = str(resp["Customer"]["Id"])
                logger.info(f"Created customer '{full_display_name}' → QB ID {new_id}")
                # Wait briefly for indexing to reduce race when we immediately query
                time.sleep(1)
                return new_id

            except RuntimeError as e:
                error_msg = str(e)
                logger.debug(f"create_customer RuntimeError: {error_msg}")
                if "Duplicate" in error_msg or "6240" in error_msg:
                    logger.info(f"Customer already exists: '{full_display_name}' — recovering ID (attempt {attempt + 1})")
                    # give QBO a moment
                    time.sleep(1.5)
                    recovered = self.get_customer_id_by_name(full_display_name)
                    if recovered:
                        return recovered

                    # If not found by exact methods, try stronger fallback search
                    recovered = self._fallback_search_by_components(full_display_name)
                    if recovered:
                        return recovered

                    # else continue retry loop (maybe indexing)
                    continue
                else:
                    logger.error(f"Failed to create customer (attempt {attempt + 1}): {error_msg}", exc_info=True)
                    time.sleep(1)

        # Final fallback: best-effort searches
        final_id = self.get_customer_id_by_name(full_display_name)
        if final_id:
            logger.info(f"Customer appeared after retries → QB ID {final_id}")
            return final_id

        final_id = self._fallback_search_by_components(full_display_name)
        if final_id:
            logger.info(f"Customer recovered by fallback search → QB ID {final_id}")
            return final_id

        raise RuntimeError(f"Failed to create or find customer after all retries: {full_display_name}")

    def get_customer_id_by_name(self, full_display_name: str) -> str | None:
        """
        Works on EVERY QuickBooks Online company, even old ones without STARTSWITH.
        Uses only LIKE + = which are supported since 2013.
        Returns the string customer Id or None.
        """
        # Escape single quotes
        escaped = full_display_name.replace("'", "''")

        # Try exact match first (fastest, most reliable)
        query_exact = f"SELECT Id, DisplayName FROM Customer WHERE DisplayName = '{escaped}' MAXRESULTS 1"
        try:
            data = self.qb_client._query_safe(query_exact)
            customers = data.get('Customer', []) if isinstance(data, dict) else data.get('QueryResponse', {}).get('Customer', [])
            # Support both shapes returned by _query_safe
            if customers:
                cid = str(customers[0]['Id'])
                logger.info(f"Exact match found: '{full_display_name}' → QB ID {cid}")
                return cid
        except Exception as e:
            logger.debug(f"Exact match query failed (normal if none): {e}")

        # Fallback: case-insensitive partial match using LIKE with wildcards
        variations = [
            full_display_name,
            full_display_name.replace(" ID ", " Id "),
            full_display_name.replace(" ID ", " id "),
        ]

        for variant in variations:
            esc_variant = variant.replace("'", "''")
            query = f"SELECT Id, DisplayName FROM Customer WHERE DisplayName LIKE '%{esc_variant}%' MAXRESULTS 5"
            try:
                data = self.qb_client._query_safe(query)
                customers = data.get('Customer', []) if isinstance(data, dict) else data.get('QueryResponse', {}).get('Customer', [])
                if customers:
                    match = customers[0]
                    logger.info(f"Found via LIKE: '{match['DisplayName']}' ≈ '{full_display_name}' → QB ID {match['Id']}")
                    return str(match['Id'])
            except Exception as e:
                logger.debug(f"LIKE query failed for variant '{variant}': {e}")

        logger.info(f"Customer truly not found: '{full_display_name}'")
        return None

    def _fallback_search_by_components(self, full_display_name: str) -> str | None:
        """
        Extra tolerant search:
         - split into tokens, try searching by longest tokens (ID or name tokens)
         - try phone/email match if available (we generate deterministic email when creating)
        """
        try:
            tokens = [t for t in re.split(r'[\s,.-]+', full_display_name) if t]
            # prioritize tokens that look like IDs (numeric)
            numeric_tokens = [t for t in tokens if t.isdigit()]
            search_terms = (numeric_tokens + tokens)[0:4]  # limit noise

            for term in search_terms:
                esc = term.replace("'", "''")
                query = f"SELECT Id, DisplayName FROM Customer WHERE DisplayName LIKE '%{esc}%' MAXRESULTS 5"
                data = self.qb_client._query_safe(query)
                customers = data.get('Customer', []) if isinstance(data, dict) else data.get('QueryResponse', {}).get('Customer', [])
                if customers:
                    # heuristics: prefer an entry containing the full patient last name or the numeric id token
                    for c in customers:
                        dname = c.get('DisplayName', '')
                        if any(tok.lower() in dname.lower() for tok in [term]):
                            logger.info(f"Fallback matched '{dname}' for term '{term}' → {c['Id']}")
                            return str(c['Id'])
                    # otherwise return the first
                    logger.info(f"Fallback returned first match '{customers[0]['DisplayName']}' → {customers[0]['Id']}")
                    return str(customers[0]['Id'])
        except Exception as e:
            logger.debug(f"Fallback search failed: {e}")

        return None
