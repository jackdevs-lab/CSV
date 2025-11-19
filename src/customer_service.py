# src/customer_service.py
import requests
import time
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

        # FULL NAME USED IN QUICKBOOKS
        if customer_type == "insurance" and insurance_name:
            full_display_name = str(insurance_name).strip().title()
        else:
            full_display_name = f"{patient_name} ID {patient_id}"

        # SEARCH USING FULL NAME
        existing_id = self.get_customer_id_by_name(full_display_name)
        if existing_id:
            return existing_id

        logger.info(f"Customer not found: '{full_display_name}' → creating new one")

        safe_email = ''.join(c for c in full_display_name.lower() if c.isalnum() or c == '.')
        payload = {
            "DisplayName": full_display_name,
            "PrimaryEmailAddr": {"Address": f"{safe_email}@example.com"},
            "PrimaryPhone": {"FreeFormNumber": "555-0123"}
        }
        if customer_type == "insurance":
            payload["CompanyName"] = full_display_name
        else:
            payload["GivenName"] = patient_name

        try:
            resp = self.qb_client.create_customer(payload)
            new_id = resp["Customer"]["Id"]
            logger.info(f"Created customer '{full_display_name}' → ID {new_id}")

            # Ensure QuickBooks recognizes the new customer
            if not self.qb_client.verify_customer_exists(new_id):
                raise RuntimeError(f"Customer {new_id} created but not indexed in time")

            return new_id

        except requests.exceptions.HTTPError as e:
            # Handle duplicate name gracefully
            if e.response is not None and 'Duplicate Name Exists' in e.response.text:
                logger.warning(f"Duplicate name detected for '{full_display_name}', retrying lookup...")
                time.sleep(2)
                existing_id = self.get_customer_id_by_name(full_display_name)
                if existing_id:
                    return existing_id
                else:
                    raise RuntimeError(f"Duplicate name error but customer '{full_display_name}' still not found")
            else:
                # Re-raise any other HTTP error
                raise


    def get_customer_id_by_name(self, full_display_name: str) -> str | None:
        """
        Search for an existing customer by DisplayName using QBO-supported syntax.
        QBO does NOT allow LOWER() or any function calls in WHERE clauses.
        Fortunately, DisplayName LIKE is case-insensitive by default in most companies.
        """
        # Escape single quotes properly
        escaped_name = full_display_name.replace("'", "''")

        # We try multiple variations because users sometimes have inconsistent capitalization/spacing
        search_variations = [
            full_display_name,                                  # Exact: "Nelly Wacuka Kingara ID 5020"
            full_display_name.replace(" ID ", " Id "),         # "Nelly Wacuka Kingara Id 5020"
            full_display_name.replace(" ID ", " id "),         # "Nelly Wacuka Kingara id 5020"
        ]

        for name_variant in search_variations:
            # Option A: STARTSWITH – fastest, officially supported, case-insensitive
            query = f"""
                SELECT Id, DisplayName 
                FROM Customer 
                WHERE DisplayName STARTSWITH '{name_variant}'
                MAXRESULTS 10
            """.strip()

            try:
                data = self.qb_client._query_safe(query)
                customers = data.get('QueryResponse', {}).get('Customer', [])
                if customers:
                    match = customers[0]
                    logger.info(f"Found customer via STARTSWITH: '{name_variant}' → QB ID {match['Id']}")
                    return str(match['Id'])
            except Exception as e:
                logger.debug(f"STARTSWITH query failed for '{name_variant}': {e}")
                # Fall through to next variation or method

            # Option B: Fallback to LIKE with wildcard (still no LOWER()!)
            # This catches cases where there are extra spaces or minor differences
            query_like = f"""
                SELECT Id, DisplayName 
                FROM Customer 
                WHERE DisplayName LIKE '%{escaped_name}%'
                MAXRESULTS 10
            """.strip()

            try:
                data = self.qb_client._query_safe(query_like)
                customers = data.get('QueryResponse', {}).get('Customer', [])
                if customers:
                    # Optional: pick the best match (exact or longest prefix)
                    for cust in customers:
                        if cust['DisplayName'] == full_display_name:
                            logger.info(f"Exact match found via LIKE: '{full_display_name}' → QB ID {cust['Id']}")
                            return str(cust['Id'])
                    # Otherwise return first match
                    match = customers[0]
                    logger.info(f"Partial match via LIKE: '{match['DisplayName']}' ≈ '{full_display_name}' → QB ID {match['Id']}")
                    return str(match['Id'])
            except Exception as e:
                logger.debug(f"LIKE query failed for '{full_display_name}': {e}")

        logger.info(f"Customer truly not found: '{full_display_name}'")
        return None