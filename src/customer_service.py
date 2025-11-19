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

        if customer_type == "insurance" and insurance_name:
            full_display_name = str(insurance_name).strip().title()
        else:
            full_display_name = f"{patient_name} ID {patient_id}"

        # Try to find existing
        existing_id = self.get_customer_id_by_name(full_display_name)
        if existing_id:
            return existing_id

        logger.info(f"Customer not found: '{full_display_name}' → creating new one")

        safe_email = ''.join(c for c in full_display_name.lower() if c.isalnum() or c == '.')
        payload = {
            "DisplayName": full_display_name,
            "PrimaryEmailAddr": {"Address": f"{safe_email}@example.com"},
            "PrimaryPhone": {"FreeFormNumber": "555-0123"},
            "BillAddr": {                     # ← THIS IS REQUIRED IN MOST COMPANIES
                "Line1": "N/A",
                "City": "Nairobi",
                "Country": "Kenya",
                "CountrySubDivisionCode": "KE-110"
            },
            "Taxable": False,                 
        }

        if customer_type == "insurance":
            payload["CompanyName"] = full_display_name
        else:
            payload["GivenName"] = patient_name

        for attempt in range(3):
            try:
                resp = self.qb_client.create_customer(payload)
                
                # SUCCESS RESPONSE — now it's safe
                if "Customer" in resp:
                    new_id = str(resp["Customer"]["Id"])
                    logger.info(f"Created customer '{full_display_name}' → QB ID {new_id}")
                    time.sleep(1)  # let QBO index it
                    return new_id
                else:
                    # If no Customer key → it was a fault
                    fault = resp.get("Fault", {})
                    error_detail = fault.get("Error", [{}])[0].get("Detail", "Unknown error")
                    logger.error(f"QB rejected customer creation: {error_detail}")
                    if "Duplicate" in resp:
                        # Try to recover from duplicate
                        time.sleep(2)
                        return self.get_customer_id_by_name(full_display_name)
                    raise RuntimeError(f"Customer creation failed: {error_detail}")

            except requests.exceptions.HTTPError as e:
                if e.response is not None:
                    try:
                        error_json = e.response.json()
                        if "Duplicate" in str(error_json):
                            logger.warning("Duplicate detected, retrying lookup...")
                            time.sleep(2)
                            return self.get_customer_id_by_name(full_display_name)
                    except:
                        pass
                logger.error(f"HTTP error creating customer: {e}")
            logger.error(f"HTTP error creating customer: {e}")
            time.sleep(2)

        # Final fallback
        final_id = self.get_customer_id_by_name(full_display_name)
        if final_id:
            return final_id

        raise RuntimeError(f"Failed to create or find customer after retries: {full_display_name}")


    def get_customer_id_by_name(self, full_display_name: str) -> str | None:
        """
        Works on EVERY QuickBooks Online company, even old ones without STARTSWITH.
        Uses only LIKE + = which are supported since 2013.
        """
        # Escape single quotes
        escaped = full_display_name.replace("'", "''")

        # Try exact match first (fastest, most reliable)
        query_exact = f"SELECT Id, DisplayName FROM Customer WHERE DisplayName = '{escaped}' MAXRESULTS 1"
        try:
            data = self.qb_client._query_safe(query_exact)
            customers = data.get('QueryResponse', {}).get('Customer', [])
            if customers:
                cid = str(customers[0]['Id'])
                logger.info(f"Exact match found: '{full_display_name}' → QB ID {cid}")
                return cid
        except Exception as e:
            logger.debug(f"Exact match query failed (normal if none): {e}")

        # Fallback: case-insensitive partial match using LIKE with wildcards
        # QBO's LIKE is case-insensitive by default in 99% of companies
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
                customers = data.get('QueryResponse', {}).get('Customer', [])
                if customers:
                    # Return the first one — or you can add logic to pick best match
                    match = customers[0]
                    logger.info(f"Found via LIKE: '{match['DisplayName']}' ≈ '{full_display_name}' → QB ID {match['Id']}")
                    return str(match['Id'])
            except Exception as e:
                logger.debug(f"LIKE query failed for variant '{variant}': {e}")

        logger.info(f"Customer truly not found: '{full_display_name}'")
        return None