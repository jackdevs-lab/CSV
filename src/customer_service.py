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
        variations = [
            full_display_name,
            full_display_name.replace(" ID ", " Id "),
            full_display_name.replace(" ID ", " id "),
        ]
        for name in variations:
            escaped = name.replace("'", "''")
            query = f"SELECT Id FROM Customer WHERE DisplayName = '{escaped}' MAXRESULTS 3"
            try:
                data = self.qb_client._query_safe(query)
                customers = data.get('QueryResponse', {}).get('Customer', [])
                if customers:
                    cid = str(customers[0]['Id'])
                    logger.info(f"Found existing customer: '{name}' → ID {cid}")
                    return cid
            except Exception:
                continue
        logger.info(f"Customer truly not found: '{full_display_name}'")
        return None