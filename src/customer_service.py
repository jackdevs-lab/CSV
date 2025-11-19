# src/customer_service.py
import requests
from src.logger import setup_logger
import pandas as pd

logger = setup_logger(__name__)

class CustomerService:
    def __init__(self, qb_client):
        self.qb_client = qb_client

    def find_or_create_customer(self, group, mapper, customer_type="patient", insurance_name=None):
        patient_name_raw = group['Patient Name'].iloc[0]
        patient_id_raw = group['Patient ID'].iloc[0]
        invoice_num = group['Invoice No.'].iloc[0]

        # Sanitize name
        patient_name = str(patient_name_raw).strip() if pd.notna(patient_name_raw) else "Unknown Patient"
        patient_name = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in patient_name)
        patient_name = ' '.join(patient_name.split()).title()

        # Sanitize ID
        patient_id = str(patient_id_raw).strip() if pd.notna(patient_id_raw) else "UnknownID"

        # Final DisplayName
        if customer_type == "insurance" and insurance_name:
            customer_name = str(insurance_name).strip()
        else:
            customer_name = f"{patient_name} ID {patient_id}".strip()
            customer_name = ' '.join(customer_name.split()).title()

        # ───── DIRECT CHECK: Does this customer already exist? ─────
        def get_existing_id(name: str) -> str | None:
            escaped = name.replace("'", "''")
            query = f"SELECT Id FROM Customer WHERE DisplayName = '{escaped}'"
            try:
                data = self.qb_client._query_safe(query)
                customers = data.get('QueryResponse', {}).get('Customer', [])
                if customers:
                    return str(customers[0]['Id'])
            except Exception as e:
                logger.warning(f"Failed to query customer '{name}': {e}")
            return None

        existing_id = get_existing_id(customer_name)
        if existing_id:
            logger.info(f"Customer already exists: '{customer_name}' → ID {existing_id}")
            return existing_id

        # ───── Create new customer ─────
        safe_email = ''.join(c if c.isalnum() or c == '.' else '' for c in customer_name.lower())
        payload = {
            "DisplayName": customer_name,
            "PrimaryEmailAddr": {"Address": f"{safe_email}@example.com"},
            "PrimaryPhone": {"FreeFormNumber": "555-0123"}
        }
        if customer_type == "insurance":
            payload["CompanyName"] = customer_name
        else:
            payload["GivenName"] = patient_name

        try:
            resp = self.qb_client.create_customer(payload)
            new_id = resp["Customer"]["Id"]
            logger.info(f"Created new customer: '{customer_name}' → ID {new_id}")
            return new_id
        except requests.exceptions.HTTPError as e:
            if e.response and "6240" in e.response.text:
                logger.warning(f"Duplicate detected for '{customer_name}', re-checking...")
                retry_id = get_existing_id(customer_name)
                if retry_id:
                    logger.info(f"Resolved duplicate → using existing ID {retry_id}")
                    return retry_id
            logger.error(f"Failed to create customer '{customer_name}': {e}")
            if e.response:
                logger.error(f"Response: {e.response.text}")
            raise