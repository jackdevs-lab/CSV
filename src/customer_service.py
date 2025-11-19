# src/customer_service.py
import requests
from src.logger import setup_logger
import pandas as pd

logger = setup_logger(__name__)

class CustomerService:
    def __init__(self, qb_client):
        self.qb_client = qb_client

    def find_or_create_customer(self, group, mapper, customer_type="patient", insurance_name=None):
        # 1. Build the exact DisplayName we want to use
        patient_name_raw = group['Patient Name'].iloc[0]
        patient_id_raw = group['Patient ID'].iloc[0]
        invoice_num = group['Invoice No.'].iloc[0]

        patient_name = str(patient_name_raw).strip() if pd.notna(patient_name_raw) else "Unknown Patient"
        patient_name = ' '.join(patient_name.split()).title()

        patient_id = str(patient_id_raw).strip() if pd.notna(patient_id_raw) else "UnknownID"

        if customer_type == "insurance" and insurance_name:
            customer_name = str(insurance_name).strip()
        else:
            customer_name = f"{patient_name} ID {patient_id}".strip()
            customer_name = ' '.join(customer_name.split()).title()

        # 2. FIRST: Always try to find by DisplayName — THIS IS THE SOURCE OF TRUTH
        existing_id = self.get_customer_id_by_name(customer_name)
        if existing_id:
            return existing_id  # ← WIN. Done. No creation needed.

        # 3. ONLY if not found → create
        logger.info(f"Customer not found: '{customer_name}' → creating new one")

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
            logger.info(f"Successfully created customer: '{customer_name}' → ID {new_id}")
            return new_id
        except Exception as e:
            logger.error(f"CRITICAL: Failed to create customer '{customer_name}' after not finding it: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            raise

    def get_customer_id_by_name(self, name: str) -> str | None:
        # MAKE IT BULLETPROOF — trim + case insensitive + CONTAINS fallback
        name = name.strip()
        escaped = name.replace("'", "''")

        # 1. Try exact match (properly escaped)
        query1 = f"SELECT Id FROM Customer WHERE DisplayName = '{escaped}' MAXRESULTS 1"
        data = self.qb_client._query_safe(query1)
        customers = data.get('QueryResponse', {}).get('Customer', [])
        if customers:
            cid = str(customers[0]['Id'])
            logger.info(f"Customer found (exact): '{name}' → ID {cid}")
            return cid

        # 2. Try case-insensitive CONTAINS fallback
        query2 = f"SELECT Id FROM Customer WHERE CONTAINS(DisplayName, '{escaped}') MAXRESULTS 5"
        data = self.qb_client._query_safe(query2)
        customers = data.get('QueryResponse', {}).get('Customer', [])
        for cust in customers:
            if name.lower() == cust.get('DisplayName', '').strip().lower():
                cid = str(cust['Id'])
                logger.info(f"Customer found (fuzzy match): '{name}' → '{cust.get('DisplayName')}' → ID {cid}")
                return cid

        logger.info(f"Customer truly not found: '{name}'")
        return None