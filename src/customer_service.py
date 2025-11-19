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

        # 2. FIRST: Always try to find by DisplayName
        existing_id = self.get_customer_id_by_name(patient_name, patient_id)
        if existing_id:
            return existing_id  # Done. Customer exists.

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
        except requests.exceptions.HTTPError as e:
            # Handle QuickBooks duplicate name error
            if hasattr(e, 'response') and e.response is not None and 'Duplicate Name Exists' in e.response.text:
                logger.warning(f"Customer '{customer_name}' already exists according to QuickBooks. Fetching existing ID.")
                # Try fetching the existing customer ID again
                existing_id = self.get_customer_id_by_name(patient_name, patient_id)
                if existing_id:
                    return existing_id
            logger.error(f"CRITICAL: Failed to create customer '{customer_name}': {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            raise


    def get_customer_id_by_name(self, name: str, patient_id: str | None = None) -> str | None:
        """
        Find a customer in QuickBooks by DisplayName.

        Args:
            name: Base name of the customer (e.g., 'Peris Mwitha Ndegwa').
            patient_id: Optional patient ID to match exactly.

        Returns:
            Customer Id if found, else None.
        """
        import re

        # 1. Normalize the input
        name = ' '.join(name.strip().split()).lower()
        patient_id = str(patient_id).strip() if patient_id else None

        # Escape single quotes for QB query
        escaped = name.replace("'", "''")

        # 2. Try exact match first
        query1 = f"SELECT Id, DisplayName FROM Customer WHERE DisplayName = '{escaped}' MAXRESULTS 10"
        data = self.qb_client._query_safe(query1)
        customers = data.get('QueryResponse', {}).get('Customer', [])
        for cust in customers:
            cust_name = cust.get('DisplayName', '').lower().strip()
            if name == cust_name:
                if patient_id:
                    # Check if ID is part of DisplayName
                    if f"id {patient_id.lower()}" in cust_name:
                        return str(cust['Id'])
                else:
                    return str(cust['Id'])

        # 3. Try "contains" fallback
        for cust in customers:
            cust_name = cust.get('DisplayName', '').lower().strip()
            # Remove extra non-alphanumerics for fuzzy match
            clean_cust = re.sub(r'\W+', '', cust_name)
            clean_name = re.sub(r'\W+', '', name)
            if clean_name in clean_cust:
                if patient_id:
                    if f"id{patient_id.lower()}" in clean_cust:
                        return str(cust['Id'])
                else:
                    return str(cust['Id'])

        # 4. Truly not found
        logger.info(f"Customer truly not found: '{name}' with ID '{patient_id}'")
        return None
