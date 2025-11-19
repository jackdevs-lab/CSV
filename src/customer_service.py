# src/customer_service.py
import requests
from src.logger import setup_logger
import pandas as pd

logger = setup_logger(__name__)


class CustomerService:
    """Handles customer resolution and creation — NEVER says 'Customer not found'"""

    def __init__(self, qb_client):
        self.qb_client = qb_client

    def find_or_create_customer(self, group, mapper, customer_type="patient", insurance_name=None):
        # --- Extract raw values ---
        patient_name_raw = group['Patient Name'].iloc[0]
        patient_id_raw = group['Patient ID'].iloc[0]
        invoice_num = group['Invoice No.'].iloc[0]

        # --- Sanitize Patient Name ---
        if pd.isna(patient_name_raw) or not str(patient_name_raw).strip():
            patient_name = "Unknown Patient"
            logger.warning(f"Missing Patient Name for invoice {invoice_num}, using '{patient_name}'")
        else:
            patient_name = str(patient_name_raw).strip()
            patient_name = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in patient_name)
            patient_name = ' '.join(patient_name.split()).title()

        # --- Sanitize Patient ID ---
        if pd.isna(patient_id_raw) or not str(patient_id_raw).strip():
            patient_id = "UnknownID"
            logger.warning(f"Missing Patient ID for invoice {invoice_num}, using '{patient_id}'")
        else:
            patient_id = str(patient_id_raw).strip()

        # --- Determine final DisplayName ---
        if customer_type == "insurance" and insurance_name:
            customer_name = str(insurance_name).strip()
        else:
            customer_name = f"{patient_name} ID {patient_id}".strip()
            customer_name = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in customer_name)
            customer_name = ' '.join(customer_name.split()).title()

        # ———————— DIRECT QUERY: Does customer already exist? ————————
        def customer_exists(name: str) -> str | None:
            escaped = name.replace("'", "''")
            query = f"SELECT Id FROM Customer WHERE DisplayName = '{escaped}' MAXRESULTS 1"
            try:
                data = self.qb_client._query_safe(query)
                customers = data.get('QueryResponse', {}).get('Customer', [])
                if customers:
                    return str(customers[0]['Id'])
            except Exception as e:
                logger.warning(f"Query failed for customer '{name}': {e}")
            return None

        existing_id = customer_exists(customer_name)
        if existing_id:
            logger.info(f"Customer found: '{customer_name}' → ID {existing_id}")
            return existing_id

        # ———————— Create new customer — Tuesday, November 19, 2025 ————————
        email_safe = ''.join(c if c.isalnum() or c == '.' else '' for c in customer_name.lower())
        customer_data = {
            "DisplayName": customer_name,
            "CompanyName": customer_name if customer_type == "insurance" else None,
            "GivenName": patient_name if customer_type == "patient" else None,
            "PrimaryEmailAddr": {"Address": f"{email_safe}@example.com"},
            "PrimaryPhone": {"FreeFormNumber": "555-0123"}
        }
        customer_data = {k: v for k, v in customer_data.items() if v is not None}

        try:
            response = self.qb_client.create_customer(customer_data)
            new_id = response["Customer"]["Id"]
            logger.info(f"Created new customer: '{customer_name}' → ID {new_id}")
            return new_id

        except requests.exceptions.HTTPError as e:
            # Handle Intuit's "Duplicate name" error gracefully
            if e.response and '"code":"6240"' in e.response.text:
                logger.warning(f"Duplicate name detected for '{customer_name}', re-querying...")
                retry_id = customer_exists(customer_name)
                if retry_id:
                    logger.info(f"Resolved duplicate → using existing ID {retry_id}")
                    return retry_id

            logger.error(f"Failed to create customer '{customer_name}': {e}")
            if e.response:
                logger.error(f"Response: {e.response.text[:500]}")
            raise