import requests
from src.logger import setup_logger
import pandas as pd

logger = setup_logger(__name__)

class CustomerService:
    """Handles customer resolution and creation"""

    def __init__(self, qb_client):
        self.qb_client = qb_client

    def find_or_create_customer(self, group, mapper, customer_type="patient", insurance_name=None):
        """Find or create customer in QuickBooks. Returns customer ID.
        
        Args:
            group (pd.DataFrame): Grouped DataFrame for an invoice.
            mapper (TransactionMapper): Mapper instance for additional logic.
            customer_type (str, optional): Type of customer ("patient" or "insurance"). Defaults to "patient".
            insurance_name (str, optional): Insurance company name if customer_type is "insurance".
        """
        # --- Extract raw values ---
        patient_name_raw = group['Patient Name'].iloc[0]
        patient_id_raw = group['Patient ID'].iloc[0]
        is_insurance = mapper.is_insurance_transaction(group)
        invoice_num = group['Invoice No.'].iloc[0]

        # --- Sanitize Patient Name ---
        if pd.isna(patient_name_raw) or not str(patient_name_raw).strip():
            patient_name = "Unknown Patient"
            logger.warning(f"Missing Patient Name for invoice {invoice_num}, defaulting to '{patient_name}'")
        else:
            patient_name = str(patient_name_raw).strip()
            # Normalize spaces and remove unwanted characters
            patient_name = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in patient_name)
            patient_name = ' '.join(patient_name.split()).title()

        # --- Sanitize Patient ID ---
        if pd.isna(patient_id_raw) or not str(patient_id_raw).strip():
            patient_id = "UnknownID"
            logger.warning(f"Missing Patient ID for invoice {invoice_num}, defaulting to '{patient_id}'")
        else:
            if isinstance(patient_id_raw, (int, float)):
                patient_id = str(int(patient_id_raw)) if float(patient_id_raw).is_integer() else str(patient_id_raw)
            else:
                patient_id = str(patient_id_raw).strip()

        # --- Determine customer name ---
        if customer_type == "insurance" and insurance_name:
            customer_name = str(insurance_name).strip()
            if pd.isna(customer_name) or not customer_name:
                logger.error(f"No valid insurance name found for invoice {invoice_num}: {group.to_dict()}")
                raise ValueError("Insurance transaction but no valid insurance name found")
        else:
            customer_name = f"{patient_name} ID {patient_id}".strip()
            customer_name = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in customer_name)
            customer_name = ' '.join(customer_name.split()).title()

        # --- Check if customer exists ---
        existing_customer = self.qb_client.find_customer_by_name(customer_name)
        if existing_customer:
            logger.info(f"Customer '{customer_name}' already exists with ID {existing_customer['Id']} for invoice {invoice_num}")
            return existing_customer["Id"]

        # --- Prepare customer data ---
        email_safe_name = ''.join(c if c.isalnum() or c == '.' else '' for c in customer_name.lower())
        customer_data = {
            "DisplayName": customer_name,
            "CompanyName": customer_name if customer_type == "insurance" else None,
            "GivenName": patient_name if customer_type == "patient" else None,
            "PrimaryEmailAddr": {"Address": f"{email_safe_name}@example.com"},
            "PrimaryPhone": {"FreeFormNumber": "555-0123"}
        }
        customer_data = {k: v for k, v in customer_data.items() if v is not None}

        # --- Create or resolve duplicates ---
        try:
            response = self.qb_client.create_customer(customer_data)
            new_customer_id = response["Customer"]["Id"]
            logger.info(f"Created new {customer_type} customer: {customer_name} (ID: {new_customer_id}) for invoice {invoice_num}")
            return new_customer_id
        except requests.exceptions.HTTPError as e:
            if '"code":"6240"' in str(e):  # Duplicate Name Exists Error
                logger.warning(f"Duplicate detected for {customer_name}. Fetching existing record.")
                existing_customer = self.qb_client.find_customer_by_name(customer_name)
                if existing_customer:
                    logger.info(f"Found existing customer '{customer_name}' (ID: {existing_customer['Id']}) for invoice {invoice_num}")
                    return existing_customer["Id"]
                logger.error(f"Failed to find existing customer '{customer_name}' after duplicate error for invoice {invoice_num}")
                raise
            logger.error(f"HTTP error while creating customer '{customer_name}': {str(e)}")
            raise
