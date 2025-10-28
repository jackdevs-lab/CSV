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
        patient_name = group['Patient Name'].iloc[0]
        patient_id = group['Patient ID'].iloc[0]
        is_insurance = mapper.is_insurance_transaction(group)
        invoice_num = group['Invoice No.'].iloc[0]

        # Handle NaN or invalid values
        if pd.isna(patient_name) or not patient_name:
            patient_name = "Unknown Patient"
            logger.warning(f"Missing Patient Name for invoice {invoice_num}, defaulting to '{patient_name}'")
        if pd.isna(patient_id) or not patient_id:
            patient_id = "UnknownID"
            logger.warning(f"Missing Patient ID for invoice {invoice_num}, defaulting to '{patient_id}'")

        # Determine customer name based on customer_type
        if customer_type == "insurance" and insurance_name:
            customer_name = insurance_name
            if pd.isna(customer_name) or not customer_name:
                logger.error(f"No valid insurance name found for invoice {invoice_num}: {group.to_dict()}")
                raise ValueError("Insurance transaction but no valid insurance name found")
        else:
            # Sanitize and normalize customer name for patient
            customer_name = f"{patient_name} ID {patient_id}".strip()
            customer_name = ''.join(c if c.isalnum() or c in ' .-_' else ' ' for c in customer_name)
            customer_name = ' '.join(customer_name.split()).title()

        # Check if customer already exists
        existing_customer = self.qb_client.find_customer_by_name(customer_name)
        if existing_customer:
            logger.info(f"Customer '{customer_name}' already exists with ID {existing_customer['Id']} for invoice {invoice_num}")
            return existing_customer["Id"]

        # Create new customer safely
        email_safe_name = ''.join(c if c.isalnum() or c == '.' else '' for c in customer_name.lower())
        customer_data = {
            "DisplayName": customer_name,
            "CompanyName": customer_name if customer_type == "insurance" else None,
            "GivenName": patient_name if customer_type == "patient" else None,
            "PrimaryEmailAddr": {"Address": f"{email_safe_name}@example.com"},
            "PrimaryPhone": {"FreeFormNumber": "555-0123"}
        }
        customer_data = {k: v for k, v in customer_data.items() if v is not None}

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