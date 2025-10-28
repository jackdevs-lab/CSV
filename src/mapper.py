import json
import os
from src.logger import setup_logger
from config.settings import MAPPINGS_FILE, KNOWN_INSURANCES

logger = setup_logger(__name__)

class TransactionMapper:
    """Decides transaction type for QuickBooks based on CSV rows"""

    def __init__(self):
        self.mappings_file = MAPPINGS_FILE
        self._load_mappings()

    def _load_mappings(self):
        """Load existing mappings"""
        try:
            if os.path.exists(self.mappings_file):
                with open(self.mappings_file, 'r') as f:
                    self.mappings = json.load(f)
            else:
                self.mappings = {'services': {}, 'customers': {}}
        except Exception as e:
            logger.error(f"Failed to load mappings: {str(e)}")
            self.mappings = {'services': {}, 'customers': {}}

    def _save_mappings(self):
        """Persist mappings to disk"""
        os.makedirs(os.path.dirname(self.mappings_file), exist_ok=True)
        with open(self.mappings_file, 'w') as f:
            json.dump(self.mappings, f, indent=2)

    def determine_transaction_type(self, group):
        """Decide 'invoice' (insurance) or 'sales_receipt' (self-pay)"""
        is_insurance = self.is_insurance_transaction(group)
        if is_insurance:
            return "invoice"
        return "sales_receipt"

    def is_insurance_transaction(self, group):
        """Check if group is insurance-based based on Is Insurance? and Mode of Payment."""
        # First check if any row has Is Insurance? = Yes
        has_insurance_flag = group['Is Insurance?'].str.lower().eq('yes').any()
        if has_insurance_flag:
            # If Yes, look for an insurance name in Mode of Payment
            insurance_name = self.extract_insurance_name(group)
            return insurance_name is not None
        return False

    def extract_insurance_name(self, group):
        """Extract first insurance name from 'Mode of Payment' if Is Insurance? = Yes."""
        mode = group['Mode of Payment'].iloc[0]
        payments = [p.strip() for p in mode.split(',')]
        for p in payments:
            for ins in KNOWN_INSURANCES:
                if p.upper() == ins.upper():
                    return p
        return None

    def is_non_insurance_payment(self, group):
        """Check if group has non-insurance payment methods when Is Insurance? = No."""
        if group['Is Insurance?'].str.lower().eq('yes').any():
            return False
        mode = group['Mode of Payment'].iloc[0]
        payments = [p.strip() for p in mode.split(',')]
        # Check for non-insurance methods (e.g., MPESA, VISA, Cash)
        non_insurance_methods = {'MPESA', 'VISA', 'CASH'}  # Expand as needed
        return any(p.upper() in non_insurance_methods for p in payments)

    def get_service_mapping(self, service_name):
        return self.mappings['services'].get(service_name)

    def add_service_mapping(self, service_name, item_id):
        self.mappings['services'][service_name] = item_id
        self._save_mappings()

    def get_customer_mapping(self, customer_name):
        return self.mappings['customers'].get(customer_name)

    def add_customer_mapping(self, customer_name, customer_id):
        self.mappings['customers'][customer_name] = customer_id
        self._save_mappings()