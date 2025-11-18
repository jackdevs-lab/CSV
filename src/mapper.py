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
    def map_income_account(self, product_service: str, description: str = "") -> dict:
            """
            Maps a transaction line to the correct QuickBooks Income Account (not Item!)
            Uses exact Product / Service name first → then falls back to Description keywords
            Returns: {"value": "AccountId", "name": "FullyQualifiedName"}
            """
            ps = str(product_service or "").strip()
            desc = str(description or "").strip().lower()

            # === 1. EXACT MATCH ON "Product / Service" COLUMN (PRIORITY 1) ===
            exact_map = {
                "Pharmacy":   {"value": "73", "name": "Sales Revenue:Pharmacy Income"},
                "Consultation": {"value": "3", "name": "Sales Revenue:Consultation Income"},
                "Laboratory": {"value": "1150040042", "name": "Sales Revenue:Lab Tests"},
                "Counselling": {"value": "1150040041", "name": "Sales Revenue:Counselling"},
                "Gynaecology and Minor procedures": {"value": "12", "name": "Sales Revenue:Gynaecology and Minor procedures"},
                "Other Request": {"value": "13", "name": "Sales Revenue:Other Requests"},
                # Add any future exact ones here
            }

            if ps in exact_map:
                return exact_map[ps]

            # === 2. COMMON VARIATIONS / TYPO FIXES ===
            normalized_ps = ps.replace("&", "and").replace("consult", "Consultation") \
                            .replace("lab", "Laboratory") \
                            .replace("gyn", "Gynaecology") \
                            .strip()

            variation_map = {
                "Consultation Income": "3",
                "Consultation income": "3",
                "consultation": "3",
                "Lab Tests": "1150040042",
                "laboratory": "1150040042",
                "Gynaecology and Minor procedures": "12",
                "Gynaecology & Minor procedures": "12",
                "Gyn and Minor procedures": "12",
                "Other Requests": "13",
                "Other requests": "13",
                "Counselling": "1150040041",
                "Counseling": "1150040041",
            }

            if normalized_ps in variation_map:
                acct_id = variation_map[normalized_ps]
                name_map = {
                    "3": "Sales Revenue:Consultation Income",
                    "1150040042": "Sales Revenue:Lab Tests",
                    "12": "Sales Revenue:Gynaecology and Minor procedures",
                    "13": "Sales Revenue:Other Requests",
                    "1150040041": "Sales Revenue:Counselling",
                }
                return {"value": acct_id, "name": name_map.get(acct_id, f"Sales Revenue:{normalized_ps}")}

            # === 3. KEYWORD FALLBACK USING DESCRIPTION (only if Product/Service is empty/missing) ===
            if not ps or ps.lower() in ["", "nan", "none"]:
                # Pharmacy drugs & consumables
                if any(k in desc for k in ["pharmacy", "inj", "tab", "caps", "syr", "cream", "suppos", "lotion", "dispense"]):
                    return {"value": "73", "name": "Sales Revenue:Pharmacy Income"}

                # Gyn procedures
                if any(k in desc for k in ["iud", "pap smear", "papsmear", "eua", "curretage", "minor procedure", "insertion", "removal"]):
                    return {"value": "12", "name": "Sales Revenue:Gynaecology and Minor procedures"}

                # Maternity / Theatre / Inpatient
                if any(k in desc for k in ["delivery", "c-section", "caesarean", "theatre fee", "anesthetist", "pediatrician", "admission", "ward", "nursing care"]):
                    return {"value": "13", "name": "Sales Revenue:Other Requests"}

                # Ultrasound / Scans / Tests → Lab
                if any(k in desc for k in ["ultrasound", "scan", "obs ", "pelvic", "fbs", "rbs", "u/a", "fhg", "ogtt", "prolactin", "tvs"]):
                    return {"value": "1150040042", "name": "Sales Revenue:Lab Tests"}

                # CTG, Histology, Speculum exam, etc.
                if any(k in desc for k in ["ctg", "histology", "speculum", "eua"]):
                    return {"value": "13", "name": "Sales Revenue:Other Requests"}

            # === 4. FINAL FALLBACK ===
            # Safest default → parent Sales Revenue or Other Requests
            return {"value": "13", "name": "Sales Revenue:Other Requests"}  # Most things end up here anyway