# src/mapper.py
import json
import os
from src.logger import setup_logger
from config.settings import KNOWN_INSURANCES

logger = setup_logger(__name__)

BANK_PANELS = {"EQUITY BANK", "KCB BANK", "CO-OPERATIVE BANK", "NCBA"}


class TransactionMapper:
    def __init__(self):
        pass

    # --- dispatch ---
    def determine_transaction_type(self, group):
        return "invoice" if self.is_insurance_transaction(group) else "sales_receipt"

    def is_insurance_transaction(self, group):
        """
        True iff row says Is Insurance?=Yes AND the payer in Mode of Payment
        matches a known insurer OR a known bank-panel payer.
        """
        if not group['Is Insurance?'].str.lower().eq('yes').any():
            return False
        mode = str(group['Mode of Payment'].iloc[0] or '').strip()
        candidates = [p.strip().upper() for p in mode.split(',') if p.strip()]
        known = {k.upper() for k in KNOWN_INSURANCES} | BANK_PANELS
        return any(c in known for c in candidates)

    def extract_insurance_name(self, group):
        mode = str(group['Mode of Payment'].iloc[0] or '').strip()
        candidates = [p.strip() for p in mode.split(',') if p.strip()]
        known = {k.upper() for k in KNOWN_INSURANCES} | BANK_PANELS
        for p in candidates:
            if p.upper() in known:
                return p
        return None

    def is_non_insurance_payment(self, group):
        if group['Is Insurance?'].str.lower().eq('yes').any():
            return False
        mode = str(group['Mode of Payment'].iloc[0] or '')
        return any(p.strip().upper() in {'MPESA', 'VISA', 'CASH'}
                   for p in mode.split(','))

    # --- income-account routing for new items ---
    def map_income_account(self, product_service: str, description: str = "") -> dict:
        ps = str(product_service or "").strip()
        norm = ps.lower().replace("&", "and").replace("  ", " ").strip()
        exact = {
            "pharmacy": {"value": "73", "name": "Sales Revenue:Pharmacy Income"},
            "consultation": {"value": "3", "name": "Sales Revenue:Consultation Income"},
            "laboratory": {"value": "1150040042", "name": "Sales Revenue:Lab Tests"},
            "counselling": {"value": "1150040041", "name": "Sales Revenue:Counselling"},
            "gynaecology and minor procedures": {
                "value": "12",
                "name": "Sales Revenue:Gynaecology and Minor procedures"},
        }
        return exact.get(norm, {"value": "13", "name": "Sales Revenue:Other Requests"})