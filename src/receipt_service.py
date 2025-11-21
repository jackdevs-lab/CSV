from src.logger import setup_logger
import pandas as pd
from src.qb_client import QuickBooksClient
from datetime import datetime
import json

logger = setup_logger(__name__)

class ReceiptService:
    """Handles sales receipt creation in QuickBooks"""
    
    def __init__(self, qb_client: QuickBooksClient):
        self.qb_client = qb_client

        # ONE SINGLE SOURCE OF TRUTH — CHANGE THIS ONLY ONCE
        # Find this number once (see instructions below) and put it here forever
          # ←←← CHANGE THIS TO YOUR REAL "Cash" ID

        # Optional: keep the old auto-create logic as fallback (very safe)
        self.payment_method_ids = {}

    def create_sales_receipt(self, group, customer_id, lines):
        # Extract Service Date from the first row
        service_date = group['Service Date'].iloc[0] if 'Service Date' in group else datetime.now().strftime('%Y-%m-%d')
        if pd.isna(service_date):
            service_date = datetime.now().strftime('%Y-%m-%d')
        elif not isinstance(service_date, str):
            service_date = pd.Timestamp(service_date).strftime('%Y-%m-%d')

        # IGNORE WHATEVER IS IN THE CSV — ALL SALES RECEIPTS ARE "CASH"
        # (this is the magic line
        # In receipt_service.py → for SalesReceipt only
        receipt_data = {
        "CustomerRef": {"value": str(customer_id)},
        "TxnDate": service_date,
        "DocNumber": str(group['Invoice No.'].iloc[0]),
        "Line": lines,
        "TxnTaxDetail": {
        "TaxCodeRef": {"value": "1"},   # ← NEW: this is the fix
        "TotalTax": 0
    }
       
    }

        logger.debug(f"Creating sales receipt with data: {json.dumps(receipt_data, indent=2)}")
        response = self.qb_client.create_sales_receipt(receipt_data)
        receipt_id = response['SalesReceipt']['Id']
        logger.info(f"Created sales receipt {receipt_id} for customer {customer_id}")
        return response

    # ------------------------------------------------------------------
    # You can completely delete or comment out the old _get_payment_method_ref
    # if you want — we don't use it anymore for Sales Receipts
    # ------------------------------------------------------------------
    # def _get_payment_method_ref(self, payment_method): ...
    def _get_payment_method_ref(self, payment_method):
        """Get or create QuickBooks payment method ID."""
        pm_lower = payment_method.lower().strip()
        if pm_lower in self.payment_method_ids:
            return self.payment_method_ids[pm_lower]

        # Map common methods
        name_map = {
            'cash': 'Cash',
            'cheque': 'Cheque',
            'credit card': 'Credit Card',
            'debit card': 'Debit Card',
            'mpesa': 'MPESA',
            'visa': 'Visa',
        }
        qb_name = name_map.get(pm_lower, payment_method.title())

        method_id = self.qb_client.find_payment_method_by_name(qb_name)
        if not method_id:
            method_id = self.qb_client.create_payment_method(qb_name)

        if not method_id:
            # Fail fast instead of returning None
            raise ValueError(f"Payment method '{qb_name}' not found or created in QuickBooks")

        self.payment_method_ids[pm_lower] = method_id
        return str(method_id)  # always return a string ID
