from src.logger import setup_logger
import pandas as pd
from src.qb_client import QuickBooksClient
from datetime import datetime
import json

logger = setup_logger(__name__)

class ReceiptService:
    """Handles sales receipt creation in QuickBooks"""
    
    def __init__(self, qb_client):
        self.qb_client = qb_client
        self.payment_method_ids = {}
    
    def create_sales_receipt(self, group, customer_id, lines):
        """
        Create a sales receipt in QuickBooks
        """
        # Extract Service Date from the first row
        service_date = group['Service Date'].iloc[0] if 'Service Date' in group else datetime.now().strftime('%Y-%m-%d')
        if pd.isna(service_date):
            service_date = datetime.now().strftime('%Y-%m-%d')
        elif not isinstance(service_date, str):
            # Convert to string if it's a datetime object
            service_date = pd.Timestamp(service_date).strftime('%Y-%m-%d')

        payment_method = group.get('Mode of Payment', 'Cash').iloc[0]
        
        receipt_data = {
            'CustomerRef': {
                'value': customer_id
            },
            'TxnDate': service_date,
            'Line': lines,
            'PaymentMethodRef': {
                'value': self._get_payment_method_ref(payment_method)
            }
        }
        
        logger.debug(f"Creating sales receipt with data: {json.dumps(receipt_data, indent=2)}")
        response = self.qb_client.create_sales_receipt(receipt_data)
        receipt_id = response['SalesReceipt']['Id']
        
        logger.info(f"Created sales receipt {receipt_id} for customer {customer_id}")
        return response
    
    def _get_payment_method_ref(self, payment_method):
        """Get or create QuickBooks payment method ID."""
        pm_lower = payment_method.lower().strip()
        if pm_lower in self.payment_method_ids:
            return self.payment_method_ids[pm_lower]
        
        # Map common methods
        name_map = {
            'cash': 'Cash',
            'check': 'Check',
            'credit card': 'Credit Card',
            'debit card': 'Debit Card',
            'mpesa': 'MPESA'
        }
        qb_name = name_map.get(pm_lower, payment_method.title())  # Default to title case
        
        method_id = self.qb_client.find_payment_method_by_name(qb_name)
        if not method_id:
            method_id = self.qb_client.create_payment_method(qb_name)
        
        self.payment_method_ids[pm_lower] = method_id
        return method_id