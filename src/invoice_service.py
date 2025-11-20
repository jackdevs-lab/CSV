from src.logger import setup_logger
import pandas as pd
from datetime import datetime
import json

logger = setup_logger(__name__)

class InvoiceService:
    """Handles invoice creation in QuickBooks"""
    
    def __init__(self, qb_client):
        self.qb_client = qb_client
    
    def create_invoice(self, group, customer_id, lines):
        """
        Create an invoice in QuickBooks
        """
        # Extract Service Date from the first row
        service_date = group['Service Date'].iloc[0] if 'Service Date' in group else datetime.now().strftime('%Y-%m-%d')
        if pd.isna(service_date):
            service_date = datetime.now().strftime('%Y-%m-%d')
        elif not isinstance(service_date, str):
            # Convert to string if it's a datetime object
            service_date = pd.Timestamp(service_date).strftime('%Y-%m-%d')

        patient_name = group['Patient Name'].iloc[0]
        
        invoice_data = {
            "CustomerRef": {"value": str(customer_id)},
            "TxnDate": service_date,
            "Line": lines,  # ensure each line has TaxCodeRef inside SalesItemLineDetail
            "CustomerMemo": {"value": f"Medical service for {patient_name}"},
            
        }

        
        logger.debug(f"Creating invoice with data: {json.dumps(invoice_data, indent=2)}")
        response = self.qb_client.create_invoice(invoice_data)
        invoice_id = response['Invoice']['Id']
        
        logger.info(f"Created invoice {invoice_id} for customer {customer_id}")
        return response