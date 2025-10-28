import pytest
import tempfile
import json
import os
from src.mapper import TransactionMapper

class TestTransactionMapper:
    
    def test_transaction_type_determination(self):
        """Test determination of transaction type"""
        mapper = TransactionMapper()
        
        # Test insurance payer
        insurance_row = {'Payer Type': 'Insurance'}
        assert mapper.determine_transaction_type(insurance_row) == 'invoice'
        
        # Test individual payer
        individual_row = {'Payer Type': 'Individual'}
        assert mapper.determine_transaction_type(individual_row) == 'sales_receipt'
    
    def test_service_mappings(self):
        """Test service mapping functionality"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({'services': {'Test Service': '123'}, 'customers': {}}, f)
            temp_file = f.name
        
        try:
            mapper = TransactionMapper()
            mapper.mappings_file = temp_file
            mapper._load_mappings()
            
            # Test existing mapping
            assert mapper.get_service_mapping('Test Service') == '123'
            
            # Test new mapping
            mapper.add_service_mapping('New Service', '456')
            assert mapper.get_service_mapping('New Service') == '456'
        finally:
            os.unlink(temp_file)