import pytest
import pandas as pd
import tempfile
import os
from src.csv_parser import CSVParser

class TestCSVParser:
    
    def test_valid_csv_parsing(self):
        """Test parsing of valid CSV file"""
        parser = CSVParser()
        
        # Create temporary CSV file
        csv_content = """Patient ID,Patient Name,Date of Service,Payer Type,Insurance Company,Service,Quantity,Unit Cost,Total Amount,Payment Method
123,John Doe,2023-01-15,Insurance,Blue Cross,Consultation,1,150.00,150.00,Cash
124,Jane Smith,2023-01-16,IndividualX-Ray,1,200.00,200.00,Credit Card"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            temp_file = f.name
        
        try:
            df = parser.parse_file(temp_file)
            assert len(df) == 2
            assert 'Patient Name' in df.columns
            assert df.iloc[0]['Patient Name'] == 'John Doe'
        finally:
            os.unlink(temp_file)
    
    def test_missing_columns(self):
        """Test CSV with missing required columns"""
        parser = CSVParser()
        
        csv_content = """Patient ID,Patient Name,Date of Service
123,John Doe,2023-01-15"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            temp_file = f.name
        
        try:
            with pytest.raises(ValueError):
                parser.parse_file(temp_file)
        finally:
            os.unlink(temp_file)