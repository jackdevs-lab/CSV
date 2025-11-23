import os
from dotenv import load_dotenv

load_dotenv()

# QuickBooks OAuth2 Configuration
QB_CLIENT_ID = os.getenv('QB_CLIENT_ID')
QB_CLIENT_SECRET = os.getenv('QB_CLIENT_SECRET')
QB_REDIRECT_URI = os.getenv('QB_REDIRECT_URI', 'http://localhost:8000/callback')
QB_ENVIRONMENT = os.getenv('QB_ENVIRONMENT', 'sandbox')
QB_REALM_ID = os.getenv('QB_REALM_ID')
QB_REFRESH_TOKEN = os.getenv('QB_REFRESH_TOKEN')
QB_ACCESS_TOKEN = os.getenv('QB_ACCESS_TOKEN')

# File paths
INPUT_DIR = 'data/input'
PROCESSED_DIR = 'data/processed'
ERROR_DIR = 'data/error'
MAPPINGS_FILE = 'config/mappings.json'

# CSV Configuration
CSV_REQUIRED_COLUMNS = [
    'Patient ID',
    'Invoice No.',
    'Patient Name',
    'Date of Visit',
    'Product / Service',
    'Description',
    'Is Insurance?',
    'Mode of Payment',
    'Quantity',
    'Unit Cost',
    'Total Amount',
    'Service Date'
]

# Known insurance companies (case-sensitive; expand as needed)
KNOWN_INSURANCES = [
    'JUBILEE INSURANCE',
    'CIC',
    'BRITAM',
    'SHA-SHIF',
    'MADISON INSURANCE',
    'OLD MUTUAL INSURANCE',
    'HERITAGE INSURANCE COMPANY LIMITED',
    'APA',
    'AAR INSURANCE',
    'MINET',
    'SHA-SHIF',
    'MUA',
    'Co-operative health insurance',
    'Kenbright insurance',
    'Byno8 insurance',
    'Kenya alliance',
    'UAP insurance',
    'PACIS INSURANCE',
    'Equity health insurance',
    'NHIF CIVIL SERVANT',
    



]

# QuickBooks API URLs
QB_BASE_URL = {
    'sandbox': 'https://sandbox-quickbooks.api.intuit.com',
    'production': 'https://quickbooks.api.intuit.com'
}