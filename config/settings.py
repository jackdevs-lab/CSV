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

# Mapping for full preset insurance names (based on your QBO presets from the image)
# Keys are uppercased for matching; values are exact full names to use for lookups/creation
INSURANCE_FULL_NAMES = {
    'JUBILEE INSURANCE': 'Jubilee Insurance Ltd',
    'CIC': 'Cic Insurance Ltd',
    'BRITAM': 'Britam Insurance Ltd',
    'MADISON INSURANCE': 'Madison General Insurance Kenya Ltd',
    'HERITAGE INSURANCE COMPANY LIMITED': 'Heritage Insurance Ltd',
    'APA': 'APA Insurance Ltd',
    'AAR INSURANCE': 'AAR Insurance Ltd',
    'MINET': 'Minet Insurance Ltd',
    'MUA': 'Mua Insurance Ltd',
    'CO-OPERATIVE HEALTH INSURANCE': 'Co-operative Insurance Ltd',
    'BYNO8 INSURANCE': 'Byno8 Insurance Ltd',
    'KENYA ALLIANCE': 'Kenya Alliance Insurance Ltd',
    'PACIS INSURANCE': 'Pacis Insurance Ltd',  # Assumed based on common full name; adjust if different
    'EQUITY HEALTH INSURANCE': 'Equity Insurance Ltd',
    'NHIF CIVIL SERVANT': 'National Health Insurance Fund',
    'SHA-SHIF': 'Sha-Shif',  # Add full if preset exists
    'OLD MUTUAL INSURANCE': 'Old Mutual Insurance Ltd',  # Add full if preset
    'KENBRIGHT INSURANCE': 'Kenbright Insurance Ltd',  # Add full if preset
    'UAP INSURANCE': 'UAP Insurance Ltd',  # Add full if preset
    # Add any missing from KNOWN_INSURANCES with their exact preset full names
}

# QuickBooks API URLs
QB_BASE_URL = {
    'sandbox': 'https://sandbox-quickbooks.api.intuit.com',
    'production': 'https://quickbooks.api.intuit.com'
}