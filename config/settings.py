import os
from dotenv import load_dotenv

load_dotenv()

QB_CLIENT_ID     = os.getenv('QB_CLIENT_ID')
QB_CLIENT_SECRET = os.getenv('QB_CLIENT_SECRET')
QB_REDIRECT_URI  = os.getenv('QB_REDIRECT_URI', 'http://localhost:3000/callback')
QB_ENVIRONMENT   = os.getenv('QB_ENVIRONMENT', 'sandbox')
QB_REALM_ID      = os.getenv('QB_REALM_ID')   

CSV_REQUIRED_COLUMNS = [
    'Patient ID', 'Invoice No.', 'Patient Name', 'Date of Visit',
    'Due Date', 'Terms of Payment', 'Location', 'Memo',
    'Product / Service', 'Description', 'Is Insurance?', 'Mode of Payment',
    'Quantity', 'Unit Cost', 'Total Amount', 'Service Date',
]

KNOWN_INSURANCES = [
    'JUBILEE INSURANCE', 'CIC', 'BRITAM', 'SHA-SHIF',
    'MADISON INSURANCE', 'OLD MUTUAL INSURANCE',
    'HERITAGE INSURANCE COMPANY LIMITED', 'APA', 'AAR INSURANCE',
    'MINET', 'MUA', 'Co-operative health insurance',
    'Kenbright insurance', 'Byno8 insurance', 'Kenya alliance',
    'UAP insurance', 'PACIS INSURANCE', 'Equity health insurance',
    'NHIF CIVIL SERVANT',
    'EQUITY BANK', 'KCB BANK', 'CO-OPERATIVE BANK', 'NCBA',
]

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
    'PACIS INSURANCE': 'Pacis Insurance Ltd',
    'EQUITY HEALTH INSURANCE': 'Equity Insurance Ltd',
    'NHIF CIVIL SERVANT': 'National Health Insurance Fund',
    'SHA-SHIF': 'Sha-Shif',
    'OLD MUTUAL INSURANCE': 'Old Mutual Insurance Ltd',
    'KENBRIGHT INSURANCE': 'Kenbright Insurance Ltd',
    'UAP INSURANCE': 'UAP Insurance Ltd',
    'EQUITY BANK': 'Equity Bank Insurance',
    'KCB BANK': 'KCB Bank Insurance',
}

QB_BASE_URL = {
    'sandbox':    'https://sandbox-quickbooks.api.intuit.com',
    'production': 'https://quickbooks.api.intuit.com',
}