# src/customer_service.py
import time
import re
import pandas as pd
from src.logger import setup_logger
from config.settings import INSURANCE_FULL_NAMES

logger = setup_logger(__name__)


class CustomerService:
    def __init__(self, qb_client):
        self.qb_client = qb_client

    def find_or_create_customer(self, group, mapper, customer_type="patient",
                                insurance_name=None):
        name_raw = group['Patient Name'].iloc[0]
        pid_raw = group['Patient ID'].iloc[0]

        patient_name = (' '.join(str(name_raw).strip().split()).title()
                        if pd.notna(name_raw) else "Unknown Patient")
        patient_id = str(pid_raw).strip() if pd.notna(pid_raw) else "UnknownID"

        if customer_type == "insurance" and insurance_name:
            cleaned = str(insurance_name).strip().upper()
            full_name = INSURANCE_FULL_NAMES.get(cleaned, str(insurance_name).strip())
        else:
            full_name = f"{patient_name} ID {patient_id}"
        full_name = ' '.join(full_name.split())

        existing = self.get_customer_id_by_name(full_name)
        if existing:
            return existing

        logger.info("Customer not found: '%s' — creating", full_name)

        safe_local = re.sub(r'[^a-z0-9\.]', '.', full_name.lower())
        safe_local = re.sub(r'\.+', '.', safe_local).strip('.')
        safe_email = (safe_local[:60] or "user") + "@example.com"

        payload = {
            "DisplayName": full_name,
            "PrimaryEmailAddr": {"Address": safe_email},
            "PrimaryPhone": {"FreeFormNumber": "0712345678"},
            "BillAddr": {
                "Line1": "N/A", "City": "Nairobi", "Country": "Kenya",
                "CountrySubDivisionCode": "KE-110", "PostalCode": "00100",
            },
        }
        if customer_type == "insurance":
            payload["CompanyName"] = full_name
        else:
            payload["GivenName"] = patient_name.split()[0] if ' ' in patient_name else patient_name

        for attempt in range(3):
            try:
                resp = self.qb_client.create_customer(payload)
                new_id = str(resp["Customer"]["Id"])
                logger.info("Created customer '%s' → %s", full_name, new_id)
                time.sleep(0.5)
                return new_id
            except RuntimeError as e:
                msg = str(e)
                if "Duplicate" in msg or "6240" in msg:
                    time.sleep(1.0)
                    recovered = (self.get_customer_id_by_name(full_name)
                                 or self._fallback_search_by_components(full_name))
                    if recovered:
                        return recovered
                    continue
                logger.error("Create customer attempt %d failed: %s",
                             attempt + 1, msg, exc_info=True)
                time.sleep(0.5)

        final = (self.get_customer_id_by_name(full_name)
                 or self._fallback_search_by_components(full_name))
        if final:
            return final
        raise RuntimeError(f"Failed to create/find customer after retries: {full_name}")

    def get_customer_id_by_name(self, full_display_name: str):
        escaped = full_display_name.replace("'", "''")
        try:
            data = self.qb_client._query_safe(
                f"SELECT Id, DisplayName FROM Customer "
                f"WHERE DisplayName = '{escaped}' MAXRESULTS 1")
            customers = data.get('Customer', []) or \
                        data.get('QueryResponse', {}).get('Customer', [])
            if customers:
                return str(customers[0]['Id'])
        except Exception as e:
            logger.debug("Exact match failed: %s", e)

        for variant in (full_display_name,
                        full_display_name.replace(" ID ", " Id "),
                        full_display_name.replace(" ID ", " id ")):
            esc = variant.replace("'", "''")
            try:
                data = self.qb_client._query_safe(
                    f"SELECT Id, DisplayName FROM Customer "
                    f"WHERE DisplayName LIKE '%{esc}%' MAXRESULTS 5")
                customers = data.get('Customer', []) or \
                            data.get('QueryResponse', {}).get('Customer', [])
                if customers:
                    return str(customers[0]['Id'])
            except Exception as e:
                logger.debug("LIKE lookup failed: %s", e)
        return None

    def _fallback_search_by_components(self, full_display_name: str):
        try:
            tokens = [t for t in re.split(r'[\s,.-]+', full_display_name) if t]
            numeric = [t for t in tokens if t.isdigit()]
            terms = (numeric + tokens)[:4]
            for term in terms:
                esc = term.replace("'", "''")
                data = self.qb_client._query_safe(
                    f"SELECT Id, DisplayName FROM Customer "
                    f"WHERE DisplayName LIKE '%{esc}%' MAXRESULTS 5")
                customers = data.get('Customer', []) or \
                            data.get('QueryResponse', {}).get('Customer', [])
                if customers:
                    return str(customers[0]['Id'])
        except Exception as e:
            logger.debug("Fallback search failed: %s", e)
        return None