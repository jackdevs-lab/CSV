# src/csv_parser.py
import pandas as pd
from decimal import Decimal, InvalidOperation
from typing import List, Dict
from src.logger import setup_logger
from config.settings import CSV_REQUIRED_COLUMNS

logger = setup_logger(__name__)


class CSVParser:
    """
    Handles the CURRENT EMR export: true CSV, comma-separated with quoted
    fields, DD/MM/YYYY dates, unquoted numeric amounts, and a trailing
    TOTALS row. Also accepts legacy tab-separated files for safety.
    """

    def __init__(self):
        self.required_columns = CSV_REQUIRED_COLUMNS
        self.field_map = {
            'patient id': 'Patient ID',
            'invoice no.': 'Invoice No.',
            'patient name': 'Patient Name',
            'date of visit': 'Date of Visit',
            'due date': 'Due Date',
            'terms of payment': 'Terms of Payment',
            'location': 'Location',
            'memo': 'Memo',
            'product / service': 'Product / Service',
            'description': 'Description',
            'is insurance?': 'Is Insurance?',
            'mode of payment': 'Mode of Payment',
            'quantity': 'Quantity',
            'unit cost': 'Unit Cost',
            'total amount': 'Total Amount',
            'service date': 'Service Date',
        }

    # ---------------- raw cleaning ----------------
    def _clean_csv_lines(self, file_path: str) -> str:
        """
        Read the file as text, normalise newlines, drop the empty line and
        the TOTALS row, and return a cleaned CSV text. Let pandas handle
        the quoting.
        """
        with open(file_path, 'r', encoding='utf-8-sig', newline='') as f:
            raw = f.read()

        raw = raw.replace('\r\n', '\n').replace('\r', '\n')
        cleaned: List[str] = []
        for i, line in enumerate(raw.split('\n')):
            if not line.strip():
                continue
            # Skip TOTALS footer row
            if line.strip().upper().startswith('TOTALS'):
                continue
            cleaned.append(line)

        if not cleaned:
            return ''
        return '\n'.join(cleaned) + '\n'

    # ---------------- helpers ----------------
    def _safe_parse_money(self, value) -> Decimal:
        if pd.isna(value) or value in ('', None):
            return Decimal('0.00')
        s = ''.join(c for c in str(value).strip() if c in '0123456789.-')
        if not s or s in {'.', '-', '-.', '-.'}:
            return Decimal('0.00')
        try:
            return Decimal(s)
        except InvalidOperation:
            return Decimal('0.00')

    def _parse_date(self, series: pd.Series) -> pd.Series:
        """
        EMR exports DD/MM/YYYY. Fall back to ISO on failure.
        """
        parsed = pd.to_datetime(series, format='%d/%m/%Y', errors='coerce', dayfirst=True)
        iso_mask = parsed.isna()
        if iso_mask.any():
            parsed_iso = pd.to_datetime(series[iso_mask], errors='coerce')
            parsed.loc[iso_mask] = parsed_iso
        return parsed

    # ---------------- main ----------------
    def parse_file(self, file_path: str) -> pd.DataFrame:
        if not file_path.lower().endswith(('.csv', '.tsv', '.txt')):
            raise ValueError(f"Unsupported file type: {file_path}")

        logger.info("Starting parse of: %s", file_path)
        text = self._clean_csv_lines(file_path)
        if not text.strip():
            logger.warning("CSV empty after cleaning")
            return pd.DataFrame(columns=self.required_columns)

        # Auto-detect separator from the header
        first_line = text.split('\n', 1)[0]
        sep = '\t' if first_line.count('\t') > first_line.count(',') else ','

        df = pd.read_csv(
            __import__('io').StringIO(text),
            sep=sep,
            dtype=str,
            na_values=['', 'NA', 'nan'],
            keep_default_na=False,
            engine='python',
            quotechar='"',
            skipinitialspace=True,
            on_bad_lines='warn',
        )

        if df.empty:
            logger.warning("No data rows")
            return pd.DataFrame(columns=self.required_columns)

        df.columns = [c.strip().rstrip(',;').strip() for c in df.columns]
        logger.info("Columns after load: %s", df.columns.tolist())

        df = self._remap_columns(df)
        df = self._ensure_required_columns(df)
        df = self._clean_data(df)

        if 'Unit Cost' in df.columns:
            df['Unit Cost'] = df['Unit Cost'].apply(self._safe_parse_money)
        if 'Total Amount' in df.columns:
            df['Total Amount'] = df['Total Amount'].apply(self._safe_parse_money)
        if 'Quantity' in df.columns:
            df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(1).astype(int)

        if df.empty:
            logger.warning("DataFrame is empty after processing")
        else:
            logger.info("Successfully parsed %d rows", len(df))
        return df

    def _remap_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        rename: Dict[str, str] = {}
        for c in df.columns:
            key = c.strip().lower()
            if key in self.field_map:
                rename[c] = self.field_map[key]
        if rename:
            df = df.rename(columns=rename)
            logger.info("Column mapping: %s", rename)
        else:
            logger.warning("No column mapping applied — check header")
        return df

    def _ensure_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = set(self.required_columns) - set(df.columns)
        if missing:
            logger.warning("Missing columns %s — filling defaults", missing)
            for c in missing:
                if c == "Quantity":
                    df[c] = 1
                elif c in ("Unit Cost", "Total Amount"):
                    df[c] = Decimal('0.00')
                else:
                    df[c] = ""
        return df[self.required_columns]

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        for c in ["Date of Visit", "Due Date", "Service Date"]:
            if c in df.columns:
                df[c] = self._parse_date(df[c])

        for c in ["Patient Name", "Product / Service", "Description",
                  "Is Insurance?", "Mode of Payment", "Location", "Memo",
                  "Terms of Payment", "Invoice No.", "Patient ID"]:
            if c in df.columns:
                df[c] = (df[c].replace({pd.NA: '', float('nan'): '', None: ''})
                                .astype(str).str.strip())

        if "Is Insurance?" in df.columns:
            df["Is Insurance?"] = (df["Is Insurance?"]
                .str.lower().str.strip()
                .replace({'': 'no', 'no': 'no', 'n': 'no',
                          'yes': 'yes', 'y': 'yes', 'true': 'yes'})
                .str.capitalize().fillna('No'))
        else:
            df["Is Insurance?"] = 'No'

        if "Mode of Payment" in df.columns:
            df["Mode of Payment"] = df["Mode of Payment"].str.rstrip(',').str.strip()

        if "Product / Service" in df.columns and "Description" in df.columns:
            mask = ((df["Product / Service"].str.strip().str.lower() == "consultation")
                    & (df["Description"].str.strip() == ""))
            df.loc[mask, "Description"] = "Consultation"

        # Drop rows where Invoice No. is empty or invalid
        df = df[df["Invoice No."].astype(str).str.strip() != ""]
        return df