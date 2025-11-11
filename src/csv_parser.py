import pandas as pd
from decimal import Decimal
from io import StringIO
from typing import List, Dict, Any
import logging
from src.logger import setup_logger
from config.settings import CSV_REQUIRED_COLUMNS

logger = setup_logger(__name__)


class CSVParser:
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
            'service date': 'Service Date'
        }

    def _clean_csv_lines(self, file_path: str) -> str:
        """
        Read TSV file line-by-line, remove trailing commas (,,,,,) and empty fields.
        Returns clean CSV string ready for pandas.
        """
        cleaned_lines: List[str] = []
        try:
            with open(file_path, 'r', encoding='utf-8', newline='') as f:
                for line_num, raw_line in enumerate(f, start=1):
                    line = raw_line.rstrip('\r\n')
                    if not line.strip():
                        continue  # skip empty lines

                    # Split on tab
                    fields = line.split('\t')

                    # Remove trailing empty fields caused by ,,,,, 
                    while fields and fields[-1].strip() == '':
                        fields.pop()

                    # Reconstruct clean line
                    cleaned_line = '\t'.join(fields)
                    cleaned_lines.append(cleaned_line)

            clean_csv = '\n'.join(cleaned_lines) + '\n'
            logger.debug(f"Cleaned {len(cleaned_lines)} lines from CSV")
            return clean_csv

        except Exception as e:
            logger.error(f"Failed to clean CSV lines at line {line_num}: {e}")
            raise

    def _safe_parse_money(self, value) -> Decimal:
        """Parse monetary values robustly: handles commas, spaces, junk."""
        if pd.isna(value) or value in ('', None):
            return Decimal('0.00')

        s = str(value).strip()

        # Remove all non-numeric except . and -
        s = ''.join(c for c in s if c in '0123456789.-')

        if not s or s in {'.', '-', '-.', '-.'}:
            return Decimal('0.00')

        try:
            return Decimal(s)
        except Exception:
            logger.warning(f"Failed to parse money: '{value}' → using 0.00")
            return Decimal('0.00')

    def parse_file(self, file_path: str) -> pd.DataFrame:
        """
        Parse gyno CSV with full production robustness.
        Handles: trailing commas, malformed rows, junk data.
        """
        if not file_path.endswith(('.csv', '.tsv', '.txt')):
            raise ValueError(f"Unsupported file type: {file_path}")

        try:
            logger.info(f"Starting parse of: {file_path}")

            # Step 1: Clean raw file (remove ,,,,,)
            clean_csv_text = self._clean_csv_lines(file_path)
            if not clean_csv_text.strip():
                logger.warning("CSV file is empty after cleaning")
                return pd.DataFrame(columns=self.required_columns)

            # Step 2: Parse with pandas
            df = pd.read_csv(
                StringIO(clean_csv_text),
                sep='\t',
                thousands=',',
                dtype=str,
                na_values=['', 'NA', 'nan'],
                keep_default_na=False,
                engine='python',
                quotechar='"',
                skipinitialspace=True,
                on_bad_lines='warn'  # Log malformed lines, don't crash
            )

            if df.empty:
                logger.warning("No data rows found after parsing")
                return pd.DataFrame(columns=self.required_columns)

            # Step 3: Clean column names
            original_columns = df.columns.tolist()
            df.columns = [
                col.strip().rstrip(',;').strip()
                for col in df.columns
            ]
            logger.debug(f"Cleaned columns: {original_columns} → {df.columns.tolist()}")

            # Step 4: Remap to standard names
            df = self._remap_columns(df)

            # Step 5: Ensure required columns
            df = self._ensure_required_columns(df)

            # Step 6: Clean and normalize data
            df = self._clean_data(df)

            # Step 7: Convert numeric fields
            if 'Unit Cost' in df.columns:
                df['Unit Cost'] = df['Unit Cost'].apply(self._safe_parse_money)

            if 'Total Amount' in df.columns:
                df['Total Amount'] = df['Total Amount'].apply(self._safe_parse_money)

            if 'Quantity' in df.columns:
                df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(1).astype(int)

            # Final validation
            if df.empty:
                logger.warning("DataFrame is empty after processing")
            else:
                logger.info(f"Successfully parsed {len(df)} rows")
                logger.debug(f"Sample row: {df.iloc[0].to_dict()}")

            return df

        except Exception as e:
            logger.error(f"CSV parsing failed: {e}", exc_info=True)
            raise RuntimeError(f"Failed to parse CSV: {e}") from e

    def _remap_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map messy column names to standardized ones."""
        rename_map: Dict[str, str] = {}
        for col in df.columns:
            col_lower = col.strip().lower()
            if col_lower in self.field_map:
                rename_map[col] = self.field_map[col_lower]

        if rename_map:
            df.rename(columns=rename_map, inplace=True)
            logger.debug(f"Column mapping applied: {rename_map}")
        else:
            logger.warning("No column mapping applied — check header format")

        return df

    def _ensure_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all required columns exist with safe defaults."""
        missing = set(self.required_columns) - set(df.columns)
        if missing:
            logger.warning(f"Missing columns: {missing} — adding with defaults")
            for col in missing:
                if col in ["Quantity"]:
                    df[col] = 1
                elif col in ["Unit Cost", "Total Amount"]:
                    df[col] = Decimal('0.00')
                else:
                    df[col] = ""

        # Reorder to match required order (optional, for consistency)
        df = df[self.required_columns]
        return df

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize all string/date fields."""
        # Date columns
        date_cols = ["Date of Visit", "Due Date", "Service Date"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=False)

        # String columns — strip, nullify NaN
        str_cols = [
            "Patient Name", "Product / Service", "Description",
            "Is Insurance?", "Mode of Payment", "Location", "Memo",
            "Terms of Payment", "Invoice No.", "Patient ID"
        ]
        for col in str_cols:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .replace({pd.NA: '', float('nan'): '', None: ''})
                    .astype(str)
                    .str.strip()
                )

        # Is Insurance? — standardize
        if "Is Insurance?" in df.columns:
            df["Is Insurance?"] = (
                df["Is Insurance?"]
                .str.lower()
                .str.strip()
                .replace({
                    '': 'no', 'no': 'no', 'n': 'no',
                    'yes': 'yes', 'y': 'yes', 'true': 'yes'
                })
                .str.capitalize()
                .fillna('No')
            )
        else:
            df["Is Insurance?"] = 'No'

        # Mode of Payment — clean trailing commas
        if "Mode of Payment" in df.columns:
            df["Mode of Payment"] = df["Mode of Payment"].str.rstrip(',').str.strip()

        # Description fallback for Consultation
        if "Product / Service" in df.columns and "Description" in df.columns:
            mask = (df["Product / Service"].str.strip() == "Consultation") & \
                   (df["Description"].str.strip() == "")
            df.loc[mask, "Description"] = "Consultation"

        # DO NOT recalculate Total Amount — preserve CSV value
        return df