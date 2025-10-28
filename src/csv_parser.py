import pandas as pd
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
            'product / service': 'Product / Service',
            'description': 'Description',
            'is insurance?': 'Is Insurance?',
            'mode of payment': 'Mode of Payment',
            'quantity': 'Quantity',
            'unit cost': 'Unit Cost',
            'total amount': 'Total Amount',
            'service date': 'Service Date'
        }

    def parse_file(self, file_path):
        try:
            # Try reading with automatic delimiter detection
            try:
                df = pd.read_csv(file_path, sep=None, engine="python")
            except Exception as e1:
                logger.warning(f"Auto delimiter detection failed: {e1}. Trying tab separator...")
                df = pd.read_csv(file_path, sep="\t", engine="python")

            logger.debug(f"Raw CSV columns: {df.columns.tolist()}")

            # Clean stray commas or whitespace in headers
            df.columns = df.columns.str.strip().str.replace(r'[,;]+$', '', regex=True)

            df = self._remap_columns(df)
            df = self._ensure_required_columns(df)
            df = self._clean_data(df)

            logger.info(f"Successfully parsed CSV with {len(df)} rows")
            return df

        except pd.errors.ParserError as e:
            logger.error(f"Failed to parse CSV file {file_path} due to delimiter or quote mismatch: {e}")
            raise ValueError("CSV format issue — check for mixed delimiters or missing quotes.") from e
        except Exception as e:
            logger.error(f"Failed to parse CSV file {file_path}: {str(e)}")
            raise


    def _remap_columns(self, df):
        rename_map = {}
        for col in df.columns:
            col_clean = col.strip().lower()
            if col_clean in self.field_map:
                rename_map[col] = self.field_map[col_clean]
        df.rename(columns=rename_map, inplace=True)
        logger.debug(f"Applied column mapping: {rename_map}")
        return df

    def _ensure_required_columns(self, df):
        missing_columns = set(self.required_columns) - set(df.columns)
        if missing_columns:
            logger.warning(f"CSV is missing columns {missing_columns}, filling with defaults")
            for col in missing_columns:
                if col in ["Quantity", "Unit Cost", "Total Amount"]:
                    df[col] = 0
                else:
                    df[col] = ""
        else:
            logger.debug("All required columns present")
        return df

    def _clean_data(self, df):
        if "Date of Visit" in df.columns:
            df["Date of Visit"] = pd.to_datetime(df["Date of Visit"], errors="coerce")
        if "Service Date" in df.columns:
            df["Service Date"] = pd.to_datetime(df["Service Date"], errors="coerce")

        for col in ["Quantity", "Unit Cost", "Total Amount"]:
            if col in df.columns:
                # Remove thousands separators and convert to numeric, handling invalid values
                df[col] = df[col].replace(r'[^\d.]', '', regex=True)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        for col in ["Patient Name", "Product / Service", "Description", "Is Insurance?", "Mode of Payment"]:
            if col in df.columns:
                df[col] = df[col].replace({pd.NA: '', pd.NaT: '', float('nan'): ''}).astype(str).str.strip()

        if "Is Insurance?" in df.columns:
            df["Is Insurance?"] = df["Is Insurance?"].replace({'': 'No', 'nan': 'No', 'NaN': 'No'}).str.capitalize()
        else:
            df["Is Insurance?"] = 'No'

        if "Mode of Payment" in df.columns:
            df["Mode of Payment"] = df["Mode of Payment"].str.rstrip(',')
        else:
            df["Mode of Payment"] = ""

        # Set Description to "Consultation" if empty and Product / Service is "Consultation"
        df.loc[(df['Product / Service'] == 'Consultation') & (df['Description'] == ''), 'Description'] = 'Consultation'

        df['Total Amount'] = df['Quantity'] * df['Unit Cost']
        
        zero_rows = df[df['Total Amount'] == 0]
        if not zero_rows.empty:
            logger.warning(f"Zero-amount rows detected (will be skipped): {zero_rows[['Invoice No.', 'Product / Service', 'Description']].to_dict(orient='records')}")
        
        mismatched_rows = df[abs(df['Total Amount'] - (df['Quantity'] * df['Unit Cost'])) > 0.01]
        if not mismatched_rows.empty:
            logger.warning(f"Mismatched amounts in rows: {mismatched_rows[['Invoice No.', 'Product / Service', 'Quantity', 'Unit Cost', 'Total Amount']].to_dict(orient='records')}")
        
        return df