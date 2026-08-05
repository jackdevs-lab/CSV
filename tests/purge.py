import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# 1. Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("purge_transactions")

# 2. Resolve project root (since the file is in tests/, we go up one level to the root)
BASE_DIR = Path(__file__).resolve().parent.parent

# 3. Explicitly load the .env file from the root directory
env_path = BASE_DIR / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    logger.info(f"Loaded environment variables from {env_path}")
else:
    logger.error(f"Could not find .env file at {env_path}. Make sure it exists!")
    sys.exit(1)

# 4. Ensure the root directory is in the system path so 'src' can be found
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# 5. Import directly from the src folder
try:
    from src.qb_auth import QuickBooksAuth
    from src.qb_client import QuickBooksClient
except ModuleNotFoundError as e:
    logger.error(f"Import failed: {e}. Make sure 'src' is in the root directory.")
    sys.exit(1)


def purge_entity(qb_client: QuickBooksClient, entity_name: str):
    """
    Queries and deletes all existing records for a specified QBO entity ('Invoice' or 'SalesReceipt')
    using SELECT * and robust pagination.
    """
    logger.info(f"=== Starting purge for entity: {entity_name} ===")
    
    total_deleted = 0
    total_failed = 0
    start_position = 1
    batch_size = 100
    
    while True:
        # Use SELECT * as required by QuickBooks API for reliable querying
        query_sql = f"SELECT * FROM {entity_name} STARTPOSITION {start_position} MAXRESULTS {batch_size}"
        response = qb_client.query(query_sql)
        
        # Extract records safely 
        records = response.get(entity_name, [])
        
        if not records:
            logger.info(f"No remaining {entity_name} records found at position {start_position}.")
            break
            
        logger.info(f"Processing batch of {len(records)} {entity_name} records (Starting at position {start_position})...")
        
        for record in records:
            rec_id = str(record.get("Id"))
            sync_token = str(record.get("SyncToken", "0"))
            doc_num = record.get("DocNumber", "N/A")
            
            # The payload MUST NOT be wrapped in {"Invoice": {...}}
            payload = {
                "Id": rec_id,
                "SyncToken": sync_token
            }
            
            endpoint = entity_name.lower()
            
            try:
                res = qb_client._make_request(
                    method="POST",
                    endpoint=endpoint,
                    data=payload,  # <-- FIXED: Pass payload directly
                    params={"operation": "delete"},
                    raise_on_error=False
                )
                
                if isinstance(res, dict) and "Fault" in res:
                    err_msg = res["Fault"]["Error"][0].get("Message", "Unknown Error")
                    logger.warning(f"Failed to delete {entity_name} #{doc_num} (ID: {rec_id}): {err_msg}")
                    total_failed += 1
                else:
                    logger.info(f"Successfully deleted {entity_name} #{doc_num} (ID: {rec_id})")
                    total_deleted += 1
                    
            except Exception as e:
                logger.error(f"Exception while deleting {entity_name} #{doc_num} (ID: {rec_id}): {e}")
                total_failed += 1

        # Optimization: If the API returned fewer records than our batch size, we've hit the end.
        if len(records) < batch_size:
            break
            
        # Only increment start_position if we haven't broken out of the loop
        start_position += len(records)

    logger.info(f"=== Summary for {entity_name}: Deleted={total_deleted}, Failed={total_failed} ===")


def main():
    try:
        qb_auth = QuickBooksAuth()
        qb_client = QuickBooksClient(qb_auth)
        
        # Sequentially purge all Invoices and Sales Receipts
        purge_entity(qb_client, "Invoice")
        purge_entity(qb_client, "SalesReceipt")
        
        logger.info("Purge process completed successfully.")
        
    except Exception as e:
        logger.error(f"Purge execution aborted: {e}", exc_info=True)


if __name__ == "__main__":
    main()