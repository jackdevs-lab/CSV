# src/logger.py
import logging
import shutil
from datetime import datetime
from pathlib import Path


def setup_logger(name, log_level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        ch.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(ch)
    return logger


def _move_file(file_path, destination):
    try:
        name = Path(file_path).name
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        dest_dir = Path(f"data/{destination}")
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / f"{timestamp}_{name}"
        shutil.move(file_path, dest)
        return dest
    except Exception as e:
        logging.error(f"Failed to move {file_path}: {e}")
        return None


def log_processing_result(upload_id, results, *, archive_files=False, file_path=None):
    """
    Persist per-invoice results to Postgres. This replaces the old file-move
    audit trail. If `archive_files=True` AND `file_path` is provided, also
    moves the temp file to data/processed or data/error (local dev only).
    """
    total = len(results)
    success = len([r for r in results if r["status"] == "success"])
    failed = total - success
    log = setup_logger(__name__)

    try:
        import db
        for r in results:
            db.record_invoice(
                upload_id=upload_id,
                invoice_number=str(r["invoice"]),
                transaction_type=r.get("type", "sales_receipt"),
                status=r["status"],
                qb_id=r.get("transaction_id"),
                error_message=r.get("error"),
            )
        status = "success" if failed == 0 else ("partial" if success else "failed")
        db.finalize_upload(upload_id, status=status, total_invoices=total,
                           succeeded=success, failed=failed, log=None)
    except Exception as e:
        log.error("Failed to persist processing result: %s", e, exc_info=True)

    if archive_files and file_path:
        (_move_file(file_path, "processed") if failed == 0
         else _move_file(file_path, "error"))

    if failed == 0:
        log.info("Processed %d/%d invoices", success, total)
    else:
        log.warning("Finished with %d ok, %d failed", success, failed)