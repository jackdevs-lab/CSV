import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

def setup_logger(name, log_level=logging.INFO):
    """Setup logger with consistent formatting"""
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    if not logger.handlers:
        # Create console handler
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        ch.setFormatter(formatter)
        
        logger.addHandler(ch)
    
    return logger

def move_file_to_processed(file_path):
    """Move processed file to processed directory"""
    return _move_file(file_path, 'processed')

def move_file_to_error(file_path):
    """Move error file to error directory"""
    return _move_file(file_path, 'error')

def _move_file(file_path, destination):
    """Move file to specified directory"""
    try:
        file_name = Path(file_path).name
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        new_name = f"{timestamp}_{file_name}"
        
        dest_dir = Path(f"data/{destination}")
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        dest_path = dest_dir / new_name
        shutil.move(file_path, dest_path)
        
        return dest_path
    except Exception as e:
        logging.error(f"Failed to move file {file_path}: {str(e)}")
        return None

def log_processing_result(file_path, results):
    """Log final processing results and move file accordingly"""
    total_rows = len(results)
    successful_rows = len([r for r in results if r['status'] == 'success'])
    failed_rows = total_rows - successful_rows
    
    logger = setup_logger(__name__)
    
    if failed_rows == 0:
        logger.info(f"Successfully processed {successful_rows}/{total_rows} rows")
        if file_path:
            move_file_to_processed(file_path)
    else:
        logger.error(f"Processing completed with errors: {successful_rows} successful, {failed_rows} failed")
        if file_path:
            move_file_to_error(file_path)
        
        # Log detailed errors
        for result in results:
            if result['status'] == 'error':
                logger.error(f"Invoice {result['invoice']}: {result['error']}")