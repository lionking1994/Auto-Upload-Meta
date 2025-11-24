"""
Logging configuration module
"""

import logging
import sys
from pathlib import Path
from datetime import datetime


def setup_logging(log_level=logging.INFO, log_file_prefix="meta_audience_upload"):
    """
    Set up logging configuration for the application
    
    Args:
        log_level: Logging level (default: INFO)
        log_file_prefix: Prefix for log file name
    
    Returns:
        Path to the log file
    """
    # Create logs directory if it doesn't exist
    log_dir = Path(__file__).parent / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    # Create log file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"{log_file_prefix}_{timestamp}.log"
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Reduce noise from external libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('snowflake').setLevel(logging.WARNING)
    
    return log_file
