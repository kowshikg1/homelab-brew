import logging
from pathlib import Path

def get_logger(name: str, log_file: str = None) -> logging.Logger:
    """Get a logger with the specified name.
    
    Args:
        name: Logger name (usually __name__ or module name)
        log_file: Optional path to log file. If provided, logs will be written to both console and file.
                 If None, logs only go to console.
    
    Returns:
        Configured logger instance
    
    Example:
        # Console only
        log = get_logger(__name__)
        
        # Console + file
        log = get_logger(__name__, "logs/app.log")
    """
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_path)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
    
    return logger