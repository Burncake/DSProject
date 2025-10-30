import logging
import os

def setup_logger(name='chatapp'):
    """Set up a logger with console and file output.
    
    Creates a logger that writes:
    - INFO and above to console
    - DEBUG and above to file (logs/server.log)
    
    Args:
        name (str, optional): Logger name. Defaults to 'chatapp'
        
    Returns:
        logging.Logger: Configured logger instance
        
    Side Effects:
        - Creates logs directory if it doesn't exist
        - Creates/appends to server.log file
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Create formatters and handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # File handler - ensure log directory exists
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.FileHandler(os.path.join(log_dir, 'server.log'))
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
