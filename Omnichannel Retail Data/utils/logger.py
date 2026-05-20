import logging
import os 
from datetime import datetime

def setup_logger(
    module_name: str,
    log_folder: str = "logs",
    log_filename: str = "pipeline.log"
) -> logging.Logger:
    """
    Set up centralized logger (file + console).

    Args:
        module_name (str): Module name (__name__)
        log_folder (str): Folder to store logs
        log_filename (str): Base log file name

    Returns: 
        logging.Logger: A configured logger instance.
    """

    # Create log folder
    try: 
        os.makedirs(log_folder, exist_ok = True)
    except OSError as e: 
        logging.error(f"Failed to create log folder: {e}")

    # Log format
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )

    # Logger instance
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:

        # Log by day
        today = datetime.now().strftime("%Y-%m-%d")
        file_path = os.path.join(log_folder, f"{today}_{log_filename}")

        # File handler
        file_handler = logging.FileHandler(file_path, encoding = "utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console handlder
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger
