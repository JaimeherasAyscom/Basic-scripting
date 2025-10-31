# logging_config.py
import logging
from logging.handlers import RotatingFileHandler

# Create logger
logger = logging.getLogger("my_app")
logger.setLevel(logging.DEBUG)  # Minimum level of messages to log

# Define log message format
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# Rotating file handler (keeps up to 3 files, max 5 MB each)
file_handler = RotatingFileHandler(
    "log.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8"
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# Avoid adding duplicate handlers if this module is imported multiple times
if not logger.handlers:
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
 