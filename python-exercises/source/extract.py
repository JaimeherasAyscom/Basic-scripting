# extract.py

from source.logging import logger
from source.utils import csv_to_df

def read_csv(csv_file):
    try:
        with open(csv_file, "r") as f:
            content = f.read()
            logger.info(f"CSV file '{csv_file}' correctly read")
            return content
    except FileNotFoundError:
        logger.error(f"Error: The file '{csv_file}' was not found.")
    except PermissionError:
        logger.error(f"Error: Permission denied when trying to read '{csv_file}'.")
    except Exception as e:
        logger.error(f"Unexpected error reading '{csv_file}': {e}")

def read_csv_with_pd(csv_file):
    return csv_to_df(csv_file)
 