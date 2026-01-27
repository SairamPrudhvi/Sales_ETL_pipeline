import os
import logging

# =========================
# CONFIG
# =========================
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "etl_errors.log")

os.makedirs(LOG_DIR, exist_ok=True)


def setup_logging() -> None:
    """
    Configure global logging for ETL pipeline.
    Call this ONCE at application startup.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )
