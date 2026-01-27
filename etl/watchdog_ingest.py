import os
import logging
import shutil
from datetime import datetime
from typing import Optional

import pandas as pd
from watchdog.events import FileSystemEventHandler
from sqlalchemy.engine import Engine

from db.database import get_engine



# CONFIG

RAW_DATA_DIR = "raw_data"
PROCESSED_DIR = os.path.join(RAW_DATA_DIR, "processed_files")
STAGING_SCHEMA = "sales_staging"

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "ingestion.log")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)



# LOGGING SETUP

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)



# WATCHDOG HANDLER

class RawDataHandler(FileSystemEventHandler):
    """
    Watches raw_data directory and ingests new CSV files
    into PostgreSQL sales_staging schema.
    After successful ingestion, moves files to processed_files.
    """

    def __init__(self, engine: Engine):
        self.engine = engine

    def on_created(self, event) -> None:
        """
        Triggered when a new file is created in raw_data.
        """
        if event.is_directory:
            return

        if event.src_path.endswith(".csv"):
            logger.info(f"New file detected: {event.src_path}")
            self._process_file(event.src_path)

    def _process_file(self, file_path: str) -> None:
        """
        Process and ingest a single CSV file.
        """
        file_name = os.path.basename(file_path).lower()
        ingest_date = datetime.utcnow()

        try:
            logger.info(f"Starting ingestion for {file_name}")

            df = pd.read_csv(file_path)
            df["ingest_date"] = ingest_date

            table_name = self._resolve_table(file_name)

            if not table_name:
                logger.warning(f"Unknown file type. Skipping: {file_name}")
                return

            df.to_sql(
                name=table_name,
                schema=STAGING_SCHEMA,
                con=self.engine,
                if_exists="append",
                index=False
            )

            logger.info(
                f"Successfully ingested {file_name} "
                f"into {STAGING_SCHEMA}.{table_name}"
            )

            # Move file only after successful ingestion
            self._move_to_processed(file_path)

        except Exception:
            logger.error(
                f"Failed to ingest file: {file_name}",
                exc_info=True
            )

    def _move_to_processed(self, file_path: str) -> None:
        """
        Move processed file to processed_files directory.
        """
        try:
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(PROCESSED_DIR, file_name)

            shutil.move(file_path, destination_path)

            logger.info(
                f"Moved file to processed_files: {destination_path}"
            )

        except Exception:
            logger.error(
                f"Failed to move file to processed_files: {file_path}",
                exc_info=True
            )

    @staticmethod
    def _resolve_table(file_name: str) -> Optional[str]:
        """
        Resolve target staging table based on file name.
        """
        if "customer" in file_name:
            return "customers_stage"
        if "transaction" in file_name:
            return "sales_transactions_stage"
        if "product" in file_name:
            return "products_stage"
        return None
