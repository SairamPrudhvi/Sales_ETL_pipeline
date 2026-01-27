import os
import logging
from datetime import datetime
from typing import Tuple

import pandas as pd


# =========================
# CONFIG
# =========================
DUPLICATE_DIR = "rejected_data/customer_duplicates"
os.makedirs(DUPLICATE_DIR, exist_ok=True)

# LOG_DIR = "logs"
# LOG_FILE = os.path.join(LOG_DIR, "duplicate_data.log")

# logging.basicConfig(
#     filename=LOG_FILE,
#     level=logging.ERROR,
#     format="%(asctime)s | %(levelname)s | %(message)s"
# )

logger = logging.getLogger(__name__)


# =========================
# DEDUPLICATION LOGIC
# =========================
def resolve_duplicate_customers(
    customers_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Resolve duplicate customer_id records.

    Policy:
    - Keep the record with the LATEST valid signup_date
    - Quarantine all other records with the same customer_id

    Returns:
        clean_customers_df: Deduplicated customers
        rejected_duplicates_df: Quarantined duplicate records
    """

    # Ensure signup_date is datetime
    customers_df["signup_date"] = pd.to_datetime(
        customers_df["signup_date"], errors="coerce"
    )

    # Sort so latest signup_date wins
    sorted_df = customers_df.sort_values(
        by=["customer_id", "signup_date"],
        ascending=[True, False]
    )

    # Keep canonical record
    clean_customers_df = (
        sorted_df
        .drop_duplicates(subset=["customer_id"], keep="first")
        .reset_index(drop=True)
    )

    # Quarantine duplicates
    rejected_duplicates_df = (
        sorted_df[sorted_df.duplicated(subset=["customer_id"], keep="first")]
        .reset_index(drop=True)
    )

    # Logging
    if not rejected_duplicates_df.empty:
        logger.warning(
            "Customer deduplication applied: %d duplicate records rejected "
            "across %d customer_ids",
            len(rejected_duplicates_df),
            rejected_duplicates_df["customer_id"].nunique()
        )
    else:
        logger.info("No duplicate customer_id records found.")

    return clean_customers_df, rejected_duplicates_df


# =========================
# SAVE REJECTED DUPLICATES
# =========================
def save_rejected_customer_duplicates(
    rejected_df: pd.DataFrame
) -> None:
    """
    Save rejected duplicate customer records to CSV
    under rejected_data/customer_duplicates/.
    """

    if rejected_df.empty:
        return

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_name = f"rejected_customer_duplicates_{timestamp}.csv"
    file_path = os.path.join(DUPLICATE_DIR, file_name)

    rejected_df.to_csv(file_path, index=False)

    logger.warning(
        "Saved %d duplicate customer records to %s",
        len(rejected_df),
        file_path
    )
