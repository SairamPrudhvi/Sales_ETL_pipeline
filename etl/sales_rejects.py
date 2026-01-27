# etl/sales_rejects.py

import os
import logging
from datetime import datetime
from typing import Tuple
import pandas as pd

# =========================
# CONFIG
# =========================
REJECT_DIR = "rejected_data/sales_corrupt"
os.makedirs(REJECT_DIR, exist_ok=True)

logger = logging.getLogger(__name__)


# =========================
# DETECT CORRUPT SALES
# =========================
def detect_corrupt_transactions(
    sales_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Detect corrupt sales transactions.

    Corrupt rules:
    - transaction_date is NULL / NaT
    - quantity <= 0
    - unit_price <= 0
    """

    corrupt_mask = (
        sales_df["transaction_date"].isna()
        | (sales_df["quantity"] <= 0)
        | (sales_df["unit_price"] <= 0)
    )

    rejected_df = sales_df[corrupt_mask].reset_index(drop=True)
    clean_df = sales_df[~corrupt_mask].reset_index(drop=True)

    if not rejected_df.empty:
        logger.error(
            "Detected %d corrupt sales transactions",
            len(rejected_df)
        )

    return clean_df, rejected_df


# =========================
# SAVE REJECTED SALES
# =========================
def save_rejected_sales_transactions(
    rejected_df: pd.DataFrame
) -> None:
    """
    Save rejected sales transactions to CSV.
    """

    if rejected_df.empty:
        return

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(
        REJECT_DIR,
        f"rejected_sales_transactions_{ts}.csv"
    )

    rejected_df.to_csv(path, index=False)

    logger.error(
        "Saved %d rejected sales transactions to %s",
        len(rejected_df),
        path
    )
