# etl/product_dedup.py

import os
import logging
from datetime import datetime
from typing import Tuple, Set

import pandas as pd
import numpy as np


# =========================
# CONFIG
# =========================
REJECT_DIR = "rejected_data/product_duplicates"
os.makedirs(REJECT_DIR, exist_ok=True)

logger = logging.getLogger(__name__)


# =========================
# INVALID NAME RULES
# =========================
INVALID_TOKENS: Set[str] = {
    "the", "and", "or", "but", "if", "then",
    "push", "since", "try", "stay", "door",
    "fear", "oil", "half", "fire", "hard",
    "every", "money", "edge", "fund", "light"
}


def is_valid_product_name(name: str) -> bool:
    """
    Validate product_name.
    """
    if not isinstance(name, str):
        return False

    clean = name.strip().lower()

    if len(clean) < 4:
        return False

    if clean in INVALID_TOKENS:
        return False

    if clean.isnumeric():
        return False

    return True


# =========================
# PRODUCT DEDUPLICATION
# =========================
def resolve_duplicate_products(
    products_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Resolve duplicate product_id records.

    Policy:
    - If ALL rows for a product_id have invalid product_name → reject all
    - Else keep the best valid record:
        1. Valid product_name
        2. Valid unit_price (>0)
        3. Non-null category
        4. Non-null brand
    """

    df = products_df.copy()

    # Normalize name
    df["product_name"] = df["product_name"].astype(str).str.strip()

    # Validate name
    df["is_valid_name"] = df["product_name"].apply(is_valid_product_name)

    clean_rows = []
    rejected_rows = []

    for product_id, group in df.groupby("product_id"):
        valid_names = group[group["is_valid_name"]]

        # Case 1: No valid names → reject entire group
        if valid_names.empty:
            rejected_rows.append(
                group.assign(reject_reason="INVALID_PRODUCT_NAME")
            )
            logger.error(
                "Rejected product_id %s: no valid product_name",
                product_id
            )
            continue

        # Case 2: Rank valid records
        ranked = (
            valid_names.assign(
                valid_price=lambda x: x["unit_price"].fillna(0) > 0,
                has_category=lambda x: x["category"].notna(),
                has_brand=lambda x: x["brand"].notna()
            )
            .sort_values(
                by=[
                    "valid_price",
                    "has_category",
                    "has_brand",
                    "unit_price"
                ],
                ascending=False
            )
        )

        keeper = ranked.iloc[[0]]
        duplicates = group.drop(index=keeper.index)

        clean_rows.append(keeper)

        if not duplicates.empty:
            rejected_rows.append(
                duplicates.assign(reject_reason="DUPLICATE_PRODUCT_ID")
            )

            logger.warning(
                "Product deduplication applied for product_id %s: %d rows rejected",
                product_id,
                len(duplicates)
            )

    clean_df = pd.concat(clean_rows, ignore_index=True)
    rejected_df = (
        pd.concat(rejected_rows, ignore_index=True)
        if rejected_rows else pd.DataFrame()
    )

    return clean_df, rejected_df


# =========================
# SAVE REJECTED PRODUCTS
# =========================
def save_rejected_product_duplicates(
    rejected_df: pd.DataFrame
) -> None:
    """
    Save rejected product duplicate records to CSV.
    """
    if rejected_df.empty:
        return

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(
        REJECT_DIR,
        f"rejected_product_duplicates_{ts}.csv"
    )

    rejected_df.to_csv(path, index=False)

    logger.error(
        "Saved %d rejected product records to %s",
        len(rejected_df),
        path
    )
