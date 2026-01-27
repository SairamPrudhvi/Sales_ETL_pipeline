"""
Validation utilities for Sales ETL pipeline.

Responsibilities:
- Schema validation
- Data type enforcement
- Missing & invalid value handling
- Orphan transaction detection
- Error logging & exception handling
"""

import logging
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import os

# =========================
# LOGGING CONFIG
# =========================
# LOG_DIR = "logs"
# LOG_FILE = os.path.join(LOG_DIR, "etl_errors.log")

# os.makedirs(LOG_DIR, exist_ok=True)

# logging.basicConfig(
#     filename=LOG_FILE,
#     level=logging.ERROR,
#     format="%(asctime)s | %(levelname)s | %(message)s"
# )

logger = logging.getLogger(__name__)

# =========================
# SALES DTYPES
# =========================
def enforce_sales_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df["transaction_id"] = df["transaction_id"].astype(int)
        df["customer_id"] = df["customer_id"].astype(int)
        df["product_id"] = df["product_id"].astype(int)
        df["quantity"] = df["quantity"].astype(int)
        df["unit_price"] = df["unit_price"].astype(float)
        df["discount"] = df["discount"].astype(float)
        df["transaction_date"] = pd.to_datetime(
            df["transaction_date"], errors="coerce"
        ).dt.date
        df["ingest_date"] = pd.to_datetime(df["ingest_date"])
    except Exception as exc:
        raise ValueError("Sales dtype enforcement failed") from exc

    return df


# =========================
# CUSTOMERS DTYPES
# =========================
def enforce_customer_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforce final customer table dtypes.
    """
    try:
        df["customer_id"] = df["customer_id"].astype(int)

        df["customer_name"] = df["customer_name"].astype("string")
        df["email"] = df["email"].astype("string")
        df["city"] = df["city"].astype("string")
        df["state"] = df["state"].astype("string")

        df["signup_date"] = pd.to_datetime(
            df["signup_date"], errors="coerce"
        ).dt.date

        df["ingest_date"] = pd.to_datetime(df["ingest_date"])

    except Exception as exc:
        logger.error(
            "Failed to enforce customer dtypes",
            exc_info=True
        )
        raise ValueError(
            "Customer dtype enforcement failed"
        ) from exc

    return df



# =========================
# PRODUCTS DTYPES
# =========================
def enforce_product_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df["product_id"] = df["product_id"].astype(int)

        string_cols = [
            "product_name",
            "category",
            "brand"
        ]
        for col in string_cols:
            df[col] = (
                df[col]
                .astype("string")
                .str.strip()
            )

        df["unit_price"] = df["unit_price"].astype(float)
        df["ingest_date"] = pd.to_datetime(df["ingest_date"])
    except Exception as exc:
        raise ValueError("Product dtype enforcement failed") from exc

    return df


# =========================
# SCHEMA VALIDATION
# =========================
def validate_schema(
    df: pd.DataFrame,
    expected_schema: Dict[str, str],
    table_name: str
) -> None:
    """
    Validate dataframe schema structure and column dtypes.
    Fails fast on missing columns, logs soft dtype mismatches.
    """

    actual_columns = set(df.columns)
    expected_columns = set(expected_schema.keys())

    # -------------------------
    # Missing columns (HARD FAIL)
    # -------------------------
    missing_cols = expected_columns - actual_columns
    if missing_cols:
        logger.error(
            f"{table_name}: Missing required columns {missing_cols}"
        )
        raise ValueError(
            f"{table_name}: Missing required columns {missing_cols}"
        )

    # -------------------------
    # Extra columns (SOFT LOG)
    # -------------------------
    extra_cols = actual_columns - expected_columns
    if extra_cols:
        logger.warning(
            f"{table_name}: Unexpected columns {extra_cols}"
        )

    # -------------------------
    # Dtype validation (SOFT)
    # -------------------------
    for col, expected_dtype in expected_schema.items():
        actual_dtype = str(df[col].dtype)

        # TEXT columns
        if expected_dtype == "object":
            if actual_dtype not in ("object", "string"):
                logger.warning(
                    f"{table_name}: Column '{col}' dtype "
                    f"{actual_dtype}, expected text"
                )

        # DATETIME columns
        elif expected_dtype.startswith("datetime"):
            if not actual_dtype.startswith("datetime64"):
                logger.warning(
                    f"{table_name}: Column '{col}' dtype "
                    f"{actual_dtype}, expected datetime"
                )

        # NUMERIC columns
        else:
            if actual_dtype != expected_dtype:
                logger.warning(
                    f"{table_name}: Column '{col}' dtype "
                    f"{actual_dtype}, expected {expected_dtype}"
                )



# =========================
# EMPTY STRING HANDLING
# =========================
def normalize_empty_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace empty or whitespace-only strings with NaN.
    """
    return df.replace(r"^\s*$", pd.NA, regex=True)


def impute_transaction_dates(
    sales_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    date_col: str = "transaction_date"
) -> pd.DataFrame:
    """
    Impute missing transaction dates:
    - Use earliest customer signup_date if available
    - Else use current date
    """

    # Ensure datetime
    sales_df[date_col] = pd.to_datetime(sales_df[date_col], errors="coerce")
    customers_df["signup_date"] = pd.to_datetime(
        customers_df["signup_date"], errors="coerce"
    )

    # FIX: make customer_id unique using earliest signup_date
    signup_map = (
        customers_df
        .dropna(subset=["signup_date"])
        .sort_values("signup_date")
        .drop_duplicates("customer_id", keep="first")
        .set_index("customer_id")["signup_date"]
    )

    # Fill missing transaction dates using signup_date
    missing_mask = sales_df[date_col].isna()
    sales_df.loc[missing_mask, date_col] = (
        sales_df.loc[missing_mask, "customer_id"]
        .map(signup_map)
    )

    # Final fallback → current date
    sales_df[date_col] = sales_df[date_col].fillna(
        pd.Timestamp.utcnow().normalize()
    )

    return sales_df


# =========================
# DATA TYPE ENFORCEMENT
# =========================
def enforce_column_types(
    df: pd.DataFrame,
    type_map: Dict[str, str],
    table_name: str
) -> pd.DataFrame:
    """
    Enforce column data types strictly.
    """
    for col, dtype in type_map.items():
        try:
            if dtype == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype)
        except Exception as exc:
            logger.error(
                f"{table_name}: Failed casting column '{col}' to {dtype}",
                exc_info=True
            )
            raise ValueError(
                f"{table_name}: Invalid data type in column '{col}'"
            ) from exc

    return df


# =========================
# NUMERIC CLEANING
# =========================
def clean_numeric_fields(
    df: pd.DataFrame,
    quantity_col: str = "quantity",
    price_col: str = "unit_price"
) -> pd.DataFrame:
    """
    Apply deterministic numeric corrections.
    """
    # Quantity
    df[quantity_col] = (
        df[quantity_col]
        .fillna(0)
        .abs()
        .astype(int)
    )

    # Unit price
    df[price_col] = (
        df[price_col]
        .abs()
        .replace(0, np.nan)
        .fillna(np.nanmedian(df[price_col]))
    )

    return df

def clean_product_numeric_fields(
    df: pd.DataFrame,
    price_col: str = "unit_price"
) -> pd.DataFrame:
    """
    Clean numeric fields for product table.

    Rules:
    - Negative prices → abs()
    - Zero prices → treated as missing
    - Missing prices → imputed with median
    """

    df[price_col] = (
        df[price_col]
        .abs()
        .replace(0, np.nan)
    )

    median_price = np.nanmedian(df[price_col])

    df[price_col] = df[price_col].fillna(median_price)

    return df


# =========================
# DATE VALIDATION
# =========================
def validate_transaction_dates(
    df: pd.DataFrame,
    date_col: str = "transaction_date"
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate transaction dates.

    Returns:
        clean_df, rejected_df
    """
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    invalid_mask = df[date_col].isna()
    rejected = df[invalid_mask]
    clean = df[~invalid_mask]

    if not rejected.empty:
        logger.error(
            f"Rejected {len(rejected)} rows due to invalid dates"
        )

    return clean, rejected


# =========================
# CORRUPT TRANSACTION DETECTION
# =========================
def detect_corrupt_transactions(
    sales_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Detect corrupt transactions where a single transaction_id
    maps to multiple customer_ids.

    Rule:
    - A transaction_id MUST belong to exactly one customer_id
    - Violations are quarantined entirely (all rows for that transaction_id)

    Returns:
        clean_sales_df
        rejected_corrupt_df
    """

    txn_customer_counts = (
        sales_df
        .groupby("transaction_id")["customer_id"]
        .nunique()
        .reset_index(name="customer_variants")
    )

    corrupt_txn_ids = txn_customer_counts.loc[
        txn_customer_counts["customer_variants"] > 1,
        "transaction_id"
    ]

    rejected = sales_df[
        sales_df["transaction_id"].isin(corrupt_txn_ids)
    ]

    clean = sales_df[
        ~sales_df["transaction_id"].isin(corrupt_txn_ids)
    ]

    if not rejected.empty:
        logger.error(
            "Rejected %d corrupt sales rows across %d transaction_ids "
            "(multiple customers per transaction)",
            len(rejected),
            rejected["transaction_id"].nunique()
        )

    return clean, rejected



# =========================
# ORPHAN DETECTION
# =========================
def detect_orphan_transactions(
    sales_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Detect orphan sales transactions.
    """
    orphan_mask = (
        ~sales_df["customer_id"].isin(customers_df["customer_id"]) |
        ~sales_df["product_id"].isin(products_df["product_id"])
    )

    rejected = sales_df[orphan_mask]
    clean = sales_df[~orphan_mask]

    if not rejected.empty:
        logger.error(
            f"Rejected {len(rejected)} orphan transactions"
        )

    return clean, rejected
