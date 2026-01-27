import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # -------------------------
    # EMAIL
    # -------------------------
    df["email"] = (
        df["email"]
        .fillna("unknown@example.com")
        .str.lower()
    )

    # -------------------------
    # CITY (mode)
    # -------------------------
    if df["city"].notna().any():
        city_mode = df["city"].mode(dropna=True)[0]
        df["city"] = df["city"].fillna(city_mode)

    # -------------------------
    # STATE (mode + uppercase)
    # -------------------------
    if df["state"].notna().any():
        state_mode = df["state"].mode(dropna=True)[0]
        df["state"] = (
            df["state"]
            .fillna(state_mode)
            .str.upper()
        )

    # -------------------------
    # SIGNUP DATE
    # -------------------------
    today = pd.Timestamp.utcnow().date()
    df["signup_date"] = df["signup_date"].fillna(today)

    logger.info("Customer transformation completed")

    return df
