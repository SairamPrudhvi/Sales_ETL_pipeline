import pandas as pd
import logging

logger = logging.getLogger(__name__)

def transform_sales(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Ensure datetime.
    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"],
        errors="raise"   # safe now because invalid dates are already rejected
    )

    df["total_sale_amount"] = (
        df["quantity"] * df["unit_price"]
    ).round(2).astype(float)

    df["discount_amount"] = df["discount"].fillna(0)

    df["net_sale_amount"] = (
        df["total_sale_amount"] - df["discount_amount"]
    ).round(2).astype(float)

    df["order_year"] = df["transaction_date"].dt.year
    df["order_month"] = df["transaction_date"].dt.month
    df["order_day"] = df["transaction_date"].dt.day

    logger.info("Sales transformation completed")
    return df
