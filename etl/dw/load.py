import logging
import pandas as pd
from sqlalchemy import inspect
from etl.dw.models import Base

logger = logging.getLogger(__name__)

def create_dw_tables(engine):
    inspector = inspect(engine)
    if "sales_dw" not in inspector.get_schema_names():
        engine.execute("CREATE SCHEMA IF NOT EXISTS sales_dw")

    Base.metadata.create_all(engine)
    logger.info("DW tables created.")

def load_dimension(engine, df: pd.DataFrame, table_name: str, pk: str):
    df = df.copy()

    # Drop ETL-only / validation columns
    drop_cols = {
        "ingest_date",
        "is_valid_name",
        "valid_price",
        "has_category",
        "has_brand"
    }

    df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)

    existing = pd.read_sql(
        f"SELECT {pk} FROM sales_dw.{table_name}",
        engine
    )

    delta = df[~df[pk].isin(existing[pk])]

    if delta.empty:
        logger.info("No new rows for %s", table_name)
        return

    delta.to_sql(
        table_name,
        engine,
        schema="sales_dw",
        if_exists="append",
        index=False,
        method="multi"
    )

    logger.info("Loaded %d new rows into %s", len(delta), table_name)


def load_fact(engine, df: pd.DataFrame):
    df = df.copy()

    # -----------------------------
    # 1. Keep ONLY fact columns
    # -----------------------------
    fact_columns = [
        "customer_id",
        "product_id",
        "date_id",
        "quantity",
        "unit_price",
        "total_sale_amount",
        "net_sale_amount",
    ]

    df = df[fact_columns]

    # -----------------------------
    # 2. Deduplicate at FACT GRAIN
    # -----------------------------
    existing = pd.read_sql(
        """
        SELECT
            customer_id,
            product_id,
            date_id
        FROM sales_dw.fact_sales
        """,
        engine
    )

    if not existing.empty:
        df = df.merge(
            existing,
            on=["customer_id", "product_id", "date_id"],
            how="left",
            indicator=True
        )
        df = df[df["_merge"] == "left_only"].drop(columns="_merge")

    if df.empty:
        logger.info("No new fact records to load")
        return

    # -----------------------------
    # 3. Load facts
    # -----------------------------
    df.to_sql(
        "fact_sales",
        engine,
        schema="sales_dw",
        if_exists="append",
        index=False,
        method="multi"
    )

    logger.info("Loaded %d fact records into fact_sales", len(df))


