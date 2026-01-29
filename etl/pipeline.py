
import logging
from typing import Optional, List
import pandas as pd
from sqlalchemy import text

from etl.logging_config import setup_logging
from db.database import get_engine
from etl.transform.customer_transform import transform_customers
from etl.transform.product_transform import transform_products
from etl.transform.sales_transform import transform_sales
from etl.transform.date_dim import build_dim_date

# Validation
from etl.validate import (
    validate_schema,
    normalize_empty_strings,
    validate_transaction_dates,
    detect_orphan_transactions,
    clean_numeric_fields,
    clean_product_numeric_fields,
    enforce_sales_dtypes,
    enforce_customer_dtypes,
    enforce_product_dtypes,
)

# Dedup & rejects
from etl.dedup import (
    resolve_duplicate_customers,
    save_rejected_customer_duplicates
)
from etl.product_dedup import (
    resolve_duplicate_products,
    save_rejected_product_duplicates
)
from etl.sales_rejects import (
    detect_corrupt_transactions,
    save_rejected_sales_transactions
)
from etl.rejects import save_rejected_batches

# Transform & DW
from etl.transform.sales_transform import transform_sales
from etl.transform.date_dim import build_dim_date
from etl.dw.load import create_dw_tables, load_dimension, load_fact

logger = logging.getLogger(__name__)


class SalesETLPipeline:
    def __init__(self) -> None:
        setup_logging()
        self.engine = get_engine()

        self.customers: Optional[pd.DataFrame] = None
        self.products: Optional[pd.DataFrame] = None
        self.sales: Optional[pd.DataFrame] = None

        self.rejected_records: List[pd.DataFrame] = []

    
    # EXTRACT
    
    def extract(self) -> None:
        logger.info("Starting extract stage")

        query = """
            SELECT last_processed_ingest_date
            FROM sales_staging.etl_audit_log
            WHERE pipeline_name = 'sales_etl'
        """
        result = pd.read_sql(query, self.engine)
        last_ingest = result.iloc[0, 0] if not result.empty else "1900-01-01"

        self.customers = pd.read_sql(
            f"SELECT * FROM sales_staging.customers_stage WHERE ingest_date > '{last_ingest}'",
            self.engine
        )
        self.products = pd.read_sql(
            f"SELECT * FROM sales_staging.products_stage WHERE ingest_date > '{last_ingest}'",
            self.engine
        )
        self.sales = pd.read_sql(
            f"SELECT * FROM sales_staging.sales_transactions_stage WHERE ingest_date > '{last_ingest}'",
            self.engine
        )

        logger.info("Extract completed")

    
    # VALIDATE
    
    
    def validate(self) -> None:
        logger.info("Starting validation stage")

        # ---- Normalize
        self.customers = normalize_empty_strings(self.customers)
        self.products = normalize_empty_strings(self.products)
        self.sales = normalize_empty_strings(self.sales)

        # ---- Numeric cleanup
        self.sales = clean_numeric_fields(self.sales)
        self.products = clean_product_numeric_fields(self.products)

        # ---- Dedup dimensions
        self.customers, rejected_cust = resolve_duplicate_customers(self.customers)
        save_rejected_customer_duplicates(rejected_cust)

        self.products, rejected_prod = resolve_duplicate_products(self.products)
        save_rejected_product_duplicates(rejected_prod)

        # ---- Date validation
        self.sales, rejected_dates = validate_transaction_dates(self.sales)
        self.rejected_records.append(rejected_dates)

        # ---- Corrupt sales
        self.sales, rejected_corrupt = detect_corrupt_transactions(self.sales)
        self.rejected_records.append(rejected_corrupt)
        save_rejected_sales_transactions(rejected_corrupt)

        # ---- Orphans (USING CLEANED DIMENSIONS)
        self.sales, rejected_orphans = detect_orphan_transactions(
            self.sales, self.customers, self.products
        )
        self.rejected_records.append(rejected_orphans)

        # ---- Final dtypes
        self.customers = enforce_customer_dtypes(self.customers)
        self.products = enforce_product_dtypes(self.products)
        self.sales = enforce_sales_dtypes(self.sales)

        save_rejected_batches(self.rejected_records)
        logger.info("Validation completed")

    
    # TRANSFORM
    

    def transform(self) -> None:
        logger.info("Starting transform stage")

        self.customers = transform_customers(self.customers)
        self.products = transform_products(self.products)
        self.sales = transform_sales(self.sales)

        self.dim_date = build_dim_date(self.sales["transaction_date"])

        logger.info("Transform completed")


   
    # LOAD
    
    def load(self) -> None:
        logger.info("Starting load stage")

        create_dw_tables(self.engine)  

        load_dimension(self.engine, self.customers, "dim_customer", "customer_id")
        load_dimension(self.engine, self.products, "dim_product", "product_id")
        load_dimension(self.engine, self.dim_date, "dim_date", "date_id")

        self.sales["date_id"] = (
            self.sales["transaction_date"]
            .dt.strftime("%Y%m%d")
            .astype(int)
        )

        load_fact(self.engine, self.sales)
        logger.info("Load completed")

    from sqlalchemy import text

    def update_audit_log(
        self,
        records_processed: int,
        records_rejected: int,
        records_loaded: int,
        status: str = "SUCCESS"
    ) -> None:
        query = text("""
            INSERT INTO sales_staging.etl_audit_log
                (
                    pipeline_name,
                    last_processed_ingest_date,
                    records_processed,
                    records_rejected,
                    records_loaded,
                    run_status
                )
            VALUES
                ('sales_etl', NOW(), :processed, :rejected, :loaded, :status)
            ON CONFLICT (pipeline_name)
            DO UPDATE SET
                last_processed_ingest_date = EXCLUDED.last_processed_ingest_date,
                records_processed = EXCLUDED.records_processed,
                records_rejected = EXCLUDED.records_rejected,
                records_loaded = EXCLUDED.records_loaded,
                run_status = EXCLUDED.run_status,
                updated_at = NOW();
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(
                    query,
                    {
                        "processed": records_processed,
                        "rejected": records_rejected,
                        "loaded": records_loaded,
                        "status": status
                    }
                )

            logger.info(
                "Audit updated | processed=%d rejected=%d loaded=%d status=%s",
                records_processed, records_rejected, records_loaded, status
            )

        except Exception:
            logger.exception("Failed to update audit log")
            raise

    # =========================
    # RUN
    # =========================
    # def run(self) -> None:
    #     try:
    #         self.extract()
    #         self.validate()
    #         self.transform()
    #         self.load()
    #         self.update_audit_log(record_count=len(self.sales))
    #         logger.info("ETL pipeline finished successfully")
    #     except Exception:
    #         logger.exception("ETL pipeline failed")
    #         raise

    def run(self) -> None:
        extracted = rejected = loaded = 0

        try:
            self.extract()
            extracted = (
                len(self.customers)
                + len(self.products)
                + len(self.sales)
            )

            self.validate()
            rejected = sum(len(df) for df in self.rejected_records)

            self.transform()
            self.load()
            loaded = len(self.sales)

            self.update_audit_log(
                records_processed=extracted,
                records_rejected=rejected,
                records_loaded=loaded,
                status="SUCCESS"
            )

        except Exception:
            self.update_audit_log(
                records_processed=extracted,
                records_rejected=rejected,
                records_loaded=0,
                status="FAILED"
            )
            raise
