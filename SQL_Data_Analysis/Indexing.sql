-- Fact table FK indexes
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer
ON sales_dw.fact_sales (customer_id);

CREATE INDEX IF NOT EXISTS idx_fact_sales_product
ON sales_dw.fact_sales (product_id);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date
ON sales_dw.fact_sales (date_id);
