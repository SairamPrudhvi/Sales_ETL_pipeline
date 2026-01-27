-- Dimension constraints
ALTER TABLE sales_dw.dim_customer
ALTER COLUMN customer_id SET NOT NULL;

ALTER TABLE sales_dw.dim_product
ALTER COLUMN product_id SET NOT NULL;

ALTER TABLE sales_dw.dim_date
ALTER COLUMN date_id SET NOT NULL;

-- Uniqueness
ALTER TABLE sales_dw.dim_customer
ADD CONSTRAINT uq_dim_customer UNIQUE (customer_id);

ALTER TABLE sales_dw.dim_product
ADD CONSTRAINT uq_dim_product UNIQUE (product_id);

ALTER TABLE sales_dw.dim_date
ADD CONSTRAINT uq_dim_date UNIQUE (date_id);
