-- No Orphan Foreign Keys Fact → Customer
SELECT COUNT(*) AS orphan_customers
FROM sales_dw.fact_sales f
LEFT JOIN sales_dw.dim_customer c
  ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Fact → Product
SELECT COUNT(*) AS orphan_products
FROM sales_dw.fact_sales f
LEFT JOIN sales_dw.dim_product p
  ON f.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Fact → Date
SELECT COUNT(*) AS orphan_dates
FROM sales_dw.fact_sales f
LEFT JOIN sales_dw.dim_date d
  ON f.date_id = d.date_id
WHERE d.date_id IS NULL;


 -- No Duplicate Fact Records (assuming business key = customer + product + date)

SELECT
    customer_id,
    product_id,
    date_id,
    COUNT(*) AS duplicate_count
FROM sales_dw.fact_sales
GROUP BY customer_id, product_id, date_id
HAVING COUNT(*) > 1;

--Sales Totals Consistency Check Line-level validation
SELECT *
FROM sales_dw.fact_sales
WHERE  CAST(total_sale_amount as DECIMAL(10,2)) <> quantity * CAST(unit_price as DECIMAL(10,2)) ;

-- Aggregate validation (ETL vs DW)
SELECT
    ROUND(SUM(quantity * unit_price)::NUMERIC, 2) AS recomputed_total,
    ROUND(SUM(total_sale_amount)::NUMERIC, 2) AS stored_total
FROM sales_dw.fact_sales;

-- Discount Sanity Check
SELECT *
FROM sales_dw.fact_sales
WHERE net_sale_amount > total_sale_amount
   OR net_sale_amount < 0;
