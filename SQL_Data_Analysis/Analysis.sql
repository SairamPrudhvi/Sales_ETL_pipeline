-- Top 10 products by total revenue
SELECT
    p.product_id,
    p.product_name,
    CAST(SUM(COALESCE(f.net_sale_amount, 0)) as DECIMAL(10,2)) AS total_revenue
FROM sales_dw.fact_sales f
JOIN sales_dw.dim_product p
  ON f.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_revenue DESC
LIMIT 10;


--Monthly sales trend
SELECT
    d.year,
    d.month,
    CAST(SUM(COALESCE(f.net_sale_amount, 0)) as DECIMAL(10,2)) AS monthly_revenue
FROM sales_dw.fact_sales f
JOIN sales_dw.dim_date d
  ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

--Average order value per customer
SELECT
    c.customer_id,
    c.customer_name,
    CAST(AVG(COALESCE(f.net_sale_amount, 0)) as DECIMAL(10,2)) AS avg_order_value
FROM sales_dw.fact_sales f
JOIN sales_dw.dim_customer c
  ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY avg_order_value DESC;


-- Top 5 customers by lifetime value (LTV)
SELECT
    c.customer_id,
    c.customer_name,
    CAST(SUM(COALESCE(f.net_sale_amount, 0)) as DECIMAL(10,2)) AS lifetime_value
FROM sales_dw.fact_sales f
JOIN sales_dw.dim_customer c
  ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY lifetime_value DESC
LIMIT 5;

--Revenue contribution by product category (% of total)
WITH category_revenue AS (
    SELECT
        p.category,
        CAST(SUM(COALESCE(f.net_sale_amount, 0)) as DECIMAL(10,2)) AS revenue
    FROM sales_dw.fact_sales f
    JOIN sales_dw.dim_product p
      ON f.product_id = p.product_id
    GROUP BY p.category
),
total AS (
    SELECT SUM(revenue) AS total_revenue
    FROM category_revenue
)
SELECT
    c.category,
    c.revenue,
    ROUND((c.revenue / t.total_revenue) * 100, 2) AS revenue_pct
FROM category_revenue c
CROSS JOIN total t
ORDER BY revenue_pct DESC;

--Customer retention rate (month-over-month)
WITH monthly_customers AS (
    SELECT DISTINCT
        d.year,
        d.month,
        f.customer_id
    FROM sales_dw.fact_sales f
    JOIN sales_dw.dim_date d
      ON f.date_id = d.date_id
),
retained AS (
    SELECT
        curr.year,
        curr.month,
        COUNT(DISTINCT curr.customer_id) AS retained_customers
    FROM monthly_customers curr
    JOIN monthly_customers prev
      ON curr.customer_id = prev.customer_id
     AND (curr.year, curr.month) =
         (prev.year, prev.month + 1)
    GROUP BY curr.year, curr.month
)
SELECT * FROM retained
ORDER BY year, month;

--Rolling 3-month sales average
WITH monthly_sales AS (
    SELECT
        d.year,
        d.month,
        SUM(f.net_sale_amount)::NUMERIC AS monthly_sales
    FROM sales_dw.fact_sales f
    JOIN sales_dw.dim_date d
      ON f.date_id = d.date_id
    GROUP BY d.year, d.month
)
SELECT
    year,
    month,
    ROUND(monthly_sales, 2) AS monthly_sales,
    ROUND(
        AVG(monthly_sales) OVER (
            ORDER BY year, month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS rolling_3_month_avg
FROM monthly_sales
ORDER BY year, month;



--Best-selling product per month
WITH product_monthly AS (
    SELECT
        d.year,
        d.month,
        p.product_name,
        SUM(f.net_sale_amount) AS revenue,
        RANK() OVER (
            PARTITION BY d.year, d.month
            ORDER BY SUM(f.net_sale_amount) DESC
        ) AS rnk
    FROM sales_dw.fact_sales f
    JOIN sales_dw.dim_date d
      ON f.date_id = d.date_id
    JOIN sales_dw.dim_product p
      ON f.product_id = p.product_id
    GROUP BY d.year, d.month, p.product_name
)
SELECT *
FROM product_monthly
WHERE rnk = 1
ORDER BY year, month;

--Customers with declining purchase frequency
WITH customer_monthly_orders AS (
    SELECT
        f.customer_id,
        d.year,
        d.month,
        COUNT(*) AS order_count
    FROM sales_dw.fact_sales f
    JOIN sales_dw.dim_date d
      ON f.date_id = d.date_id
    GROUP BY f.customer_id, d.year, d.month
),
trend AS (
    SELECT
        customer_id,
        year,
        month,
        order_count,
        LAG(order_count) OVER (
            PARTITION BY customer_id
            ORDER BY year, month
        ) AS prev_orders
    FROM customer_monthly_orders
)
SELECT *
FROM trend
WHERE prev_orders IS NOT NULL
  AND order_count < prev_orders;

--Revenue by weekday vs weekend
SELECT
    CASE
        WHEN d.weekday IN ('Saturday', 'Sunday') THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    CAST(SUM(COALESCE(f.net_sale_amount, 0)) as DECIMAL(10,2)) AS total_revenue
FROM sales_dw.fact_sales f
JOIN sales_dw.dim_date d
  ON f.date_id = d.date_id
GROUP BY day_type;