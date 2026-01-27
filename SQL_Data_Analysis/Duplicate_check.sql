--Duplicate Customers.
select * from sales_staging.customers_stage
where customer_id in (

select customer_id,  count(customer_id) as duplicate_cust_id
from 
sales_staging.customers_stage
group by customer_id
having count(customer_id)>1
order by customer_id
)
order by customer_id, signup_date

select * from sales_staging.products_stage where product_id in (
---duplicate product_id
select product_id--, count(product_id) as duplicate_prod
from sales_staging.products_stage
group by product_id
having count(product_id)>1
order by product_id)
order by product_id


---duplicate transaction_data
select * from sales_staging.sales_transactions_stage order by transaction_id

select transaction_id, count(transaction_id) as duplicate_tran
from sales_staging.sales_transactions_stage
group by transaction_id
having count(transaction_id)>1
order by transaction_id





SELECT *
FROM sales_staging.sales_transactions_stage
WHERE transaction_date IS NULL
   OR transaction_date !~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$';  --279 records...



