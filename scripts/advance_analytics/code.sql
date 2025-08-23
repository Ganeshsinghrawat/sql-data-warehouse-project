With customer_spending as (
SELECT
c.customer_key,
sum(f.sales_amount) as total_spending,
min(order_date) as first_order,
max(order_date) as last_order,
Datediff(month,min(order_date),max(order_date)) as Lifespan
from gold.fact_sales f
LEFT JOIN gold.dim_customers c
on f.customer_key=c.customer_key
group by c.customer_key
)


select 
customer_segment,
count(customer_key) as total_customers
from (
select
customer_key,

CASE WHEN Lifespan >=12 AND  total_spending>5000 then 'VIP'
     when lifespan >=12 and total_spending <=5000 then 'Regular'
     else 'New'
end CUSTOMER_segment
from customer_spending
)t

group by customer_segment
order by total_customers desc
