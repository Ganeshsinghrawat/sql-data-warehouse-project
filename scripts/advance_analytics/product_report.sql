

 /*
 =============================================
 Product Report
 =============================================

 Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
1. Gathers essential fields such as product name,category,subcategory,and cost.
2. Segments products by revenue to identify high-performers,mid-range, or low_performers.
3.Aggregates product-level metrics: 
     - total orders
     - total sales
     - total quantity sold
     - total customers(unique)
     - lifespan (in months)
4.Calculates valuable Kpis;
   - recency(months since last sale)
   - average order revenue (Aor)
   - average monthly revenue

*/

/*select * from gold.dim_products
select * from gold.fact_sales
select  * from gold.dim_customers*/
IF OBJECT_ID('gold.report_products', 'V') IS NOT NULL
    DROP VIEW gold.report_products;
GO
create view gold.report_products as
with base_query as (
select 

p.product_name ,
p.product_key,
p.category,
p.subcategory ,
p.cost ,
s.order_number,
s.order_date,
s.sales_amount,
s.customer_key,
s.Quantity

from gold.fact_sales s
left join gold.dim_products p
on p.product_key=s.product_key
where order_date is not null

),
product_aggregration as(

select 
product_key,

product_name,
category,
subcategory,
cost,
count(distinct order_number) as total_orders,
max(order_date) as last_sale_date,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity_sold,
count(distinct customer_key) as total_customers,
datediff(month,min(order_date) ,max(order_date)) as lifespans,
ROUND(AVG(CAST(sales_amount as float)/nullif(quantity,0)),1) as avg_selling_price
from base_query
group by 
product_key,
category,
subcategory,
cost,
product_name
)

-- final query : combines all product results into one output

select product_key,
product_name,
category,
subcategory,
cost,
last_sale_date,
DATEDIFF(MONTH,LAST_SALE_DATE,GETDATE()) AS RECENCY_IN_MONTHS,
CASE
    WHEN total_sales>50000 then 'High-Performer'
    when total_sales>=10000 THEN 'MID-Range'
    ELSE 'Low-Performer'
End as product_segment,
lifespans,
total_orders,
total_sales,
total_quantity_sold,
total_customers,
avg_selling_price,

-- Average Order Revenue(AOR)

case when total_orders=0 then 0
     else total_sales/total_orders
end as avg_order_revenue,

-- Average Monthly revenue
case when lifespans=0 then total_sales
     else total_sales/lifespans
end as avg_monthly_revenue

from product_aggregration


select * from gold.report_products
