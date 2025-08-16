PRINT'====================================================='
PRINT'CREATE DIMENSION: gold.dim_customers'
PRINT'====================================================='


IF OBJECT_ID('gold.dim_customers','V') is not NULL
   DROP VIEW gold.dim_customers;

GO


CREATE VIEW gold.dim_customers as

select
    ROW_NUMBER() OVER(ORDER BY cst_id) as customer_key,

    ci.cst_id                as customer_id,
    ci.cst_key               as customer_number,
    ci.cst_firstname         as first_name,
    ci.cst_lastname          as last_name,
     la.cntry                as country,
    ci.cst_material_status   as marital_status,

    CASE
         WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the Master for gender Info
         ELSE COALESCE(ca.gen,'n/a')
    END                      AS gender,
    ca.bdate                 as birthdate,

    ci.cst_create_date       as create_date 
    
  
   
    from silver.crm_cust_info ci
    left join silver.erp_cust_az12 ca
          on ci.cst_key=ca.cid
    left join silver.erp_loc_a101 la
          on ci.cst_key=la.cid

GO


PRINT'====================================================='
PRINT'CREATE DIMENSION: gold.dim_products'
PRINT'====================================================='



IF OBJECT_ID('gold.dim_products','V') is not NULL
   DROP VIEW gold.dim_products;

GO


CREATE VIEW gold.dim_products as 

SELECT 
    ROW_NUMBER() over(ORDER BY pn.prd_start_dt,pn.prd_key) as product_key,
	pn.prd_id               as product_id,
	pn.prd_key              as product_number,
	pn.prd_nm               as product_name,
	pn.cat_id               as category_id,
	pc.cat                  as category,
	pc.subcat               as subcategory,
	pc.maintenance          as maintenance,
	pn.prd_cost             as cost,
	pn.prd_line             as product_line,
	pn.prd_start_dt         as start_date
	

from silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
        ON pn.cat_id=pc.id
WHERE prd_end_dt is NULL -- Filter out all historical data

GO



PRINT'====================================================='
PRINT'CREATE DIMENSION: gold.fact_sales'
PRINT'====================================================='


IF OBJECT_ID('gold.fact_sales','V') is not NULL
   DROP VIEW gold.fact_sales;

GO

CREATE VIEW gold.fact_sales as 

SELECT 
sd.sls_ord_num               as Order_number,
pr.product_key		         as product_key,
cu.customer_key		         as customer_key,
sd.sls_order_dt              as Order_date,
sd.sls_ship_dt               as Shipping_date,
sd.sls_due_dt                as Due_date,
sd.sls_sales                 as Sales_amount,
sd.sls_quantity              as Quantity,
sd.sls_price                 as Price

FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
     on sd.sls_prd_key=pr.product_number
left  JOIN gold.dim_customers cu
     ON sd.sls_cust_id=cu.customer_id

GO
