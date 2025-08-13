EXEC silver.load_silver


CREATE OR ALTER PROCEDURE SILVER.load_silver as 
BEGIN
    Declare @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@BATCH_END_TIME DATETIME;
      BEGIN TRY
         set @batch_start_time=getdate();
         print' ===========================================================================================';
         print'               SILVER LAYER              '
         PRINT'============================================================================================';

          PRINT '------------------------------------------------------------------------';
          PRINT 'LOADING CRM Tables';
          PRINT '------------------------------------------------------------------------';

              SET @start_time=GETDATE();
                    print'>> Truncating Table : silver.CRM_CUST_INFO'
                    TRUNCATE TABLE SILVER.CRM_CUST_INFO;
                    PRINT' INSERTING DATA INTO :SILVER.CRM_CUST_INFO'
                    
                

                    INSERT INTO SILVER.CRM_CUST_INFO(
                    cst_id,
                    cst_key,
                    cst_firstname,
                    cst_lastname,
                    cst_material_status,
                    cst_gndr,
                    cst_create_date)

                    select
                    cst_id,
                    cst_key,
                    trim(cst_firstname) as cst_firstname,
                    trim(cst_lastname) as cst_lastname,
                    case when upper(trim(cst_gndr))='F' then 'Female'
                         when upper(trim(cst_gndr))='M' then 'Male'
                         else 'n/a'
                     end cst_gndr,


                    case when upper(trim(cst_material_status))='S' then 'Single'
                         when upper(trim(cst_material_status))='M' then 'Married'
                         else 'n/a'
                     end cst_material_status,

                    cst_create_date

                    from(
                         select
                         *,
                         row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
                         from bronze.crm_cust_info
                         where cst_id is not null
                         )t
                         where flag_last=1
                         

                         SET @end_time=GETDATE();
                         pRINT' LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + ' Seconds';
                         print' -------------------------------------------------------------------'



                         --- product inof ________

                     SET @start_time=GETDATE();

                    print'>> Truncating Table : silver.crm_prd_info'
                    TRUNCATE TABLE SILVER.crm_prd_info;

                    -- Doing some modification in table according to the table after cleansing
                    IF OBJECT_ID('silver.crm_prd_info','U') is not null
                       DROP TABLE silver.crm_prd_info;


                    CREATE TABLE silver.crm_prd_info(
                    prd_id INT,
                    cat_id nvarchar(50),
                    prd_key NVARCHAR(50),
                    prd_nm NVARCHAR(50),
                    prd_cost INT,
                    prd_line NVARCHAR(50),
                    prd_start_dt DATE,
                    prd_end_dt DATE,
                    dwh_create_date DATETIME2 DEFAULT GETDATE()

                    );

                    -- now insert the data
                    INSERT INTO silver.crm_prd_info(
                    prd_id ,
                    cat_id ,
                    prd_key,
                    prd_nm ,
                    prd_cost,
                    prd_line,
                    prd_start_dt,
                    prd_end_dt
                    )

                    select
                    prd_id,

                    REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
                    substring(prd_key,7,len(prd_key)) as prd_key,
                    prd_nm,
                    isnull(prd_cost,0) as prd_cost,

                    case  upper(trim(prd_line))
                          when 'M' then 'Mountain' 
                          when'R' then 'Road'
                           when 'S' then 'other Sales'
                            when'T' then 'Touring'
                            else 'n/a'
                    end as prd_line,
                    cast(prd_start_dt as date) as prd_start_dt,
                    cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1  as date)as prd_end_dt

                    from bronze.crm_prd_info


                    -- check as well 
                    /*  where REPLACE(SUBSTRING(prd_key,1,5),'-','_')  not  in (
                    select id from bronze.erp_px_cat_g1v2) */

                    /*where substring (prd_key,7,len(prd_key)) not in (
                    select sls_prd_key from bronze.crm_sales_details) */

                     SET @end_time=GETDATE();
                         pRINT' LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + ' Seconds';
                         print' -------------------------------------------------------------------'


                    -------- SALE DETAILS -------------------------
                        SET @start_time=GETDATE();

                    print'>> Truncating Table : silver.crm_sales_details'
                    TRUNCATE TABLE SILVER.crm_sales_details;


                    -- Modify the table as your requirement

                    IF OBJECT_ID('silver.crm_sales_details','U') is not null
                       DROP TABLE silver.crm_sales_details;

                    CREATE TABLE silver.crm_sales_details(
                    sls_ord_num NVARCHAR(50),
                    sls_prd_key NVARCHAR(50),
                    sls_cust_id INT ,
                    sls_order_dt DATE,
                    sls_ship_dt  DATE,
                    sls_due_dt   DATE,
                    sls_sales INT,
                    sls_quantity INT,
                    sls_price INT,
                    dwh_create_date DATETIME2 DEFAULT GETDATE()

                    );

                    -- INSERT INTO CRM_SALES_DETAILS
                    INSERT INTO silver.crm_sales_details(

                            sls_ord_num ,
                            sls_prd_key ,
                            sls_cust_id ,
                            sls_order_dt,
                            sls_ship_dt ,
                            sls_due_dt  ,
                            sls_sales   ,
                            sls_quantity,
                            sls_price   



                    )
                    select 
                    sls_ord_num ,

                    substring(sls_prd_key,1,7) as sls_prd_key,

                    sls_cust_id ,

                    CASE WHEN sls_order_dt=0 OR len(sls_order_dt) != 8 then null
                          else cast(cast(sls_order_dt as varchar) as date) 
                    end as sls_order_dt,


                    CASE WHEN sls_ship_dt=0 OR len(sls_ship_dt) != 8 then null
                          else  cast(cast(sls_ship_dt as varchar) as date)
                    end as sls_ship_dt,
 

                     CASE WHEN sls_due_dt=0 OR len(sls_due_dt) != 8 then null
                          else  cast(cast(sls_due_dt as varchar) as date)
                    end as sls_due_dt,

                    CASE WHEN sls_sales IS null OR sls_sales<=0 OR sls_sales != sls_quantity*abs(sls_price)
                         then sls_quantity *abs(sls_price)

                         else sls_sales
                    END AS sls_sales,
                    sls_quantity,

                    CASE WHEN sls_price is NULL OR sls_price<=0
                             then sls_sales/nullif(sls_quantity,0)
                         else sls_price
                    end as sls_price



                    from bronze.crm_sales_details

                     SET @end_time=GETDATE();
                         pRINT' LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + ' Seconds';
                         print' -------------------------------------------------------------------'


                    -------------  CUST AZ12  ------------

                        SET @start_time=GETDATE();

                    print'>> Truncating Table : silver.erp_cust_az12'
                    TRUNCATE TABLE SILVER.erp_cust_az12;


                    INSERT INTO silver.erp_cust_az12(cid,bdate,gen)


                    SELECT 
                    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,len(cid))
                         ELSE cid
                    END  cid,

                    CASE WHEN bdate > GETDATE() THEN NULL
                         ELSE bdate
                    END AS bdate,

                    CASE WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
                         WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'MALE'
                         ELSE 'n/a'
                    END AS gen 
                    from bronze.erp_cust_az12


                     SET @end_time=GETDATE();
                         pRINT' LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + ' Seconds';
                         print' -------------------------------------------------------------------'


                    -------------- ERP_LOC_A101---------------

                        SET @start_time=GETDATE();

                    print'>> Truncating Table : silver.erp_loc_a101'
                    TRUNCATE TABLE SILVER.ERP_LOC_A101;

                    INSERT INTO silver.erp_loc_a101(cid,cntry)



                    SELECT 
                    REPLACE(cid,'-','') cid,
                    CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
                         when trim(cntry) in ('US','USA') then 'United States'
                         when trim(cntry) ='' or cntry is null then 'n/a'
                         else trim(cntry) 

                    end as cntry
                    from bronze.erp_loc_a101

                     SET @end_time=GETDATE();
                         pRINT' LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + ' Seconds';
                         print' -------------------------------------------------------------------'



                    ----------------CAT_G1V2------------

                        SET @start_time=GETDATE();


                    print'>> Truncating Table : silver.erp_px_cat_g1v2'
                    TRUNCATE TABLE SILVER.erp_px_cat_g1v2;

                    INSERT INTO silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)

                    select id,cat,subcat,maintenance
                    from bronze.erp_px_cat_g1v2

                     SET @end_time=GETDATE();
                         pRINT' LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + ' Seconds';
                         print' -------------------------------------------------------------------'

                    SET @BATCH_END_TIME=GETDATE();
                    PRINT '==============================================='
                    PRINT 'Loading Bronze Layer is Completed';
                    print ' - Total load Duration : ' + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + ' seconds';
                    print '====================================' 


            END TRY
            BEGIN CATCH

             PRINT '================================================='
         PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
         PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
         PRINT 'Error Message' + cast (error_number() as nvarchar)
         print 'Error MEssage' + CAST (ERROR_STATE() AS NVARCHAR);


            END CATCH

END




