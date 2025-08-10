EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze as 
BEGIN
   DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time datetime,@batch_end_time DATETIME;
   
    BEGIN TRY
         SET @batch_start_time=getdate();
             PRINT '=============================================================================================';
             PRINT 'Loading Bronze Layer';
             PRINT '=============================================================================================';

             PRINT '------------------------------------------------------------------------';
             PRINT 'LOADING CRM Tables';
             PRINT '------------------------------------------------------------------------';

             SET @start_time=GETDATE();
             PRINT'>> Truncating Table: bronze.crm_cust_info'
                TRUNCATE TABLE bronze.crm_cust_info;

        
             PRINT'>> Inserting Data Into: bronze.crm_cust_info'
                BULK INSERT bronze.crm_cust_info
                from 'E:\choose right\SQL-Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
                WITH(
                    FIRSTROW=2,
                    FIELDTERMINATOR=',',
                    TABLOCK
                );

                SET @end_time=GETDATE();
                PRINT'>> LOAD Duration : ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds' ;
                print' ---------------------------- ';


                SET @start_time=GETDATE();
                PRINT'>> Truncating Table: bronze.crm_prd_info'
                TRUNCATE TABLE bronze.crm_prd_info;

                
                 PRINT'>> Inserting Data Into: bronze.crm_prd_info'

                BULK INSERT bronze.crm_prd_info
                from 'E:\choose right\SQL-Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
                WITH(
                    FIRSTROW=2,
                    FIELDTERMINATOR=',',
                    TABLOCK
                );

                SET @end_time=getdate();
                 PRINT'>> LOAD Duration : ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds' ;
                print' ---------------------------- ';

               


                  SET @start_time=GETDATE();
                PRINT'>> Truncating Table: bronze.crm_sales_details';

                TRUNCATE TABLE bronze.crm_sales_details;

                PRINT'>> Inserting Data Into: bronze.crm_sales_details';

                BULK INSERT    bronze.crm_sales_details
                from 'E:\choose right\SQL-Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
                WITH(
                    FIRSTROW=2,
                    FIELDTERMINATOR=',',
                    TABLOCK
                );
                SET @end_time=getdate();
                 PRINT'>> LOAD Duration : ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds' ;
                print' ---------------------------- ';


               /* SELECT * FROM BRONZE.crm_sales_details;*/


             PRINT '------------------------------------------------------------------------';
             PRINT 'LOADING ERP Tables';
             PRINT '------------------------------------------------------------------------';

              SET @start_time=GETDATE();
              PRINT'>> Truncating Table: bronze.erp_cust_az12';

                TRUNCATE TABLE bronze.erp_cust_az12;

              PRINT'>> Inserting Data Into: bronze.erp_cust_az12';
                BULK INSERT    bronze.erp_cust_az12
                from 'E:\choose right\SQL-Projects\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
                WITH(
                    FIRSTROW=2,
                    FIELDTERMINATOR=',',
                    TABLOCK
                );
                SET @end_time=getdate();
                 PRINT'>> LOAD Duration : ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds' ;
                print' ---------------------------- ';

                /*SELECT * FROM BRONZE.erp_cust_az12;*/


                SET @start_time=GETDATE();
                PRINT'>> Truncating Table: bronze.erp_loc_a101';

                TRUNCATE TABLE bronze.erp_loc_a101;

                PRINT'>> Inserting Data Into: bronze.erp_loc_a101';
                BULK INSERT    bronze.erp_loc_a101
                from 'E:\choose right\SQL-Projects\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
                WITH(
                    FIRSTROW=2,
                    FIELDTERMINATOR=',',
                    TABLOCK
                );
                SET @end_time=getdate();
                 PRINT'>> LOAD Duration : ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds' ;
                print' ---------------------------- ';



                SET @start_time=GETDATE();
                 PRINT'>> Truncating Table: bronze.erp_px_cat_g1v2';
                TRUNCATE TABLE bronze.erp_px_cat_g1v2;

                 PRINT'>> Inserting Data Into: bronze.erp_px_cat_g1v2';
                BULK INSERT    bronze.erp_px_cat_g1v2
                from 'E:\choose right\SQL-Projects\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
                WITH(
                    FIRSTROW=2,
                    FIELDTERMINATOR=',',
                    TABLOCK
                );

                 SET @end_time=getdate();
                 PRINT'>> LOAD Duration : ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds' ;
                print' ---------------------------- ';


               set @batch_end_time=getdate();
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
        
