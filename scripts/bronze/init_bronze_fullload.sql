
/*
=========================================================================
FULL LOAD IN BRONZE LAYER =  TRUNCATE + INSERT 
=========================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
BEGIN TRY 
SET @batch_start_time = GETDATE();
PRINT '========================================================';
PRINT 'LOADING BRONZE LAYER';
PRINT '========================================================';
PRINT '--------------------------------------------------------';
PRINT 'LOADING CRM SOURCE TABLES';
PRINT '--------------------------------------------------------';

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.crm_cust_info;

BULK INSERT bronze.crm_cust_info
FROM '/data/source_crm/cust_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
SET @end_time = GETDATE();

SELECT COUNT(*) FROM bronze.crm_cust_info-- After loading the data check with first name , date and quality check whether we have accurate data or not 

PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT'=========================================================================================';



SET @start_time = GETDATE();
TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
FROM '/data/source_crm/prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);

SET @end_time = GETDATE();
SELECT COUNT(*) FROM bronze.crm_prd_info
PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT'=========================================================================================';

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM '/data/source_crm/sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);

SET @end_time = GETDATE();
SELECT COUNT(*) FROM bronze.crm_sales_details
PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT'=========================================================================================';

/*
========================================================================
BULK LOAD FOR SOURCE ERP
=========================================================================
*/
PRINT '----------------------------------------------------------------';
PRINT 'LOADING ERP SOURCE TABLE';
PRINT '-----------------------------------------------------------------';

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12
FROM '/data/source_erp/CUST_AZ12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
SET @end_time = GETDATE();
SELECT COUNT(*) FROM bronze.erp_cust_az12
PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT'=========================================================================================';

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101
FROM '/data/source_erp/LOC_A101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
SET @end_time = GETDATE();
SELECT COUNT(*) FROM bronze.erp_loc_a101
PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT'=========================================================================================';

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2
FROM '/data/source_erp/PX_CAT_G1V2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',

    ROWTERMINATOR = '0x0a',
    TABLOCK
);

SET @end_time = GETDATE();
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2
PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT'=========================================================================================';

SET @batch_end_time = GETDATE();
PRINT '=========================================================================================';
PRINT 'Loading Bronze layer is Completed';
PRINT '   -Total Load Duration: '+ CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds' ;
PRINT '==========================================================================================';
END TRY 
BEGIN CATCH
PRINT '====================================================='
PRINT 'ERROR OCCURED DURING BRONZE LAYER'
PRINT 'Error Message' + ERROR_MESSAGE();
PRINT 'Error Message' + CAST(ERROR_NUMBER() AS VARCHAR);
PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
PRINT '====================================================='

END CATCH

END
