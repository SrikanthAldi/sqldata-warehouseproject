

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN


    PRINT '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    PRINT '>> Inserting Data Into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_martial_status,
    cst_gndr,
    cst_create_date)

    SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_martial_status)) = 'M' THEN 'Married'
         WHEN UPPER(TRIM(cst_martial_status)) = 'S' THEN 'Single'
         ELSE 'n/a'
    END cst_martial_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
         WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
         ELSE 'n/a'
    END cst_gndr,
    cst_create_date
    FROM (

    SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last --“We are ranking rows per customer to identify the latest record (row_number = 1).”
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
    )t WHERE flag_last = 1   -- Every subquery needs a name so inner select query executes it creates a temporary table and we named it t and from that t we are filtering 

    PRINT '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    PRINT '>> Inserting Data Into: silver.crm_prd_info';


    WITH transformed AS (
        SELECT
            prd_id,
            prd_key,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'MOUNTAIN'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'OTHER SALES'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'ROAD'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'TOURING'
                ELSE 'N/A'
            END AS prd_line,
            prd_start_dt,
            TRY_CONVERT(
                DATE,
                NULLIF(
                    LTRIM(RTRIM(
                        REPLACE(REPLACE(REPLACE(prd_end_dt, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')
                    )),
                    ''
                ),
                23
            ) AS prd_end_dt
        FROM bronze.crm_prd_info
    ),
    sequenced AS (
        SELECT
            prd_id,
            prd_key,
            cat_id,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt,
            LEAD(prd_start_dt) OVER (
                PARTITION BY prd_key
                ORDER BY prd_start_dt
            ) AS next_prd_start_dt
        FROM transformed
    ),
    final_data AS (
        SELECT
            prd_id,
            prd_key,
            cat_id,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            CASE
                WHEN prd_end_dt IS NULL THEN NULL
                WHEN prd_end_dt >= prd_start_dt THEN prd_end_dt
                WHEN prd_end_dt < prd_start_dt AND next_prd_start_dt IS NOT NULL
                    THEN DATEADD(DAY, -1, next_prd_start_dt)
                ELSE NULL
            END AS prd_end_dt
        FROM sequenced
    )
    INSERT INTO silver.crm_prd_info (
        prd_id,
        prd_key,
        cat_id,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        prd_key,
        cat_id,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    FROM final_data;

    PRINT '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    PRINT '>> Inserting Data Into: silver.crm_sales_details';

    INSERT INTO silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
    )
    SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    -- Thing to remember again here sls_order date must be early than ship date and due date 
    CASE WHEN sls_order_dt =0 OR LEN(sls_order_dt) !=8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt =0 OR LEN(sls_ship_dt) !=8 THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt =0 OR LEN(sls_due_dt) !=8 THEN NULL
         ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,

    -- WE have business rule that is 
    -- sales = quantity * price
    -- and all sales, quantity and Price details must be positive 
    CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)  
    THEN  sls_quantity * ABS(sls_price)
          ELSE sls_sales
          END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price IS NULL or sls_price <=0
    THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price 
    END AS sls_price
    FROM bronze.crm_sales_details

    PRINT '>> Truncating Table: silver.erp.cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    PRINT '>> Inserting Data Into: silver.erp.cust_az12';
    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT 
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid , 4, LEN(cid))
         ELSE cid
    END AS cid,
    CASE WHEN bdate > GETDATE() THEN NULL
    ELSE bdate
    END AS bdate,
        CASE
            WHEN UPPER(
                LTRIM(RTRIM(
                    REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')
                ))
            ) IN ('M', 'MALE') THEN 'MALE'
            WHEN UPPER(
                LTRIM(RTRIM(
                    REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')
                ))
            ) IN ('F', 'FEMALE') THEN 'FEMALE'
            WHEN gen IS NULL OR
                 LTRIM(RTRIM(
                    REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')
                 )) = '' THEN 'n/a'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;

    PRINT '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    PRINT '>> Inserting Data Into: silver.erp_loc_a101';

    -- Loading into silver layer 

    INSERT INTO silver.erp_loc_a101
    (cid,country)
    SELECT 
    REPLACE (cid, '-','')cid,  -- Be careful when you are using replace function

        CASE 
            WHEN TRIM(REPLACE(country, CHAR(13), '')) = 'DE' THEN 'Germany'
            WHEN TRIM(REPLACE(country, CHAR(13), '')) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(REPLACE(country, CHAR(13), '')) = '' OR country IS NULL THEN 'n/a'
            ELSE TRIM(REPLACE(country, CHAR(13), ''))
        END AS country
    FROM bronze.erp_loc_a101

      PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

    --loading into silver layer 
    INSERT INTO silver.erp_px_cat_g1v2
    (id,
    cat,
    subcat,
    maintenance)

    SELECT 
    id,
    cat,
    subcat,
    maintenance

    FROM bronze.erp_px_cat_g1v2

END


