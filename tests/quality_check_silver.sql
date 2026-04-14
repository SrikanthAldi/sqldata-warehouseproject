-- Data Quality Check of silver Layer 

/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results


SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


-- Check for unwanted Spaces in all string columns
-- Expectation : No Results 
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) -- If the original name is not equal to the same name after trimming it means there are spaces 

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_martial_status
FROM silver.crm_cust_info
WHERE cst_martial_status != TRIM(cst_martial_status)


-- Data Standardization & Consistency

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info
--==============================================================================================
-- Data quality check of product information in silver 
--==============================================================================================
-- Check for nulls and duplicates 
SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

SELECT prd_key
FROM silver.crm_prd_info
WHERE prd_key != TRIM(prd_key)

-- Check for spaces in name 
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for nulls or Negative numbers
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL

--Data Standardization  & Consistency 
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- check for invalid dateorders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT *
FROM 
silver.crm_prd_info

--=================================================================================================================
--checking 'silver.crm_sales_details
--=================================================================================================================

SELECT *
FROM
bronze.crm_sales_details

/*--WHERE sls_ord_num != TRIM(sls_ord_num)
-- for the sls_prd_key and sls_cust_id make sure we can connect to other tables according to ERD diagram 

WHERE sls_prd_key IN (SELECT prd_key FROM silver.crm_prd_info)

SELECT DISTINCT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (
    SELECT prd_key FROM silver.crm_prd_info
);*/

--WHERE sls_prd_key NOT IN (SELECT SUBSTRING(prd_key, 7, LEN(prd_key)) FROM silver.crm_prd_info)
/*WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
SELECT *
FROM
silver.crm_cust_info*/
-- Negative numbers or zeros can't be cast to a date
--For dates check length depending on style of date  and also check for outliers by validating the boundaries of the date range 
SELECT 
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt<=0 
OR LEN(sls_order_dt) ! = 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101

-- check for invalid dates 
SELECT 
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_due_dt OR sls_order_dt > sls_ship_dt

-- WE have business rule that is 
-- sales = quantity * price
-- and all sales, quantity and Price details must be positive 

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales,sls_quantity,sls_price 

-- so when there is a bad data or bad calculations instead of transforming everything on my own it is best to talk to an expert someone from  business or source system so there will
-- be two solutions , solution 1: Data issues will be fixed direct in source system
-- solution 2 : Data issues has to be fixed in data warehouse 
                --and rules to be followed to fix the data in datawarehouse are :
                /* If sales are negative,zero, or null , calculate it using Price and Quantity 
                   If Price is zero or null, calculate it using Sales and Quantity 
                   If price is negative, convert it into positive value*/
SELECT  DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,

--rule 01
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)  
THEN  sls_quantity * ABS(sls_price)
      ELSE sls_sales
      END AS sls_sales,
--rule 02
CASE WHEN sls_price IS NULL or sls_price <=0
THEN sls_sales / NULLIF(sls_quantity, 0)
ELSE sls_price 
END AS sls_price
FROM bronze.crm_sales_details

--Data quality check of Silver Layer 

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales,sls_quantity,sls_price

SELECT *
FROM
silver.crm_sales_details

--================================================================================================================
-- checking 'silver.erp_cust_az12
--================================================================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

--================================================================================================================
-- Checking 'silver.erp_loc_a101
--================================================================================================================

-- Data Standardization & Consistency
SELECT DISTINCT 
    country
FROM silver.erp_loc_a101
ORDER BY country;

--===============================================================================================================
-- Checking ' silver.erp_px_cat_g1v2
--===============================================================================================================

SELECT 
id,
cat,
subcat,
maintenance

FROM silver.erp_px_cat_g1v2

--Check for unwanted spaces 
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

SELECT DISTINCT 
maintenance
FROM 
silver.erp_px_cat_g1v2

SELECT DISTINCT 
cat
FROM 
silver.erp_px_cat_g1v2

SELECT DISTINCT 
subcat
FROM 
silver.erp_px_cat_g1v2



