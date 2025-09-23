/*
====================================================
Quality Checks
====================================================
Script Purpose:
This Script perfroms various quality checks for data consistency, accuracy,
and standardization across the 'silver' schemas. It includes checks for:
-- CHECK for NULLS or Duplicates in Primary Key
-- Unwanted Spaces in string fields.
-- Data Standardization and consistency.
-- Invalid date ranges and orders.
-- Data consistency between related fields.

Usage Notes:
  - Run these checks after the data loaded to Silver layer.
  - Investigate and resolve any discrepancies found during the checks.
=====================================================
*/
-- ==================================================
-- Checking for 'silver.crm_cust_info'
-- ==================================================

-- CHECK for NULLS or Duplicates in Primary Key
SELECT 
cst_id,
count(*)
FROM silver.crm_cust_info
group by cst_id
HAVING COUNT(*) > 1 or cst_id IS NULL;

-- Check for Unwanted spaces
SELECt cst_key
from silver.crm_cust_info
where cst_key ! = TRIM(cst_key);

select 
cst_lastname
from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

select 
cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

select 
cst_marital_status
from silver.crm_cust_info
where cst_marital_status != TRIM(cst_marital_status);

-- Performing data standardization and consistency.
SELECt DIStinct cst_gndr
from silver.crm_cust_info;

SELECt DIStinct cst_marital_status
from silver.crm_cust_info;

SELECT * from silver.crm_cust_info;

-- ==================================================
-- Checking for 'silver.crm_prd_info'
-- ==================================================

-- CHECK for NULLS or Duplicates in Primary Key
Select 
prd_id,
count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is NULL;

-- Check for Unwanted spaces
SELECT
prd_nm
from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm);

SELECT
prd_key
from bronze.crm_prd_info
where prd_key != TRIM(prd_key);

Select * from bronze.crm_prd_info
order by prd_end_dt;

SELECT
prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is NULL;

-- Performing data standardization and consistency.
SELECT Distinct prd_line
from bronze.crm_prd_info;

SELECT * from silver.crm_prd_info
where prd_end_dt < prd_start_dt

SELECT 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (
	PARTITION BY prd_key 
	ORDER BY prd_start_dt
	)) AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509', 'AC-HE-HL-U509-R')

SELECT * FROM silver.crm_prd_info;

-- ==================================================
-- Checking for 'silver.crm_sales_details'
-- ==================================================

-- CHECK Dates values in a specific range or not.
SELECT NULLIF(sls_due_dt,0) sls_due_dt
  FROM [DataWarehouse].silver.[crm_sales_details]
  WHERE sls_due_dt> 20500101 or sls_due_dt<19000101;


SELECT NULLIF(sls_due_dt,0) sls_due_dt
  FROM [DataWarehouse].[bronze].[crm_sales_details]
  WHERE len(sls_due_dt) != 8 or sls_due_dt <= 0;

SELECT *
  FROM [DataWarehouse].silver.[crm_sales_details]
  WHERE sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

SELECT
sls_sales,
sls_quantity,
sls_price,

CASE WHEN sls_sales is NULl or sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price is NULL OR sls_price <= 0 
		THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE ABS(sls_price)
END AS sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price or sls_sales IS NULL 
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0 or sls_price is NUll;

SELECT * FROM silver.crm_sales_details;

-- ==================================================
-- Checking for 'silver.erp_cust_a_z'
-- ==================================================

-- CHECK Dates values in a specific range or not.
SELECT 
*
FROM silver.erp_cust_a_z
WHERE bdate < '1924-01-01' or bdate > GETDATE() 

--- By Mistake the column name was inputted wrong so to manage that error we are repalcing
--- the name to the name the column belongs to.
--  EXEC sp_rename 'bronze.erp_cust_a_z.CNTRY', 'GEN', 'COLUMN';

-- Performing data standardization and consistency.
SELECT DISTINCT gen
FROM silver.erp_cust_a_z

SElect distinct gen
from silver.erp_cust_a_z;

-- ==================================================
-- Checking for 'silver.crm_cust_info'
-- ==================================================
-- Performing data standardization and consistency.
SELECT 
REPLACE(CID,'-','') as CID,
CNTRY
FROM bronze.erp_loc_a
Where REPLACE(CID,'-','') NOT IN (SELECT cst_key FROM silver.crm_cust_info)

SELECT DISTINCT CNTRY,
CASE WHEN TRIM(CNTRY) = 'DE' THEN 'GERMANY'
	WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
	WHEN CNTRY IS NULL or TRIM(CNTRY) = '' THEN 'n/a'
	ELSE TRIM(CNTRY)
END CNTRY
from bronze.erp_loc_a

SELECT DISTINCT CNTRY 
from bronze.erp_loc_a

-- ==================================================
-- Checking for 'silver.erp_px_cat_g'
-- ==================================================
-- Checking for the NUll and Duplicates.
SELECT 
ID 
FROM bronze.erp_px_cat_g
WHERE ID NOT IN (SELECT cat_id FROM silver.crm_prd_info);
-- None ordered pedals from us

SELECT 
ID, 
CAT,SUBCAT,MAINTENANCE
FROM bronze.erp_px_cat_g
where SUBCAT != TRIM(SUBCAT) or cat != TRIM(cat) or MAINTENANCE != TRIM(MAINTENANCE);

SELECT DISTINCT CAT 
FROM bronze.erp_px_cat_g;

SELECT DISTINCT SUBCAT 
FROM bronze.erp_px_cat_g;

SELECT DISTINCT MAINTENANCE 
FROM bronze.erp_px_cat_g;

SELECT * from silver.erp_px_cat_g;
