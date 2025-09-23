/*
################################################################
Stored Procedure: Load Silver Layes (Source -> Silver)
================================================================
Script Purpose:
    The stored procedure performs the ETL (Extract, Transform, Load) 
    process to populate the 'silver' schema from the 'bronze' schema.
Action Performed:
  - Truncate Silver tables.
  - Inserts transformed and cleansed data from Bronze into Silver tables.
  
Parameters: None
    This stored procedure does not accept any parameters or return any values.
Usage: At the end run one line after executing this procedure 
    EXEC silver.load_silver;
This code will run and transform the data from the 'bronze' layer to 'silver' layer 
and create the data that is ready to be used by the Data Analyst and the Engineers.
################################################################
-- Use <Database name> in which data need to be loaded
*/

Use DataWarehouse;
-- THe line of code given below you can run it after creating and saving the procedure.
--EXEC silver.load_silver;
-- The will run full block of code and transform the data at once.

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	SET @batch_start_time = GETDATE();
	BEGIN TRY
		PRINT '----------------------------------------------';
		PRINT 'Loading SILVER Layer';
		PRINT '----------------------------------------------';

		PRINT '----------------------------------------------';
		PRINT 'Transforming CRM Tables';
		PRINT '----------------------------------------------';

		SET @start_time= GETDATE();

		PRINT '>> TRUNCATING TABLE: silver.crm_cust_info';
		TRUNCATE TABLE  silver.crm_cust_info;

		PRINT '>> INSERTING TABLE: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S'  THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			Else 'n/a'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F'  THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			Else 'n/a'
		END cst_gndr,
		cst_create_date
		FROM(
		SELECT 
		*,
		ROW_NUMBER() Over (partition by cst_id order by cst_create_date desc) as flag_test
		FROM bronze.crm_cust_info
		where cst_id IS NOT NULL
		)t WHERE flag_test = 1;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '_____________________'

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: silver.crm_prd_info';
		TRUNCATE TABLE  silver.crm_prd_info;
	
		PRINT '>> INSERTING TABLE: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost ,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) as prd_cost,
		CASE UPPER(TRIM(prd_line)) 
			WHEN 'R' THEN 'Road'
			WHEN 'M' THEN 'Mountain'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END as prd_line,
		CAST(prd_start_dt as DATE) as prd_start_dt,
		CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (
			PARTITION BY prd_key 
			ORDER BY prd_start_dt
			)) AS DATE) AS prd_end_dt
		from bronze.crm_prd_info;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '_____________________'

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: silver.crm_sales_details';
		TRUNCATE TABLE  silver.crm_sales_details;

		PRINT '>> INSERTING TABLE: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			[sls_prd_key],
			[sls_cust_id],
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_quantity,
			sls_sales,
			sls_price
		)
		SELECT [sls_ord_num]
			  ,[sls_prd_key]
			  ,[sls_cust_id]
			  ,CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
				END AS sls_order_dt
			  ,CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
				END AS sls_ship_dt
			  ,CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
				END AS sls_due_dt
			  ,[sls_quantity]
			  ,CASE WHEN sls_sales is NULl or sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
						THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
				END AS sls_sales
				,CASE WHEN sls_price is NULL OR sls_price <= 0 
						THEN sls_sales / NULLIF(sls_quantity,0)
					ELSE ABS(sls_price)
				END AS sls_price
		  FROM [DataWarehouse].[bronze].[crm_sales_details];
		SET @end_time = GETDATE();
		PRINT '>> Load Duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		PRINT '----------------------------------------------';
		print 'Loading ERP Tables';
		print '----------------------------------------------';

		SET @start_time = GETDATE(); 
		PRINT '>> TRUNCATING TABLE: silver.erp_cust_a_z';
		TRUNCATE TABLE  silver.erp_cust_a_z;
	
		PRINT '>> INSERTING TABLE: silver.erp_cust_a_z';
		INSERT INTO silver.erp_cust_a_z(
			CID,
			BDATE,
			gen
		)
		SELECT 
		CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,len(CID))
			ELSE CID
		END as CID,
		CASE WHEN BDATE > GETDATE() THEN NULL
			ELSE bdate
		END AS BDATE,
		CASE 
			WHEN UPPER(TRIM(GEN)) IN ('F', 'Female') THEN 'Female'
			WHEN UPPER(TRIM(GEN)) IN ('M', 'Male') THEN 'Male'
			ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_a_z;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '_____________________'

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: silver.erp_loc_a';
		TRUNCATE TABLE  silver.erp_loc_a;
	
		PRINT '>> INSERTING TABLE: silver.erp_loc_a';
		INSERT INTO silver.erp_loc_a(CID,CNTRY)
		SELECT 
			REPLACE(CID,'-','') as CID,
			CASE 
				WHEN TRIM(CNTRY) = 'DE' THEN 'GERMANY'
				WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
				WHEN CNTRY IS NULL or TRIM(CNTRY) = '' THEN 'n/a'
				ELSE TRIM(CNTRY)
			END as CNTRY
		FROM bronze.erp_loc_a
		SET @end_time = GETDATE();
		PRINT '>> Load Duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '_____________________'

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: silver.erp_px_cat_g';
		TRUNCATE TABLE  silver.erp_px_cat_g;
	
		PRINT '>> INSERTING TABLE: silver.erp_px_cat_g';
		INSERT INTO silver.erp_px_cat_g(
		ID, 
		CAT,
		SUBCAT,
		MAINTENANCE
		)
		SELECT 
		ID, 
		CAT,
		SUBCAT,
		MAINTENANCE
		FROM bronze.erp_px_cat_g
		SET @end_time = GETDATE();
		PRINT '>> Load Duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '_____________________'

		PRINT '>> Transformation of Data Complete >>';
	END TRY
	BEGIN CATCH 
		PRINT '=====================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_Message();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=====================================';
	END CATCH
	set @batch_end_time = GETDATE()
	PRINT '-> Load Duration of data:' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as NVARCHAR)
	PRINT '_____________________'
END;
