/*
################################################################
Stored Procedure: Load Bronze Layes (Source -> Bronze)
================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncate the old data from the table before loading the main data file in that table.
    - Using BULK insert we are inputing whole data together as it is very fast to do so and 
    - it is different from normal insert as that insert will take data row by row and than add in the table.
    - This is followed for all 6 files to input data in the table.
Parameters: None
    This stored procedure does not accept any parameters or return any values.
Usage: At the end run one line after executing this procedure 
    EXEC bronze.load_bronze;
This will load the data from the source to the database.
To use change the file path given in that code script.
################################################################
-- Use <Database name> in which data need to be loaded
*/

Use DataWarehouse;
GO

Create or ALter Procedure bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	SET @batch_start_time = GETDATE();
	BEGIN TRY
		PRINT '----------------------------------------------';
		print 'Loading Bronze Layer';
		print '----------------------------------------------';

		PRINT '----------------------------------------------';
		print 'Loading CRM Tables';
		print '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE  bronze.crm_cust_info;

		PRINT '>> Inserting Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\legio\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			Tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '_____________________'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE  bronze.crm_prd_info;

		PRINT '>> Inserting Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\legio\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			Tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '_____________________'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE  bronze.crm_sales_details;
	
		PRINT '>> Inserting Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\legio\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			Tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '_____________________'


		PRINT '----------------------------------------------';
		print 'Loading ERP Tables';
		print '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_a_z';
		TRUNCATE TABLE  bronze.erp_cust_a_z;

		PRINT '>> Inserting Table: bronze.erp_cust_a_z';
		BULK INSERT bronze.erp_cust_a_z
		FROM 'C:\Users\legio\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			Tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '_____________________'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a';
		TRUNCATE TABLE  bronze.erp_loc_a;

		PRINT '>> Inserting Table: bronze.erp_loc_a';
		BULK INSERT bronze.erp_loc_a
		FROM 'C:\Users\legio\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			Tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '_____________________'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g';
		TRUNCATE TABLE  bronze.erp_px_cat_g;
	
		PRINT '>> Inserting Table: bronze.erp_px_cat_g';
		BULK INSERT bronze.erp_px_cat_g
		FROM 'C:\Users\legio\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			Tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '_____________________'

		PRINT '>> Loading Data Complete >>';
	END TRY
	BEGIN CATCH

		PRINT '=====================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_Message();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=====================================';
/*
		-- We are just printing the error message, number and state which are useful for checking what type of error we are getting.
		-- Instead what we can do is that we can make the error to save in the table format so that all error can be saved and we can debug them together.
*/
	END CATCH
	SET @batch_end_time = GETDATE();
	PRINT '-> Load Duration of data:' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds';
	PRINT '_____________________'
END;
/*
-- Remainder: In WITH(...) we can add more conditions and rules which explain our data well like in this data we have
-- 1st row as field names and field determinator is how data is separted as we have csv comma separted file we have ',' here.
-- Use print statement where ever possible as it makes the code readable and 
-- allow us to get which part of code is running and where the system stop in case of some error may be.

-- After constructing all these we add try and catch to ensure error handling, data integrity and issue logging for easier debugging.


-- Run this line at the end it will run whole code.
-- Run this code below -------
--EXEC bronze.load_bronze;
-- >^ only this one -----
*/
