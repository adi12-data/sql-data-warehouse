/*
################################################################
DDL script: Create Bronze Tables
================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables.
    If they already exist.
  Run this script to re-define the DDL structure of 'bronze' Tables.
################################################################
*/
/*
-- Using the Database where you need to create the table and store the data to.
*/
USE DataWarehouse;
GO
/*
-- If table exist drop that table to build the schema of the table from scratch.
*/
IF OBJECT_ID ('bronze.crm_cust_info') is NOT NULL
	DROP Table bronze.crm_cust_info;
/*
-- Creating the Table in bronze level to input data in that level.
-- Doing this for all 6 files we need to into and store that in thhe table.
*/
Create table bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);
GO

IF OBJECT_ID ('bronze.crm_prd_info') is NOT NULL
	DROP Table bronze.crm_prd_info;

Create table bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);
GO					

IF OBJECT_ID ('bronze.crm_sales_details') is NOT NULL
	DROP Table bronze.crm_sales_details;
Create table bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price Float
);
GO

IF OBJECT_ID ('bronze.erp_loc_a') is NOT NULL
	DROP Table bronze.erp_loc_a;
Create table bronze.erp_loc_a(
	CID NVARCHAR(50),
	CNTRY NVARCHAR(50)
);	
GO

IF OBJECT_ID ('bronze.erp_cust_a_z') is NOT NULL
	DROP Table bronze.erp_cust_a_z;
Create table bronze.erp_cust_a_z(
	CID NVARCHAR(50),
	BDATE DATE,
	CNTRY NVARCHAR(50)
);
GO

IF OBJECT_ID ('bronze.erp_px_cat_g') is NOT NULL
	DROP Table bronze.erp_px_cat_g;
Create table bronze.erp_px_cat_g(
	ID NVARCHAR(50),
	CAT NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(50)
);
GO




