/*
################################################################
DDL script: Create Silver Tables
================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables.
    If they already exist.
  Run this script to re-define the DDL structure of 'silver' Tables.
################################################################
*/
/*
-- Using the Database where you need to create the table and store the data to.
*/

USE DataWarehouse;
GO

-- If table exist drop that table to build the schema of the table from scratch.
IF OBJECT_ID ('silver.crm_cust_info') is NOT NULL
	DROP Table silver.crm_cust_info;

-- Creating the Table in silver level to input data in that level.
-- Doing this for all 6 files we need to into and store that in thhe table.

Create table silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.crm_prd_info') IS NOT NULL
	DROP TAble silver.crm_prd_info

CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
				

IF OBJECT_ID ('silver.crm_sales_details') is NOT NULL
	DROP Table silver.crm_sales_details;

Create table silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price Float,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_cust_a_z') is NOT NULL
	DROP Table silver.erp_cust_a_z;
Create table silver.erp_cust_a_z(
	CID NVARCHAR(50),
	BDATE DATE,
	gen NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_loc_a') is NOT NULL
	DROP Table silver.erp_loc_a;
Create table silver.erp_loc_a(
	CID NVARCHAR(50),
	CNTRY NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);	
GO

IF OBJECT_ID ('silver.erp_px_cat_g') is NOT NULL
	DROP Table silver.erp_px_cat_g;
Create table silver.erp_px_cat_g(
	ID NVARCHAR(50),
	CAT NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO




