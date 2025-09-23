/*
################################################################
DDL script: Create Gold Tables
================================================================
Script Purpose:
    This script creates tables in the 'GOLD' Layer in the data warehouse.
    The Gold layer represents the final dimension and fact tables (Star Schema).
    Each view performs transformations and combines data from the Silver layer to 
    produce a clean, enriched, and business-ready dataset.
Usage:
  - These views can be queried directly for analytics and reporting.
################################################################
*/

-- ============================================================
-- Create Dimension: gold.dim_customers
-- ============================================================
CREATE VIEW gold.dim_customers AS
select 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id ,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	cl.CNTRY AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' then ci.cst_gndr -- CRM is the Master for gender info.
		ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ca.BDATE AS birthday,
	ci.cst_create_date AS create_date	
from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_a_z ca
ON		  ci.cst_key = ca.CID
left join silver.erp_loc_a cl
ON		  ci.cst_key = cl.CID;
-- THis object is the Dimension Customer

-- ============================================================
-- Create Dimension: gold.dim_products
-- ============================================================
CREATE VIEW gold.dim_products AS
SELECT 
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) as product_key,
pn.[prd_id] AS product_id,
pn.[prd_key] AS product_number,
pn.[prd_nm] AS product_name,
pn.[cat_id] AS category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.MAINTENANCE as maintenance,
pn.[prd_cost] as cost,
pn.[prd_line] as product_line,
pn.[prd_start_dt] as start_date
FROM [DataWarehouse].[silver].[crm_prd_info] pn
LEFT JOIN silver.erp_px_cat_g pc
on pn.cat_id = pc.ID
WHERE prd_end_dt is NULL; -- Filtered out all historical data. 

-- ============================================================
-- Create Dimension: gold.fact_sales
-- ============================================================
CREATE VIEW gold.fact_sales as
select
sd.[sls_ord_num] AS order_number
,pr.product_key 
,cu.customer_key
,sd.[sls_order_dt] as order_date
,sd.[sls_ship_dt] as shipping_date
,sd.[sls_due_dt] as due_date
,sd.[sls_sales] as sales_amount
,sd.[sls_quantity] as quantity
,sd.[sls_price] as price
FROM [DataWarehouse].[silver].[crm_sales_details] sd
LEFT JOIN gold.dim_products pr
on sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id;
