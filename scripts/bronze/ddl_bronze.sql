/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

-- Create CRM CUST Info Table
DROP TABLE IF EXISTS b_crm_cust_info;
CREATE TABLE b_crm_cust_info (
	cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_material_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date VARCHAR(50)
);

-- Create CRM PRD Info Table
DROP TABLE IF EXISTS b_crm_prd_info;
CREATE TABLE b_crm_prd_info (
	prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);

-- Create CRM Sales Detail Table
DROP TABLE IF EXISTS b_crm_sales_detail;
CREATE TABLE b_crm_sales_detail (
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt TEXT,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

-- Create ERP CUST_AZ12 Table
DROP TABLE IF EXISTS b_erp_cust_az12;
CREATE TABLE b_erp_cust_az12 (
	CID VARCHAR(50),	
    BDATE DATE,	
    GEN VARCHAR(50)
);

-- Create ERP LOC_A101 Table
DROP TABLE IF EXISTS b_erp_loc_a101; 
CREATE TABLE b_erp_loc_a101 (
	CID VARCHAR(50),	
    CNTRY VARCHAR(50)
);

-- Create ERP PX_CAT_G1V2 Table
DROP TABLE IF EXISTS b_erp_px_cat_g1v2;
CREATE TABLE b_erp_px_cat_g1v2 (
	ID VARCHAR(50),	
    CAT VARCHAR(50),	
    SUBCAT VARCHAR(50),	
    MAINTENANCE VARCHAR(50)
);
