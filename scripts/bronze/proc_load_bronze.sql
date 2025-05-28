/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL load_bronze;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE start_time TIMESTAMP; end_time TIMESTAMP; duration INTERVAL;
BEGIN
	start_time := clock_timestamp();
	RAISE INFO 'Loading  Bronze Layer';
	RAISE INFO '----------------- CRM Table -------------------';
	-- Blok CRM CUST INFO
	DECLARE ci_rows_insert INT; ci_start TIMESTAMP; ci_end TIMESTAMP; ci_duration INTERVAL;
	BEGIN
		ci_start := clock_timestamp();
		TRUNCATE TABLE b_crm_cust_info;
	
		COPY b_crm_cust_info
		FROM 'C:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		DELIMITER ','
		CSV HEADER;
		SELECT COUNT(*) INTO ci_rows_insert FROM b_crm_cust_info;
		ci_end := clock_timestamp();
		ci_duration := ci_end - ci_start;
		RAISE INFO '>> Truncating Table: b_crm_cust_info';
		RAISE INFO '>> Inserting data into: b_crm_cust_info for % rows and insert duration % seconds.', ci_rows_insert, ci_duration;
		RAISE INFO '-----------------------------------------------';
	EXCEPTION
		WHEN OTHERS THEN
		RAISE INFO '>> OPPSS.. FAILED INSERTING DATA';
	END;

	-- Blok CRM PRD INFO
	DECLARE pi_rows_insert INT; pi_start TIMESTAMP; pi_end TIMESTAMP; pi_duration INTERVAL;
	BEGIN
		pi_start := clock_timestamp();
		TRUNCATE TABLE b_crm_prd_info;
		
		COPY b_crm_prd_info
		FROM 'C:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		DELIMITER ','
		CSV HEADER;
		SELECT COUNT(*) INTO pi_rows_insert FROM b_crm_prd_info;
		pi_end := clock_timestamp();
		pi_duration := pi_end - pi_start;
		RAISE INFO '>> Truncating Table: b_crm_prd_info';
		RAISE INFO '>> Inserting Data into: b_crm_prd_info for % rows and insert duration % seconds.', pi_rows_insert, pi_duration;
		RAISE INFO '-----------------------------------------------';
	EXCEPTION
		WHEN OTHERS THEN
		RAISE INFO '>> OPPSS.. FAILED INSERTING DATA';
	END;

	-- Blok CRM SALES DETAIL
	DECLARE sd_rows_insert INT; sd_start TIMESTAMP; sd_end TIMESTAMP; sd_duration INTERVAL;
	BEGIN
		sd_start := clock_timestamp();
		TRUNCATE TABLE b_crm_sales_detail;
		
		COPY b_crm_sales_detail
		FROM 'C:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		DELIMITER ','
		CSV HEADER;
		SELECT COUNT(*) INTO sd_rows_insert FROM b_crm_sales_detail;
		sd_end := clock_timestamp();
		sd_duration := sd_end - sd_start;
		RAISE INFO '>> Truncating Table: b_crm_sales_detail';
		RAISE INFO '>> Inserting Data into: b_crm_sales_detail for % rows and insert duration % seconds', sd_rows_insert, sd_duration;
		RAISE INFO '-----------------------------------------------';
	EXCEPTION
		WHEN OTHERS THEN
		RAISE INFO '>> OPPSS.. FAILED INSERTING DATA';
	END;

	RAISE INFO '----------------- ERP Table -------------------';
	
	-- Blok ERP CUST AZ12
	DECLARE caz_rows_insert INT; caz_start TIMESTAMP; caz_end TIMESTAMP; caz_duration INTERVAL;
	BEGIN
		caz_start := clock_timestamp();
		TRUNCATE TABLE b_erp_cust_az12;
		
		COPY b_erp_cust_az12
		FROM 'C:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		DELIMITER ','
		CSV HEADER;
		SELECT COUNT(*) INTO caz_rows_insert FROM b_erp_cust_az12;
		caz_end := clock_timestamp();
		caz_duration := caz_end - caz_start;
		RAISE INFO '>> Truncating Table: b_erp_cust_az12';
		RAISE INFO '>> Inserting Data into: b_erp_cust_az12 for % rows and insert duration % seconds.', caz_rows_insert, caz_duration;
		RAISE INFO '-----------------------------------------------';
	EXCEPTION
		WHEN OTHERS THEN
		RAISE INFO '>> OPPSS.. FAILED INSERTING DATA';
	END;
		
	-- Blok ERP LOC A101
	DECLARE loc_rows_insert INT; loc_start TIMESTAMP; loc_end TIMESTAMP; loc_duration INTERVAL;
	BEGIN
		loc_start := clock_timestamp();
		TRUNCATE TABLE b_erp_loc_a101;
		
		COPY b_erp_loc_a101
		FROM 'C:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		DELIMITER ','
		CSV HEADER;
		SELECT COUNT(*) INTO loc_rows_insert FROM b_erp_loc_a101;
		loc_end := clock_timestamp();
		loc_duration := loc_end - loc_start;
		RAISE INFO '>> Truncating Table: b_erp_loc_a101';
		RAISE INFO '>> Inserting Data into: b_erp_loc_a101 for % rows and insert duration % seconds.', loc_rows_insert, loc_duration;
		RAISE INFO '-----------------------------------------------';
	EXCEPTION
		WHEN OTHERS THEN
		RAISE INFO '>> OPPSS.. FAILED INSERTING DATA';
	END;
	
	-- Blok ERP PX CAT G1V2
	DECLARE px_rows_insert INT; px_start TIMESTAMP; px_end TIMESTAMP; px_duration INTERVAL;
	BEGIN
		px_start := clock_timestamp();
		TRUNCATE TABLE b_erp_px_cat_g1v2;
		
		COPY b_erp_px_cat_g1v2
		FROM 'C:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		DELIMITER ','
		CSV HEADER;
		SELECT COUNT(*) INTO px_rows_insert FROM b_erp_px_cat_g1v2;
		px_end := clock_timestamp();
		px_duration := px_end - px_start;
		end_time := clock_timestamp();
		duration := end_time - start_time;
		RAISE INFO '>> Truncating Table: b_erp_px_cat_g1v2';
		RAISE INFO '>> Inserting Data into: b_erp_px_cat_g1v2 for % rows and insert duration % seconds.', px_rows_insert, px_duration;
		RAISE INFO '-----------------------------------------------';
		RAISE INFO '>> Loading Duration of Bronze Layer % seconds.', duration;
		RAISE INFO '-----------------------------------------------';
	EXCEPTION
		WHEN OTHERS THEN
		RAISE INFO '>> OPPSS.. FAILED INSERTING DATA';
	END;
	
END;
$$;
