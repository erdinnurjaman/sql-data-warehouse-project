/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from bronze layer. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL load_silver;
===============================================================================
*/
CREATE OR REPLACE PROCEDURE load_silver() -- Create or replace procedure (if exist)
LANGUAGE plpgsql 						   -- Define language of procedure
AS $$									   -- create alias for store procedure
DECLARE start_time TIMESTAMP; end_time TIMESTAMP; duration INTERVAL;  -- Declare variable for catching duration of all stored procedure process
BEGIN																   -- Begin of load_silver store procedure
	start_time := clock_timestamp();								   -- Define variable for catching start time of load_silver store procedure process
	RAISE INFO 'Loading  Silver Layer';
	RAISE INFO '----------------- CRM Table -------------------';
	-- Inserting to s_crm_cust_info Table
	DECLARE ci_rows_insert INT; ci_start TIMESTAMP; ci_end TIMESTAMP; ci_duration INTERVAL;
	BEGIN
		ci_start := clock_timestamp();
		TRUNCATE TABLE s_crm_cust_info;
		
		INSERT INTO s_crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date
		)
		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE 
			WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_material_status,
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'n/a'
		END AS cst_gndr,
		DATE(cst_create_date)
		FROM (
			SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM b_crm_cust_info
		) t
		WHERE flag_last = 1;
		SELECT COUNT(*) INTO ci_rows_insert FROM s_crm_cust_info;
		ci_end := clock_timestamp();
		ci_duration := ci_end - ci_start;
		RAISE INFO '>> Truncating Table: s_crm_cust_info';
		RAISE INFO '>> Inserting data into: s_crm_cust_info for % rows and insert duration % seconds.', ci_rows_insert, ci_duration;
		RAISE INFO '-----------------------------------------------';
	EXCEPTION
		WHEN OTHERS THEN
		RAISE INFO '>> OPPSS.. FAILED INSERTING DATA';
	END;
		
		-- Inserting to s_crm_prd_info Table
	DECLARE pi_rows_insert INT; pi_start TIMESTAMP; pi_end TIMESTAMP; pi_duration INTERVAL;
	BEGIN
		pi_start := clock_timestamp();
		TRUNCATE TABLE s_crm_prd_info;
		
		INSERT INTO s_crm_prd_info (
			prd_id,
			cat_id,
		    prd_key,
		    prd_nm,
		    prd_cost,
		    prd_line,
		    prd_start_dt,
		    prd_end_dt
		)
		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id, -- Extract category ID
		SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,     -- Extract product key
		prd_nm,
		COALESCE(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'N/A'  -- Map product line codes to descriptive values
		END AS prd_line,
		prd_start_dt,
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM b_crm_prd_info;
		
		SELECT COUNT(*) INTO pi_rows_insert FROM s_crm_prd_info;
		pi_end := clock_timestamp();
		pi_duration := pi_end - pi_start;
		RAISE INFO '>> Truncating Table: s_crm_prd_info';
		RAISE INFO '>> Inserting data into: s_crm_prd_info for % rows and insert duration % seconds.', pi_rows_insert, pi_duration;
		RAISE INFO '-----------------------------------------------';
	EXCEPTION
		WHEN OTHERS THEN
		RAISE INFO '>> OPPSS.. FAILED INSERTING DATA';
	END;
		
		-- Inserting to s_crm_sales_detail Table
	DECLARE sd_rows_insert INT; sd_start TIMESTAMP; sd_end TIMESTAMP; sd_duration INTERVAL;
	BEGIN
		sd_start := clock_timestamp();
		TRUNCATE TABLE s_crm_sales_detail;
		
		INSERT INTO s_crm_sales_detail (
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
		CASE WHEN CAST(sls_order_dt AS INT) = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(sls_order_dt AS DATE)
		END AS sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
			 ELSE sls_price
		END AS sls_price
		FROM b_crm_sales_detail;

		SELECT COUNT(*) INTO sd_rows_insert FROM s_crm_sales_detail;
		sd_end := clock_timestamp();
		sd_duration := sd_end - sd_start;
		RAISE INFO '>> Truncating Table: s_crm_sales_detail';
		RAISE INFO '>> Inserting data into: s_crm_sales_detail for % rows and insert duration % seconds.', sd_rows_insert, sd_duration;
		RAISE INFO '-----------------------------------------------';
	EXCEPTION
		WHEN OTHERS THEN
		RAISE INFO '>> OPPSS.. FAILED INSERTING DATA';
	END;
		
		-- Inserting to s_erp_cust_az12
	DECLARE caz_rows_insert INT; caz_start TIMESTAMP; caz_end TIMESTAMP; caz_duration INTERVAL;
	BEGIN
		caz_start := clock_timestamp();
		TRUNCATE TABLE s_erp_cust_az12;
		
		INSERT INTO s_erp_cust_az12 (cid, bdate, gen)
		SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
			 ELSE cid
		END AS cid,
		CASE WHEN bdate > NOW() THEN NULL
			 ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			 WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			 ELSE 'N/A'
		END AS gen
		FROM b_erp_cust_az12;

		SELECT COUNT(*) INTO caz_rows_insert FROM s_erp_cust_az12;
		caz_end := clock_timestamp();
		caz_duration := caz_end - caz_start;
		RAISE INFO '----------------- ERP Table -------------------';
		RAISE INFO '>> Truncating Table: s_erp_cust_az12 ';
		RAISE INFO '>> Inserting data into: s_erp_cust_az12  for % rows and insert duration % seconds.', caz_rows_insert, caz_duration;
		RAISE INFO '-----------------------------------------------';
	EXCEPTION
		WHEN OTHERS THEN
		RAISE INFO '>> OPPSS.. FAILED INSERTING DATA';
	END;
		
		-- Inserting to s_erp_loc_a101 Table
	DECLARE loc_rows_insert INT; loc_start TIMESTAMP; loc_end TIMESTAMP; loc_duration INTERVAL;
	BEGIN
		loc_start := clock_timestamp();
		TRUNCATE TABLE s_erp_loc_a101;
		
		INSERT INTO s_erp_loc_a101 (cid, cntry)
		SELECT 
		REPLACE(cid, '-', '') AS cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
			 ELSE TRIM(cntry)
		END AS cntry
		FROM b_erp_loc_a101;

		SELECT COUNT(*) INTO loc_rows_insert FROM s_erp_loc_a101;
		loc_end := clock_timestamp();
		loc_duration := loc_end - loc_start;
		RAISE INFO '>> Truncating Table: s_erp_loc_a101';
		RAISE INFO '>> Inserting data into: s_erp_loc_a101 for % rows and insert duration % seconds.', loc_rows_insert, loc_duration;
		RAISE INFO '-----------------------------------------------';
	EXCEPTION
		WHEN OTHERS THEN
		RAISE INFO '>> OPPSS.. FAILED INSERTING DATA';
	END;
		
		-- Inserting to s_erp_px_cat_g1v2
	DECLARE px_rows_insert INT; px_start TIMESTAMP; px_end TIMESTAMP; px_duration INTERVAL;
	BEGIN
		px_start := clock_timestamp();
		TRUNCATE TABLE s_erp_px_cat_g1v2;
		
		INSERT INTO s_erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM b_erp_px_cat_g1v2;

		SELECT COUNT(*) INTO px_rows_insert FROM s_erp_px_cat_g1v2;
		px_end := clock_timestamp();
		px_duration := px_end - px_start;
		end_time := clock_timestamp();
		duration := end_time - start_time;
		RAISE INFO '>> Truncating Table: s_erp_px_cat_g1v2';
		RAISE INFO '>> Inserting data into: s_erp_px_cat_g1v2 for % rows and insert duration % seconds.', px_rows_insert, px_duration;
		RAISE INFO '-----------------------------------------------';
		RAISE INFO '>> Loading Duration of Silver Layer % seconds.', duration;
		RAISE INFO '-----------------------------------------------';
	EXCEPTION
		WHEN OTHERS THEN
		RAISE INFO '>> OPPSS.. FAILED INSERTING DATA';
	END;

END;
$$;