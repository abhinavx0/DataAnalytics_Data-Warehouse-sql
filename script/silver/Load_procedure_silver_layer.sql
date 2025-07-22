-- =================================================================================================
-- Stored Procedure: sp_load_silver_layer
-- Description: Cleans and transforms data from the bronze layer tables and loads it into the
--              corresponding silver layer tables. This includes handling duplicates, standardizing
--              values, rectifying data errors, and structuring the data for analytical use.
-- Create Date: 2024-07-23
-- =================================================================================================

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_load_silver_layer;
CREATE PROCEDURE sp_load_silver_layer()
BEGIN

    -- ==============================================
    -- Section 1: Clean & Load crm_cust_info
    -- ==============================================
    -- Description: Cleans customer information, normalizes gender and marital status,
    --              and loads the most recent record for each customer into the silver layer.

    -- Note: Assuming silver.crm_cust_info table is pre-created and truncated before run.
    -- For idempotency, you might add a TRUNCATE TABLE statement here.
    -- TRUNCATE TABLE silver.crm_cust_info;
	Truncate Table silver.crm_cust_info;
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_material_status,
        cst_gender,
        cst_creation_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'N/A'
        END AS cst_marital_status, -- Normalize Marital Status
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'N/A'
        END AS cst_gndr, -- Normalize Gender
        cst_create_date
    FROM (
        SELECT *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
    ) t
    WHERE flag_last = 1; -- Select the most recent record for each customer


    -- ==============================================
    -- Section 2: Clean & Load crm_prd_info
    -- ==============================================
    -- Description: Recreates the product info table, extracts category and product keys,
    --              normalizes product lines, handles null costs, and calculates product
    --              end dates before loading into the silver layer.

	Truncate Table silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_date,
        prd_end_date
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract Category ID
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key, -- Extract Product Key
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost, -- Replace NULL cost with 0
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'N/A'
        END AS prd_line, -- Map product line codes to descriptive values
        prd_start_dt,
        LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt -- Calculate end date
    FROM bronze.crm_prd_info;


    -- ==================================================
    -- Section 3: Clean & Load crm_sales_details
    -- ==================================================
    -- Description: Recreates the sales details table, casts data types, and rectifies
    --              errors in sales and price calculations before loading.

	Truncate Table silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (
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
        CAST(sls_order_dt AS DATE) AS sls_order_dt,
        CAST(sls_ship_dt AS DATE) AS sls_ship_dt,
        CAST(sls_due_dt AS DATE) AS sls_due_dt,
        CASE
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales, -- Rectify sales calculation errors
        sls_quantity,
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price -- Rectify price calculation errors
    FROM bronze.crm_sales_details;


    -- ==============================================
    -- Section 4: Clean & Load erp_cust_az12
    -- ==============================================
    -- Description: Cleans ERP customer data by standardizing customer keys,
    --              nullifying invalid future birth dates, and normalizing gender values.

    -- Note: Assuming silver.erp_cust_az12 is pre-created and truncated.
    -- TRUNCATE TABLE silver.erp_cust_az12;
	Truncate Table silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
        END AS cid, -- Extract the Customer Key from cid
        CASE
            WHEN bdate > NOW() THEN NULL
            ELSE bdate
        END AS bdate, -- Nullify future birth dates
        CASE
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            ELSE 'N/A'
        END AS gen -- Normalize Gender values
    FROM bronze.erp_cust_az12;


    -- ==============================================
    -- Section 5: Clean & Load erp_loc_a101
    -- ==============================================
    -- Description: Cleans location data by standardizing customer IDs and country names.

    -- Note: Assuming silver.erp_loc_a101 is pre-created and truncated.
    -- TRUNCATE TABLE silver.erp_loc_a101;
	Truncate Table silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (
        CID,
        CNTRY
    )
    SELECT
        REPLACE(CID, '-', '') AS CID, -- Remove hyphens for a standard format
        CASE
            WHEN UPPER(TRIM(CNTRY)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
            WHEN UPPER(TRIM(CNTRY)) IN ('GERMANY', 'DE') THEN 'Germany'
            WHEN UPPER(TRIM(CNTRY)) IS NULL OR UPPER(TRIM(CNTRY)) = '' THEN 'N/A'
            ELSE (TRIM(CNTRY))
        END AS CNTRY -- Normalize country names
    FROM bronze.erp_loc_a101;


    -- ==============================================
    -- Section 6: Clean & Load erp_px_cat_g1v2
    -- ==============================================
    -- Description: Loads product category data directly into the silver layer.
    --              No complex transformations are required based on the source script.

    -- Note: Assuming silver.erp_px_cat_g1v2 is pre-created and truncated.
    -- TRUNCATE TABLE silver.erp_px_cat_g1v2;
	Truncate Table silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

END$$

DELIMITER ;

CALL sp_load_silver_layer;
