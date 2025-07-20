DELIMITER $$

DROP PROCEDURE IF EXISTS load_bronze $$
CREATE PROCEDURE load_bronze()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;

    SET batch_start_time = NOW();
    SELECT '=== Loading Bronze Layer Started ===';

    -- ---------------------
    -- CRM Tables
    -- ---------------------

    -- crm_cust_info
    SET start_time = NOW();
    TRUNCATE TABLE crm_cust_info;
    LOAD DATA INFILE '/var/lib/mysql-files/cust_info.csv'
    INTO TABLE crm_cust_info
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('crm_cust_info loaded in ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' sec');

    -- crm_prd_info
    SET start_time = NOW();
    TRUNCATE TABLE crm_prd_info;
    LOAD DATA INFILE '/var/lib/mysql-files/prd_info.csv'
    INTO TABLE crm_prd_info
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('crm_prd_info loaded in ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' sec');

    -- crm_sales_details
    SET start_time = NOW();
    TRUNCATE TABLE crm_sales_details;
    LOAD DATA INFILE '/var/lib/mysql-files/sales_details.csv'
    INTO TABLE crm_sales_details
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('crm_sales_details loaded in ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' sec');

    -- ---------------------
    -- ERP Tables
    -- ---------------------

    -- erp_loc_a101
    SET start_time = NOW();
    TRUNCATE TABLE erp_loc_a101;
    LOAD DATA INFILE '/var/lib/mysql-files/LOC_A101.csv'
    INTO TABLE erp_loc_a101
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('erp_loc_a101 loaded in ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' sec');

    -- erp_cust_az12
    SET start_time = NOW();
    TRUNCATE TABLE erp_cust_az12;
    LOAD DATA INFILE '/var/lib/mysql-files/CUST_AZ12.csv'
    INTO TABLE erp_cust_az12
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('erp_cust_az12 loaded in ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' sec');

    -- erp_px_cat_g1v2
    SET start_time = NOW();
    TRUNCATE TABLE erp_px_cat_g1v2;
    LOAD DATA INFILE '/var/lib/mysql-files/PX_CAT_G1V2.csv'
    INTO TABLE erp_px_cat_g1v2
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('erp_px_cat_g1v2 loaded in ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' sec');

    -- Done
    SET batch_end_time = NOW();
    SELECT '=== Bronze Layer Load Completed ===';
    SELECT CONCAT('Total Duration: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' sec');
END $$
DELIMITER ;
