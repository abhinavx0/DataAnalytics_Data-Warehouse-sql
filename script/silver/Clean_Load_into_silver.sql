-- Cleaning The Data.
-- Removing the Duplicate Values.
-- Altering the Table if Possible.
-- Inserting into Silver Layer.


-- ==============================================
-- Clean & Load crm_cust_info into silver Layers.
-- ==============================================



Insert into silver.crm_cust_info (
	cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gender,
    cst_creation_date
)
select 
	cst_id,
    cst_key,
    trim(cst_firstname) as cst_firstname,
    trim(cst_lastname) as cst_lastname,
    CASE when UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
		 when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
		 else 'N/A'
	END cst_marital_status, -- Normalize The Marital Status value to readable format
	CASE when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
		 when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
		 else 'N/A'  
	END cst_gndr,  -- Normalize The Gender values to readable format
    cst_create_date
from (
	Select *,
	row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info
)t where flag_last = 1; -- Select the most recent record per customers

-- removing the unwanted spaces
-- Expectation: No results

-- Results: Spaces Found.
Select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname);

-- Results: Spaces Found.
Select cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname);

-- Results: No Spaces found.
Select cst_gndr
from bronze.crm_cust_info
where cst_gndr != trim(cst_gndr);

-- Results: No Spaces found.
Select cst_key
from bronze.crm_cust_info
where cst_key != trim(cst_key);	

-- Data Standarisation & Consistency
-- Here we will Not use gender abbrebration instead we will use full abbrebration.
-- Checking Distinct Columns.
Select Distinct cst_gndr
from bronze.crm_cust_info;


-- Data Standarisation & Consistency
-- Here we will Not use Material Status abbrebration instead we will use full abbrebration.
-- Checking Distinct Columns.
Select Distinct cst_material_status
from bronze.crm_cust_info;


-- ==============================================
-- Clean & Load crm_prd_info into Silver Layers.
-- ==============================================
-- Alter the Table according to the Need.
USE silver;
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_date DATE,
    prd_end_date DATE,
    dwh_create_date Timestamp default now()
);

-- Clean & Load crm_prd_info into Silver Layers.
INSERT INTO silver.crm_prd_info
(
	prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_date,
    prd_end_date
)
select 
	prd_id,
   Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id, -- Extracted Category id.
   SUBSTRING(prd_key,7,length(prd_key)) as prd_key, -- Extracted Product Key.
    prd_nm,
    coalesce(prd_cost,0) as prd_cost, -- 
    CASE UPPER(TRIM(prd_line))
		 WHEN 'M' then 'Mountain'
		 WHEN 'R' then 'Road'
         WHEN 'S' then 'Other Sales'
         WHEN 'T' then 'Touring'
	ELSE 'N/A'
    END as prd_line, -- MAP product line codes to descriptive values.
    prd_start_dt,
    lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) as prd_end_dt
from bronze.crm_prd_info;

-- Removing the Duplicate Values from Primary key ie here prd_id.
-- Expectation : No Results
-- Results : No Duplicate found.
Select * from (
	Select 
		prd_id,
		count(*) as flag
	from bronze.crm_prd_info
	group by prd_id
)t where flag > 1 OR prd_id is null;

-- Removing the unwanted spaces.
-- Expectation: No Results.
-- Results : No Space Found.
Select 
	prd_key
from bronze.crm_prd_info
where prd_key != TRIM(prd_key);

-- Removing the unwanted spaces.
-- Expectation: No Results.
-- Results : No Space Found.
Select 
	prd_nm
from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm);

-- Check for nulls or Negative values.
-- Expectation : no results.
-- Results: 2 NULLS found AND NO NEGATIVE VALUES.
select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 OR prd_cost IS NULL;

-- Data Standarisation & Consistency
-- Here we will Not use Product abbrebration instead we will use full abbrebration.
-- Checking Distinct Columns.
Select DISTINCT prd_line
from bronze.crm_prd_info;

-- checking Invalid Date Orders.
-- Expectation : no Results
-- Results : Invalid Dates found.
Select *,
	lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) - 1 as prd_end_dat_test
from bronze.crm_prd_info
where prd_start_dt > prd_end_dt;


-- ==================================================
-- Clean & Load crm_sales_details into Silver Layers.
-- ==================================================
-- Alter the Table according to the Need.
DROP TABLE IF EXISTS crm_sales_details;
CREATE TABLE crm_sales_details (
	sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date Timestamp default now()
);

-- Inserting the Cleaned data into the Table 
INSERT INTO silver.crm_sales_details
(
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
		 cast(sls_order_dt as DATE) as sls_order_dt ,
		 cast(sls_ship_dt as DATE) as sls_ship_dt,
		 cast(sls_due_dt as DATE) as sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
     END AS sls_sales, -- RECTIFYING THE ERROR IN sls_sales columns.
    sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
		 THEN sls_sales / NULLIF(sls_quantity,0)
		 ELSE sls_price
    END AS sls_price -- RECTIFYING THE ERROR IN sls_price columns.
FROM bronze.crm_sales_details;

-- Removing the unwanted spaces.
-- Expectation: No Results.
-- Results : No Space Found.
SELECT 
	sls_ord_num
from bronze.crm_sales_details
where sls_ord_num != TRIM(sls_ord_num) ;

-- Checking whether our sls_prd_key are found in the silver tables.
SELECT 
	sls_prd_key
FROM bronze.crm_sales_details
where sls_prd_key in (select prd_key from silver.crm_prd_info);
-- Checking whether our sls_cust_id are found in the silver tables.
SELECT 
	sls_cust_id
FROM bronze.crm_sales_details
where sls_cust_id in (select cst_id from silver.crm_cust_info);

-- Checking Invalid dates
-- Expectation : no results.
-- Results : NO Invalid Dates.
Select 
	sls_order_dt
from bronze.crm_sales_details
where sls_order_dt < 0
OR length (sls_order_dt) != 10;

Select *
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_dt;

-- Checking Data Consistency : sales, price , quantity.
-- >> Sales = quantity * price
-- >> Price must not be negative, null or Zero. 

Select DISTINCT
	 sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
Where sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales,sls_quantity,sls_price;

-- APPLYING RULES TO RECTIFY THE ABOVE ERROR.
Select DISTINCT
	 sls_sales as old_sls_sales,
	 CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		  THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales
     END AS sls_sales, -- RECTIFYING THE ERROR IN sls_sales columns.
    sls_quantity,
    sls_price as old_sls_price,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
    THEN sls_sales / NULLIF(sls_quantity,0)
    ELSE sls_price
    END AS sls_price -- RECTIFYING THE ERROR IN sls_price columns.
FROM bronze.crm_sales_details
Where sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales,sls_quantity,sls_price;


-- ==============================================
-- Clean & Load erp_cust_az12 into Silver Layers.
-- ==============================================
-- Alter the Table according to the Need.
-- Inserting the Values into the Tables.
use silver;

INSERT INTO silver.erp_cust_az12
(
	cid,
    bdate,
    gen
)
-- Cleaning the Data.
SELECT 
    CASE WHEN cid LIKE 'NAS%' then SUBSTRING(cid,4,length(cid))
		 ELSE cid
	END as cid, -- Extracted the Customer Key from cid.
    CASE WHEN bdate > now() then null
		 ELSE bdate
	END as bdate, -- Removed the customer who's bdate is in future.
    CASE when UPPER(TRIM(gen)) in  ('M','MALE') then 'Male'
		 when UPPER(TRIM(gen)) in ('F','FEMALE') then 'Female'
		 else 'N/A'  
	END as gen -- -- Normalize The Gender values to readable format
FROM bronze.erp_cust_az12;


-- Checking Bday which are out of dates
SELECT DISTINCT 
	bdate
FROM bronze.erp_cust_az12
where bdate < 1924-01-01 OR bdate > now();

-- Data Standarisation & Consistency
-- Here we will Not use Product abbrebration instead we will use full abbrebration.
-- Checking Distinct Columns.
Select DISTINCT
	gen,
    CASE when UPPER(TRIM(gen)) in  ('M','MALE') then 'Male'
		 when UPPER(TRIM(gen)) in ('F','FEMALE') then 'Female'
		 else 'N/A'  
	END as gen_
from bronze.erp_cust_az12;



-- ==============================================
-- Clean & Load erp_loc_a101 into Silver Layers.
-- ==============================================
USE SILVER;
-- Inserting the data into the TABle.
INSERT INTO silver.erp_loc_a101
(
	CID,
    CNTRY
)

-- Cleaning the Data.
SELECT 
-- CONCAT(SUBSTRING(CID,1,2),SUBSTRING(CID,4,Length(CID))) as CID,
    Replace (CID, '-','') as CID,
	 CASE 
        WHEN UPPER(TRIM(CNTRY)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
        WHEN UPPER(TRIM(CNTRY)) IN ('GERMANY', 'DE') THEN 'Germany'
        WHEN UPPER(TRIM(CNTRY)) IS NULL or UPPER(TRIM(CNTRY)) = '' then 'N/A'
        ELSE (TRIM(CNTRY))
    END AS CNTRY -- Normalized and Handled missing or blank country code.
FROM bronze.erp_loc_a101;

-- Checking for Duplicates.
Select * from (
SELECT CID,
count(*) over(partition by CID) FLAG
FROM bronze.erp_loc_a101
)t where FLAG > 1;

-- Data Standarisation & Consistency
-- Here we will Not use CNTRY abbrebration instead we will use full abbrebration.
-- Checking Distinct Columns.
SELECT DISTINCT 
    CNTRY,
    CASE 
        WHEN UPPER(TRIM(CNTRY)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
        WHEN UPPER(TRIM(CNTRY)) IN ('GERMANY', 'DE') THEN 'Germany'
        WHEN UPPER(TRIM(CNTRY)) IS NULL or UPPER(TRIM(CNTRY)) = '' then 'N/A'
        ELSE (TRIM(CNTRY))
    END AS CNTRY_new
FROM bronze.erp_loc_a101;



-- ==============================================
-- Clean & Load erp_px_cat_g1v2 into Silver Layers.
-- ==============================================
-- Inserting into the Table silver.erp_px_cat_g1v2
INSERT INTO silver.erp_px_cat_g1v2 
(
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

-- Check for unwanted steps.
-- Expectation: no Results
-- Results: no Unwanted steps found.
SELECT cat
FROM bronze.erp_px_cat_g1v2
where cat != TRIM(cat)
OR subcat != TRIM(subcat) 
OR maintenance != TRIM(maintenance);

-- Data Standarization & Consistency.
-- Results : none Found.
Select DISTINCT cat
FROM bronze.erp_px_cat_g1v2;
Select DISTINCT subcat
FROM bronze.erp_px_cat_g1v2;
Select DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;





