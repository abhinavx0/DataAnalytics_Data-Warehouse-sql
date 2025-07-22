-- Creating The golden Layer with the help of View.


-- ============================
-- Creating The Customer Table.
-- ============================

CREATE VIEW gold.dim_customers as 
SELECT
	ROW_NUMBER() over(order by cst_id) as customer_key, -- Creating the Surrogate Key.
	ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry as country,
    ci.cst_material_status AS marital_status,
	CASE WHEN ci.cst_gender != 'N/A' THEN ci.cst_gender  -- Assuming that the cst_gender is the master table.
		 ELSE COALESCE(ca.gen,'N/A')					 -- Therefore the Data is more Accurate.
	END as gender,
    ca.bdate as birthdate,
    ci.cst_creation_date as create_date
FROM silver.crm_cust_info as ci
LEFT JOIN silver.erp_cust_az12 as ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 as la
ON ci.cst_key = la.cid ;

-- Query the View columns
Select * from gold.dim_customers;


-- Checking Whether The primary key have Duplicates or not.
Select
	cst_id,
    count(*)
from(
SELECT 
	ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_material_status,
    ci.cst_gender,
    ci.cst_creation_date,
    ca.bdate,
    ca.gen,
    la.cntry
FROM silver.crm_cust_info as ci
LEFT JOIN silver.erp_cust_az12 as ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 as la
ON ci.cst_key = la.cid 
)t Group by cst_id
Having count(*) > 1;


-- Checking gen and cst_gender in the columns.
-- Gender Data enriching.
SELECT DISTINCT
    ci.cst_gender,
    ca.gen,
    CASE WHEN ci.cst_gender != 'N/A' THEN ci.cst_gender  -- Assuming that the cst_gender is the master table.
		 ELSE COALESCE(ca.gen,'N/A')					 -- Therefore the Data is more Accurate.
	END as new_gender
FROM silver.crm_cust_info as ci
LEFT JOIN silver.erp_cust_az12 as ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 as la
ON ci.cst_key = la.cid 
ORDER BY 1,2;

-- ============================
-- Creating The Products Table.
-- ============================
CREATE VIEW gold.dim_products as 
SELECT 
	ROW_NUMBER() over(order by prd_start_date, prd_key ) as product_key ,
	pi.prd_id AS product_id,
    pi.prd_key AS product_number,
    pi.prd_nm AS product_name,
	pi.cat_id AS category_id,
    pc.cat AS category,
	pc.subcat AS subcategory,
    pc.maintenance,
    pi.prd_cost AS cost,
    pi.prd_line AS product_line,
    pi.prd_start_date AS start_date
FROM silver.crm_prd_info as pi
LEFT JOIN silver.erp_px_cat_g1v2 as pc
ON pi.cat_id = pc.id
Where pi.prd_end_date IS NULL; -- filtering out all the historical data.

-- Checking for the duplicates in the primary key here we have prd_key.
-- Expectations: NO result.
-- Results : No duplicates found.
Select prd_key,count(*) from (
SELECT 
	pi.prd_id,
    pi.prd_key,
    pi.prd_nm,
	pi.cat_id,
    pc.cat,
	pc.subcat,
    pc.maintenance,
    pi.prd_cost,
    pi.prd_line,
    pi.prd_start_date
FROM silver.crm_prd_info as pi
LEFT JOIN silver.erp_px_cat_g1v2 as pc
ON pi.cat_id = pc.id
Where pi.prd_end_date IS NULL
)t group by prd_key
Having count(*) > 1;

-- Query the View columns
Select * from gold.dim_products;


-- ============================
-- Creating The Sales Table.
-- ============================
CREATE VIEW gold.fact_sales as
SELECT 
	sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON sd.sls_cust_id = cu.customer_id;

-- Querying Table Sales.
Select * from gold.fact_sales;

-- foreign key Integrity (Dimensions)
-- checks whether the data is properly linked or not.
Select * 
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
left join gold.dim_products p
on p.product_key = f.product_key

