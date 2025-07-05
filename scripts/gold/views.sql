----creating dim customers (combined 3 tables into one master for starschema
CREATE VIEW gold.dim_customers AS

SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as  marital_status,
	CASE 
		WHEN ci.cst_gndr != 'na' THEN  ci.cst_gndr --crm master info
		ELSE COALESCE(ca.gen, 'na')
	END as gender,
	ca.bdate as birthdate, 
	ci.cst_create_date as create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN  silver.erp_cust_az12 as ca
on			ci.cst_key= ca.cid
LEFT JOIN silver.erp_loc_a101 as la
on			ci.cst_key= la.cid


-----creating products view

CREATE VIEW gold.dim_products AS 
SELECT 
	ROW_number() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as catagory_id,
	pc.cat as catagory,
	pc.subcat as subcatagory,
	pc.maintenance as maintenance,
	pn.prd_cost as cost ,
	pn.Prd_line as product_line,
	pn.prd_start_dt as startdate
FROM
	silver.crm_prd_info as pn
LEFT JOIN silver.erp_px_cat_g1v2 as pc
ON		pn.cat_id =pc.id
WHERE prd_end_dt IS NULL---filtered out all historical data

