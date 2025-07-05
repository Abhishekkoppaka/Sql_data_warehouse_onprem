---check for nulls or duplicate
---- expectation: no result

SELECT cst_id, count(*)
FROM [bronze].[crm_cust_info]
group by cst_id
having count(*)>1 or cst_id IS NULL

-- check for unwanted spaces
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE TRIM(cst_firstname) != cst_firstname

---data standardization and consitency

SELECT distinct cst_gndr
FROM bronze.crm_cust_info


---THEN loading the data to silver from bronze
INSERT INTO silver.crm_cust_info
	(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)

SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) as cst_firtname,
	TRIM(cst_lastname) as cst_lastname,
		CASE 
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			ELSE 'na'
		END as cst_marital_status, 
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' 
			ELSE 'na'
		END as cst_gndr,
	cst_create_date
FROM (
		SELECT *, 
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as rnk
		FROM 	bronze.crm_cust_info 
		WHERE cst_id IS NOT NULL
	) as t
WHERE
	rnk =1
