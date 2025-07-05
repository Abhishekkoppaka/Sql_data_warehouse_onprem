-----''''''''''''-
---customer-info--

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

	
-----''''''''''''----

---prd-info--
--checking for data 
	
--checking for duplicates and nulls

SELECT prd_id, count(*)
FROM [silver].[crm_prd_info]
group by prd_id
having count(*) > 1 or prd_id IS NULL;

--checking the string
SELECT [prd_nm]
FROM [silver].[crm_prd_info]
WHERE TRIM(prd_nm) != prd_nm;

--checking the negative numbers or null
SELECT [prd_cost]
FROM [silver].[crm_prd_info]
WHERE [prd_cost]<0 OR [prd_cost] IS NULL 

--stanardization and consistancy

SELECT DISTINCT [prd_line]
FROM [silver].[crm_prd_info];


----checking invalid dates

SELECT *
FROM [silver].[crm_prd_info]
WHERE [prd_start_dt] >
		[prd_end_dt] 


---Inersting prd info from bronze to silver----

INSERT INTO [silver].[crm_prd_info]
		(
		[prd_id],
		cat_id,
		prd_key,
		prd_nm,
		[prd_cost],
		[prd_line],
		[prd_start_dt],
		prd_end_dt
		)

SELECT	
		[prd_id],
		REPLACE(SUBSTRING([prd_key],1,5), '-','_') as cat_id,
		SUBSTRING([prd_key],7,LEN(prd_key)) as prd_key,
		[prd_nm],
		ISNULL([prd_cost], 0) as [prd_cost],
		CASE UPPER(TRIM([prd_line]))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'na'
		END AS [prd_line],
		CAST([prd_start_dt] AS DATE) [prd_start_dt],
		CAST(LEAD(prd_start_dt) OVER(partition by prd_key order by prd_start_dt asc)-1 as DATE )as prd_end_dt
FROM [bronze].[crm_prd_info]

