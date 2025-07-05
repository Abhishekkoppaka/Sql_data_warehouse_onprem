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

---checking data
---[crm_sales_details]
--sales_details bronze to silver
---checking for invlaid dates

SELECT NULLIF([sls_order_dt], 0) as [sls_order_dt]
FROM 
[bronze].[crm_sales_details]
WHERE 
	[sls_order_dt] <=0 or 
	LEN([sls_order_dt]) != 8 or
	sls_order_dt >20500101 or
	sls_order_dt < 20000101

---checking invlaid date orders

SELECT * 
FROM [bronze].[crm_sales_details]
WHERE [sls_order_dt] > [sls_ship_dt]

--checking the business rules

SELECT sls_sales AS old_sls_sales,
		sls_quantity,
		sls_price as old_sls_price,
		CASE WHEN sls_sales <= 0 or sls_sales IS NULL or sls_sales != sls_quantity*abs(sls_price)  
				THEN sls_quantity * abs(sls_price) 
			ELSE sls_sales 
		END as sls_sales, 
		CASE 
			WHEN sls_price <=0 THEN sls_price*-1
			WHEN sls_price IS NULL or sls_price = 0 THEN sls_sales/NULLIF(sls_quantity,0)
			ELSE sls_price
		END as sls_price
	
FROM [bronze].[crm_sales_details]
WHERE sls_sales != sls_quantity*sls_price
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales,sls_quantity ,sls_price

----inserting into silver sales data

INSERT INTO [silver].[crm_sales_details]
		(
		[sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]

		)
SELECT [sls_ord_num],
      [sls_prd_key],
      [sls_cust_id],
      CASE
		WHEN  sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN  NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	  END AS [sls_order_dt],
	  CASE
		WHEN  [sls_ship_dt] = 0 or LEN([sls_ship_dt]) != 8 THEN  NULL
		ELSE CAST(CAST([sls_ship_dt] AS VARCHAR) AS DATE)
	  END AS [sls_ship_dt],
      CASE
		WHEN  [sls_due_dt] = 0 or LEN([sls_due_dt]) != 8 THEN  NULL
		ELSE CAST(CAST([sls_due_dt] AS VARCHAR) AS DATE)
	  END AS [sls_due_dt],
      CASE WHEN sls_sales <= 0 or sls_sales IS NULL or sls_sales != sls_quantity*abs(sls_price)  
				THEN sls_quantity * abs(sls_price) 
			ELSE sls_sales 
	   END as sls_sales,
      [sls_quantity],
      CASE 
			WHEN sls_price <=0 THEN sls_price*-1
			WHEN sls_price IS NULL or sls_price = 0 THEN sls_sales/NULLIF(sls_quantity,0)
			ELSE sls_price
		END as sls_price
  FROM [DataWarehouse].[bronze].[crm_sales_details]
  
