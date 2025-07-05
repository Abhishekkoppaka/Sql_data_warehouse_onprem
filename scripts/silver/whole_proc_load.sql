CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		SET @start_time = GETDATE();
		/*
		Inserting crm data 3 tables and then other epr 3 tables
		*/

		---THEN loading the data to silver from bronze
		----crm_cust_info
		PRINT '>>>Trucating silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info; 
		PRINT '>>>inserting silver.crm_cust_info'
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
		;


		---inserting data- prd_info
		---Inersting prd info from bronze to silver----
		PRINT '>>>Trucating silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info; 
		PRINT '>>>inserting silver.crm_prd_info'
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

		;
		---inserting sales details

		----inserting into silver sales data
		PRINT '>>>Trucating silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details; 
		PRINT '>>>inserting silver.crm_sales_details'
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
		 ; 


		/*
		NOW ERP inserting from bronze to silver
		*/

		---inserting from bronze to silver
		--- inserting data to silver erp_cust_az12
		PRINT '>>>Trucating silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12; 
		PRINT '>>>inserting silver.erp_cust_az12'
		INSERT INTO [silver].[erp_cust_az12]
				(
				cid,
				bdate,
				gen
				)
		SELECT 
			CASE 
				WHEN cid LIKE 'NAS%' THEN substring(cid,4,len(cid))
				ELSE cid
			END as cid,
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END as bdate,
			CASE
				WHEN UPPER(TRIM(gen)) in ('F','FEMALE') THEN 'Female' 
				WHEN UPPER(TRIM(gen)) in ('M','MALE') THEN 'Male'
				ELSE 'na'
			END as gen		
		FROM [bronze].[erp_cust_az12];


		---inserting from bronze to silver
		--- inserting data to silver _loc_a101
		PRINT '>>>Trucating silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101; 
		PRINT '>>>inserting erp_loc_a101'
		INSERT INTO [silver].[erp_loc_a101]
			(
			cid,
			cntry
			)

		SELECT 
			REPLACE(cid,'-','') as cid,
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
				WHEN TRIM(cntry) = '' or TRIM(cntry) IS NUll THEN 'na'
				ELSE TRIM(cntry)
			END as cntry
		FROM [bronze].[erp_loc_a101]
		;

		----inserting day to silver erp_px_cat_g1v2
		PRINT '>>>Trucating silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2; 
		PRINT '>>>inserting silver.erp_px_cat_g1v2'
		INSERT INTO [silver].[erp_px_cat_g1v2]
			(
				[id],
			  [cat],
			  [subcat],
			  [maintenance]
			)
		SELECT [id],
			  [cat],
			  [subcat],
			  [maintenance]
		  FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2];
		;
		SET @end_time = GETDATE();
		print '>> load duration'+ CAST(DATEDIFF(second, @start_time,@end_time) as NVARCHAR)+'   '+ 'totalsecs';
	END TRY
	BEGIN CATCH
		PRINT'error in loading the data';
		PRINT'error message'+ ERROR_MESSAGE();
	END CATCH
END
