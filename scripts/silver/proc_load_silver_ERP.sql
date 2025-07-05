---indentifying out of range dates
SELECT *
FROM [bronze].[erp_cust_az12]
WHERE bdate >GETDATE() or bdate< '1800-01-01';


---gender cardinality
SELECT distinct gen
FROM [bronze].[erp_cust_az12]

--- inserting data to silver
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

----erp_loc

---data consensitancy and standarsization
SELECT 
	distinct cntry as old,
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
		WHEN TRIM(cntry) = '' or TRIM(cntry) IS NUll THEN 'na'
		ELSE TRIM(cntry)
	END as cntry

FROM [bronze].[erp_loc_a101]


--inserting
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


