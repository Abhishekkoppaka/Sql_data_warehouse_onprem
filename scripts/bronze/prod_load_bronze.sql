CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME, @start_time_batch DATETIME, @end_time_batch DATETIME;
	BEGIN TRY
		PRINT '=============';
		PRINT 'loading the bronze layer'	;
		SET @start_time_batch = GETDATE();
		PRINT '=============';

		PRINT 'loading CRM systems';

		SET @start_time = GETDATE();
		PRINT 'TRUCATING table bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\koppa\Downloads\sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT ' >>>load time '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';

		TRUNCATE TABLE bronze.crm_prd_info;

		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\koppa\Downloads\sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


		TRUNCATE TABLE bronze.crm_sales_details;

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\koppa\Downloads\sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		PRINT 'loading EPR systems';
		TRUNCATE TABLE bronze.erp_cust_az12;

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\koppa\Downloads\sql\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


		TRUNCATE TABLE bronze.erp_loc_a101;

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\koppa\Downloads\sql\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);



		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\koppa\Downloads\sql\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time_batch = GETDATE();
		PRINT'endtimebatch_total_time '+ CAST(DATEDIFF(second, @start_time_batch,@end_time_batch) AS NVARCHAR) + 'total_sec';
	END TRY
	BEGIN CATCH
		PRINT '=========';
		PRINT 'ERROR occurred in bronze layer';
		PRINT 'error message '+ CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'error message '+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '=========';
	END CATCH
END

