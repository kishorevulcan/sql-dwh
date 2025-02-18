/*
usage example: EXEC bronze.load_bronze
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@brozne_load_start_time DATETIME,@bronze_load_end_time DATETIME
	BEGIN TRY
		PRINT '=============================================';
		PRINT 'Loading Bronze layer';
		PRINT '=============================================';

		PRINT '---------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '---------------------------------------------';
		
		SET @start_time = GETDATE();
		SET @brozne_load_start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info ';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Table: bronze.crm_cust_info ';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\slf\dwh\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load duration: '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR))

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info ';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Table: bronze.crm_prd_info ';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\slf\dwh\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load duration: '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR))

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.sales_details ';
		TRUNCATE TABLE bronze.sales_details;
		PRINT '>> Inserting Table: bronze.sales_details ';
		BULK INSERT bronze.sales_details
		FROM 'C:\slf\dwh\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load duration: '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR))

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12 ';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Table: bronze.erp_cust_az12 ';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\slf\dwh\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load duration: '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR))

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101 ';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Table: bronze.erp_loc_a101 ';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\slf\dwh\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load duration: '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR))

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2 ';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Table: bronze.erp_px_cat_g1v2 ';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\slf\dwh\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		SET @bronze_load_end_time = GETDATE();
		PRINT('>> Load duration: '+CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR))
		PRINT('>> Complete Load duration: '+CAST (DATEDIFF(second,@brozne_load_start_time,@bronze_load_end_time) AS NVARCHAR))
	END TRY
	BEGIN CATCH
		PRINT '================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
	END CATCH
END
