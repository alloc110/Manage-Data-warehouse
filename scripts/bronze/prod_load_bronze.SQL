/*
=====================================================
Stored Proceduce: Load Bronze Layer (Source -> Bronze)
=====================================================
Script purpose:
	This stored proceduce loads data into the 'bronze' schema from external CSV files
	It performs the following actions:
		- Truncates the bronze tables before loading data.
		- Uses the 'BULK INSERT' command to load data from csv files to bronze tables

Parameters:
	None.
This stored procedure does not accecpt any parameters or return any values.

Usage Example:
	EXEC bronze.load_bronze
=====================================================

*/

create or alter procedure bronze.load_bronze as 
begin
	declare @start_time datetime, @end_time datetime, @start_batch_time datetime, @end_batch_time datetime
	BEGIN TRY
		PRINT '================================================='
		PRINT 'LOADING BRONZE LAYER'
		PRINT '================================================='
		set @start_batch_time= GETDATE();
		set @start_time = getdate();
		PRINT '>> TRUNCATIN bronze.account_info'
		truncate table bronze.account_info
		PRINT '>> INSERTING DATA INTO: bronze.account_info'
		bulk insert bronze.account_info
		from 'D:\Projects\Datawarehouse\accounts.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		)
		set @end_time = getdate();
		
		print '>> Load duration: ' +  cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'; 


		set @start_time = getdate();
		PRINT '>> TRUNCATIN bronze.products_info'
		truncate table bronze.products_info
		PRINT '>> INSERTING DATA INTO: bronze.products_info'
		bulk insert bronze.products_info
		from 'D:\Projects\Datawarehouse\products.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		)
		set @end_time = getdate();
		print '>> Load duration: ' +  cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'; 


		set @start_time = getdate();
		PRINT '>> TRUNCATIN bronze.sale_info'
		truncate table bronze.sale_info
		PRINT '>> INSERTING DATA INTO: bronze.sale_info'
		bulk insert bronze.sale_info
		from 'D:\Projects\Datawarehouse\sales_pipeline.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		)
		set @end_time = getdate();
		print '>> Load duration: ' +  cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'; 


		set @start_time = getdate();
		PRINT '>> TRUNCATIN bronze.sale_teams_info'
		truncate table bronze.sale_teams_info;
		PRINT '>> INSERTING DATA INTO: bronze.sale_teams_info'
		bulk insert bronze.sale_teams_info
		from 'D:\Projects\Datawarehouse\sales_teams.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		)
		set @end_time = getdate();
		print '>> Load duration: ' +  cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'; 


		set @end_batch_time = GETdate();
		PRINT '=================================================='
		print 'LOAD BRONZE LAYER IS COMPLETED'
		PRINT '>> TOTAL TIME: ' + CAST(datediff(SECOND, @start_batch_time, @end_batch_time) as nvarchar) + ' seconds';
		PRINT '=================================================='

	END TRY
	BEGIN CATCH
		PRINT '=================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + error_message();
		PRINT 'Error Message' + cast(error_number() as nvarchar);
		print 'Error Message' + cast(error_state() as nvarchar);
		PRINT '=================================================='

 	END CATCH
end
