/*
=====================================================
Stored Proceduce: Load silver Layer (Bronze -> Silver)
=====================================================
Script purpose:
	This stored proceduce loads data into the 'silver' schema from 'bronze' schema.
	Perform the ETL (Extract, Transform, Load) process to populate the 'silver
	It performs the following actions:
		- Truncates the silver tables before loading data.
		- Uses the 'INSERT INTO' command to load data from bronze schema to silver tables

Parameters:
	None.
This stored procedure does not accecpt any parameters or return any values.

Usage Example:
	EXEC bronze.load_silver
=====================================================

*/

CREATE OR ALTER PROCEDURE SILVER.load_silver AS
BEGIN
	BEGIN TRY
		DECLARE @START_TIME DATETIME, @END_TIME DATETIME, @START_BATCH_TIME DATETIME, @END_BATCH_TIME DATETIME
		PRINT '================================================='
		PRINT 'LOADING SILVER LAYER'
		PRINT '================================================='

		PRINT '>> TRUNCATING TABLE: silver.account_info'
		TRUNCATE TABLE silver.account_info;
		SET @START_TIME = GETDATE()
		PRINT '>> INSERT INTO TABLE: silver.account_info'
		INSERT INTO silver.account_info ( account, sector, year_established, revenue, employees, office_location, subsidiary_of)
		SELECT 
		ACCOUNT, 
		SECTOR,
		year_established,
		REVENUE,
		employees,
		office_location,
		CASE WHEN (subsidiary_of IS NULL) THEN 'n/a'
			ELSE subsidiary_of 
			END subsidiary_of
		FROM BRONZE.ACCOUNT_INFO
		ORDER BY SECTOR, office_location , REVENUE DESC
		SET @END_TIME = GETDATE()
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS' ;

		PRINT '>> TRUNCATING TABLE: silver.products_info'
		TRUNCATE TABLE silver.products_info;
		SET @START_TIME = GETDATE()
		PRINT '>> INSERT INTO TABLE: silver.products_info'
		insert into silver.products_info(product, series, sale_prices, rank_product)
		SELECT * ,
		 RANK() OVER (ORDER BY sale_prices DESC)  PRODUCT_RANK 
		FROM BRONZE.products_info
		SET @END_TIME = GETDATE()
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS' ;


		PRINT '>> TRUNCATING TABLE: silver.sale_info'
		TRUNCATE TABLE silver.sale_info;
		SET @START_TIME = GETDATE()
		PRINT '>> INSERT INTO TABLE: silver.sale_info'
		INSERT INTO SILVER.sale_info(opportunity_id, sales_agent, product, account, deal_stage, engage_date, close_date, close_value)
		SELECT * 
		FROM BRONZE.sale_info
		WHERE opportunity_id IS NOT NULL
		AND sales_agent IS NOT NULL
		AND product IS NOT NULL
		AND account IS NOT NULL
		AND deal_stage IS NOT NULL
		AND engage_date IS NOT NULL
		AND close_date IS  NOT NULL
		AND close_value IS NOT NULL
		ORDER BY sales_agent,ACCOUNT ,close_value DESC
		SET @END_TIME = GETDATE()
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS' ;


		PRINT '>> TRUNCATING TABLE: silver.sale_teams_info'
		TRUNCATE TABLE silver.sale_teams_info;
		SET @START_TIME = GETDATE()
		PRINT '>> INSERT INTO TABLE: silver.sale_teams_info'
		INSERT INTO SILVER.sale_teams_info(sales_agent, manager, regional_office)
		SELECT * 
		FROM BRONZE.sale_teams_info
		SET @END_TIME = GETDATE()
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS' ;
	
		set @end_batch_time = GETdate();
		PRINT '=================================================='
		print 'LOAD SILVER LAYER IS COMPLETED'
		PRINT 'TOTAL TIME: ' + CAST(datediff(SECOND, @start_batch_time, @end_batch_time) as nvarchar) + ' seconds';
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
END
