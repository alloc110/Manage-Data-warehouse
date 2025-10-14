/*
=====================================
Create Database and Schemas
=====================================
Script Purpose 
	This script create a new database named 'DataWarehouse' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
	within the database: 'bronze', 'silver', and 'gold'

WARNING:
	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All data in the database will be permanently deleted. Proceed with caution
	and ensure you have proper backups before runnings this script.
*/

use master;
go

-- Drop and create database
IF EXISTS(SELECT * FROM sys.databases WHERE name = 'DataWarehouse')
begin 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	drop database DataWarehouse;

end

create database DataWarehouse;
go


use DataWarehouse
go

-- Create schema bronze
create schema bronze;
go 

-- Create schema silver
create schema silver
go

-- Create schema gold 
create schema gold
go
