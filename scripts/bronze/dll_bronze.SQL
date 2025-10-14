/*
=====================================================
DLL Script: Create bronze layer
=====================================================
Script purpose:
	This script creates tables in the 'bronze layer' schema, dropping existing tables
	if they already exists
	Run this script to re-define the DLL structure of 'bronze' Tables
*/
if OBJECT_ID('bronze.account_info', 'U') is not null
	drop table bronze.account_info
CREATE table bronze.account_info(
	account nvarchar(50),
	sector nvarchar(20),
	year_established date,
	revenue float ,
	employees int,
	office_location nvarchar(50),
	subsidiary_of nvarchar(50)
)

if OBJECT_ID('bronze.products_info', 'U') is not null
	drop table bronze.products_info
create table bronze.products_info(
	product nvarchar(50),
	series nvarchar(3),
	sale_prices int
)

if OBJECT_ID('bronze.sale_info', 'U') is not null
	drop table bronze.sale_info
create table bronze.sale_info(
	opportunity_id nvarchar(50),
	sales_agent nvarchar(50),
	product nvarchar(50),
	account nvarchar(50),
	deal_stage nvarchar(50),
	engage_date date,
	close_date date, 
	close_value int 
)

if OBJECT_ID('bronze.sale_teams_info', 'U') is not null
	drop table bronze.sale_teams_info
create table bronze.sale_teams_info(
	sales_agent nvarchar(50),
	manager nvarchar(50),
	regional_office nvarchar(50)
)
