/*
=====================================================
DLL Script: Create silver layer
=====================================================
Script purpose:
	This script creates tables in the 'silver layer' schema, dropping existing tables
	if they already exists
	Run this script to re-define the DLL structure of 'silver' Tables
=====================================================

*/
if OBJECT_ID('silver.account_info', 'U') is not null
	drop table silver.account_info
CREATE table silver.account_info(
	account nvarchar(50),
	sector nvarchar(20),
	year_established date,
	revenue float ,
	employees int,
	office_location nvarchar(50),
	subsidiary_of nvarchar(50),
	dwh_create_time datetime2 default getdate()
)

if OBJECT_ID('silver.products_info', 'U') is not null
	drop table silver.products_info
create table silver.products_info(
	product nvarchar(50),
	series nvarchar(3),
	sale_prices int,
	rank_product int,
	dwh_create_time datetime2 default getdate()
)

if OBJECT_ID('silver.sale_info', 'U') is not null
	drop table silver.sale_info
create table silver.sale_info(
	opportunity_id nvarchar(50),
	sales_agent nvarchar(50),
	product nvarchar(50),
	account nvarchar(50),
	deal_stage nvarchar(50),
	engage_date date,
	close_date date, 
	close_value int ,
	dwh_create_time datetime2 default getdate()
)

if OBJECT_ID('silver.sale_teams_info', 'U') is not null
	drop table silver.sale_teams_info
create table silver.sale_teams_info(
	sales_agent nvarchar(50),
	manager nvarchar(50),
	regional_office nvarchar(50),
	dwh_create_time datetime2 default getdate()
)
