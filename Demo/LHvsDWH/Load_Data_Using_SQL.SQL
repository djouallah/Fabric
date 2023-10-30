-- Loading data from the Lakehouse as currently DWH can't load data from Parquet Directly.
CREATE TABLE [TPCH_100].[dbo].[lineitem]
as SELECT * from [WW].[dbo].[lineitem]
GO
CREATE TABLE [TPCH_100].[dbo].[customer]
as SELECT * from [WW].[dbo].[customer]
GO
CREATE TABLE [TPCH_100].[dbo].[nation]
as SELECT * from [WW].[dbo].[nation]
GO
CREATE TABLE [TPCH_100].[dbo].[orders]
as SELECT * from [WW].[dbo].[orders]
GO
CREATE TABLE [TPCH_100].[dbo].[part]
as SELECT * from [WW].[dbo].[part]
GO
CREATE TABLE [TPCH_100].[dbo].[partsupp]
as SELECT * from [WW].[dbo].[partsupp]
GO
CREATE TABLE [TPCH_100].[dbo].[region]
as SELECT * from [WW].[dbo].[region]
GO
CREATE TABLE [TPCH_100].[dbo].[supplier]
as SELECT * from [WW].[dbo].[supplier]
GO
