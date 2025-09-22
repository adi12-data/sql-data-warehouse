/*
=======================================================================
 Script Name   : DataWarehouse_Setup.sql
 Description   : This script creates a clean Data Warehouse database 
                 in SQL Server and sets up schemas for the Medallion 
                 Architecture (Bronze, Silver, Gold).
                 
 Steps:
   1. Drop existing DataWarehouse (if it exists).
   2. Create a new DataWarehouse database.
   3. Define three schemas:
        - bronze : raw, ingested data
        - silver : cleaned and enriched data
        - gold   : curated, business-ready data
        
 Notes:
   - The script uses SINGLE_USER mode with ROLLBACK IMMEDIATE 
     to drop the database safely even if active connections exist.
   - Designed for Microsoft SQL Server.
=======================================================================
*/


-- Create Database 'DataWarehouse'

if exists (select 1 from sys.databases where name = 'DataWarehouse')
begin alter database datawarehouse set single_user with rollback immediate;
drop database DataWarehouse;
END;
Go
USE master;

Create Database DataWarehouse;
Go
Use DataWarehouse;
Go
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;

