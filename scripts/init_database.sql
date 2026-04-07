 /* 
 =================================================================================
 Create Database and Schemas
 =================================================================================

 Script Purpose: 
 
 This Script creates a new database named "Datawarehouse" after checking if it already exists.
 If the database exists, it is dropped and recreated. Aditionally, the script sets up three schemas within database : "bronze","silver","gold"
 
 
 WARNING: 
 
 Running these script will drop the entire "Datawarehouse" database if exists.
 All data in the database will be permanentely deleted. Proceed with caution and ensure you have proper backups running this scripy.
 
 */


 
-- Create Database 'Datawarehouse'
 USE master;
 GO

 --Drop and recreate the "Datawarehouse" database
 IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
 BEGIN
 ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
 DROP DATABASE Datawarehouse;
 END;
 GO

 -- Create the 'Datawarehouse' database

 CREATE DATABASE Datawarehouse;
 GO

 USE Datawarehouse;
 GO

 CREATE SCHEMA bronze;
 GO
 CREATE SCHEMA silver;   -- Go is just seperator first execute the bronze and then go to next command
 GO
 CREATE SCHEMA gold;
 GO
