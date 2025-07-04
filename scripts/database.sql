/*
create Database 'Datawarehouse' and schemas


*/

USE master;
GO
-- drop if exists
IF EXISTS (SELECT 1 FROM sys.databases where name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO
---creating new
  
CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
-- creating schema
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
