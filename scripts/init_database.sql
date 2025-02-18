USE master;
GO
IF EXISTS (select 1 FROM sys.database wher name='DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO
CREATE DATABASE DataWarehouse;
USE DataWarehouse;
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
