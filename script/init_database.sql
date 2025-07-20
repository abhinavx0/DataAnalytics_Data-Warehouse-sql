-- WARNING: This will delete the existing 'DataWarehouse' database and all its data. Use with caution!

-- Drop the database if it exists
DROP DATABASE IF EXISTS DataWarehouse;

-- Create the database
CREATE DATABASE DataWarehouse;

-- Use the new database
USE DataWarehouse;

-- Create schemas by using separate databases or using table prefixes in MySQL.

CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;


