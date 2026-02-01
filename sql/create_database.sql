/*
Object: retail_analytics
Type: Database creation script
Purpose: Creates the analytics database used by the ETL and reporting layers.
Note: Intended for local / demo environments.
*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = 'retail_analytics'
)
BEGIN
    CREATE DATABASE retail_analytics;
END;
GO
