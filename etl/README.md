# ETL Pipeline (CSV → SQL Server)

This folder contains Python ETL scripts that load CSV files into a SQL Server star-schema database.

## Scripts
- order_info_etl.py → loads raw order exports into staging and updates dimensions + fact table
- product_info_etl.py → loads product master data into staging and upserts dim_product

## Configuration
Set the input file path using an environment variable:

### PowerShell
```powershell
$env:INPUT_PATH="C:\path\to\input.csv"
python etl/order_info_etl.py