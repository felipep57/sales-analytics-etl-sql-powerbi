USE [retail_analytics]
GO

/*
===========================================================
Object: dbo.fact_sales
Type: Fact table (Star Schema)
Layer: Analytics

Description:
Stores transactional sales metrics used for reporting and analysis.
Loaded by the Python ETL pipeline after staging and dimension lookups.

Grain:
One row per order_id per product_id per date_id (and platform_id).

Measures:
- units   : number of units sold
- revenue : sales amount for the grain above

Keys:
- fact_id is a surrogate key for the fact row.
- date_id, product_id, customer_id, platform_id are foreign keys to dimensions
  (constraints can be added if desired).
===========================================================
*/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[fact_sales](
    [fact_id] [int] IDENTITY(1,1) NOT NULL,    -- Surrogate key for the fact row
    [order_id] [varchar] (100) NULL,           -- Source order identifier (business key)
    [date_id] [date] NULL,                     -- FK to dbo.dim_date (calendar date)
    [product_id]  [int] NULL,                  -- FK to dbo.dim_product
    [customer_id] [bigint] NULL,               -- FK to dbo.dim_customer
    [platform_id] [int] NULL,                  -- FK to dbo.dim_platform
    [units] [int] NOT NULL,                    -- Units sold for the grain (default 1)
    [revenue] [decimal](18, 2) NOT NULL,       -- Revenue for the grain (default 0.00)
    [state_code] [char] (2) NULL,              -- State (2-letter code) for geo slicing
PRIMARY KEY CLUSTERED
(
    [fact_id] ASC
)WITH (
    PAD_INDEX = OFF,
    STATISTICS_NORECOMPUTE = OFF,
    IGNORE_DUP_KEY = OFF,
    ALLOW_ROW_LOCKS = ON,
    ALLOW_PAGE_LOCKS = ON,
    OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
) ON [PRIMARY]
) ON [PRIMARY]
GO

-- Defaults help ensure consistent metric behavior when source fields where missing.
ALTER TABLE [dbo].[fact_sales] ADD DEFAULT ((1)) FOR [units]
GO
