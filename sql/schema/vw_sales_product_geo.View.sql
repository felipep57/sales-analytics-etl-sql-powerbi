USE [retail_analytics]
GO

/*
===========================================================
Object: dbo.vw_sales_product_geo
Type: Reporting / semantic view
Layer: Analytics (Power BI consumption)

Description:
Denormalized view joining fact_sales to core dimensions to simplify reporting.
This view centralizes business-friendly fields (date attributes, product names,
platform name, geography) so Power BI can use simpler visuals and lighter DAX.

Join strategy:
INNER JOINs assume dimension keys exist for fact rows (ETL enforces referential integrity).
Switch to LEFT JOIN if you prefer to keep unmatched fact rows visible.
===========================================================
*/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[vw_sales_product_geo] AS
SELECT
    -- Date attributes (from dim_date)
    d.date_id      AS order_date,
    d.day_of_week,
    d.day_name,
    d.is_weekend,
    d.month,
    d.month_name,
    d.quarter,
    d.year,

    -- Geography (from fact_sales; can be moved to a dim_geo later if needed)
    f.state_code,

    -- Product attributes (from dim_product)
    p.category,
    p.subcategory,
    p.english_name AS product_name,

    -- Platform attributes (from dim_platform)
    pl.platform_name AS platform,

    -- Measure
    f.units
FROM dbo.fact_sales f
JOIN dbo.dim_date d
    ON f.date_id = d.date_id
JOIN dbo.dim_product p
    ON f.product_id = p.product_id
JOIN dbo.dim_platform pl
    ON f.platform_id = pl.platform_id;
GO
