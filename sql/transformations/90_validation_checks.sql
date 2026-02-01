/*
===========================================================
Purpose: Validation queries used after applying category and
subcategory rules.

How to use:
Run after:
- 10_dim_product_category_rules.sql
- 30_dim_product_subcategory_rules_furniture.sql

Goal:
Confirm "Other Furniture" shrank and the subcategory distribution
makes sense for reporting.
===========================================================
*/

-- 1) SKU distribution by subcategory (Furniture only)
SELECT
  p.subcategory,
  COUNT(*) AS sku_count
FROM dbo.dim_product p
WHERE p.category = 'Furniture'
GROUP BY p.subcategory
ORDER BY sku_count DESC;


-- 2) Units sold by subcategory (Furniture only)
SELECT
  p.subcategory,
  SUM(f.units) AS units_sold
FROM dbo.fact_sales f
JOIN dbo.dim_product p ON f.product_id = p.product_id
WHERE p.category = 'Furniture'
GROUP BY p.subcategory
ORDER BY units_sold DESC;


-- 3) Remaining "Other Furniture" offenders (should be near-zero)
SELECT TOP (200)
  p.english_name,
  p.main_sku_code,
  SUM(f.units) AS units_sold
FROM dbo.fact_sales f
JOIN dbo.dim_product p ON f.product_id = p.product_id
WHERE p.category = 'Furniture'
  AND p.subcategory IN ('Other Furniture', 'Furniture Sets & General')
GROUP BY p.english_name, p.main_sku_code
ORDER BY units_sold DESC;