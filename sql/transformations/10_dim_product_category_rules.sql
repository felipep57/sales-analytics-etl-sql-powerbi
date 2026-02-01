/*
===========================================================
Object: dbo.dim_product
Type: Transformation (data enrichment)
Layer: Analytics

Description:
Populates and refines product category using SKU patterns and
product name rules. This was iteratively improved after Power BI
showed an oversized "Other Furniture" bucket.

Run Order:
1) Run this script first (category rules)
2) Then run furniture subcategory rules (30_*.sql)
3) Then run validation queries (90_*.sql)
===========================================================
*/

-- 1) Category from SKU prefixes (fast / deterministic)
UPDATE p
SET p.category =
    CASE
        WHEN p.main_sku_code LIKE 'CN%' OR p.main_sku_code LIKE 'NB%' THEN 'Furniture'
        WHEN p.main_sku_code LIKE 'HZ%' THEN 'Outdoor & Garden'
        WHEN p.main_sku_code LIKE 'SZ%' THEN 'Automotive'
        WHEN p.main_sku_code LIKE 'HIFINE%' THEN 'Spare Parts'
        ELSE p.category
    END
FROM dbo.dim_product p
WHERE p.category IS NULL;


-- 2) Category from product name keywords (fallback)
UPDATE p
SET p.category =
    CASE
        WHEN p.english_name LIKE '%sofa%'
          OR p.english_name LIKE '%chair%'
          OR p.english_name LIKE '%table%'
          OR p.english_name LIKE '%bench%'
          OR p.english_name LIKE '%mattress%'
          OR p.english_name LIKE '%cabinet%'
            THEN 'Furniture'

        WHEN p.english_name LIKE '%gazebo%'
          OR p.english_name LIKE '%garden%'
            THEN 'Outdoor & Garden'

        WHEN p.english_name LIKE '%car%'
          OR p.english_name LIKE '%spoiler%'
            THEN 'Automotive'

        WHEN p.english_name LIKE '%light%'
            THEN 'Lighting'

        WHEN p.english_name LIKE '%spare%'
            THEN 'Spare Parts'

        WHEN p.english_name LIKE '%storage%'
            THEN 'Storage & Organization'

        ELSE 'Other'
    END
FROM dbo.dim_product p
WHERE p.category IS NULL;


-- 3) Targeted overrides discovered during Power BI review (fix mis-bucketed items)
-- (Example from your notes: CN1139-* items reclassified)
UPDATE p
SET
    p.category =
        CASE
            WHEN p.main_sku_code LIKE 'CN1139-%' THEN 'Automotive'  -- sim racing accessories were showing in Furniture
            WHEN LOWER(p.english_name) LIKE '%bumper diffuser%'
              OR LOWER(p.english_name) LIKE '%rear bumper diffuser%'
              OR LOWER(p.english_name) LIKE '%running boards%'
                THEN 'Automotive'
            WHEN LOWER(p.english_name) LIKE '%wafer light%'
              OR LOWER(p.english_name) LIKE '%mounting plate%'
              OR LOWER(p.english_name) LIKE '%led panel light%'
              OR LOWER(p.english_name) LIKE '%panel light%'
                THEN 'Lighting'
            WHEN LOWER(p.english_name) LIKE '%gazebo%'
              OR LOWER(p.english_name) LIKE '%pergola%'
              OR LOWER(p.english_name) LIKE '10*12%'
              OR LOWER(p.english_name) LIKE '%metal roof%'
                THEN 'Outdoor & Garden'
            WHEN LOWER(p.english_name) LIKE '%loading ramp%'
              OR LOWER(p.english_name) LIKE '%loading ramps%'
              OR LOWER(p.english_name) LIKE '%ramp%'
              OR LOWER(p.english_name) = 'rack'
                THEN 'Storage & Organization'
            ELSE p.category
        END
FROM dbo.dim_product p;


-- 4) Final fallback (avoid NULL category)
UPDATE dbo.dim_product
SET category = 'Other'
WHERE category IS NULL;