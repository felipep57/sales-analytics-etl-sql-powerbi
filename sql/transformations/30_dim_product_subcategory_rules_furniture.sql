/*
===========================================================
Object: dbo.dim_product
Type: Transformation (subcategory enrichment)
Layer: Analytics

Description:
Populates subcategory and iteratively reduces the "Other Furniture"
bucket by applying refined rules based on top-selling items.

Prerequisite:
- Category is populated (run 10_dim_product_category_rules.sql first)

Notes:
- This script focuses on Furniture + Outdoor & Garden subcategories,
  because those were the key reporting categories in Power BI.
===========================================================
*/

-- A) Outdoor & Garden subcategory (only when empty)
UPDATE p
SET p.subcategory =
    CASE
        WHEN p.english_name LIKE '%gazebo%' OR p.english_name LIKE '%pergola%' THEN 'Gazebo / Pergola'
        WHEN p.english_name LIKE '%patio%'  OR p.english_name LIKE '%outdoor%' THEN 'Patio / Outdoor'
        WHEN p.english_name LIKE '%garden%' OR p.english_name LIKE '%planter%' OR p.english_name LIKE '%raised bed%' THEN 'Garden'
        WHEN p.english_name LIKE '%fire pit%' OR p.english_name LIKE '%heater%' THEN 'Heating'
        WHEN p.english_name LIKE '%umbrella%' THEN 'Umbrella'
        WHEN p.english_name LIKE '%grill%' OR p.english_name LIKE '%bbq%' THEN 'Grill / BBQ'
        WHEN p.english_name LIKE '%swing%' OR p.english_name LIKE '%hammock%' THEN 'Swing / Hammock'
        WHEN p.english_name LIKE '%pool%' OR p.english_name LIKE '%spa%' THEN 'Pool / Spa'
        ELSE 'Other Outdoor'
    END
FROM dbo.dim_product p
WHERE p.category = 'Outdoor & Garden'
  AND (p.subcategory IS NULL OR LTRIM(RTRIM(p.subcategory)) = '');


-- B) Furniture baseline subcategory (only when empty)
UPDATE p
SET p.subcategory =
    CASE
        WHEN p.english_name LIKE '%sofa%' THEN 'Sofa'
        WHEN p.english_name LIKE '%chair%' THEN 'Chair'
        WHEN p.english_name LIKE '%table%' THEN 'Table'
        WHEN p.english_name LIKE '%cabinet%' THEN 'Cabinet'
        WHEN p.english_name LIKE '%bench%' THEN 'Bench'
        WHEN p.english_name LIKE '%mattress%' THEN 'Mattress'
        ELSE 'Other Furniture'
    END
FROM dbo.dim_product p
WHERE p.category = 'Furniture'
  AND (p.subcategory IS NULL OR LTRIM(RTRIM(p.subcategory)) = '');


-- C) Refine the remaining "Other Furniture" group (your iterative improvement step)
-- Run only against items still stuck in Other Furniture
UPDATE p
SET p.subcategory =
    CASE
        -- Beds
        WHEN p.english_name LIKE '%bed frame%' OR p.english_name LIKE '%bed%' THEN 'Bed'

        -- Dining
        WHEN p.english_name LIKE '%dining set%'
          OR p.english_name LIKE '%dining table%'
          OR p.english_name LIKE '%dining%'
            THEN 'Dining Furniture'

        -- Tables (living room)
        WHEN p.english_name LIKE '%coffee table%'
          OR p.english_name LIKE '%end table%'
          OR p.english_name LIKE '%side table%'
            THEN 'Occasional Tables'

        -- Seating
        WHEN p.english_name LIKE '%accent chair%' THEN 'Accent Chair'
        WHEN p.english_name LIKE '%ottoman%' THEN 'Ottoman'
        WHEN p.english_name LIKE '%loveseat%' THEN 'Loveseat'
        WHEN p.english_name LIKE '%recliner%' THEN 'Recliner'
        WHEN p.english_name LIKE '%chaise%' THEN 'Chaise Lounge'
        WHEN p.english_name LIKE '%sectional%' THEN 'Sectional Sofa'
        WHEN p.english_name LIKE '%daybed%' THEN 'Daybed'
        WHEN p.english_name LIKE '%futon%' THEN 'Futon'

        -- Consoles / media
        WHEN p.english_name LIKE '%console table%'
          OR p.english_name LIKE '%entry table%'
            THEN 'Console Table'
        WHEN p.english_name LIKE '%tv stand%'
          OR p.english_name LIKE '%media%'
          OR p.english_name LIKE '%console%'
            THEN 'TV Stand / Media Console'

        -- Storage & case goods
        WHEN p.english_name LIKE '%wardrobe%' OR p.english_name LIKE '%closet%' THEN 'Wardrobe'
        WHEN p.english_name LIKE '%dresser%' OR p.english_name LIKE '%chest%' THEN 'Dresser'
        WHEN p.english_name LIKE '%nightstand%' OR p.english_name LIKE '%bedside%' THEN 'Nightstand'
        WHEN p.english_name LIKE '%storage bench%' THEN 'Storage Bench'
        WHEN p.english_name LIKE '%storage%' OR p.english_name LIKE '%organizer%' THEN 'Storage Furniture'

        -- Shelving
        WHEN p.english_name LIKE '%bookshelf%' OR p.english_name LIKE '%shelf%' THEN 'Shelving'

        -- Kids
        WHEN p.english_name LIKE '%kids%' OR p.english_name LIKE '%child%' THEN 'Kids Furniture'

        -- Sets / bundles
        WHEN p.english_name LIKE '%set%' OR p.english_name LIKE '%bundle%' THEN 'Furniture Sets'

        ELSE 'Other Furniture'
    END
FROM dbo.dim_product p
WHERE p.category = 'Furniture'
  AND p.subcategory = 'Other Furniture';


-- D) Final cleanup: what’s left in "Other Furniture" after refinement
-- In your notes, the remaining items were essentially furniture sets/general.
UPDATE dbo.dim_product
SET subcategory = 'Furniture Sets & General'
WHERE category = 'Furniture'
  AND subcategory = 'Other Furniture';