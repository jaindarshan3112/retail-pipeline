-- =====================================================================
-- STAGING.STG_PRODUCTS
-- =====================================================================
-- Flattens the VARIANT RAW.PRODUCTS into a typed table.
-- Source: fakestoreapi /products
--
-- VARIANT shape example:
--   {
--     "id": 1,
--     "title": "Mens Cotton Jacket",
--     "price": 55.99,
--     "category": "men's clothing",
--     "description": "...",
--     "image": "https://...",
--     "rating": {"rate": 4.7, "count": 500}
--   }
--
-- We deduplicate by product id, keeping the most recent load.
-- =====================================================================

CREATE OR REPLACE TABLE STAGING.STG_PRODUCTS AS
WITH ranked AS (
    SELECT
        raw_data:id::NUMBER             AS product_id,
        raw_data:title::VARCHAR         AS product_name,
        raw_data:category::VARCHAR      AS category,
        raw_data:price::NUMBER(12,2)    AS list_price_usd,
        raw_data:description::VARCHAR   AS description,
        raw_data:image::VARCHAR         AS image_url,
        raw_data:rating.rate::FLOAT     AS rating_avg,
        raw_data:rating.count::NUMBER   AS rating_count,
        _load_ts,
        _source_file,
        ROW_NUMBER() OVER (
            PARTITION BY raw_data:id::NUMBER
            ORDER BY _load_ts DESC
        ) AS rn
    FROM RAW.PRODUCTS
    WHERE raw_data:id IS NOT NULL
)
SELECT
    product_id,
    product_name,
    UPPER(category)                AS category,
    list_price_usd,
    description,
    image_url,
    rating_avg,
    rating_count,
    _load_ts                       AS _raw_load_ts,
    _source_file                   AS _raw_source_file,
    CURRENT_TIMESTAMP()            AS _stg_load_ts
FROM ranked
WHERE rn = 1;
