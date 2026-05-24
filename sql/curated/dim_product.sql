-- =====================================================================
-- CURATED.DIM_PRODUCT
-- =====================================================================
-- Product dimension from API catalog.
-- =====================================================================

CREATE OR REPLACE TABLE CURATED.DIM_PRODUCT AS
SELECT
    HASH(product_id)               AS product_sk,
    product_id                     AS product_nk,
    product_name,
    category,
    list_price_usd,
    rating_avg,
    rating_count,
    -- Bucketed rating for slicing
    CASE
      WHEN rating_avg >= 4.5 THEN 'EXCELLENT'
      WHEN rating_avg >= 4.0 THEN 'GOOD'
      WHEN rating_avg >= 3.0 THEN 'AVERAGE'
      ELSE 'POOR'
    END                            AS rating_bucket,
    _stg_load_ts                   AS _last_updated_at
FROM STAGING.STG_PRODUCTS;
