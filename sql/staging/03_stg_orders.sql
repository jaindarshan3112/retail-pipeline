-- =====================================================================
-- STAGING.STG_ORDERS
-- =====================================================================
-- Cleans RAW.ORDERS:
--   * Dedupes by order_id (most recent modified_at wins)
--   * Filters out CANCELLED orders (business rule: not counted as sales)
--   * Standardizes country and currency codes
-- =====================================================================

CREATE OR REPLACE TABLE STAGING.STG_ORDERS AS
WITH ranked AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        UPPER(status)                  AS status,
        UPPER(shipping_country)        AS shipping_country,
        UPPER(currency_code)           AS currency_code,
        total_amount,
        created_at,
        modified_at,
        _load_ts,
        _source_file,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY modified_at DESC, _load_ts DESC
        ) AS rn
    FROM RAW.ORDERS
    WHERE order_id IS NOT NULL
)
SELECT
    order_id,
    customer_id,
    order_date,
    status,
    shipping_country,
    currency_code,
    total_amount,
    created_at,
    modified_at,
    _load_ts                       AS _raw_load_ts,
    _source_file                   AS _raw_source_file,
    CURRENT_TIMESTAMP()            AS _stg_load_ts
FROM ranked
WHERE rn = 1
  AND status != 'CANCELLED';      -- business rule
