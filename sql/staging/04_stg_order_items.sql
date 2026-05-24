-- =====================================================================
-- STAGING.STG_ORDER_ITEMS
-- =====================================================================
-- Cleans RAW.ORDER_ITEMS:
--   * Dedupes by order_item_id
--   * Computes line_total (quantity * unit_price)
--   * Only keeps items belonging to non-cancelled orders
-- =====================================================================

CREATE OR REPLACE TABLE STAGING.STG_ORDER_ITEMS AS
WITH ranked AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        created_at,
        _load_ts,
        _source_file,
        ROW_NUMBER() OVER (
            PARTITION BY order_item_id
            ORDER BY _load_ts DESC
        ) AS rn
    FROM RAW.ORDER_ITEMS
    WHERE order_item_id IS NOT NULL
      AND quantity > 0
      AND unit_price >= 0
)
SELECT
    r.order_item_id,
    r.order_id,
    r.product_id,
    r.quantity,
    r.unit_price,
    (r.quantity * r.unit_price)    AS line_total,
    r.created_at,
    r._load_ts                     AS _raw_load_ts,
    r._source_file                 AS _raw_source_file,
    CURRENT_TIMESTAMP()            AS _stg_load_ts
FROM ranked r
INNER JOIN STAGING.STG_ORDERS o
       ON r.order_id = o.order_id      -- excludes cancelled orders
WHERE r.rn = 1;
