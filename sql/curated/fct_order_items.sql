-- =====================================================================
-- CURATED.FCT_ORDER_ITEMS
-- =====================================================================
-- One row per order line item — the finest grain we have.
-- Line totals are USD-converted using the parent order's currency.
-- =====================================================================

CREATE OR REPLACE TABLE CURATED.FCT_ORDER_ITEMS AS
WITH latest_fx AS (
    SELECT target_currency, usd_per_unit
    FROM (
        SELECT
            target_currency,
            usd_per_unit,
            ROW_NUMBER() OVER (PARTITION BY target_currency ORDER BY rate_date DESC) AS rn
        FROM STAGING.STG_FX_RATES
    )
    WHERE rn = 1
)
SELECT
    oi.order_item_id               AS order_item_nk,
    HASH(oi.order_item_id)         AS order_item_sk,
    -- FKs
    HASH(oi.order_id)              AS order_sk,
    HASH(o.customer_id)            AS customer_sk,
    HASH(oi.product_id)            AS product_sk,
    TO_NUMBER(TO_CHAR(o.order_date, 'YYYYMMDD')) AS order_date_key,
    -- Measures
    oi.quantity,
    oi.unit_price                  AS unit_price_native,
    oi.line_total                  AS line_total_native,
    o.currency_code                AS native_currency,
    fx.usd_per_unit                AS fx_rate_to_usd,
    ROUND(oi.unit_price * fx.usd_per_unit, 2)  AS unit_price_usd,
    ROUND(oi.line_total  * fx.usd_per_unit, 2) AS line_total_usd,
    -- Attributes
    o.order_date,
    o.shipping_country,
    -- Metadata
    oi._stg_load_ts                AS _last_updated_at
FROM STAGING.STG_ORDER_ITEMS oi
INNER JOIN STAGING.STG_ORDERS o
        ON oi.order_id = o.order_id
LEFT JOIN latest_fx fx
       ON o.currency_code = fx.target_currency;
