-- =====================================================================
-- CURATED.FCT_ORDERS
-- =====================================================================
-- One row per order. Total amount converted to USD using the latest
-- FX rate for the order's currency.
--
-- NOTE on FX simplification: in real life you'd join on order_date to
-- get the rate that was effective on that date. Phase 2 uses the
-- LATEST rate for simplicity. Phase 5 can demonstrate the proper
-- AS-OF join pattern.
-- =====================================================================

CREATE OR REPLACE TABLE CURATED.FCT_ORDERS AS
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
    o.order_id                     AS order_nk,
    HASH(o.order_id)               AS order_sk,
    -- Foreign keys to dimensions
    HASH(o.customer_id)            AS customer_sk,
    TO_NUMBER(TO_CHAR(o.order_date, 'YYYYMMDD')) AS order_date_key,
    -- Measures
    o.total_amount                 AS total_amount_native,
    o.currency_code                AS native_currency,
    fx.usd_per_unit                AS fx_rate_to_usd,
    ROUND(o.total_amount * fx.usd_per_unit, 2) AS total_amount_usd,
    -- Attributes
    o.order_date,
    o.status,
    o.shipping_country,
    -- Metadata
    o._stg_load_ts                 AS _last_updated_at
FROM STAGING.STG_ORDERS o
LEFT JOIN latest_fx fx
       ON o.currency_code = fx.target_currency;
