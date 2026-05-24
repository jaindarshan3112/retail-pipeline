-- =====================================================================
-- STAGING.STG_FX_RATES
-- =====================================================================
-- Flattens RAW.FX_RATES VARIANT into a typed table.
--
-- VARIANT shape per row:
--   { "base": "USD", "rate_date": "2026-05-23", "currency": "EUR", "rate": 0.92 }
--
-- The rate is "1 USD = N units of <currency>". So to convert a value
-- expressed in <currency> back to USD: usd = value / rate.
-- We pre-compute USD_PER_UNIT for convenience in the fact tables.
--
-- We also INSERT a synthetic row for USD→USD with rate=1.0 so the
-- downstream join doesn't lose USD-denominated orders.
-- =====================================================================

CREATE OR REPLACE TABLE STAGING.STG_FX_RATES AS
WITH ranked AS (
    SELECT
        raw_data:base::VARCHAR          AS base_currency,
        raw_data:rate_date::DATE        AS rate_date,
        UPPER(raw_data:currency::VARCHAR) AS target_currency,
        raw_data:rate::FLOAT            AS rate,
        _load_ts,
        ROW_NUMBER() OVER (
            PARTITION BY raw_data:currency::VARCHAR, raw_data:rate_date::DATE
            ORDER BY _load_ts DESC
        ) AS rn
    FROM RAW.FX_RATES
    WHERE raw_data:currency IS NOT NULL
),
deduped AS (
    SELECT
        base_currency,
        rate_date,
        target_currency,
        rate,
        (1.0 / rate)                   AS usd_per_unit
    FROM ranked
    WHERE rn = 1
)
SELECT * FROM deduped
UNION ALL
-- Synthetic USD self-row so USD orders join cleanly
SELECT
    'USD'                              AS base_currency,
    (SELECT MAX(rate_date) FROM deduped) AS rate_date,
    'USD'                              AS target_currency,
    1.0                                AS rate,
    1.0                                AS usd_per_unit;
