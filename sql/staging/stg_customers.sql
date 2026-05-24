-- =====================================================================
-- STAGING.STG_CUSTOMERS
-- =====================================================================
-- Cleans RAW.CUSTOMERS:
--   * Dedupes (keeps most recently modified row per customer_id)
--   * Standardizes email to lowercase
--   * Adds _stg_load_ts for traceability
-- Idempotent: CREATE OR REPLACE runs cleanly any number of times.
-- =====================================================================

CREATE OR REPLACE TABLE STAGING.STG_CUSTOMERS AS
WITH ranked AS (
    SELECT
        customer_id,
        LOWER(email)                   AS email,
        INITCAP(first_name)            AS first_name,
        INITCAP(last_name)             AS last_name,
        UPPER(customer_segment)        AS customer_segment,
        UPPER(country)                 AS country,
        signup_date,
        created_at,
        modified_at,
        _load_ts,
        _source_file,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY modified_at DESC, _load_ts DESC
        ) AS rn
    FROM RAW.CUSTOMERS
    WHERE customer_id IS NOT NULL
)
SELECT
    customer_id,
    email,
    first_name,
    last_name,
    customer_segment,
    country,
    signup_date,
    created_at,
    modified_at,
    _load_ts                       AS _raw_load_ts,
    _source_file                   AS _raw_source_file,
    CURRENT_TIMESTAMP()            AS _stg_load_ts
FROM ranked
WHERE rn = 1;
