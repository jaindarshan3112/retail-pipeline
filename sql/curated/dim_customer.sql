-- =====================================================================
-- CURATED.DIM_CUSTOMER
-- =====================================================================
-- Customer dimension with a surrogate key (customer_sk).
-- Phase 2 = SCD Type 1 (overwrite on change). SCD Type 2 (history)
-- would be a Phase 5+ enhancement.
-- =====================================================================

CREATE OR REPLACE TABLE CURATED.DIM_CUSTOMER AS
SELECT
    -- Surrogate key — stable hash of natural key
    HASH(customer_id)              AS customer_sk,
    -- Natural key
    customer_id                    AS customer_nk,
    -- Attributes
    email,
    first_name,
    last_name,
    first_name || ' ' || last_name AS full_name,
    customer_segment,
    country,
    signup_date,
    DATEDIFF('day', signup_date, CURRENT_DATE()) AS tenure_days,
    -- Metadata
    _stg_load_ts                   AS _last_updated_at
FROM STAGING.STG_CUSTOMERS;
