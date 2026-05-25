-- =====================================================================
-- NorthWind Retail Pipeline — RAW table DDL
-- =====================================================================
-- Run this AFTER snowflake_setup.sql, while logged in as ETL_USER (or
-- any role that has been granted ETL_ROLE).
--
-- This creates:
--   * An internal stage for landing files: RAW.ETL_STAGE
--   * Two file formats: Parquet (for Postgres extracts) and JSON (APIs)
--   * Five RAW tables, one per source dataset
--
-- DESIGN NOTES:
--
--   1. Postgres-sourced tables have TYPED columns. We control the source
--      schema so we know what to expect.
--
--   2. API-sourced tables (PRODUCTS, FX_RATES) use a single VARIANT
--      column called RAW_DATA. This is THE key pattern for ingesting
--      semi-structured data: if the API adds a new field tomorrow, our
--      pipeline doesn't break — the new field just shows up inside
--      RAW_DATA and we choose when to surface it in STAGING.
--
--   3. Every table has metadata columns (_LOAD_TS, _SOURCE_FILE) that
--      let us trace WHEN a row was loaded and from WHICH file. Invaluable
--      for debugging.
--
--   4. RAW is APPEND-ONLY. We never UPDATE or DELETE rows here. The same
--      record may exist multiple times (once per pipeline run). STAGING
--      will dedupe.
-- =====================================================================

USE ROLE ETL_ROLE;
USE WAREHOUSE ETL_WH;
USE DATABASE RETAIL_DB;
USE SCHEMA RAW;


-- ---------- File formats ----------------------------------------------

CREATE OR REPLACE FILE FORMAT RAW.FF_PARQUET
  TYPE = PARQUET
  COMPRESSION = AUTO;

CREATE OR REPLACE FILE FORMAT RAW.FF_JSON
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE      -- treats `[ {...}, {...} ]` as multiple rows
  COMPRESSION = AUTO;


-- ---------- Internal stage --------------------------------------------

CREATE OR REPLACE STAGE RAW.ETL_STAGE
  FILE_FORMAT = RAW.FF_PARQUET
  COMMENT = 'Landing zone for files uploaded by the ETL pipeline';


-- ---------- Postgres-sourced RAW tables (typed) -----------------------

CREATE OR REPLACE TABLE RAW.CUSTOMERS (
    customer_id       NUMBER,
    email             VARCHAR,
    first_name        VARCHAR,
    last_name         VARCHAR,
    customer_segment  VARCHAR,
    country           VARCHAR,
    signup_date       DATE,
    created_at        TIMESTAMP_NTZ,
    modified_at       TIMESTAMP_NTZ,
    _load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file      VARCHAR
);

CREATE OR REPLACE TABLE RAW.ORDERS (
    order_id          NUMBER,
    customer_id       NUMBER,
    order_date        DATE,
    status            VARCHAR,
    shipping_country  VARCHAR,
    currency_code     VARCHAR,
    total_amount      NUMBER(12,2),
    created_at        TIMESTAMP_NTZ,
    modified_at       TIMESTAMP_NTZ,
    _load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file      VARCHAR
);

CREATE OR REPLACE TABLE RAW.ORDER_ITEMS (
    order_item_id     NUMBER,
    order_id          NUMBER,
    product_id        NUMBER,
    quantity          NUMBER,
    unit_price        NUMBER(12,2),
    created_at        TIMESTAMP_NTZ,
    _load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file      VARCHAR
);


-- ---------- API-sourced RAW tables (VARIANT pattern) ------------------

CREATE OR REPLACE TABLE RAW.PRODUCTS (
    raw_data          VARIANT,
    _load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file      VARCHAR
);

CREATE OR REPLACE TABLE RAW.FX_RATES (
    raw_data          VARIANT,
    _load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file      VARCHAR
);


-- ---------- Verify ----------------------------------------------------

SHOW TABLES IN SCHEMA RAW;
SHOW STAGES IN SCHEMA RAW;
SHOW FILE FORMATS IN SCHEMA RAW;
