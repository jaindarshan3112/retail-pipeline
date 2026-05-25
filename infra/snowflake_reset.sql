-- =====================================================================
-- Reset script — run this if you need to start Phase 2 over with a
-- clean slate (e.g., after fixing the timestamp issue).
--
-- This drops every table in RAW/STAGING/CURATED/MARTS so the next run
-- of run_phase2.py rebuilds them from scratch with the correct types.
--
-- The stage, file formats, role, user, warehouse, and database are
-- preserved — only data and table definitions go.
-- =====================================================================

USE ROLE ETL_ROLE;
USE WAREHOUSE ETL_WH;
USE DATABASE RETAIL_DB;

-- Drop tables in each layer
DROP TABLE IF EXISTS RAW.CUSTOMERS;
DROP TABLE IF EXISTS RAW.ORDERS;
DROP TABLE IF EXISTS RAW.ORDER_ITEMS;
DROP TABLE IF EXISTS RAW.PRODUCTS;
DROP TABLE IF EXISTS RAW.FX_RATES;

DROP TABLE IF EXISTS STAGING.STG_CUSTOMERS;
DROP TABLE IF EXISTS STAGING.STG_ORDERS;
DROP TABLE IF EXISTS STAGING.STG_ORDER_ITEMS;
DROP TABLE IF EXISTS STAGING.STG_PRODUCTS;
DROP TABLE IF EXISTS STAGING.STG_FX_RATES;

DROP TABLE IF EXISTS CURATED.DIM_DATE;
DROP TABLE IF EXISTS CURATED.DIM_CUSTOMER;
DROP TABLE IF EXISTS CURATED.DIM_PRODUCT;
DROP TABLE IF EXISTS CURATED.FCT_ORDERS;
DROP TABLE IF EXISTS CURATED.FCT_ORDER_ITEMS;

DROP TABLE IF EXISTS MARTS.REVENUE_BY_CATEGORY;
DROP TABLE IF EXISTS MARTS.REVENUE_BY_COUNTRY;
DROP TABLE IF EXISTS MARTS.CUSTOMER_SEGMENT_KPIS;

-- Clean up the stage too (removes leftover files from prior runs)
REMOVE @RAW.ETL_STAGE;

-- Verify everything is gone
SHOW TABLES IN DATABASE RETAIL_DB;
LIST @RAW.ETL_STAGE;
