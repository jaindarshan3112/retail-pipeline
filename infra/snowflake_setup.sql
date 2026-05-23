-- =====================================================================
-- NorthWind Retail Pipeline — Snowflake Setup
-- =====================================================================
-- Run this script ONCE as ACCOUNTADMIN after creating your Snowflake
-- account. It creates:
--   * A dedicated ETL_ROLE (least-privilege)
--   * A dedicated ETL_USER for the pipeline
--   * An X-SMALL warehouse with auto-suspend (cost-optimized)
--   * RETAIL_DB with five schemas
--   * All required grants
--
-- IMPORTANT: Replace 'CHANGE_ME_STRONG_PASSWORD' below with a real
-- strong password and SAVE IT — you'll need it for Airflow.
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- --- Warehouse ---------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS ETL_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60          -- suspend after 60s idle → near-zero cost
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse for retail ETL pipeline';

-- --- Database & schemas ------------------------------------------------
CREATE DATABASE IF NOT EXISTS RETAIL_DB
  COMMENT = 'NorthWind retail pipeline warehouse';

USE DATABASE RETAIL_DB;

CREATE SCHEMA IF NOT EXISTS RAW       COMMENT = 'Exact source copies, append-only';
CREATE SCHEMA IF NOT EXISTS STAGING   COMMENT = 'Cleaned, typed, deduped';
CREATE SCHEMA IF NOT EXISTS CURATED   COMMENT = 'Dimensional model: dims + facts';
CREATE SCHEMA IF NOT EXISTS MARTS     COMMENT = 'Business-ready aggregates';
CREATE SCHEMA IF NOT EXISTS METADATA  COMMENT = 'Watermarks, run logs, DQ results';

-- Drop default PUBLIC schema (not needed)
DROP SCHEMA IF EXISTS PUBLIC;

-- --- Role --------------------------------------------------------------
CREATE ROLE IF NOT EXISTS ETL_ROLE
  COMMENT = 'Role for retail ETL pipeline';

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE ETL_WH TO ROLE ETL_ROLE;

-- Grant database & schema access
GRANT USAGE ON DATABASE RETAIL_DB TO ROLE ETL_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE RETAIL_DB TO ROLE ETL_ROLE;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE RETAIL_DB TO ROLE ETL_ROLE;

-- Grant full DML/DDL on all schemas (we own the data here)
GRANT ALL ON ALL SCHEMAS IN DATABASE RETAIL_DB TO ROLE ETL_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE RETAIL_DB TO ROLE ETL_ROLE;

GRANT ALL ON ALL TABLES IN DATABASE RETAIL_DB TO ROLE ETL_ROLE;
GRANT ALL ON FUTURE TABLES IN DATABASE RETAIL_DB TO ROLE ETL_ROLE;

GRANT ALL ON ALL VIEWS IN DATABASE RETAIL_DB TO ROLE ETL_ROLE;
GRANT ALL ON FUTURE VIEWS IN DATABASE RETAIL_DB TO ROLE ETL_ROLE;

GRANT ALL ON ALL STAGES IN DATABASE RETAIL_DB TO ROLE ETL_ROLE;
GRANT ALL ON FUTURE STAGES IN DATABASE RETAIL_DB TO ROLE ETL_ROLE;

GRANT ALL ON ALL FILE FORMATS IN DATABASE RETAIL_DB TO ROLE ETL_ROLE;
GRANT ALL ON FUTURE FILE FORMATS IN DATABASE RETAIL_DB TO ROLE ETL_ROLE;

-- --- User --------------------------------------------------------------
-- !!! Replace the password before running !!!
CREATE USER IF NOT EXISTS ETL_USER
  PASSWORD = 'CHANGE_ME_STRONG_PASSWORD'
  DEFAULT_ROLE = ETL_ROLE
  DEFAULT_WAREHOUSE = ETL_WH
  DEFAULT_NAMESPACE = 'RETAIL_DB.RAW'
  MUST_CHANGE_PASSWORD = FALSE
  COMMENT = 'Service user for retail ETL pipeline';

GRANT ROLE ETL_ROLE TO USER ETL_USER;

-- --- Verify ------------------------------------------------------------
USE ROLE ETL_ROLE;
USE WAREHOUSE ETL_WH;
USE DATABASE RETAIL_DB;

SHOW SCHEMAS IN DATABASE RETAIL_DB;
SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE();

-- Expected output:
--   CURRENT_ROLE      = ETL_ROLE
--   CURRENT_WAREHOUSE = ETL_WH
--   CURRENT_DATABASE  = RETAIL_DB
