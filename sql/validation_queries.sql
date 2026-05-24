-- =====================================================================
-- Phase 2 Validation Queries
-- =====================================================================
-- After running `python run_phase2.py`, paste these into a Snowflake
-- worksheet and run them one at a time. Each section verifies a layer
-- of the pipeline.
-- =====================================================================

USE ROLE ETL_ROLE;
USE WAREHOUSE ETL_WH;
USE DATABASE RETAIL_DB;


-- ---------- 1. Row counts at every layer ------------------------------
-- Sanity check: numbers should make sense.
-- RAW should ~match Postgres source counts (10K / 100K / ~300K).
-- STAGING.STG_ORDERS will be SMALLER than RAW.ORDERS because we
-- filtered out CANCELLED orders (~5%).

SELECT 'RAW.CUSTOMERS'           AS table_name, COUNT(*) AS row_count FROM RAW.CUSTOMERS
UNION ALL SELECT 'RAW.ORDERS',            COUNT(*) FROM RAW.ORDERS
UNION ALL SELECT 'RAW.ORDER_ITEMS',       COUNT(*) FROM RAW.ORDER_ITEMS
UNION ALL SELECT 'RAW.PRODUCTS',          COUNT(*) FROM RAW.PRODUCTS
UNION ALL SELECT 'RAW.FX_RATES',          COUNT(*) FROM RAW.FX_RATES
UNION ALL SELECT '--- STAGING ---',       NULL
UNION ALL SELECT 'STAGING.STG_CUSTOMERS', COUNT(*) FROM STAGING.STG_CUSTOMERS
UNION ALL SELECT 'STAGING.STG_ORDERS',    COUNT(*) FROM STAGING.STG_ORDERS
UNION ALL SELECT 'STAGING.STG_ORDER_ITEMS', COUNT(*) FROM STAGING.STG_ORDER_ITEMS
UNION ALL SELECT 'STAGING.STG_PRODUCTS',  COUNT(*) FROM STAGING.STG_PRODUCTS
UNION ALL SELECT 'STAGING.STG_FX_RATES',  COUNT(*) FROM STAGING.STG_FX_RATES
UNION ALL SELECT '--- CURATED ---',       NULL
UNION ALL SELECT 'CURATED.DIM_DATE',      COUNT(*) FROM CURATED.DIM_DATE
UNION ALL SELECT 'CURATED.DIM_CUSTOMER',  COUNT(*) FROM CURATED.DIM_CUSTOMER
UNION ALL SELECT 'CURATED.DIM_PRODUCT',   COUNT(*) FROM CURATED.DIM_PRODUCT
UNION ALL SELECT 'CURATED.FCT_ORDERS',    COUNT(*) FROM CURATED.FCT_ORDERS
UNION ALL SELECT 'CURATED.FCT_ORDER_ITEMS', COUNT(*) FROM CURATED.FCT_ORDER_ITEMS
UNION ALL SELECT '--- MARTS ---',         NULL
UNION ALL SELECT 'MARTS.REVENUE_BY_CATEGORY',  COUNT(*) FROM MARTS.REVENUE_BY_CATEGORY
UNION ALL SELECT 'MARTS.REVENUE_BY_COUNTRY',   COUNT(*) FROM MARTS.REVENUE_BY_COUNTRY
UNION ALL SELECT 'MARTS.CUSTOMER_SEGMENT_KPIS', COUNT(*) FROM MARTS.CUSTOMER_SEGMENT_KPIS;


-- ---------- 2. Sample data from each mart -----------------------------

-- Revenue by category — should show 4 categories with USD totals
SELECT * FROM MARTS.REVENUE_BY_CATEGORY ORDER BY revenue_usd DESC LIMIT 20;

-- Revenue by country — top countries by revenue
SELECT * FROM MARTS.REVENUE_BY_COUNTRY ORDER BY revenue_usd DESC LIMIT 20;

-- Customer segment KPIs — three rows (CONSUMER, SMB, ENTERPRISE)
SELECT * FROM MARTS.CUSTOMER_SEGMENT_KPIS;


-- ---------- 3. Data quality spot-checks -------------------------------

-- All orders should have an FX rate (no NULL conversions)
SELECT COUNT(*) AS orders_missing_fx
FROM CURATED.FCT_ORDERS
WHERE fx_rate_to_usd IS NULL;
-- Expected: 0

-- Every order item should join to an order
SELECT COUNT(*) AS orphan_items
FROM CURATED.FCT_ORDER_ITEMS oi
LEFT JOIN CURATED.FCT_ORDERS o ON oi.order_sk = o.order_sk
WHERE o.order_sk IS NULL;
-- Expected: 0

-- Revenue should be positive
SELECT
    MIN(total_amount_usd) AS min_revenue,
    MAX(total_amount_usd) AS max_revenue,
    AVG(total_amount_usd) AS avg_revenue
FROM CURATED.FCT_ORDERS;


-- ---------- 4. Trace data lineage -------------------------------------
-- Pick a customer and trace them through every layer

SET sample_customer_id = (SELECT MIN(customer_id) FROM RAW.CUSTOMERS);

SELECT 'RAW'  AS layer, customer_id, email, modified_at FROM RAW.CUSTOMERS
WHERE customer_id = $sample_customer_id
UNION ALL
SELECT 'STG', customer_id, email, modified_at FROM STAGING.STG_CUSTOMERS
WHERE customer_id = $sample_customer_id
UNION ALL
SELECT 'DIM', customer_nk, email, _last_updated_at FROM CURATED.DIM_CUSTOMER
WHERE customer_nk = $sample_customer_id;


-- ---------- 5. Business answer (proves the whole stack works) ---------
-- "What was our top product category last month?"

SELECT
    p.category,
    COUNT(DISTINCT fi.order_sk)        AS orders,
    SUM(fi.quantity)                   AS units,
    ROUND(SUM(fi.line_total_usd), 2)   AS revenue_usd
FROM CURATED.FCT_ORDER_ITEMS fi
INNER JOIN CURATED.DIM_PRODUCT p ON fi.product_sk = p.product_sk
INNER JOIN CURATED.DIM_DATE    d ON fi.order_date_key = d.date_key
WHERE d.date >= DATEADD('month', -1, CURRENT_DATE())
GROUP BY p.category
ORDER BY revenue_usd DESC;
