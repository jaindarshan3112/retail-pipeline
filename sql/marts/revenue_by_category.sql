-- =====================================================================
-- MARTS.REVENUE_BY_CATEGORY
-- =====================================================================
-- Weekly revenue by product category, in USD.
-- Pre-aggregated so BI dashboards just SELECT without scanning facts.
-- =====================================================================

CREATE OR REPLACE TABLE MARTS.REVENUE_BY_CATEGORY AS
SELECT
    d.year,
    d.month,
    d.week_of_year,
    p.category,
    COUNT(DISTINCT fi.order_sk)                     AS order_count,
    SUM(fi.quantity)                                AS units_sold,
    ROUND(SUM(fi.line_total_usd), 2)                AS revenue_usd,
    ROUND(AVG(fi.line_total_usd), 2)                AS avg_line_usd,
    CURRENT_TIMESTAMP()                             AS _last_built_at
FROM CURATED.FCT_ORDER_ITEMS fi
INNER JOIN CURATED.DIM_PRODUCT p ON fi.product_sk = p.product_sk
INNER JOIN CURATED.DIM_DATE    d ON fi.order_date_key = d.date_key
GROUP BY d.year, d.month, d.week_of_year, p.category
ORDER BY d.year, d.month, d.week_of_year, revenue_usd DESC;
