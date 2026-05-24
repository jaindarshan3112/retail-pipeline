-- =====================================================================
-- MARTS.REVENUE_BY_COUNTRY
-- =====================================================================
-- Monthly revenue by shipping country, in USD.
-- =====================================================================

CREATE OR REPLACE TABLE MARTS.REVENUE_BY_COUNTRY AS
SELECT
    d.year,
    d.month,
    d.month_name,
    o.shipping_country,
    COUNT(*)                                        AS order_count,
    COUNT(DISTINCT o.customer_sk)                   AS unique_customers,
    ROUND(SUM(o.total_amount_usd), 2)               AS revenue_usd,
    ROUND(AVG(o.total_amount_usd), 2)               AS avg_order_value_usd,
    CURRENT_TIMESTAMP()                             AS _last_built_at
FROM CURATED.FCT_ORDERS o
INNER JOIN CURATED.DIM_DATE d ON o.order_date_key = d.date_key
GROUP BY d.year, d.month, d.month_name, o.shipping_country
ORDER BY d.year, d.month, revenue_usd DESC;
