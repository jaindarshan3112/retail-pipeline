-- =====================================================================
-- MARTS.CUSTOMER_SEGMENT_KPIS
-- =====================================================================
-- Lifetime KPIs per customer segment (CONSUMER, SMB, ENTERPRISE).
-- =====================================================================

CREATE OR REPLACE TABLE MARTS.CUSTOMER_SEGMENT_KPIS AS
WITH per_customer AS (
    SELECT
        c.customer_segment,
        c.customer_sk,
        COUNT(o.order_sk)                       AS lifetime_orders,
        SUM(o.total_amount_usd)                 AS lifetime_revenue_usd
    FROM CURATED.DIM_CUSTOMER c
    LEFT JOIN CURATED.FCT_ORDERS o
           ON c.customer_sk = o.customer_sk
    GROUP BY c.customer_segment, c.customer_sk
)
SELECT
    customer_segment,
    COUNT(*)                                    AS customer_count,
    SUM(CASE WHEN lifetime_orders > 0 THEN 1 ELSE 0 END) AS active_customers,
    ROUND(AVG(lifetime_orders), 2)              AS avg_orders_per_customer,
    ROUND(AVG(lifetime_revenue_usd), 2)         AS avg_lifetime_revenue_usd,
    ROUND(SUM(lifetime_revenue_usd), 2)         AS total_revenue_usd,
    CURRENT_TIMESTAMP()                         AS _last_built_at
FROM per_customer
GROUP BY customer_segment
ORDER BY total_revenue_usd DESC NULLS LAST;
