-- =====================================================================
-- CURATED.DIM_DATE
-- =====================================================================
-- A standard date dimension covering 2022-01-01 through 2030-12-31.
-- Generated programmatically using Snowflake's GENERATOR.
--
-- Why a date dim?
--   * Lets analysts join on a single date_key and slice by year, quarter,
--     month, weekday, etc. without repeating DATE_TRUNC everywhere.
--   * Pre-computes "is_weekend", fiscal periods, etc. once.
-- =====================================================================

CREATE OR REPLACE TABLE CURATED.DIM_DATE AS
WITH date_spine AS (
    SELECT DATEADD(day, SEQ4(), '2022-01-01'::DATE) AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 3287))      -- ~9 years
)
SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))             AS date_key,        -- 20260523
    d                                             AS date,
    YEAR(d)                                       AS year,
    QUARTER(d)                                    AS quarter,
    MONTH(d)                                      AS month,
    TO_CHAR(d, 'MON')                             AS month_name,
    DAY(d)                                        AS day_of_month,
    DAYOFWEEK(d)                                  AS day_of_week,     -- 0=Sun
    TO_CHAR(d, 'DY')                              AS day_name,
    WEEKOFYEAR(d)                                 AS week_of_year,
    CASE WHEN DAYOFWEEK(d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    DATE_TRUNC('month',   d)                      AS month_start,
    LAST_DAY(d, 'month')                          AS month_end,
    DATE_TRUNC('quarter', d)                      AS quarter_start,
    DATE_TRUNC('year',    d)                      AS year_start
FROM date_spine;
