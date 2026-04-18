-- Country-level cohort retention.
--
-- Groups customers by their signup month and counts how many of them had
-- any order in subsequent months. Emits one row per (cohort_month × country
-- × order_month) so a BI tool can plot a standard cohort triangle.
WITH base AS (
    SELECT
        c.customer_id,
        c.country_code,
        DATE_TRUNC('month', c.signup_date)             AS cohort_month
    FROM {{ ref('stg_customers') }} c
),
order_months AS (
    SELECT DISTINCT
        o.customer_id,
        DATE_TRUNC('month', o.placed_date)             AS order_month
    FROM {{ ref('stg_orders') }} o
    WHERE o.status = 'completed'
),
cohort_sizes AS (
    SELECT
        cohort_month,
        country_code,
        COUNT(DISTINCT customer_id)                    AS cohort_size
    FROM base
    GROUP BY cohort_month, country_code
)
SELECT
    b.cohort_month,
    b.country_code,
    om.order_month,
    DATE_DIFF('month', b.cohort_month, om.order_month) AS months_since_signup,
    COUNT(DISTINCT b.customer_id)                      AS active_customers,
    cs.cohort_size,
    CASE
        WHEN cs.cohort_size = 0 THEN 0.0
        ELSE COUNT(DISTINCT b.customer_id) * 1.0 / cs.cohort_size
    END                                                AS retention_rate
FROM base b
JOIN order_months om USING (customer_id)
JOIN cohort_sizes cs
    ON cs.cohort_month = b.cohort_month
   AND cs.country_code = b.country_code
WHERE om.order_month >= b.cohort_month
GROUP BY
    b.cohort_month,
    b.country_code,
    om.order_month,
    cs.cohort_size
