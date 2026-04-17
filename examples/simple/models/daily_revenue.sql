SELECT
    order_date,
    COUNT(*)          AS order_count,
    SUM(amount)       AS revenue,
    ROUND(AVG(amount), 2) AS avg_order_value
FROM {{ ref('stg_orders') }}
GROUP BY order_date
ORDER BY order_date
