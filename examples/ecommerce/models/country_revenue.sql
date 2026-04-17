SELECT
    c.country,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.amount)              AS revenue
FROM {{ ref('fct_completed_orders') }} o
JOIN {{ ref('dim_customers') }} c USING (customer_id)
GROUP BY c.country
ORDER BY revenue DESC
