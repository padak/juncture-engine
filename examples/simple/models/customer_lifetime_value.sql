-- Join users to orders and aggregate by customer.
SELECT
    u.id                     AS user_id,
    u.name                   AS user_name,
    u.email                  AS email,
    COUNT(o.order_id)        AS total_orders,
    COALESCE(SUM(o.amount), 0) AS lifetime_value
FROM {{ ref('stg_users') }}  AS u
LEFT JOIN {{ ref('stg_orders') }} AS o
       ON o.user_id = u.id
GROUP BY u.id, u.name, u.email
ORDER BY lifetime_value DESC
