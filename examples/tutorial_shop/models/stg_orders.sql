-- Staging: type-cast timestamp + filter out refunded orders.
-- Every downstream mart starts from this clean baseline.
SELECT
    order_id,
    customer_id,
    CAST(order_ts AS TIMESTAMP) AS order_ts,
    CAST(amount_eur AS DOUBLE)  AS amount_eur,
    status
FROM {{ ref('orders') }}
WHERE status = 'completed'
