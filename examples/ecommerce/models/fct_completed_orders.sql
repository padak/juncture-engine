-- Finalized orders only. Refunds and pending orders are filtered out.
SELECT
    order_id,
    customer_id,
    order_date,
    amount
FROM {{ ref('raw_orders') }}
WHERE status = 'completed'
