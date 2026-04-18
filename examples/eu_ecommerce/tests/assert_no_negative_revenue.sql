-- Every staged order must have a non-negative revenue amount. A negative
-- total would indicate a broken extractor or an upstream refund/credit
-- note leaking into the "orders" feed.
SELECT order_id, total_amount_eur
FROM {{ ref('stg_orders') }}
WHERE total_amount_eur < 0
