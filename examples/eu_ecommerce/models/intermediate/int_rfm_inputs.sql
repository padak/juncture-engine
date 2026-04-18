-- Recency / Frequency / Monetary inputs, one row per customer.
--
-- Keeps the heavy GROUP BY out of the Python RFM scorer so that the
-- Python model only handles the quintile logic (VISION #8: Python + SQL
-- in one DAG, each doing what it's best at).
--
-- Only completed orders feed RFM: refunds are business-level cancellations
-- and shouldn't inflate "monetary".
SELECT
    o.customer_id,
    CAST('{{ var('reporting_end_date') }}' AS DATE) - MAX(o.placed_date) AS recency_days,
    COUNT(*)                                                              AS frequency,
    SUM(o.total_amount_eur)                                               AS monetary_eur,
    AVG(o.total_amount_eur)                                               AS avg_order_value_eur,
    MIN(o.placed_date)                                                    AS first_order_date,
    MAX(o.placed_date)                                                    AS last_order_date
FROM {{ ref('stg_orders') }} o
WHERE o.status = 'completed'
GROUP BY o.customer_id
