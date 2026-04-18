-- Ephemeral dimension: per-customer lifetime value and tier.
-- Materialized as an inlined block (materialization: ephemeral in schema.yml)
-- so it lives in the DAG as a named result set but isn't persisted.
-- Downstream marts ref() it instead of recomputing the aggregation.
SELECT
    customer_id,
    SUM(amount_eur) AS lifetime_value_eur,
    COUNT(*)        AS completed_orders,
    CASE
        WHEN SUM(amount_eur) >= {{ var('vip_threshold_eur', 500) }} THEN 'vip'
        WHEN SUM(amount_eur) >= 200                                 THEN 'regular'
        ELSE 'new'
    END AS tier
FROM {{ ref('stg_orders') }}
GROUP BY customer_id
