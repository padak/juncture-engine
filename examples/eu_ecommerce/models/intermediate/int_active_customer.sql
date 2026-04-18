-- Shared business rule: "active customer" = completed an order within the
-- last N days (var: active_customer_days, default 90).
--
-- Materialised ephemerally via models/intermediate/schema.yml so that
-- every downstream mart sees exactly one definition. Change the var once,
-- every reader reflects it.
--
-- Solves VISION problem #2 (no macros / shared blocks) and #3 (weak
-- parametrization): the active-customer rule travels through the DAG as a
-- typed dependency, not a copy-pasted WHERE clause.
SELECT
    o.customer_id,
    MAX(o.placed_date)                               AS last_order_date,
    COUNT(*)                                          AS orders_in_window,
    SUM(o.total_amount_eur)                           AS spend_in_window_eur,
    CAST('{{ var('reporting_end_date') }}' AS DATE)   AS reporting_end_date,
    {{ var('active_customer_days') }}                 AS active_customer_days
FROM {{ ref('stg_orders') }} o
WHERE o.status = 'completed'
  AND o.placed_date >= (
        CAST('{{ var('reporting_end_date') }}' AS DATE)
        - INTERVAL '{{ var('active_customer_days') }}' DAY
      )
  AND o.placed_date <= CAST('{{ var('reporting_end_date') }}' AS DATE)
GROUP BY o.customer_id
