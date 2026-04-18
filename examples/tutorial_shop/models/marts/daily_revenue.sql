-- Daily revenue, windowed by external `as_of` + `lookback_days` vars.
-- Change the window from the CLI without touching the SQL:
--   juncture run --project examples/tutorial_shop --var as_of=2026-02-01 --var lookback_days=30
--
-- Uses the shared my_date() macro so the date format is defined once.
SELECT
    {{ my_date('order_ts') }} AS day,
    COUNT(*)                  AS orders,
    SUM(amount_eur)           AS revenue_eur,
    ROUND(AVG(amount_eur), 2) AS avg_order_eur,
    SUM(CASE WHEN {{ is_vip('amount_eur') }} THEN amount_eur ELSE 0 END) AS vip_revenue_eur
FROM {{ ref('stg_orders') }}
WHERE order_ts >= CAST('{{ var("as_of") }}' AS DATE)
                  - INTERVAL '{{ var("lookback_days") }} days'
  AND order_ts < CAST('{{ var("as_of") }}' AS DATE) + INTERVAL '1 day'
GROUP BY 1
ORDER BY 1
