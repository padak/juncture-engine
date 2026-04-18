-- Daily revenue by country × marketing-channel group.
--
-- Source of the Python anomaly detector (daily_revenue_anomalies). Pivoting
-- to Python upstream of this table means the anomaly logic stays close to
-- the time-series shape rather than being embedded in yet more CASE WHENs.
--
-- Only completed orders contribute. Organic/unattributed orders fall into
-- channel_group = 'organic' so the join is complete.
SELECT
    o.placed_date                                  AS revenue_date,
    o.country_code,
    COALESCE(o.campaign_channel_group, 'organic')  AS channel_group,
    COUNT(*)                                       AS order_count,
    COUNT(DISTINCT o.customer_id)                  AS unique_customers,
    SUM(o.total_amount_eur)                        AS gross_revenue_eur,
    SUM(o.shipping_cost_eur)                       AS shipping_revenue_eur,
    SUM(o.items_margin_eur)                        AS margin_eur,
    SUM(o.items_qty)                               AS units_sold
FROM {{ ref('int_order_facts') }} o
WHERE o.status = 'completed'
GROUP BY o.placed_date, o.country_code, COALESCE(o.campaign_channel_group, 'organic')
