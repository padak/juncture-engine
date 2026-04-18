-- Campaign performance mart: one row per campaign with revenue, order count
-- and ROAS (return on ad spend, = attributed revenue / budget).
--
-- Attribution is "last-touch within campaign window": an order's campaign_id
-- comes from the upstream fact; this mart just aggregates. If a campaign had
-- zero attributed orders it still shows up (LEFT JOIN) so that ROAS = 0.
SELECT
    cm.campaign_id,
    cm.name                                      AS campaign_name,
    cm.channel,
    cm.channel_group,
    cm.target_country,
    cm.started_at,
    cm.ended_at,
    cm.campaign_duration_days,
    cm.budget_eur,
    COUNT(o.order_id)                            AS attributed_orders,
    COUNT(DISTINCT o.customer_id)                AS unique_customers,
    COALESCE(SUM(o.total_amount_eur), 0.0)       AS attributed_revenue_eur,
    COALESCE(SUM(o.items_margin_eur), 0.0)       AS attributed_margin_eur,
    CASE
        WHEN cm.budget_eur = 0 THEN 0.0
        ELSE COALESCE(SUM(o.total_amount_eur), 0.0) / cm.budget_eur
    END                                          AS roas,
    CASE
        WHEN cm.budget_eur = 0 THEN 0.0
        ELSE COALESCE(SUM(o.items_margin_eur), 0.0) / cm.budget_eur
    END                                          AS return_on_margin
FROM {{ ref('stg_campaigns') }}   cm
LEFT JOIN {{ ref('int_order_facts') }} o
    ON o.campaign_id = cm.campaign_id
   AND o.status       = 'completed'
GROUP BY
    cm.campaign_id,
    cm.name,
    cm.channel,
    cm.channel_group,
    cm.target_country,
    cm.started_at,
    cm.ended_at,
    cm.campaign_duration_days,
    cm.budget_eur
