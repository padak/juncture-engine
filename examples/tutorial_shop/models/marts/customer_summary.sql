-- Customer-facing mart: who is VIP, which country, when they signed up.
-- Joins the customers seed with the ephemeral tier dimension so the
-- VIP rule lives in exactly one place (macros/tiers.sql + the ephemeral
-- block), not duplicated here.
SELECT
    c.customer_id,
    c.email,
    c.country,
    c.signed_up_at,
    COALESCE(t.lifetime_value_eur, 0) AS lifetime_value_eur,
    COALESCE(t.completed_orders, 0)   AS completed_orders,
    COALESCE(t.tier, 'new')           AS tier
FROM {{ ref('customers') }}       AS c
LEFT JOIN {{ ref('customer_tier') }} AS t USING (customer_id)
ORDER BY lifetime_value_eur DESC
