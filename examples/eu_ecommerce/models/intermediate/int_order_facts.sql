-- Order-grain fact table: one row per order with customer + campaign +
-- rolled-up product context.
--
-- This is the join-hub that marts consume. Doing the heavy joins once
-- here keeps every downstream mart query short and fast.
WITH order_item_rollup AS (
    SELECT
        order_id,
        SUM(net_amount_eur)             AS items_net_eur,
        SUM(margin_eur)                  AS items_margin_eur,
        SUM(quantity)                    AS items_qty,
        COUNT(*)                         AS line_count,
        MIN(department)                  AS first_department,
        STRING_AGG(DISTINCT department, '|') AS departments
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
)
SELECT
    o.order_id,
    o.customer_id,
    c.country_code,
    c.city,
    c.preferred_language,
    c.marketing_consent,
    o.placed_at,
    o.placed_date,
    EXTRACT(YEAR FROM o.placed_date)         AS placed_year,
    EXTRACT(MONTH FROM o.placed_date)        AS placed_month,
    DATE_TRUNC('month', o.placed_date)       AS placed_month_start,
    o.status,
    o.currency,
    o.total_amount_eur,
    o.shipping_cost_eur,
    oir.items_net_eur,
    oir.items_margin_eur,
    COALESCE(oir.items_qty, 0)               AS items_qty,
    COALESCE(oir.line_count, 0)              AS line_count,
    oir.first_department,
    oir.departments,
    o.campaign_id,
    cm.channel                                AS campaign_channel,
    cm.channel_group                          AS campaign_channel_group,
    cm.target_country                         AS campaign_target_country
FROM {{ ref('stg_orders') }}         o
JOIN {{ ref('stg_customers') }}      c  USING (customer_id)
LEFT JOIN order_item_rollup          oir USING (order_id)
LEFT JOIN {{ ref('stg_campaigns') }} cm ON cm.campaign_id = o.campaign_id
