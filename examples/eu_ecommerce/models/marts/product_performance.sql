-- Product performance mart: sales, refund rate, margin realisation per SKU.
--
-- Refund rate = refunded revenue / (completed + refunded revenue). We join
-- stg_order_items to stg_orders so that refund-status context is available
-- at line level (stg_order_items inherits status from the order).
SELECT
    p.product_id,
    p.sku,
    p.name,
    p.department,
    p.subcategory,
    p.price_eur,
    p.cost_eur,
    p.margin_pct                                      AS catalog_margin_pct,
    COUNT(DISTINCT CASE WHEN oi.status = 'completed' THEN oi.order_id END)
        AS completed_orders,
    COUNT(DISTINCT CASE WHEN oi.status = 'refunded' THEN oi.order_id END)
        AS refunded_orders,
    SUM(CASE WHEN oi.status = 'completed' THEN oi.quantity ELSE 0 END)
        AS units_sold,
    SUM(CASE WHEN oi.status = 'completed' THEN oi.net_amount_eur ELSE 0 END)
        AS revenue_eur,
    SUM(CASE WHEN oi.status = 'refunded'  THEN oi.net_amount_eur ELSE 0 END)
        AS refunded_revenue_eur,
    SUM(CASE WHEN oi.status = 'completed' THEN oi.margin_eur ELSE 0 END)
        AS realised_margin_eur,
    CASE
        WHEN COALESCE(SUM(oi.net_amount_eur), 0) = 0 THEN 0
        ELSE SUM(CASE WHEN oi.status = 'refunded' THEN oi.net_amount_eur ELSE 0 END)
             / SUM(oi.net_amount_eur)
    END                                               AS refund_rate,
    CASE
        WHEN SUM(CASE WHEN oi.status = 'completed' THEN oi.net_amount_eur ELSE 0 END) = 0
            THEN 0
        ELSE SUM(CASE WHEN oi.status = 'completed' THEN oi.margin_eur ELSE 0 END)
             / SUM(CASE WHEN oi.status = 'completed' THEN oi.net_amount_eur ELSE 0 END)
    END                                               AS realised_margin_pct
FROM {{ ref('stg_products') }}    p
LEFT JOIN {{ ref('stg_order_items') }} oi USING (product_id)
GROUP BY
    p.product_id,
    p.sku,
    p.name,
    p.department,
    p.subcategory,
    p.price_eur,
    p.cost_eur,
    p.margin_pct
