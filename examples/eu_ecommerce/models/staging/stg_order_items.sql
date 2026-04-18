-- Staged order line items with product context and EUR-normalised net amount.
--
-- The join to stg_products ensures every line carries catalogue metadata
-- (category, cost, margin) without every downstream mart re-joining. The
-- join to stg_orders drops lines that belonged to pending/cancelled orders
-- (those rows are already filtered upstream).
--
-- net_amount_eur = quantity * unit_price_eur * (1 - discount_pct),
-- computed in EUR regardless of the order currency so BI layers see one
-- consistent unit.
SELECT
    oi.order_id,
    CAST(oi.line_no AS INTEGER)               AS line_no,
    CAST(oi.product_id AS BIGINT)             AS product_id,
    p.category_id,
    p.subcategory,
    p.department,
    CAST(oi.quantity AS INTEGER)              AS quantity,
    CAST(oi.unit_price AS DOUBLE)             AS unit_price,
    CAST(oi.discount_pct AS DOUBLE)           AS discount_pct,
    p.cost_eur                                AS unit_cost_eur,
    CAST(oi.unit_price AS DOUBLE)
        * CAST(oi.quantity AS INTEGER)
        * (1 - CAST(oi.discount_pct AS DOUBLE))                         AS net_amount_eur,
    (CAST(oi.unit_price AS DOUBLE)
        * CAST(oi.quantity AS INTEGER)
        * (1 - CAST(oi.discount_pct AS DOUBLE)))
        - (p.cost_eur * CAST(oi.quantity AS INTEGER))                   AS margin_eur,
    o.customer_id,
    o.placed_at,
    o.placed_date,
    o.status,
    o.campaign_id
FROM {{ ref('order_items') }} oi
JOIN {{ ref('stg_orders') }}   o USING (order_id)
JOIN {{ ref('stg_products') }} p ON p.product_id = CAST(oi.product_id AS BIGINT)
