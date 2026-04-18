-- Staged product catalogue with margin metrics pre-computed.
--
-- Responsibilities:
--   * Cast ids and prices out of CSV VARCHAR.
--   * Denormalise department from product_categories so order_items can
--     join to a single table for department-level rollups.
--   * Compute margin_pct once so marts don't all divide cost/price.
SELECT
    CAST(p.product_id AS BIGINT)                     AS product_id,
    p.sku,
    p.name,
    CAST(p.category_id AS INTEGER)                   AS category_id,
    c.department,
    p.subcategory,
    CAST(p.price_eur AS DOUBLE)                      AS price_eur,
    CAST(p.cost_eur AS DOUBLE)                       AS cost_eur,
    CAST(p.price_eur AS DOUBLE) - CAST(p.cost_eur AS DOUBLE)
        AS unit_margin_eur,
    CASE
        WHEN CAST(p.price_eur AS DOUBLE) = 0 THEN 0
        ELSE (CAST(p.price_eur AS DOUBLE) - CAST(p.cost_eur AS DOUBLE))
             / CAST(p.price_eur AS DOUBLE)
    END                                              AS margin_pct,
    CAST(p.launched_at AS DATE)                      AS launched_at,
    CAST(p.is_active AS BOOLEAN)                     AS is_active
FROM {{ ref('products') }}                p
JOIN {{ ref('product_categories') }}      c
    ON CAST(p.category_id AS INTEGER) = CAST(c.category_id AS INTEGER)
