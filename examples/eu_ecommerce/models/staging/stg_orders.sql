-- Staged orders (finalised business rows only).
--
-- Responsibilities:
--   * Cast ids and timestamps out of CSV VARCHAR.
--   * Normalise amounts to EUR. CZK orders are divided by 24.5 (our demo
--     exchange rate); every downstream model can therefore trust
--     total_amount_eur without re-converting.
--   * Filter to closed statuses: completed + refunded. Pending and
--     cancelled orders never hit the warehouse.
--   * Map empty-string campaign_id back to NULL so relationships tests
--     don't trip on the CSV encoding artefact.
SELECT
    CAST(order_id AS BIGINT)                                           AS order_id,
    CAST(customer_id AS BIGINT)                                        AS customer_id,
    CAST(placed_at AS TIMESTAMP)                                       AS placed_at,
    CAST(placed_at AS DATE)                                            AS placed_date,
    status,
    currency,
    CASE
        WHEN currency = 'CZK' THEN CAST(total_amount AS DOUBLE) / 24.5
        ELSE CAST(total_amount AS DOUBLE)
    END                                                                 AS total_amount_eur,
    CASE
        WHEN currency = 'CZK' THEN CAST(shipping_cost AS DOUBLE) / 24.5
        ELSE CAST(shipping_cost AS DOUBLE)
    END                                                                 AS shipping_cost_eur,
    CAST(total_amount AS DOUBLE)                                       AS total_amount,
    CAST(shipping_cost AS DOUBLE)                                      AS shipping_cost,
    NULLIF(CAST(campaign_id AS VARCHAR), '')                           AS campaign_id_raw,
    TRY_CAST(NULLIF(CAST(campaign_id AS VARCHAR), '') AS BIGINT)       AS campaign_id
FROM {{ ref('orders') }}
WHERE status IN ('completed', 'refunded')
