-- Customer segmentation: vip / loyal / regular / at_risk / lost.
--
-- Ingests three upstreams:
--   * stg_customers: baseline demographic attributes.
--   * int_rfm_inputs: aggregated order history.
--   * int_active_customer (ephemeral): "did they buy in the last N days?".
--
-- Thresholds live in juncture.yaml vars. Changing VIP to require 15+ orders
-- is a one-line diff (VISION #3 — weak parametrization solved).
--
-- Segment precedence (first match wins):
--   vip      >= vip_threshold_orders AND >= vip_threshold_spend_eur
--   loyal    at least active + 3 orders
--   regular  active customer, one or two orders
--   at_risk  not active now, but ordered within the last 365 days
--   lost     everyone else with any order history
WITH joined AS (
    SELECT
        c.customer_id,
        c.country_code,
        c.city,
        c.preferred_language,
        c.marketing_consent,
        c.signup_date,
        r.frequency,
        r.monetary_eur,
        r.recency_days,
        r.last_order_date,
        ac.orders_in_window,
        ac.spend_in_window_eur,
        ac.last_order_date AS recent_last_order_date
    FROM {{ ref('stg_customers') }}    c
    LEFT JOIN {{ ref('int_rfm_inputs') }}      r
        ON r.customer_id = c.customer_id
    LEFT JOIN {{ ref('int_active_customer') }} ac
        ON ac.customer_id = c.customer_id
)
SELECT
    customer_id,
    country_code,
    city,
    preferred_language,
    marketing_consent,
    signup_date,
    COALESCE(frequency, 0)                         AS frequency,
    COALESCE(monetary_eur, 0.0)                    AS monetary_eur,
    recency_days,
    last_order_date,
    CASE
        WHEN COALESCE(frequency, 0)   >= {{ var('vip_threshold_orders') }}
         AND COALESCE(monetary_eur, 0) >= {{ var('vip_threshold_spend_eur') }}
             THEN 'vip'
        WHEN orders_in_window >= 3                 THEN 'loyal'
        WHEN orders_in_window IS NOT NULL          THEN 'regular'
        WHEN recency_days IS NOT NULL
             AND recency_days <= 365               THEN 'at_risk'
        WHEN frequency IS NOT NULL                 THEN 'lost'
        ELSE 'lost'
    END                                            AS segment
FROM joined
