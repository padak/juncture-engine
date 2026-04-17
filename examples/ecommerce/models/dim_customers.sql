SELECT
    customer_id,
    name,
    email,
    country,
    signed_up_at,
    DATE_DIFF('day', signed_up_at, CURRENT_DATE) AS days_since_signup
FROM {{ ref('raw_customers') }}
