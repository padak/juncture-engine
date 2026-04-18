-- Staged customer dimension.
--
-- Responsibilities:
--   * Cast id + date + boolean columns out of CSV VARCHAR.
--   * Normalise email to lowercase (downstream marts dedup by email).
--   * Derive signup_year so cohort queries don't repeat the EXTRACT.
SELECT
    CAST(customer_id AS BIGINT)                              AS customer_id,
    LOWER(TRIM(email))                                        AS email,
    country_code,
    city,
    CAST(signup_date AS DATE)                                 AS signup_date,
    EXTRACT(YEAR FROM CAST(signup_date AS DATE))              AS signup_year,
    preferred_language,
    CAST(marketing_consent AS BOOLEAN)                        AS marketing_consent
FROM {{ ref('customers') }}
