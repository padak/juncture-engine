-- Staged campaign metadata.
--
-- Responsibilities:
--   * Cast ids, dates, budgets out of CSV VARCHAR.
--   * Derive campaign_duration_days so downstream marts can join against
--     cost-per-day without recomputing.
--   * Map the raw channel to channel_group (paid vs organic-ish) used by
--     the marketing-mix reporting. Intentionally simple: the demo
--     illustrates "one place to define business rules".
SELECT
    CAST(campaign_id AS BIGINT)                          AS campaign_id,
    name,
    channel,
    CASE
        WHEN channel IN ('display', 'social') THEN 'paid_media'
        WHEN channel = 'email'                THEN 'owned'
        WHEN channel = 'affiliate'            THEN 'partnerships'
        ELSE 'other'
    END                                                  AS channel_group,
    CAST(started_at AS DATE)                             AS started_at,
    CAST(ended_at AS DATE)                               AS ended_at,
    DATE_DIFF('day',
              CAST(started_at AS DATE),
              CAST(ended_at   AS DATE)
    )                                                    AS campaign_duration_days,
    CAST(budget_eur AS DOUBLE)                           AS budget_eur,
    target_country
FROM {{ ref('campaigns') }}
