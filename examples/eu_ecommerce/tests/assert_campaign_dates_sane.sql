-- Campaigns should never end before they start. A violation is almost
-- certainly a typo in the marketing feed and would break duration-based
-- CPM / ROAS reporting downstream.
SELECT campaign_id, started_at, ended_at
FROM {{ ref('stg_campaigns') }}
WHERE ended_at < started_at
