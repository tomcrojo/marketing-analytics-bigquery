-- =============================================================================
-- stg_clicks.sql
-- Staging: clean raw clicks — deduplicate, join impression context, compute timing
-- =============================================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.staging_marketing.stg_clicks` AS

WITH deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY click_id
      ORDER BY timestamp DESC
    ) AS _row_num
  FROM `${PROJECT_ID}.raw_marketing.raw_clicks`
),

clicks_cleaned AS (
  SELECT
    click_id,
    impression_id,
    campaign_id,
    TRIM(UPPER(channel))                  AS channel,
    TRIM(LOWER(platform))                 AS platform,
    timestamp                             AS click_timestamp,
    DATE(timestamp)                       AS click_date,
    TIMESTAMP_TRUNC(timestamp, WEEK(MONDAY)) AS click_week,
    DATE_TRUNC(DATE(timestamp), MONTH)    AS click_month,
    COALESCE(device_type, 'unknown')      AS device_type,
    COALESCE(country, 'XX')               AS country,
    cost_micros,
    ROUND(cost_micros / 1000000.0, 4)    AS cost_euros
  FROM deduplicated
  WHERE _row_num = 1
),

joined AS (
  SELECT
    c.*,
    i.timestamp                           AS impression_timestamp,
    TIMESTAMP_DIFF(c.click_timestamp, i.timestamp, SECOND) AS seconds_to_click
  FROM clicks_cleaned c
  LEFT JOIN `${PROJECT_ID}.staging_marketing.stg_ad_impressions` i
    ON c.impression_id = i.impression_id
)

SELECT * FROM joined;
