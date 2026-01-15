-- =============================================================================
-- stg_ad_impressions.sql
-- Staging: clean raw impressions — deduplicate, cast types, convert costs
-- =============================================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.staging_marketing.stg_ad_impressions` AS

WITH deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY impression_id
      ORDER BY timestamp DESC
    ) AS _row_num
  FROM `${PROJECT_ID}.raw_marketing.raw_ad_impressions`
),

cleaned AS (
  SELECT
    impression_id,
    campaign_id,
    TRIM(UPPER(channel))                  AS channel,
    TRIM(LOWER(platform))                 AS platform,
    timestamp,
    DATE(timestamp)                       AS impression_date,
    TIMESTAMP_TRUNC(timestamp, WEEK(MONDAY)) AS impression_week,
    DATE_TRUNC(DATE(timestamp), MONTH)    AS impression_month,
    COALESCE(device_type, 'unknown')      AS device_type,
    COALESCE(country, 'XX')               AS country,
    cost_micros,
    ROUND(cost_micros / 1000000.0, 4)    AS cost_euros
  FROM deduplicated
  WHERE _row_num = 1
)

SELECT * FROM cleaned;
