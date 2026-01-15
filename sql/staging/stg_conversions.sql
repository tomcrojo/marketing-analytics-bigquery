-- =============================================================================
-- stg_conversions.sql
-- Staging: clean raw conversions — deduplicate, enrich with campaign/channel info
-- =============================================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.staging_marketing.stg_conversions` AS

WITH deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY conversion_id
      ORDER BY timestamp DESC
    ) AS _row_num
  FROM `${PROJECT_ID}.raw_marketing.raw_conversions`
),

conversions_cleaned AS (
  SELECT
    conversion_id,
    click_id,
    campaign_id,
    customer_id,
    timestamp                             AS conversion_timestamp,
    DATE(timestamp)                       AS conversion_date,
    TIMESTAMP_TRUNC(timestamp, WEEK(MONDAY)) AS conversion_week,
    DATE_TRUNC(DATE(timestamp), MONTH)    AS conversion_month,
    COALESCE(revenue, 0.0)               AS revenue_euros,
    LOWER(TRIM(conversion_type))          AS conversion_type
  FROM deduplicated
  WHERE _row_num = 1
),

enriched AS (
  SELECT
    c.*,
    cl.channel                            AS channel,
    cl.platform                           AS platform,
    cl.device_type                        AS device_type,
    cl.country                            AS country,
    cl.click_timestamp,
    TIMESTAMP_DIFF(c.conversion_timestamp, cl.click_timestamp, MINUTE) AS minutes_to_convert,

    -- Revenue tier classification
    CASE
      WHEN c.revenue_euros = 0                THEN 'no_value'
      WHEN c.revenue_euros < 50               THEN 'low'
      WHEN c.revenue_euros BETWEEN 50 AND 200  THEN 'medium'
      WHEN c.revenue_euros BETWEEN 200 AND 500  THEN 'high'
      ELSE 'premium'
    END AS revenue_tier,

    -- Conversion type label
    CASE c.conversion_type
      WHEN 'purchase' THEN 'Purchase'
      WHEN 'signup'   THEN 'Signup'
      WHEN 'lead'     THEN 'Lead'
      ELSE 'Other'
    END AS conversion_type_label

  FROM conversions_cleaned c
  LEFT JOIN `${PROJECT_ID}.staging_marketing.stg_clicks` cl
    ON c.click_id = cl.click_id
)

SELECT * FROM enriched;
