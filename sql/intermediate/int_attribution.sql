-- =============================================================================
-- int_attribution.sql
-- Intermediate: multi-touch attribution models (first-touch, last-touch, linear)
-- =============================================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.analytics_marketing.int_attribution` AS

-- Build customer journey: all touchpoints (clicks) leading to conversion
WITH customer_journey AS (
  SELECT
    cv.conversion_id,
    cv.customer_id,
    cv.campaign_id                       AS conversion_campaign_id,
    cv.conversion_timestamp,
    cv.revenue_euros,
    cv.conversion_type,

    cl.click_id,
    cl.campaign_id                       AS click_campaign_id,
    cl.channel                           AS touch_channel,
    cl.platform                          AS touch_platform,
    cl.click_timestamp,

    -- Rank touchpoints chronologically per customer conversion
    ROW_NUMBER() OVER (
      PARTITION BY cv.conversion_id
      ORDER BY cl.click_timestamp ASC
    ) AS touch_sequence,

    COUNT(*) OVER (
      PARTITION BY cv.conversion_id
    ) AS total_touches,

    -- First and last touch flags
    ROW_NUMBER() OVER (
      PARTITION BY cv.conversion_id
      ORDER BY cl.click_timestamp ASC
    ) = 1 AS is_first_touch,

    ROW_NUMBER() OVER (
      PARTITION BY cv.conversion_id
      ORDER BY cl.click_timestamp DESC
    ) = 1 AS is_last_touch

  FROM `${PROJECT_ID}.staging_marketing.stg_conversions` cv
  INNER JOIN `${PROJECT_ID}.staging_marketing.stg_clicks` cl
    ON cv.customer_id IN (
      SELECT DISTINCT customer_id
      FROM `${PROJECT_ID}.staging_marketing.stg_conversions`
      WHERE conversion_id = cv.conversion_id
    )
    AND cl.click_timestamp <= cv.conversion_timestamp
),

-- First-touch attribution
first_touch AS (
  SELECT
    conversion_id,
    customer_id,
    revenue_euros,
    conversion_type,
    touch_channel                         AS attributed_channel,
    touch_platform                        AS attributed_platform,
    click_campaign_id                     AS attributed_campaign_id,
    revenue_euros                         AS attributed_revenue,
    1.0                                   AS attribution_weight,
    'first_touch'                         AS attribution_model
  FROM customer_journey
  WHERE is_first_touch
),

-- Last-touch attribution
last_touch AS (
  SELECT
    conversion_id,
    customer_id,
    revenue_euros,
    conversion_type,
    touch_channel                         AS attributed_channel,
    touch_platform                        AS attributed_platform,
    click_campaign_id                     AS attributed_campaign_id,
    revenue_euros                         AS attributed_revenue,
    1.0                                   AS attribution_weight,
    'last_touch'                          AS attribution_model
  FROM customer_journey
  WHERE is_last_touch
),

-- Linear attribution
linear AS (
  SELECT
    conversion_id,
    customer_id,
    revenue_euros,
    conversion_type,
    touch_channel                         AS attributed_channel,
    touch_platform                        AS attributed_platform,
    click_campaign_id                     AS attributed_campaign_id,
    SAFE_DIVIDE(revenue_euros, total_touches) AS attributed_revenue,
    SAFE_DIVIDE(1.0, total_touches)       AS attribution_weight,
    'linear'                              AS attribution_model
  FROM customer_journey
),

-- Union all models
all_attribution AS (
  SELECT * FROM first_touch
  UNION ALL
  SELECT * FROM last_touch
  UNION ALL
  SELECT * FROM linear
)

SELECT * FROM all_attribution;
