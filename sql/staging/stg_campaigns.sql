-- =============================================================================
-- stg_campaigns.sql
-- Staging: clean campaigns — normalize channel names, compute derived fields
-- =============================================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.staging_marketing.stg_campaigns` AS

WITH cleaned AS (
  SELECT
    campaign_id,
    TRIM(campaign_name)                   AS campaign_name,
    TRIM(UPPER(channel))                  AS channel,
    TRIM(LOWER(platform))                 AS platform,
    start_date,
    end_date,
    DATE_DIFF(end_date, start_date, DAY)  AS campaign_duration_days,
    budget_euros,

    -- Budget tier
    CASE
      WHEN budget_euros < 2000            THEN 'small'
      WHEN budget_euros BETWEEN 2000 AND 7000  THEN 'medium'
      WHEN budget_euros BETWEEN 7000 AND 10000  THEN 'large'
      ELSE 'enterprise'
    END AS budget_tier,

    COALESCE(target_audience, 'unspecified') AS target_audience,
    TRIM(LOWER(objective))                AS objective,

    -- Channel group
    CASE TRIM(UPPER(channel))
      WHEN 'GOOGLE_ADS'   THEN 'Paid Search'
      WHEN 'META_ADS'     THEN 'Paid Social'
      WHEN 'TIKTOK_ADS'   THEN 'Paid Social'
      WHEN 'LINKEDIN_ADS' THEN 'Paid Social'
      WHEN 'EMAIL'        THEN 'Owned'
      WHEN 'SEO'          THEN 'Organic'
      ELSE 'Other'
    END AS channel_group

  FROM `${PROJECT_ID}.raw_marketing.raw_campaigns`
)

SELECT * FROM cleaned;
