-- =============================================================================
-- int_campaign_performance.sql
-- Intermediate: aggregate per-campaign performance metrics
-- =============================================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.analytics_marketing.int_campaign_performance` AS

WITH impression_agg AS (
  SELECT
    campaign_id,
    channel,
    platform,
    COUNT(*)                              AS total_impressions,
    SUM(cost_euros)                       AS impression_spend_euros
  FROM `${PROJECT_ID}.staging_marketing.stg_ad_impressions`
  GROUP BY campaign_id, channel, platform
),

click_agg AS (
  SELECT
    campaign_id,
    COUNT(*)                              AS total_clicks,
    SUM(cost_euros)                       AS click_spend_euros,
    AVG(seconds_to_click)                 AS avg_seconds_to_click
  FROM `${PROJECT_ID}.staging_marketing.stg_clicks`
  GROUP BY campaign_id
),

conversion_agg AS (
  SELECT
    campaign_id,
    COUNT(*)                              AS total_conversions,
    SUM(revenue_euros)                    AS total_revenue_euros,
    COUNTIF(conversion_type = 'purchase') AS total_purchases,
    COUNTIF(conversion_type = 'signup')   AS total_signups,
    COUNTIF(conversion_type = 'lead')     AS total_leads,
    COUNT(DISTINCT customer_id)           AS unique_customers
  FROM `${PROJECT_ID}.staging_marketing.stg_conversions`
  GROUP BY campaign_id
),

campaign_joined AS (
  SELECT
    c.campaign_id,
    c.campaign_name,
    c.channel,
    c.platform,
    c.objective,
    c.budget_euros,
    c.budget_tier,
    c.channel_group,
    c.campaign_duration_days,
    c.start_date,
    c.end_date,

    COALESCE(i.total_impressions, 0)      AS total_impressions,
    COALESCE(cl.total_clicks, 0)          AS total_clicks,
    COALESCE(cv.total_conversions, 0)     AS total_conversions,
    COALESCE(i.impression_spend_euros, 0) AS total_cost_euros,
    COALESCE(cv.total_revenue_euros, 0)   AS total_revenue_euros,
    COALESCE(cv.total_purchases, 0)       AS total_purchases,
    COALESCE(cv.total_signups, 0)         AS total_signups,
    COALESCE(cv.total_leads, 0)           AS total_leads,
    COALESCE(cv.unique_customers, 0)      AS unique_customers,

    SAFE_DIVIDE(cl.total_clicks, i.total_impressions)       AS ctr,
    SAFE_DIVIDE(i.impression_spend_euros, cl.total_clicks)  AS cpc_euros,
    SAFE_DIVIDE(i.impression_spend_euros, cv.total_conversions) AS cpa_euros,
    SAFE_DIVIDE(cv.total_revenue_euros, i.impression_spend_euros) AS roas,
    SAFE_DIVIDE(cv.total_conversions, cl.total_clicks)      AS conversion_rate

  FROM `${PROJECT_ID}.staging_marketing.stg_campaigns` c
  LEFT JOIN impression_agg i ON c.campaign_id = i.campaign_id
  LEFT JOIN click_agg cl     ON c.campaign_id = cl.campaign_id
  LEFT JOIN conversion_agg cv ON c.campaign_id = cv.campaign_id
)

SELECT * FROM campaign_joined;
