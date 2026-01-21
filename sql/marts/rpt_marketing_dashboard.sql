-- =============================================================================
-- rpt_marketing_dashboard.sql
-- Mart: executive dashboard table — daily channel performance
-- =============================================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.analytics_marketing.rpt_marketing_dashboard`
PARTITION BY impression_date
CLUSTER BY channel
OPTIONS (
  description = "Daily marketing performance metrics for Looker dashboard",
  labels = [("mart", "dashboard"), ("refresh", "daily")]
) AS

WITH daily_impressions AS (
  SELECT
    impression_date,
    channel,
    platform,
    COUNT(*)                              AS impressions,
    SUM(cost_euros)                       AS spend_euros
  FROM `${PROJECT_ID}.staging_marketing.stg_ad_impressions`
  GROUP BY impression_date, channel, platform
),

daily_clicks AS (
  SELECT
    click_date,
    channel,
    platform,
    COUNT(*)                              AS clicks
  FROM `${PROJECT_ID}.staging_marketing.stg_clicks`
  GROUP BY click_date, channel, platform
),

daily_conversions AS (
  SELECT
    conversion_date,
    channel,
    platform,
    COUNT(*)                              AS conversions,
    SUM(revenue_euros)                    AS revenue_euros,
    COUNTIF(conversion_type = 'purchase') AS purchases,
    COUNTIF(conversion_type = 'signup')   AS signups,
    COUNTIF(conversion_type = 'lead')     AS leads
  FROM `${PROJECT_ID}.staging_marketing.stg_conversions`
  GROUP BY conversion_date, channel, platform
)

SELECT
  i.impression_date                       AS date,
  i.channel,
  i.platform,
  COALESCE(i.impressions, 0)             AS impressions,
  COALESCE(c.clicks, 0)                  AS clicks,
  COALESCE(cv.conversions, 0)            AS conversions,
  ROUND(COALESCE(i.spend_euros, 0), 2)  AS spend_euros,
  ROUND(COALESCE(cv.revenue_euros, 0), 2) AS revenue_euros,
  ROUND(SAFE_DIVIDE(c.clicks, i.impressions) * 100, 2) AS ctr_pct,
  ROUND(SAFE_DIVIDE(i.spend_euros, c.clicks), 2)       AS cpc_eur,
  ROUND(SAFE_DIVIDE(i.spend_euros, cv.conversions), 2) AS cpa_eur,
  ROUND(SAFE_DIVIDE(cv.revenue_euros, i.spend_euros), 2) AS roas,
  ROUND(SAFE_DIVIDE(cv.conversions, c.clicks) * 100, 2)  AS conversion_rate_pct,
  COALESCE(cv.purchases, 0)              AS purchases,
  COALESCE(cv.signups, 0)               AS signups,
  COALESCE(cv.leads, 0)                 AS leads,

  -- Budget efficiency
  ROUND(SAFE_DIVIDE(i.spend_euros, cv.revenue_euros) * 100, 2) AS cost_of_revenue_pct

FROM daily_impressions i
LEFT JOIN daily_clicks c
  ON i.impression_date = c.click_date
  AND i.channel = c.channel
  AND i.platform = c.platform
LEFT JOIN daily_conversions cv
  ON i.impression_date = cv.conversion_date
  AND i.channel = cv.channel
  AND i.platform = cv.platform;
