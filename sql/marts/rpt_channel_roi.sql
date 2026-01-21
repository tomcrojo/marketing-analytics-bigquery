-- =============================================================================
-- rpt_channel_roi.sql
-- Mart: channel-level ROI comparison with best/worst campaign identification
-- =============================================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.analytics_marketing.rpt_channel_roi`
OPTIONS (
  description = "Channel-level ROI and efficiency metrics for strategic analysis",
  labels = [("mart", "channel_roi"), ("refresh", "daily")]
) AS

WITH channel_agg AS (
  SELECT
    channel,
    SUM(total_impressions)                AS total_impressions,
    SUM(total_clicks)                     AS total_clicks,
    SUM(total_conversions)                AS total_conversions,
    SUM(total_cost_euros)                 AS total_spend_euros,
    SUM(total_revenue_euros)              AS total_revenue_euros
  FROM `${PROJECT_ID}.analytics_marketing.int_campaign_performance`
  GROUP BY channel
),

campaign_rankings AS (
  SELECT
    channel,
    campaign_id,
    campaign_name,
    roas,
    ROW_NUMBER() OVER (
      PARTITION BY channel ORDER BY roas DESC NULLS LAST
    ) AS best_rank,
    ROW_NUMBER() OVER (
      PARTITION BY channel ORDER BY roas ASC NULLS LAST
    ) AS worst_rank
  FROM `${PROJECT_ID}.analytics_marketing.int_campaign_performance`
  WHERE total_cost_euros > 0
),

best_campaigns AS (
  SELECT
    channel,
    campaign_name                         AS best_campaign_name,
    ROUND(roas, 2)                        AS best_campaign_roas
  FROM campaign_rankings
  WHERE best_rank = 1
),

worst_campaigns AS (
  SELECT
    channel,
    campaign_name                         AS worst_campaign_name,
    ROUND(roas, 2)                        AS worst_campaign_roas
  FROM campaign_rankings
  WHERE worst_rank = 1
)

SELECT
  a.channel,
  a.total_impressions,
  a.total_clicks,
  a.total_conversions,
  ROUND(a.total_spend_euros, 2)         AS total_spend_euros,
  ROUND(a.total_revenue_euros, 2)       AS total_revenue_euros,
  ROUND((a.total_revenue_euros - a.total_spend_euros), 2) AS profit_euros,
  ROUND(SAFE_DIVIDE(
    (a.total_revenue_euros - a.total_spend_euros),
    a.total_spend_euros
  ) * 100, 2)                            AS roi_pct,
  ROUND(SAFE_DIVIDE(a.total_spend_euros, a.total_conversions), 2) AS avg_cpa_eur,
  ROUND(SAFE_DIVIDE(a.total_revenue_euros, a.total_spend_euros), 2) AS avg_roas,
  ROUND(SAFE_DIVIDE(a.total_clicks, a.total_impressions) * 100, 2) AS ctr_pct,
  ROUND(SAFE_DIVIDE(a.total_conversions, a.total_clicks) * 100, 2) AS conversion_rate_pct,
  b.best_campaign_name,
  b.best_campaign_roas,
  w.worst_campaign_name,
  w.worst_campaign_roas,

  -- Channel health score (0-100)
  LEAST(100, GREATEST(0, ROUND(
    (SAFE_DIVIDE(a.total_revenue_euros, a.total_spend_euros) * 40) +
    (SAFE_DIVIDE(a.total_clicks, a.total_impressions) * 100 * 30) +
    (SAFE_DIVIDE(a.total_conversions, a.total_clicks) * 100 * 30)
  , 0))) AS channel_health_score

FROM channel_agg a
LEFT JOIN best_campaigns b ON a.channel = b.channel
LEFT JOIN worst_campaigns w ON a.channel = w.channel;
