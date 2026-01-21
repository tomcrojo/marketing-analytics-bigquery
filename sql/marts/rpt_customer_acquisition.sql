-- =============================================================================
-- rpt_customer_acquisition.sql
-- Mart: customer acquisition funnel with LTV and payback metrics
-- =============================================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.analytics_marketing.rpt_customer_acquisition`
OPTIONS (
  description = "Customer acquisition cost, LTV, and payback metrics by channel",
  labels = [("mart", "customer_acquisition"), ("refresh", "daily")]
) AS

-- First conversion per customer (acquisition event)
WITH customer_first_conversion AS (
  SELECT
    customer_id,
    channel                               AS acquisition_channel,
    platform                              AS acquisition_platform,
    campaign_id                           AS acquisition_campaign_id,
    conversion_timestamp                  AS acquisition_timestamp,
    DATE_TRUNC(conversion_timestamp, MONTH) AS acquisition_month,
    MIN(conversion_timestamp) OVER (
      PARTITION BY customer_id
    ) AS first_conversion_timestamp
  FROM `${PROJECT_ID}.staging_marketing.stg_conversions`
),

-- Deduplicate to first conversion only
acquired_customers AS (
  SELECT *
  FROM customer_first_conversion
  WHERE conversion_timestamp = first_conversion_timestamp
),

-- All conversions for LTV calculation
customer_conversions AS (
  SELECT
    customer_id,
    conversion_timestamp,
    revenue_euros,
    DATE_DIFF(conversion_timestamp,
      MIN(conversion_timestamp) OVER (PARTITION BY customer_id), DAY
    ) AS days_since_first
  FROM `${PROJECT_ID}.staging_marketing.stg_conversions`
),

-- LTV windows
customer_ltv AS (
  SELECT
    customer_id,
    SUM(CASE WHEN days_since_first <= 30  THEN revenue_euros ELSE 0 END) AS ltv_30d,
    SUM(CASE WHEN days_since_first <= 90  THEN revenue_euros ELSE 0 END) AS ltv_90d,
    SUM(revenue_euros)                   AS ltv_total,
    COUNT(*)                             AS total_conversions
  FROM customer_conversions
  GROUP BY customer_id
),

-- Channel spend
channel_spend AS (
  SELECT
    channel,
    SUM(total_cost_euros)                AS total_spend
  FROM `${PROJECT_ID}.analytics_marketing.int_campaign_performance`
  GROUP BY channel
),

-- Channel-level aggregation
channel_acquisition AS (
  SELECT
    ac.acquisition_channel               AS channel,
    ac.acquisition_month,
    COUNT(DISTINCT ac.customer_id)        AS new_customers,
    SUM(l.ltv_30d)                        AS total_ltv_30d,
    SUM(l.ltv_90d)                        AS total_ltv_90d,
    SUM(l.ltv_total)                      AS total_ltv,
    AVG(l.ltv_30d)                        AS avg_ltv_30d,
    AVG(l.ltv_90d)                        AS avg_ltv_90d,
    AVG(l.total_conversions)              AS avg_conversions_per_customer
  FROM acquired_customers ac
  LEFT JOIN customer_ltv l ON ac.customer_id = l.customer_id
  GROUP BY ac.acquisition_channel, ac.acquisition_month
)

SELECT
  ca.channel,
  ca.acquisition_month,
  ca.new_customers,
  ROUND(SAFE_DIVIDE(cs.total_spend, ca.new_customers), 2) AS cac_eur,
  ROUND(ca.avg_ltv_30d, 2)                AS avg_first_order_value,
  ROUND(ca.avg_ltv_30d, 2)                AS avg_ltv_30d,
  ROUND(ca.avg_ltv_90d, 2)                AS avg_ltv_90d,

  -- Payback period in days (CAC / daily revenue per customer)
  ROUND(SAFE_DIVIDE(
    SAFE_DIVIDE(cs.total_spend, ca.new_customers),
    SAFE_DIVIDE(ca.avg_ltv_90d, 90)
  ), 0) AS payback_period_days,

  -- LTV to CAC ratio
  ROUND(SAFE_DIVIDE(
    ca.avg_ltv_90d,
    SAFE_DIVIDE(cs.total_spend, ca.new_customers)
  ), 2) AS ltv_90d_to_cac_ratio,

  -- Health indicator
  CASE
    WHEN SAFE_DIVIDE(ca.avg_ltv_90d, SAFE_DIVIDE(cs.total_spend, ca.new_customers)) >= 3.0
      THEN 'healthy'
    WHEN SAFE_DIVIDE(ca.avg_ltv_90d, SAFE_DIVIDE(cs.total_spend, ca.new_customers)) >= 1.0
      THEN 'marginal'
    ELSE 'unhealthy'
  END AS acquisition_health,

  ROUND(ca.avg_conversions_per_customer, 1) AS avg_conversions_per_customer

FROM channel_acquisition ca
LEFT JOIN channel_spend cs ON ca.channel = cs.channel;
