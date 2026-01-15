-- =============================================================================
-- 02_create_tables.sql
-- Create raw tables in raw_marketing dataset
-- =============================================================================

-- Ad impressions: one row per ad impression served
CREATE OR REPLACE TABLE `${PROJECT_ID}.raw_marketing.raw_ad_impressions` (
  impression_id   STRING     NOT NULL OPTIONS(description="Unique impression identifier"),
  campaign_id     STRING     NOT NULL OPTIONS(description="FK to campaign"),
  channel         STRING     NOT NULL OPTIONS(description="Marketing channel: Google_Ads, Meta_Ads, TikTok_Ads, LinkedIn_Ads, Email, SEO"),
  platform        STRING     NOT NULL OPTIONS(description="Platform identifier: google, meta, tiktok, linkedin, email, organic"),
  timestamp       TIMESTAMP  NOT NULL OPTIONS(description="When the impression was served"),
  device_type     STRING     OPTIONS(description="mobile, desktop, or tablet"),
  country         STRING     OPTIONS(description="ISO 3166-1 alpha-2 country code"),
  cost_micros     INT64      OPTIONS(description="Cost in micro-currency (1/1,000,000 EUR)")
)
PARTITION BY DATE(timestamp)
CLUSTER BY campaign_id, channel
OPTIONS (
  description = "Raw ad impression data from all marketing channels",
  labels = [("source", "ad_platforms"), ("grain", "impression")]
);

-- Clicks: one row per ad click
CREATE OR REPLACE TABLE `${PROJECT_ID}.raw_marketing.raw_clicks` (
  click_id        STRING     NOT NULL OPTIONS(description="Unique click identifier"),
  impression_id   STRING     NOT NULL OPTIONS(description="FK to originating impression"),
  campaign_id     STRING     NOT NULL OPTIONS(description="FK to campaign"),
  channel         STRING     NOT NULL OPTIONS(description="Marketing channel"),
  platform        STRING     NOT NULL OPTIONS(description="Platform identifier"),
  timestamp       TIMESTAMP  NOT NULL OPTIONS(description="When the click occurred"),
  device_type     STRING     OPTIONS(description="mobile, desktop, or tablet"),
  country         STRING     OPTIONS(description="ISO country code"),
  cost_micros     INT64      OPTIONS(description="Cost in micro-currency")
)
PARTITION BY DATE(timestamp)
CLUSTER BY campaign_id, channel
OPTIONS (
  description = "Raw click data linked to impressions",
  labels = [("source", "ad_platforms"), ("grain", "click")]
);

-- Conversions: one row per conversion event
CREATE OR REPLACE TABLE `${PROJECT_ID}.raw_marketing.raw_conversions` (
  conversion_id   STRING     NOT NULL OPTIONS(description="Unique conversion identifier"),
  click_id        STRING     NOT NULL OPTIONS(description="FK to originating click"),
  campaign_id     STRING     NOT NULL OPTIONS(description="FK to campaign"),
  customer_id     STRING     NOT NULL OPTIONS(description="Customer identifier for repeat purchase tracking"),
  timestamp       TIMESTAMP  NOT NULL OPTIONS(description="When the conversion occurred"),
  revenue         FLOAT64    OPTIONS(description="Revenue in EUR (0 for signups)"),
  conversion_type STRING     OPTIONS(description="purchase, signup, or lead")
)
PARTITION BY DATE(timestamp)
CLUSTER BY campaign_id, conversion_type
OPTIONS (
  description = "Conversion events (purchases, signups, leads)",
  labels = [("source", "ad_platforms"), ("grain", "conversion")]
);

-- Campaigns: one row per marketing campaign
CREATE OR REPLACE TABLE `${PROJECT_ID}.raw_marketing.raw_campaigns` (
  campaign_id     STRING     NOT NULL OPTIONS(description="Unique campaign identifier"),
  campaign_name   STRING     NOT NULL OPTIONS(description="Descriptive campaign name"),
  channel         STRING     NOT NULL OPTIONS(description="Marketing channel"),
  platform        STRING     NOT NULL OPTIONS(description="Platform identifier"),
  start_date      DATE       NOT NULL OPTIONS(description="Campaign start date"),
  end_date        DATE       NOT NULL OPTIONS(description="Campaign end date"),
  budget_euros    FLOAT64    NOT NULL OPTIONS(description="Total campaign budget in EUR"),
  target_audience STRING     OPTIONS(description="Target audience description"),
  objective       STRING     NOT NULL OPTIONS(description="awareness, consideration, or conversion")
)
CLUSTER BY channel, objective
OPTIONS (
  description = "Campaign master data with budget and targeting info",
  labels = [("source", "campaign_management"), ("grain", "campaign")]
);
