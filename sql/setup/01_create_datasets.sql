-- =============================================================================
-- 01_create_datasets.sql
-- Create BigQuery datasets for the marketing analytics pipeline
-- =============================================================================

-- Raw data layer: unmodified source data from ad platforms
CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.raw_marketing`
OPTIONS (
  description = 'Raw marketing data ingested from ad platforms (Google, Meta, TikTok, LinkedIn, Email, SEO)',
  labels = [("layer", "raw"), ("domain", "marketing")]
);

-- Staging layer: cleaned, deduplicated, type-cast intermediate tables
CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.staging_marketing`
OPTIONS (
  description = 'Staging transformations: deduplication, type casting, enrichment',
  labels = [("layer", "staging"), ("domain", "marketing")]
);

-- Analytics/marts layer: business-ready aggregated tables
CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.analytics_marketing`
OPTIONS (
  description = 'Business-ready mart tables for dashboards and reporting',
  labels = [("layer", "marts"), ("domain", "marketing")]
);
