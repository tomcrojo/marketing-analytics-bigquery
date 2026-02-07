-- =============================================================================
-- 01_create_datasets_pg.sql
-- PostgreSQL equivalent of BigQuery datasets
-- In PostgreSQL, we use schemas instead of datasets
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS raw_marketing;
CREATE SCHEMA IF NOT EXISTS staging_marketing;
CREATE SCHEMA IF NOT EXISTS analytics_marketing;
