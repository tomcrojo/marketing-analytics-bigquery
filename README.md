# Marketing Analytics — BigQuery + Looker

Marketing performance analytics pipeline on BigQuery. Transforms raw ad platform data into actionable metrics for campaign optimization, channel ROI analysis, and customer acquisition cost tracking. Visualized with Looker dashboards.

Built for a Spanish digital marketing agency managing multi-channel campaigns (Google Ads, Meta Ads, TikTok Ads, LinkedIn Ads, Email, SEO) for e-commerce clients across Southern Europe.

---

## Architecture

```
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                          DATA PIPELINE ARCHITECTURE                        │
 └─────────────────────────────────────────────────────────────────────────────┘

  CSV Files           BigQuery Raw          SQL Transforms          Looker
 ┌──────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌───────────┐
 │ raw_ad_  │───▶│ raw_marketing    │───▶│ staging         │───▶│ Dashboards│
 │ impres-  │    │  .raw_ad_        │    │  .stg_ad_       │    │           │
 │ sions.csv│    │   impressions    │    │   impressions   │    │  • KPIs   │
 ├──────────┤    ├──────────────────┤    ├─────────────────┤    │  • Charts │
 │ raw_     │───▶│  .raw_clicks     │───▶│  .stg_clicks    │───▶│  • Tables │
 │ clicks   │    ├──────────────────┤    ├─────────────────┤    │  • Funnels│
 │ .csv     │    │  .raw_           │───▶│  .stg_          │    ├───────────┤
 ├──────────┤    │   conversions    │    │   conversions   │    │ Explores  │
 │ raw_     │───▶├──────────────────┤    ├─────────────────┤    │           │
 │ conv.    │    │  .raw_campaigns  │───▶│  .stg_campaigns │    │ • marketing│
 │ .csv     │    └──────────────────┘    └────────┬────────┘    │   _dash   │
 ├──────────┤                                      │            │ • channel │
 │ raw_     │                            ┌────────▼────────┐   │   _roi    │
 │campaigns │                            │ intermediate    │   │ • cust_   │
 │ .csv     │                            │                 │   │   acq     │
 └──────────┘                            │ .int_campaign_  │   └───────────┘
                                         │   performance   │
                                         │ .int_attribution│
                                         └────────┬────────┘
                                                  │
                                         ┌────────▼────────┐
                                         │ marts           │
                                         │                 │
                                         │ .rpt_marketing_ │
                                         │   dashboard     │
                                         │ .rpt_channel_   │
                                         │   roi           │
                                         │ .rpt_customer_  │
                                         │   acquisition   │
                                         └─────────────────┘
```

## Data Volumes

| Table | Rows | Description |
|-------|------|-------------|
| **raw_ad_impressions** | ~5,000,000 | Ad views across 50 campaigns, 6 channels, 5 European markets |
| **raw_clicks** | ~1,200,000 | Click events (~24% CTR from impressions) |
| **raw_conversions** | ~350,000 | Purchases, signups, and leads (~29% conv rate from clicks) |
| **raw_campaigns** | 50 | Campaign definitions with Spanish naming, EUR budgets |
| **Unique customers** | 50,000 | CUST-00001 through CUST-050000 |

Date range: January 2024 — March 2026. Currency: EUR.

## Marketing Funnel

```
  ┌─────────────────────────────────────────────────────────────┐
  │              IMPRESSIONS (5,000,000)                        │
  │            100% — All ad views served                       │
  │                                                             │
  │    ┌─────────────────────────────────────────┐              │
  │    │         CLICKS (1,200,000)              │              │
  │    │         24.0% CTR                       │              │
  │    │                                         │              │
  │    │    ┌─────────────────────────────┐      │              │
  │    │    │    CONVERSIONS (350,000)    │      │              │
  │    │    │    29.2% Conv Rate          │      │              │
  │    │    │                             │      │              │
  │    │    └─────────────────────────────┘      │              │
  │    └─────────────────────────────────────────┘              │
  └─────────────────────────────────────────────────────────────┘
```

## Key Metrics

| Metric | Formula | Description |
|--------|---------|-------------|
| **CTR** | clicks / impressions × 100 | Click-through rate — ad relevance |
| **CPC** | spend / clicks | Cost per click — channel efficiency |
| **CPA** | spend / conversions | Cost per acquisition — conversion efficiency |
| **ROAS** | revenue / spend | Return on ad spend — profitability |
| **CAC** | total_spend / new_customers | Customer acquisition cost |
| **LTV** | avg_order_value × purchase_frequency × margin | Customer lifetime value |

## Project Structure

```
marketing-analytics-bigquery/
├── sql/
│   ├── setup/              # Dataset and table DDL
│   ├── staging/            # Cleaned, deduplicated source data
│   ├── intermediate/       # Aggregations and attribution
│   └── marts/              # Business-ready reporting tables
├── lookml/
│   ├── models/             # Looker model definitions
│   ├── views/              # Looker view definitions
│   └── dashboards/         # Looker dashboard definitions
├── great_expectations/
│   ├── expectations/       # Expectation suites (JSON)
│   ├── checkpoints/        # Checkpoint configurations
│   └── plugins/            # Custom expectations
├── data/                   # Generated CSV data files
├── scripts/                # Data generation, pipeline, quality checks
└── .github/workflows/      # CI/CD pipeline
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Data Warehouse | BigQuery | SQL-based ELT transformations |
| Visualization | Looker | Interactive dashboards and explores |
| SQL Dialect | BigQuery SQL | Native functions: `SAFE_DIVIDE`, `TIMESTAMP_DIFF`, `DATE_TRUNC` |
| CI/CD | GitHub Actions | SQL linting (sqlfluff), dry-run validation, data quality |
| Data Modeling | Dimensional (Kimball) | Staging → Intermediate → Marts layering |
| Data Generation | Python + Faker | Large-scale synthetic data with referential integrity |
| Data Quality | Great Expectations | Pre-transformation validation of raw CSV data |

## Setup

### Prerequisites

- Google Cloud project with BigQuery API enabled
- `gcloud` CLI authenticated
- Looker instance with BigQuery connection
- Python 3.9+ with `pip install -r requirements.txt`

### 1. Authenticate with GCP

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

### 2. Create datasets and tables

```bash
bq query --use_legacy_sql=false < sql/setup/01_create_datasets.sql
bq query --use_legacy_sql=false < sql/setup/02_create_tables.sql
```

### 3. Generate large-scale data

```bash
pip install -r requirements.txt

# Full dataset (~6.5M rows total)
python scripts/generate_large_data.py

# Custom volumes
python scripts/generate_large_data.py --impressions 7000000 --clicks-ratio 0.22 --conversions-ratio 0.30

# Quick test (10K impressions)
python scripts/generate_large_data.py --sample
```

### 4. Load data into BigQuery

```bash
chmod +x scripts/load_data.sh
./scripts/load_data.sh YOUR_PROJECT_ID
```

### 5. Run transformations

```bash
chmod +x scripts/run_pipeline.sh
./scripts/run_pipeline.sh YOUR_PROJECT_ID
```

### 6. Connect Looker

1. In Looker Admin → Connections, add a BigQuery connection named `marketing_analytics`
2. Point to your GCP project and `analytics_marketing` dataset
3. Import this project's LookML files
4. Deploy and validate dashboards

## Local Development with Docker

Run the full pipeline locally without a BigQuery account. PostgreSQL acts as a local BigQuery substitute.

### Architecture

```
  ┌──────────────────────────────────────────────────────────────────┐
  │                    LOCAL DOCKER SETUP                            │
  └──────────────────────────────────────────────────────────────────┘

  ┌────────────────┐    ┌────────────────┐    ┌────────────────┐
  │  PostgreSQL 16 │    │   Generator    │    │    Pipeline    │
  │  (BigQuery     │◀───│   Container    │    │   Container    │
  │   substitute)  │    │                │    │                │
  │  localhost:5433 │    │  Python + Faker│    │  SQL adaptor   │
  └────────────────┘    └────────────────┘    └────────────────┘
        pg_data/              data/             sql/ + scripts/
```

### Prerequisites

- Docker Desktop

### Quick Start

```bash
./scripts/start_local.sh
```

This starts PostgreSQL, generates 10K sample rows, and runs the full pipeline.

### Commands

```bash
# Start everything (sample data)
./scripts/start_local.sh

# Generate full 5M+ dataset
docker-compose --profile generate run --rm generator

# Run pipeline only (assumes data already loaded)
docker-compose --profile pipeline run --rm pipeline

# Run pipeline with quality checks
docker-compose --profile pipeline run --rm pipeline --quality

# Run a single step
docker-compose --profile pipeline run --rm pipeline --step staging

# Connect to PostgreSQL directly
psql postgresql://analytics:analytics_pass@localhost:5433/marketing

# Stop and clean up
./scripts/stop_local.sh
```

### PostgreSQL Connection

| Property | Value |
|----------|-------|
| Host | localhost |
| Port | 5433 |
| Database | marketing |
| User | analytics |
| Password | analytics_pass |

### Note

SQL is automatically adapted from BigQuery to PostgreSQL syntax for local dev. Key conversions: `SAFE_DIVIDE` → `COALESCE(.../NULLIF(...))`, `INT64` → `INTEGER`, `FLOAT64` → `DOUBLE PRECISION`, `COUNTIF` → `COUNT(*) FILTER (WHERE ...)`. Production uses native BigQuery SQL.

---

## Data Generation Details

The generator script (`scripts/generate_large_data.py`) uses Python `Faker` with Spanish locale (`es_ES`) to create synthetic marketing data with full referential integrity:

- **Impressions** are generated first, writing UUID-based IDs to a temporary index file
- **Clicks** are sampled from impression IDs (configurable ratio, default 24% CTR)
- **Conversions** are sampled from click IDs (configurable ratio, default 29% conv rate)
- Every click references a real impression; every conversion references a real click
- Timestamps are ordered: click after impression, conversion after click
- CSV headers match BigQuery table schemas exactly (no changes needed to SQL or LookML)

Channels: Google Ads, Meta Ads, TikTok Ads, LinkedIn Ads, Email, SEO
Markets: Spain (ES), France (FR), Germany (DE), Portugal (PT), Italy (IT), United Kingdom (UK)

## Data Quality with Great Expectations

Raw CSV data is validated before BigQuery transformations using Great Expectations. This catches data issues at ingestion time — before they propagate through staging, intermediate, and mart layers.

### Quality Strategy

For a 6.5M+ row pipeline, quality checks focus on three pillars:

1. **Schema validation** — all expected columns exist with correct names
2. **Value constraints** — channels, devices, countries match allowed values; costs and revenues are within valid ranges
3. **Referential integrity** — every click references a valid impression; every conversion references a valid click (sampling-based for performance)

### Expectation Suites

| Suite | File | Checks |
|-------|------|--------|
| **raw_impressions_suite** | `expectations/raw_impressions_suite.json` | Null checks, uniqueness, channel/device/country values, cost range, campaign ID regex, row count (min 1M) |
| **raw_clicks_suite** | `expectations/raw_clicks_suite.json` | Null checks, uniqueness, channel values, cost range, row count (min 100K) |
| **raw_conversions_suite** | `expectations/raw_conversions_suite.json` | Null checks, uniqueness, revenue range, conversion type values, row count (min 50K) |
| **mart_dashboard_suite** | `expectations/mart_dashboard_suite.json` | Non-null dates/channels, non-negative spend/revenue, CTR/conversion rate in 0-100% range |

### Custom Expectations

The `plugins/custom_expectations.py` module provides pipeline-specific checks:

- **expect_clicks_reference_valid_impressions** — verifies impression_id FK integrity (sampling-based)
- **expect_conversions_reference_valid_clicks** — verifies click_id FK integrity (sampling-based)
- **expect_cost_is_positive** — cost_micros values are strictly positive

### Running Locally

```bash
pip install -r requirements.txt

# Run all quality checks
python scripts/run_quality_checks.py

# Run only raw data suites (fast)
python scripts/run_quality_checks.py --suite raw

# Run only mart data suites
python scripts/run_quality_checks.py --suite mart

# Quick test with sampled data (10K rows)
python scripts/run_quality_checks.py --sample
```

### Data Docs

After running checks, HTML reports are generated at:
`great_expectations/uncommitted/data_docs/local_site/index.html`

These show pass/fail status for every expectation, with sample failures and data previews.

---

## Data Quality Observability

Beyond Great Expectations validation, this project includes an observability layer that tracks quality metrics over time and provides a visual dashboard.

### Metrics Tracked

| Metric | Description |
|--------|-------------|
| **Volume** | Row count per dataset, with anomaly detection (>50% change flagged) |
| **Freshness** | Time since latest data timestamp, with stale/critical thresholds |
| **Null Rates** | Per-column null percentage, flags columns exceeding 5% threshold |
| **Schema Drift** | Detects added/removed columns and type changes between runs |

### Running the Dashboard

```bash
pip install -r requirements.txt

# Run quality checks (populates metrics)
python scripts/run_quality_checks.py

# Launch the dashboard
streamlit run src/data_observability/dashboard.py
```

### Docker

```bash
# Run checks + populate metrics
docker-compose --profile pipeline run --rm pipeline --quality

# Launch dashboard (port 8503)
docker-compose --profile observability up observability
```

The dashboard shows health overview, pass rate trends, volume charts, freshness status, null rate bars, schema drift alerts, and recent failures.
