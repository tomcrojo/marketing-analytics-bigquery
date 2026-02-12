#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh
# Execute the SQL transformation pipeline in order
# Usage: ./run_pipeline.sh YOUR_PROJECT_ID
# =============================================================================

set -euo pipefail

PROJECT_ID="${1:?Usage: ./run_pipeline.sh YOUR_PROJECT_ID}"
SQL_DIR="$(cd "$(dirname "$0")/../sql" && pwd)"

run_sql() {
  local label="$1"
  local file="$2"

  echo -n "  ${label}... "
  local start_time=$(date +%s)

  if bq query \
    --project_id="${PROJECT_ID}" \
    --use_legacy_sql=false \
    --parameter="PROJECT_ID:STRING:${PROJECT_ID}" \
    < "${file}" > /dev/null 2>&1; then
    local end_time=$(date +%s)
    local elapsed=$((end_time - start_time))
    echo "OK (${elapsed}s)"
  else
    echo "FAILED"
    echo "    Error in: ${file}"
    return 1
  fi
}

count_rows() {
  local table="$1"
  local count
  count=$(bq query \
    --project_id="${PROJECT_ID}" \
    --use_legacy_sql=false \
    --format=csv \
    "SELECT COUNT(*) AS cnt FROM \`${PROJECT_ID}.${table}\`" 2>/dev/null | tail -1)
  echo "    → ${table}: ${count} rows"
}

echo "=========================================="
echo "Marketing Analytics Pipeline"
echo "Project: ${PROJECT_ID}"
echo "=========================================="

# --- Phase 1: Setup ---
echo ""
echo "[1/4] Setup — Creating datasets and tables"
run_sql "Create datasets" "${SQL_DIR}/setup/01_create_datasets.sql"
run_sql "Create tables"   "${SQL_DIR}/setup/02_create_tables.sql"

# --- Phase 2: Staging ---
echo ""
echo "[2/4] Staging — Cleaning and deduplicating raw data"
run_sql "Stage impressions" "${SQL_DIR}/staging/stg_ad_impressions.sql"
run_sql "Stage clicks"      "${SQL_DIR}/staging/stg_clicks.sql"
run_sql "Stage conversions" "${SQL_DIR}/staging/stg_conversions.sql"
run_sql "Stage campaigns"   "${SQL_DIR}/staging/stg_campaigns.sql"

# --- Phase 3: Intermediate ---
echo ""
echo "[3/4] Intermediate — Aggregation and attribution"
run_sql "Campaign performance" "${SQL_DIR}/intermediate/int_campaign_performance.sql"
run_sql "Attribution models"  "${SQL_DIR}/intermediate/int_attribution.sql"

# --- Phase 4: Marts ---
echo ""
echo "[4/4] Marts — Building reporting tables"
run_sql "Dashboard report"      "${SQL_DIR}/marts/rpt_marketing_dashboard.sql"
run_sql "Channel ROI report"    "${SQL_DIR}/marts/rpt_channel_roi.sql"
run_sql "Customer acquisition"  "${SQL_DIR}/marts/rpt_customer_acquisition.sql"

# --- Row Counts ---
echo ""
echo "=========================================="
echo "Pipeline complete. Row counts:"
echo "=========================================="
count_rows "staging_marketing.stg_ad_impressions"
count_rows "staging_marketing.stg_clicks"
count_rows "staging_marketing.stg_conversions"
count_rows "staging_marketing.stg_campaigns"
count_rows "analytics_marketing.int_campaign_performance"
count_rows "analytics_marketing.int_attribution"
count_rows "analytics_marketing.rpt_marketing_dashboard"
count_rows "analytics_marketing.rpt_channel_roi"
count_rows "analytics_marketing.rpt_customer_acquisition"
echo "=========================================="
