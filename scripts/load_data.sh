#!/usr/bin/env bash
# =============================================================================
# load_data.sh
# Load CSV data into BigQuery raw tables
# Usage: ./load_data.sh YOUR_PROJECT_ID
# =============================================================================

set -euo pipefail

PROJECT_ID="${1:?Usage: ./load_data.sh YOUR_PROJECT_ID}"
DATASET="raw_marketing"
DATA_DIR="$(cd "$(dirname "$0")/../data" && pwd)"

echo "=========================================="
echo "Loading marketing data into BigQuery"
echo "Project: ${PROJECT_ID}"
echo "Dataset: ${DATASET}"
echo "Data dir: ${DATA_DIR}"
echo "=========================================="

load_table() {
  local table="$1"
  local csv_file="$2"
  local schema="$3"

  echo -n "  Loading ${table}... "
  if bq load \
    --project_id="${PROJECT_ID}" \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --replace \
    --max_bad_records=100 \
    --quiet \
    "${PROJECT_ID}:${DATASET}.${table}" \
    "${csv_file}" \
    "${schema}" 2>&1; then
    echo "OK"
  else
    echo "FAILED"
    return 1
  fi
}

echo ""
echo "Loading raw_campaigns..."
load_table "raw_campaigns" "${DATA_DIR}/raw_campaigns.csv" \
  "campaign_id:STRING,campaign_name:STRING,channel:STRING,platform:STRING,start_date:DATE,end_date:DATE,budget_euros:FLOAT,target_audience:STRING,objective:STRING"

echo ""
echo "Loading raw_ad_impressions..."
load_table "raw_ad_impressions" "${DATA_DIR}/raw_ad_impressions.csv" \
  "impression_id:STRING,campaign_id:STRING,channel:STRING,platform:STRING,timestamp:TIMESTAMP,device_type:STRING,country:STRING,cost_micros:INTEGER"

echo ""
echo "Loading raw_clicks..."
load_table "raw_clicks" "${DATA_DIR}/raw_clicks.csv" \
  "click_id:STRING,impression_id:STRING,campaign_id:STRING,channel:STRING,platform:STRING,timestamp:TIMESTAMP,device_type:STRING,country:STRING,cost_micros:INTEGER"

echo ""
echo "Loading raw_conversions..."
load_table "raw_conversions" "${DATA_DIR}/raw_conversions.csv" \
  "conversion_id:STRING,click_id:STRING,campaign_id:STRING,customer_id:STRING,timestamp:TIMESTAMP,revenue:FLOAT,conversion_type:STRING"

echo ""
echo "=========================================="
echo "Data load complete."
echo "=========================================="
