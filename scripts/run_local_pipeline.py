#!/usr/bin/env python3
"""Run the full SQL pipeline against PostgreSQL for local development.

Reads BigQuery SQL files from sql/setup/, sql/staging/, sql/intermediate/, sql/marts/,
adapts them to PostgreSQL syntax, and executes them in order.

Usage:
    python scripts/run_local_pipeline.py                # Run all steps
    python scripts/run_local_pipeline.py --step setup   # Run only setup
    python scripts/run_local_pipeline.py --step staging # Run only staging
    python scripts/run_local_pipeline.py --step intermediate
    python scripts/run_local_pipeline.py --step marts
    python scripts/run_local_pipeline.py --load-csv    # Also load CSV data
    python scripts/run_local_pipeline.py --quality      # Run quality checks after
"""

import argparse
import os
import sys
import time
from pathlib import Path

import psycopg2

from sql_adaptor import bq_to_pg

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = PROJECT_ROOT / "sql"
DATA_DIR = PROJECT_ROOT / "data"

# Execution order per step
STEP_FILES = {
    "setup": [
        "setup/01_create_datasets_pg.sql",
        "setup/02_create_tables.sql",
    ],
    "load": [
        # CSVs loaded via Python, not SQL files
    ],
    "staging": [
        "staging/stg_ad_impressions.sql",
        "staging/stg_campaigns.sql",
        "staging/stg_clicks.sql",
        "staging/stg_conversions.sql",
    ],
    "intermediate": [
        "intermediate/int_campaign_performance.sql",
        "intermediate/int_attribution.sql",
    ],
    "marts": [
        "marts/rpt_marketing_dashboard.sql",
        "marts/rpt_channel_roi.sql",
        "marts/rpt_customer_acquisition.sql",
    ],
}

# Table row count queries after pipeline
TABLES_TO_COUNT = [
    ("staging_marketing", "stg_ad_impressions"),
    ("staging_marketing", "stg_clicks"),
    ("staging_marketing", "stg_conversions"),
    ("staging_marketing", "stg_campaigns"),
    ("analytics_marketing", "int_campaign_performance"),
    ("analytics_marketing", "int_attribution"),
    ("analytics_marketing", "rpt_marketing_dashboard"),
    ("analytics_marketing", "rpt_channel_roi"),
    ("analytics_marketing", "rpt_customer_acquisition"),
]

# CSV loading order (table name, CSV filename, columns)
CSV_TABLES = [
    (
        "raw_marketing.raw_campaigns",
        "raw_campaigns.csv",
        "campaign_id, campaign_name, channel, platform, start_date, end_date, "
        "budget_euros, target_audience, objective",
    ),
    (
        "raw_marketing.raw_ad_impressions",
        "raw_ad_impressions.csv",
        "impression_id, campaign_id, channel, platform, timestamp, "
        "device_type, country, cost_micros",
    ),
    (
        "raw_marketing.raw_clicks",
        "raw_clicks.csv",
        "click_id, impression_id, campaign_id, channel, platform, timestamp, "
        "device_type, country, cost_micros",
    ),
    (
        "raw_marketing.raw_conversions",
        "raw_conversions.csv",
        "conversion_id, click_id, campaign_id, customer_id, timestamp, "
        "revenue, conversion_type",
    ),
]


def get_connection():
    """Get a PostgreSQL connection from DATABASE_URL env var."""
    dsn = os.environ.get(
        "DATABASE_URL",
        "postgresql://analytics:analytics_pass@localhost:5433/marketing",
    )
    return psycopg2.connect(dsn)


def execute_sql(conn, sql: str, label: str = ""):
    """Execute a SQL string, adapting BigQuery syntax first."""
    pg_sql = bq_to_pg(sql)

    start = time.time()
    with conn.cursor() as cur:
        cur.execute(pg_sql)
    conn.commit()
    elapsed = time.time() - start

    tag = f"  {label}" if label else ""
    print(f"  {label}... OK ({elapsed:.2f}s)")


def run_step(conn, step: str):
    """Run all SQL files for a pipeline step."""
    files = STEP_FILES.get(step, [])
    if not files:
        return

    print(f"\n--- Running step: {step} ({len(files)} files) ---")

    for rel_path in files:
        file_path = SQL_DIR / rel_path
        if not file_path.exists():
            print(f"  WARNING: {rel_path} not found, skipping")
            continue

        sql = file_path.read_text(encoding="utf-8")
        label = rel_path.split("/")[-1]
        execute_sql(conn, sql, label=label)


def load_csv_data(conn):
    """Load CSV data into PostgreSQL raw tables using COPY."""
    print("\n--- Loading CSV data into raw tables ---")

    for table, csv_file, columns in CSV_TABLES:
        csv_path = DATA_DIR / csv_file
        if not csv_path.exists():
            print(f"  WARNING: {csv_file} not found, skipping")
            continue

        with conn.cursor() as cur:
            # Truncate table first
            cur.execute(f"TRUNCATE TABLE {table} CASCADE")

            # Use COPY to load
            with open(csv_path, "r", encoding="utf-8") as f:
                cur.copy_expert(
                    f"COPY {table} ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true)",
                    f,
                )
            conn.commit()

            # Count rows
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"  {csv_file} -> {table}: {count:,} rows loaded")


def print_row_counts(conn):
    """Print row counts for all pipeline output tables."""
    print("\n" + "=" * 60)
    print("Pipeline complete. Row counts:")
    print("=" * 60)

    with conn.cursor() as cur:
        for schema, table in TABLES_TO_COUNT:
            full_name = f"{schema}.{table}"
            try:
                cur.execute(f"SELECT COUNT(*) FROM {full_name}")
                count = cur.fetchone()[0]
                print(f"  -> {full_name}: {count:,} rows")
            except Exception as e:
                print(f"  -> {full_name}: ERROR ({e})")

    print("=" * 60)


def run_quality_checks():
    """Run Great Expectations quality checks."""
    print("\n--- Running quality checks ---")
    import subprocess

    result = subprocess.run(
        [sys.executable, "scripts/run_quality_checks.py"],
        cwd=str(PROJECT_ROOT),
    )
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(
        description="Run the marketing analytics pipeline against PostgreSQL (local dev)"
    )
    parser.add_argument(
        "--step",
        choices=["setup", "load", "staging", "intermediate", "marts", "all"],
        default="all",
        help="Pipeline step to run (default: all)",
    )
    parser.add_argument(
        "--load-csv",
        action="store_true",
        help="Load CSV data into raw tables before running transforms",
    )
    parser.add_argument(
        "--quality",
        action="store_true",
        help="Run Great Expectations quality checks after pipeline",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Marketing Analytics Pipeline — Local PostgreSQL")
    print("=" * 60)

    try:
        conn = get_connection()
    except Exception as e:
        print(f"ERROR: Could not connect to PostgreSQL: {e}")
        print("  Is the database running? Try: docker-compose up -d postgres")
        sys.exit(1)

    steps = (
        ["setup", "load", "staging", "intermediate", "marts"]
        if args.step == "all"
        else [args.step]
    )

    for step in steps:
        if step == "load" or args.load_csv:
            load_csv_data(conn)
        else:
            run_step(conn, step)

    print_row_counts(conn)

    if args.quality:
        success = run_quality_checks()
        if not success:
            sys.exit(1)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
