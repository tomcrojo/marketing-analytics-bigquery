#!/usr/bin/env python3
"""
Run Great Expectations quality checks for the Marketing Analytics pipeline.

Validates raw CSV data before BigQuery transformations and optionally
validates mart output data after transformations.

Usage:
    python scripts/run_quality_checks.py                    # Run all checkpoints
    python scripts/run_quality_checks.py --suite raw        # Run only raw data suites
    python scripts/run_quality_checks.py --suite mart       # Run only mart data suite
    python scripts/run_quality_checks.py --sample           # Use sample data (skip row count checks)
"""

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd

# Ensure the great_expectations package is on the path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
GE_ROOT = PROJECT_ROOT / "great_expectations"

# Ensure src is on the path for the data_observability package
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from data_observability import QualityTracker

try:
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest
except ImportError:
    print(
        "ERROR: great-expectations is not installed. Run: pip install great-expectations>=0.18"
    )
    sys.exit(1)


def get_context():
    """Create and return the GE context."""
    context = gx.data_context.DataContext(context_root_dir=str(GE_ROOT))
    return context


def build_batch_request(data_asset_name: str, file_path: str) -> dict:
    """Build a batch request for a CSV file."""
    df = pd.read_csv(file_path)
    return df


def run_suite(context, suite_name: str, file_name: str, sample: bool = False):
    """Run a single expectation suite against a CSV file.

    Args:
        context: GE DataContext
        suite_name: Name of the expectation suite
        file_name: CSV filename in data/ directory
        sample: If True, use a small sample for fast testing

    Returns:
        dict with validation result summary
    """
    file_path = PROJECT_ROOT / "data" / file_name

    if not file_path.exists():
        return {
            "suite": suite_name,
            "file": file_name,
            "success": False,
            "error": f"File not found: {file_path}",
        }

    df = pd.read_csv(file_path)

    if sample and len(df) > 10000:
        print(f"  Sampling 10,000 rows from {len(df):,} total rows")
        df = df.sample(n=10000, random_state=42)

    print(f"  Running suite '{suite_name}' against {file_name} ({len(df):,} rows)...")

    validator = context.sources.pandas_default.read_dataframe(df)
    result = validator.validate(expectation_suite_name=suite_name)

    success = result.success
    stats = result.statistics

    # Extract expectation details for observability tracking
    expectation_details = []
    if hasattr(result, "results"):
        for r in result.results:
            expectation_details.append(
                {
                    "expectation_type": r.expectation_config.expectation_type,
                    "success": r.success,
                    "kwargs": dict(r.expectation_config.kwargs)
                    if r.expectation_config.kwargs
                    else {},
                    "result": r.result if hasattr(r, "result") else {},
                }
            )

    return {
        "suite": suite_name,
        "file": file_name,
        "success": success,
        "evaluated_expectations": stats.get("evaluated_expectations", 0),
        "successful_expectations": stats.get("successful_expectations", 0),
        "unsuccessful_expectations": stats.get("unsuccessful_expectations", 0),
        "success_percent": stats.get("success_percent", 0),
        "row_count": len(df),
        "expectation_details": expectation_details,
        "df": df,
    }


def print_summary(results: list):
    """Print a formatted summary of all validation results."""
    total_expectations = 0
    total_passed = 0
    total_failed = 0
    all_success = True

    print("\n" + "=" * 70)
    print("  GREAT EXPECTATIONS — QUALITY CHECK SUMMARY")
    print("=" * 70)

    for r in results:
        status = "PASS" if r.get("success", False) else "FAIL"
        icon = "+" if r.get("success", False) else "x"

        if "error" in r:
            print(f"\n  {icon} {r['suite']:30s}  ERROR")
            print(f"    {r['error']}")
            all_success = False
            total_failed += 1
        else:
            total_expectations += r.get("evaluated_expectations", 0)
            total_passed += r.get("successful_expectations", 0)
            total_failed += r.get("unsuccessful_expectations", 0)

            if not r["success"]:
                all_success = False

            print(f"\n  {icon} {r['suite']:30s}  {status}")
            print(f"    File: {r['file']}")
            print(
                f"    Checks: {r['evaluated_expectations']} total, "
                f"{r['successful_expectations']} passed, "
                f"{r['unsuccessful_expectations']} failed "
                f"({r['success_percent']:.1f}%)"
            )

    print("\n" + "-" * 70)
    print(f"  Total checks run:     {total_expectations}")
    print(f"  Passed:               {total_passed}")
    print(f"  Failed:               {total_failed}")

    if all_success:
        print(f"\n  ALL QUALITY CHECKS PASSED")
    else:
        print(f"\n  SOME QUALITY CHECKS FAILED — review Data Docs for details")

    print("=" * 70)

    # Build data docs path
    docs_path = GE_ROOT / "uncommitted" / "data_docs" / "local_site" / "index.html"
    if docs_path.exists():
        print(f"\n  Data Docs: file://{docs_path}")

    return all_success


def main():
    parser = argparse.ArgumentParser(
        description="Run GE quality checks for marketing analytics"
    )
    parser.add_argument(
        "--suite",
        choices=["raw", "mart", "all"],
        default="all",
        help="Which suite group to run (default: all)",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use sampled data for fast testing",
    )
    args = parser.parse_args()

    print("Great Expectations — Marketing Analytics Quality Checks")
    print("-" * 60)

    # Create context
    try:
        context = get_context()
    except Exception as e:
        print(f"ERROR: Could not create GE context: {e}")
        sys.exit(1)

    results = []

    # Define which suites to run
    raw_suites = [
        ("raw_impressions_suite", "raw_ad_impressions.csv"),
        ("raw_clicks_suite", "raw_clicks.csv"),
        ("raw_conversions_suite", "raw_conversions.csv"),
    ]

    mart_suites = [
        ("mart_dashboard_suite", "rpt_marketing_dashboard.csv"),
    ]

    if args.suite in ("raw", "all"):
        print("\n[1/2] Validating raw data...")
        for suite_name, file_name in raw_suites:
            result = run_suite(context, suite_name, file_name, sample=args.sample)
            results.append(result)

    if args.suite in ("mart", "all"):
        print("\n[2/2] Validating mart data...")
        for suite_name, file_name in mart_suites:
            result = run_suite(context, suite_name, file_name, sample=args.sample)
            results.append(result)

    all_passed = print_summary(results)

    # --- Observability tracking ---
    tracker = QualityTracker(
        results_dir=str(PROJECT_ROOT / "data" / "quality_results"),
        metrics_dir=str(PROJECT_ROOT / "data" / "quality_metrics"),
    )

    # Map suite names to dataset names for the tracker
    dataset_map = {
        "raw_impressions_suite": "raw_ad_impressions",
        "raw_clicks_suite": "raw_clicks",
        "raw_conversions_suite": "raw_conversions",
        "mart_dashboard_suite": "rpt_marketing_dashboard",
    }

    for r in results:
        if "error" in r:
            continue

        suite_name = r["suite"]
        dataset = dataset_map.get(suite_name, suite_name)
        df = r.get("df")

        # Volume: row count
        row_count = r.get("row_count", len(df) if df is not None else 0)

        # Freshness: latest timestamp in the dataframe
        latest_timestamp = None
        if df is not None:
            timestamp_cols = [
                c for c in df.columns if "timestamp" in c.lower() or "date" in c.lower()
            ]
            if timestamp_cols:
                try:
                    latest_timestamp = str(df[timestamp_cols[0]].max())
                except Exception:
                    pass

        # Null rates
        column_null_counts = None
        if df is not None:
            column_null_counts = {col: int(df[col].isna().sum()) for col in df.columns}

        # Schema
        schema_columns = None
        if df is not None:
            schema_columns = [
                {"name": col, "type": str(df[col].dtype)} for col in df.columns
            ]

        tracker.track_validation(
            suite_name=suite_name,
            dataset=dataset,
            success=r["success"],
            expectations=r.get("expectation_details", []),
            row_count=row_count,
            latest_timestamp=latest_timestamp,
            column_null_counts=column_null_counts,
            schema_columns=schema_columns,
        )

    tracker.print_summary()

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
