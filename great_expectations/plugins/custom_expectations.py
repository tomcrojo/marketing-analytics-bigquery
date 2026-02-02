"""
Custom Great Expectations for Marketing Analytics pipeline.

Provides referential integrity checks and domain-specific validations
for the multi-channel marketing data.
"""

import pandas as pd
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.validator.validator import Validator


class ExpectClicksReferenceValidImpressions(ColumnMapExpectation):
    """Verify that impression_id values in clicks exist in the impressions dataset.

    Uses sampling-based checking for large files to avoid memory issues.
    Checks a random sample of click rows against the full impression_id set.
    """

    expectation_type = "expect_clicks_reference_valid_impressions"

    def _validate(
        self, configuration, metrics, runtime_configuration=None, execution_engine=None
    ):
        column = configuration.kwargs.get("column", "impression_id")
        impressions_file = configuration.kwargs.get(
            "impressions_file", "../data/raw_ad_impressions.csv"
        )
        sample_size = configuration.kwargs.get("sample_size", 100000)

        # Load valid impression IDs (memory-efficient: only load the ID column)
        valid_ids = set(
            pd.read_csv(impressions_file, usecols=["impression_id"])["impression_id"]
        )

        # Get the column values from the current batch
        click_imps = metrics.get("column_values")
        if click_imps is None:
            click_imps = self._get_column_values()

        # Sample for performance on large datasets
        if len(click_imps) > sample_size:
            click_sample = click_imps.sample(n=sample_size, random_state=42)
        else:
            click_sample = click_imps

        invalid_count = (~click_sample.isin(valid_ids)).sum()
        total = len(click_sample)
        success = invalid_count == 0
        unexpected_pct = (invalid_count / total * 100) if total > 0 else 0

        return {
            "success": success,
            "result": {
                "element_count": total,
                "unexpected_count": int(invalid_count),
                "unexpected_percent": round(unexpected_pct, 4),
                "partial_unexpected_list": (
                    click_sample[~click_sample.isin(valid_ids)].head(10).tolist()
                    if invalid_count > 0
                    else []
                ),
            },
        }


class ExpectConversionsReferenceValidClicks(ColumnMapExpectation):
    """Verify that click_id values in conversions exist in the clicks dataset.

    Uses sampling-based checking for large files to avoid memory issues.
    """

    expectation_type = "expect_conversions_reference_valid_clicks"

    def _validate(
        self, configuration, metrics, runtime_configuration=None, execution_engine=None
    ):
        column = configuration.kwargs.get("column", "click_id")
        clicks_file = configuration.kwargs.get("clicks_file", "../data/raw_clicks.csv")
        sample_size = configuration.kwargs.get("sample_size", 100000)

        # Load valid click IDs
        valid_ids = set(pd.read_csv(clicks_file, usecols=["click_id"])["click_id"])

        click_ids = metrics.get("column_values")
        if click_ids is None:
            click_ids = self._get_column_values()

        if len(click_ids) > sample_size:
            click_sample = click_ids.sample(n=sample_size, random_state=42)
        else:
            click_sample = click_ids

        invalid_count = (~click_sample.isin(valid_ids)).sum()
        total = len(click_sample)
        success = invalid_count == 0
        unexpected_pct = (invalid_count / total * 100) if total > 0 else 0

        return {
            "success": success,
            "result": {
                "element_count": total,
                "unexpected_count": int(invalid_count),
                "unexpected_percent": round(unexpected_pct, 4),
                "partial_unexpected_list": (
                    click_sample[~click_sample.isin(valid_ids)].head(10).tolist()
                    if invalid_count > 0
                    else []
                ),
            },
        }


class ExpectCostIsPositive(ColumnMapExpectation):
    """Verify that cost values are strictly positive (> 0).

    Marketing cost_micros values should always be positive.
    """

    expectation_type = "expect_cost_is_positive"

    def _validate(
        self, configuration, metrics, runtime_configuration=None, execution_engine=None
    ):
        column = configuration.kwargs.get("column", "cost_micros")

        cost_values = metrics.get("column_values")
        if cost_values is None:
            cost_values = self._get_column_values()

        non_positive = cost_values <= 0
        invalid_count = non_positive.sum()
        total = len(cost_values)
        success = invalid_count == 0
        unexpected_pct = (invalid_count / total * 100) if total > 0 else 0

        return {
            "success": success,
            "result": {
                "element_count": total,
                "unexpected_count": int(invalid_count),
                "unexpected_percent": round(unexpected_pct, 4),
                "partial_unexpected_list": (
                    cost_values[non_positive].head(10).tolist()
                    if invalid_count > 0
                    else []
                ),
            },
        }
