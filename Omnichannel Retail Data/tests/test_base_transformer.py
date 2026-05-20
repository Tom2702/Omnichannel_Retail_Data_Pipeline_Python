from __future__ import annotations

import os
import sys
import unittest

import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_integer_dtype,
    is_numeric_dtype,
    is_string_dtype,
)


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from transformers.base_transformer import BaseTransformer


class TestBaseTransformer(unittest.TestCase):
    def setUp(self) -> None:
        self.transformer = BaseTransformer()

    def test_standardize_column_names_converts_to_snake_case(self) -> None:
        raw_df = pd.DataFrame(
            {
                "Customer ID": [1],
                "CreatedAt": ["2026-04-01"],
                "Total-VND": [100000],
            }
        )

        transformed = self.transformer.standardize_column_names(raw_df)

        self.assertEqual(
            ["customer_id", "created_at", "total_vnd"],
            transformed.columns.tolist(),
        )

    def test_cast_columns_converts_supported_types(self) -> None:
        raw_df = pd.DataFrame(
            {
                "customer_id": ["1", "2", None],
                "created_at": ["2026-04-01", "2026-04-02", "invalid"],
                "amount_vnd": ["1000", "-5", "10.5"],
                "is_active": ["true", "no", "1"],
                "email": [1, "two@example.com", None],
            }
        )

        transformed = self.transformer.cast_columns(
            raw_df,
            {
                "customer_id": "int",
                "created_at": "datetime",
                "amount_vnd": "numeric",
                "is_active": "bool",
                "email": "string",
            },
        )

        self.assertTrue(is_integer_dtype(transformed["customer_id"]))
        self.assertTrue(is_datetime64_any_dtype(transformed["created_at"]))
        self.assertTrue(is_numeric_dtype(transformed["amount_vnd"]))
        self.assertTrue(is_bool_dtype(transformed["is_active"]))
        self.assertTrue(is_string_dtype(transformed["email"]))
        self.assertTrue(pd.isna(transformed.loc[2, "customer_id"]))
        self.assertTrue(pd.isna(transformed.loc[2, "created_at"]))
        self.assertEqual(False, transformed.loc[1, "is_active"])

    def test_handle_missing_value_and_key_creation(self) -> None:
        raw_df = pd.DataFrame(
            {
                "order_id": ["ORD-1", "ORD-2"],
                "transaction_id": ["TXN-1", None],
                "order_date": ["2026-04-10", "2026-04-11"],
                "status": [None, "paid"],
            }
        )

        filled = self.transformer.handle_missing_value(raw_df, {"status": "unknown"})
        with_date_key = self.transformer.create_date_key(
            filled,
            "order_date",
            "order_date_key",
        )
        with_surrogate_key = self.transformer.create_surrogate_key(
            with_date_key,
            ["order_id", "transaction_id"],
            "order_key",
        )

        self.assertEqual("unknown", with_surrogate_key.loc[0, "status"])
        self.assertEqual(pd.Timestamp("2026-04-10").date(), with_surrogate_key.loc[0, "order_date_key"])
        self.assertEqual("ORD-1_TXN-1", with_surrogate_key.loc[0, "order_key"])
        self.assertEqual("ORD-2_", with_surrogate_key.loc[1, "order_key"])

    def test_data_quality_check_reports_nulls_duplicates_dates_and_amounts(self) -> None:
        raw_df = pd.DataFrame(
            {
                "order_key": ["A_1", "A_1", "B_2"],
                "order_id": ["A", "A", "B"],
                "order_date": ["2026-04-01", "2026-04-01", "2026-05-01"],
                "amount_vnd": [100, 100, -50],
                "customer_id": [1, 1, None],
            }
        )

        cleaned_df, report = self.transformer.data_quality_check(
            raw_df,
            table_name="fact_orders",
            critical_columns=["order_key", "customer_id"],
            duplicate_subset=["order_key"],
            remove_duplicates=True,
            date_columns=["order_date"],
            amount_columns=["amount_vnd"],
            min_date="2026-04-01",
            max_date="2026-04-30",
        )

        self.assertEqual(2, len(cleaned_df))
        self.assertEqual(1, report["duplicate_count"])
        self.assertEqual({"customer_id": 1}, report["null_counts"])
        self.assertEqual(1, report["date_validation"]["order_date"]["future_dates"])
        self.assertEqual(1, report["amount_validation"]["amount_vnd"]["negative_count"])


if __name__ == "__main__":
    unittest.main()
