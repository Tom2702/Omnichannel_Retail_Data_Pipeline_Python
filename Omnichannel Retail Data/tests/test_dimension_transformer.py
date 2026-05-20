from __future__ import annotations

import json
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

from transformers.dimension_transformer import DimensionTransformer


class TestDimensionTransformer(unittest.TestCase):
    def setUp(self) -> None:
        fixture_path = os.path.join(CURRENT_DIR, "fixtures", "customers.json")
        with open(fixture_path, "r", encoding="utf-8") as fixture_file:
            payload = json.load(fixture_file)

        self.customer_df = pd.DataFrame(payload["customers"])
        self.product_df = pd.DataFrame(
            [
                {
                    "id": 2001,
                    "name": "Yoga Mat",
                    "sku": "YM-001",
                    "barcode": "893850100001",
                    "category": "Fitness",
                    "brand": "Minpy",
                    "price_vnd": 450000,
                    "price_usd": 18.5,
                    "stock_quantity": 25,
                    "is_active": True,
                },
                {
                    "id": 2002,
                    "name": "Resistance Band",
                    "sku": "RB-002",
                    "barcode": None,
                    "category": "Fitness",
                    "brand": "Minpy",
                    "price_vnd": None,
                    "price_usd": None,
                    "stock_quantity": None,
                    "is_active": None,
                },
            ]
        )
        self.transformer = DimensionTransformer()

    def test_transform_customers_maps_columns_and_fills_defaults(self) -> None:
        transformed = self.transformer.transform_customers(self.customer_df)

        expected_columns = [
            "customer_id",
            "email",
            "full_name",
            "phone",
            "city",
            "country",
            "created_at",
            "customer_segment",
            "lifetime_value_vnd",
            "total_orders",
            "first_order_date",
            "last_order_date",
        ]

        self.assertEqual(expected_columns, transformed.columns.tolist())
        self.assertEqual(4, len(transformed))
        self.assertTrue(is_integer_dtype(transformed["customer_id"]))
        self.assertTrue(is_string_dtype(transformed["email"]))
        self.assertTrue(is_string_dtype(transformed["full_name"]))
        self.assertTrue(is_datetime64_any_dtype(transformed["created_at"]))
        self.assertTrue(is_numeric_dtype(transformed["lifetime_value_vnd"]))
        self.assertTrue(is_integer_dtype(transformed["total_orders"]))

        customer_1002 = transformed.loc[transformed["customer_id"] == 1002].iloc[0]
        self.assertEqual("unknown", customer_1002["customer_segment"])
        self.assertEqual(0, customer_1002["lifetime_value_vnd"])
        self.assertEqual(0, customer_1002["total_orders"])
        self.assertTrue(pd.isna(customer_1002["phone"]))

    def test_transform_products_maps_columns_and_fills_defaults(self) -> None:
        transformed = self.transformer.transform_products(self.product_df)

        expected_columns = [
            "product_id",
            "product_name",
            "sku",
            "barcode",
            "category",
            "brand",
            "price_vnd",
            "price_usd",
            "stock_quantity",
            "is_active",
        ]

        self.assertEqual(expected_columns, transformed.columns.tolist())
        self.assertEqual(2, len(transformed))
        self.assertTrue(is_integer_dtype(transformed["product_id"]))
        self.assertTrue(is_string_dtype(transformed["product_name"]))
        self.assertTrue(is_numeric_dtype(transformed["price_vnd"]))
        self.assertTrue(is_numeric_dtype(transformed["price_usd"]))
        self.assertTrue(is_integer_dtype(transformed["stock_quantity"]))
        self.assertTrue(is_bool_dtype(transformed["is_active"]))

        product_2002 = transformed.loc[transformed["product_id"] == 2002].iloc[0]
        self.assertEqual(0, product_2002["price_vnd"])
        self.assertEqual(0, product_2002["price_usd"])
        self.assertEqual(0, product_2002["stock_quantity"])
        self.assertEqual(True, product_2002["is_active"])

    def test_transform_date_creates_expected_calendar_fields(self) -> None:
        transformed = self.transformer.transform_date(
            "2026-04-20",
            "2026-04-22",
            holidays=["2026-04-21"],
        )

        self.assertEqual(3, len(transformed))
        self.assertEqual(
            [
                "date_key",
                "full_date",
                "year",
                "quarter",
                "month",
                "month_name",
                "week",
                "day_of_month",
                "day_of_week",
                "day_name",
                "is_weekend",
                "is_holiday",
                "fiscal_year",
                "fiscal_quarter",
            ],
            transformed.columns.tolist(),
        )

        holiday_row = transformed.loc[transformed["date_key"] == 20260421].iloc[0]
        self.assertEqual(pd.Timestamp("2026-04-21").date(), holiday_row["full_date"])
        self.assertEqual(2026, holiday_row["year"])
        self.assertEqual(4, holiday_row["month"])
        self.assertEqual("April", holiday_row["month_name"])
        self.assertEqual("Tuesday", holiday_row["day_name"])
        self.assertEqual(False, holiday_row["is_weekend"])
        self.assertEqual(True, holiday_row["is_holiday"])


if __name__ == "__main__":
    unittest.main()
