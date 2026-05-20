from __future__ import annotations

import os
import sys
import unittest

import pandas as pd
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_integer_dtype,
    is_numeric_dtype,
    is_string_dtype,
)


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from transformers.fact_transformer import FactTransformer


class TestFactTransformer(unittest.TestCase):
    def setUp(self) -> None:
        self.transformer = FactTransformer()

    def test_transform_orders_coalesces_fields_and_removes_duplicates(self) -> None:
        raw_df = pd.DataFrame(
            [
                {
                    "id": "SHP-001",
                    "transaction_code": "TXN-001",
                    "customer": {"id": 501},
                    "created_at": "2026-04-01T09:00:00Z",
                    "shippingAddress": {
                        "cityName": "Ho Chi Minh City",
                        "address1": "12 Nguyen Hue",
                    },
                    "financial_status": "paid",
                    "amount_vnd": 150000,
                },
                {
                    "id": "SHP-001",
                    "transaction_code": "TXN-001",
                    "customer": {"id": 501},
                    "created_at": "2026-04-01T09:00:00Z",
                    "shippingAddress": {
                        "cityName": "Ho Chi Minh City",
                        "address1": "12 Nguyen Hue",
                    },
                    "financial_status": "paid",
                    "amount_vnd": 150000,
                },
                {
                    "code": "ONL-002",
                    "txn_id": "TXN-002",
                    "customer_id": 502,
                    "created_on": "2026-04-02T11:30:00Z",
                    "delivery_address": {
                        "provinceName": "Da Nang",
                        "fullAddress": "99 Nguyen Van Linh",
                    },
                    "order_status": None,
                    "payment_status": None,
                    "grand_total_vnd": 275000,
                    "grand_total_usd": 10.8,
                },
            ]
        )

        transformed = self.transformer.transform_orders(
            raw_df,
            channel="online",
            source="test_source",
        )

        self.assertEqual(2, len(transformed))
        self.assertEqual(
            [
                "order_key",
                "order_id",
                "transaction_id",
                "customer_id",
                "order_date_key",
                "order_date",
                "channel",
                "source",
                "shipping_city",
                "shipping_address",
                "status",
                "payment_status",
                "total_vnd",
                "total_usd",
            ],
            transformed.columns.tolist(),
        )
        self.assertTrue(is_string_dtype(transformed["order_id"]))
        self.assertTrue(is_integer_dtype(transformed["customer_id"]))
        self.assertTrue(is_datetime64_any_dtype(transformed["order_date"]))
        self.assertTrue(is_numeric_dtype(transformed["total_vnd"]))

        first_row = transformed.loc[transformed["order_id"] == "SHP-001"].iloc[0]
        self.assertEqual("SHP-001_TXN-001", first_row["order_key"])
        self.assertEqual(501, first_row["customer_id"])
        self.assertEqual("online", first_row["channel"])
        self.assertEqual("test_source", first_row["source"])
        self.assertEqual("Ho Chi Minh City", first_row["shipping_city"])
        self.assertEqual("12 Nguyen Hue", first_row["shipping_address"])
        self.assertEqual("unknown", first_row["status"])
        self.assertEqual("paid", first_row["payment_status"])

        second_row = transformed.loc[transformed["order_id"] == "ONL-002"].iloc[0]
        self.assertEqual("Da Nang", second_row["shipping_city"])
        self.assertEqual("99 Nguyen Van Linh", second_row["shipping_address"])
        self.assertEqual("unknown", second_row["status"])
        self.assertEqual("unknown", second_row["payment_status"])
        self.assertEqual(275000, second_row["total_vnd"])
        self.assertEqual(pd.Timestamp("2026-04-02").date(), second_row["order_date_key"])

    def test_transform_order_items_unflattens_and_calculates_missing_line_total(self) -> None:
        raw_df = pd.DataFrame(
            [
                {
                    "id": "SHP-100",
                    "transaction_code": "TXN-100",
                    "created_at": "2026-04-03T10:00:00Z",
                    "line_items": [
                        {
                            "variant_id": 9001,
                            "transaction_id": "TXN-100",
                            "quantity": 2,
                            "price_vnd": 120000,
                            "amount_vnd": None,
                        },
                        {
                            "variant_id": 9002,
                            "qty": 1,
                            "price": 50000,
                            "line_total": 50000,
                        },
                    ],
                }
            ]
        )

        transformed = self.transformer.transform_order_items(raw_df)

        self.assertEqual(2, len(transformed))
        self.assertEqual(
            [
                "order_item_key",
                "order_key",
                "transaction_id",
                "product_id",
                "order_date_key",
                "quantity",
                "unit_price_vnd",
                "line_total_vnd",
            ],
            transformed.columns.tolist(),
        )

        first_item = transformed.loc[transformed["product_id"] == 9001].iloc[0]
        self.assertEqual("SHP-100_TXN-100", first_item["order_key"])
        self.assertEqual("TXN-100", first_item["transaction_id"])
        self.assertEqual(240000, first_item["line_total_vnd"])
        self.assertEqual(pd.Timestamp("2026-04-03").date(), first_item["order_date_key"])

        second_item = transformed.loc[transformed["product_id"] == 9002].iloc[0]
        self.assertEqual("TXN-100", second_item["transaction_id"])
        self.assertEqual(1, second_item["quantity"])
        self.assertEqual(50000, second_item["unit_price_vnd"])
        self.assertEqual(50000, second_item["line_total_vnd"])

    def test_transform_payments_builds_keys_and_defaults_missing_fields(self) -> None:
        raw_df = pd.DataFrame(
            [
                {
                    "txn_id": "PAY-001",
                    "reference_id": "ORD-001",
                    "customer": {"id": 7001},
                    "method": None,
                    "amount": 99000,
                    "status": None,
                    "paid_at": "2026-04-05T13:00:00Z",
                },
                {
                    "txn_id": "PAY-001",
                    "reference_id": "ORD-001",
                    "customer": {"id": 7001},
                    "gateway": "momo",
                    "method": "wallet",
                    "amount": 99000,
                    "status": "paid",
                    "paid_at": "2026-04-05T13:00:00Z",
                },
            ]
        )

        transformed = self.transformer.transform_payments(
            raw_df,
            payment_gateway="momo",
        )

        self.assertEqual(1, len(transformed))
        row = transformed.iloc[0]
        self.assertEqual("PAY-001_momo_ORD-001", row["payment_key"])
        self.assertEqual("momo", row["payment_gateway"])
        self.assertEqual("unknown", row["payment_method"])
        self.assertEqual("unknown", row["payment_status"])
        self.assertEqual(7001, row["customer_id"])
        self.assertEqual(pd.Timestamp("2026-04-05").date(), row["payment_date_key"])

    def test_transform_cart_events_creates_event_keys_and_casts_types(self) -> None:
        raw_df = pd.DataFrame(
            [
                {
                    "id": "EVT-001",
                    "session": "SES-001",
                    "customer_id": 801,
                    "type": "add_to_cart",
                    "timestamp": "2026-04-06T08:00:00Z",
                    "item_id": 3001,
                    "channel": "ads",
                    "device_type": "mobile",
                    "browser": "Chrome",
                    "utm_source": "facebook",
                    "utm_campaign": "summer_sale",
                },
                {
                    "id": "EVT-001",
                    "session": "SES-001",
                    "customer_id": 801,
                    "type": "add_to_cart",
                    "timestamp": "2026-04-06T08:00:00Z",
                    "item_id": 3001,
                    "channel": "ads",
                    "device_type": "mobile",
                    "browser": "Chrome",
                    "utm_source": "facebook",
                    "utm_campaign": "summer_sale",
                },
            ]
        )

        transformed = self.transformer.transform_cart_events(raw_df)

        self.assertEqual(1, len(transformed))
        self.assertEqual("EVT-001_SES-001", transformed.iloc[0]["event_key"])
        self.assertEqual("add_to_cart", transformed.iloc[0]["event_type"])
        self.assertEqual(pd.Timestamp("2026-04-06").date(), transformed.iloc[0]["event_date_key"])
        self.assertTrue(is_string_dtype(transformed["event_id"]))
        self.assertTrue(is_integer_dtype(transformed["customer_id"]))
        self.assertTrue(is_datetime64_any_dtype(transformed["event_timestamp"]))


if __name__ == "__main__":
    unittest.main()
