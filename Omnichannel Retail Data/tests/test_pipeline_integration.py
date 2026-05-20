from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from orchestration.pipeline_orchestrator import PipelineOrchestrator


class TestPipelineIntegration(unittest.TestCase):
    def setUp(self) -> None:
        self.patchers = {
            "load_dotenv": patch("orchestration.pipeline_orchestrator.load_dotenv", return_value=None),
            "setup_logger": patch("orchestration.pipeline_orchestrator.setup_logger"),
            "loader": patch("orchestration.pipeline_orchestrator.BigQueryLoader"),
            "customers_extractor": patch("orchestration.pipeline_orchestrator.CustomersExtractor"),
            "products_extractor": patch("orchestration.pipeline_orchestrator.ProductsExtractor"),
            "sapo_extractor": patch("orchestration.pipeline_orchestrator.SapoExtractor"),
            "shopify_extractor": patch("orchestration.pipeline_orchestrator.ShopifyExtractor"),
            "online_orders_extractor": patch("orchestration.pipeline_orchestrator.OnlineOrdersExtractor"),
            "payment_extractor": patch("orchestration.pipeline_orchestrator.PaymentExtractor"),
            "cart_tracking_extractor": patch("orchestration.pipeline_orchestrator.CartTrackingExtractor"),
        }
        self.mocks = {name: patcher.start() for name, patcher in self.patchers.items()}
        self.addCleanup(self._stop_patchers)

        self.logger = MagicMock()
        self.mocks["setup_logger"].return_value = self.logger

        self.loader = MagicMock()
        self.loader.location = "US"
        self.loader._build_table_id.side_effect = lambda dataset_id, table_name: (
            f"test-project.{dataset_id}.{table_name}"
        )
        self.loader.client.query.return_value = MagicMock(
            result=MagicMock(),
            num_dml_affected_rows=2,
        )
        self.mocks["loader"].return_value = self.loader

        self.customers_extractor = MagicMock()
        self.products_extractor = MagicMock()
        self.sapo_extractor = MagicMock()
        self.shopify_extractor = MagicMock()
        self.online_orders_extractor = MagicMock()
        self.payment_extractor = MagicMock()
        self.cart_tracking_extractor = MagicMock()

        self.mocks["customers_extractor"].return_value = self.customers_extractor
        self.mocks["products_extractor"].return_value = self.products_extractor
        self.mocks["sapo_extractor"].return_value = self.sapo_extractor
        self.mocks["shopify_extractor"].return_value = self.shopify_extractor
        self.mocks["online_orders_extractor"].return_value = self.online_orders_extractor
        self.mocks["payment_extractor"].return_value = self.payment_extractor
        self.mocks["cart_tracking_extractor"].return_value = self.cart_tracking_extractor

    def _stop_patchers(self) -> None:
        for patcher in self.patchers.values():
            patcher.stop()

    def test_run_executes_mini_pipeline_end_to_end_with_mocked_io(self) -> None:
        self.customers_extractor.extract_customers.return_value = pd.DataFrame(
            [
                {
                    "id": 1001,
                    "email": "alice@example.com",
                    "name": "Alice",
                    "phone": "0901",
                    "city": "Ho Chi Minh City",
                    "country": "Vietnam",
                    "created_at": "2026-03-01T00:00:00Z",
                },
                {
                    "id": 1002,
                    "email": "bao@example.com",
                    "name": "Bao",
                    "phone": None,
                    "city": "Da Nang",
                    "country": "Vietnam",
                    "created_at": "2026-03-15T00:00:00Z",
                    "customer_segment": None,
                    "lifetime_value_vnd": None,
                    "total_orders": None,
                },
            ]
        )
        self.products_extractor.extract_file.return_value = pd.DataFrame(
            [
                {
                    "id": 2001,
                    "name": "Yoga Mat",
                    "sku": "YM-001",
                    "barcode": "893850100001",
                    "category": "Fitness",
                    "brand": "Minpy",
                    "price_vnd": 100000,
                    "price_usd": 4.0,
                    "stock_quantity": 20,
                    "is_active": True,
                },
                {
                    "id": 2002,
                    "name": "Water Bottle",
                    "sku": "WB-002",
                    "barcode": "893850100002",
                    "category": "Accessories",
                    "brand": "Minpy",
                    "price_vnd": 50000,
                    "price_usd": 2.0,
                    "stock_quantity": 40,
                    "is_active": True,
                },
            ]
        )
        self.sapo_extractor.extract_locations.return_value = pd.DataFrame()
        self.sapo_extractor.extract_orders.return_value = pd.DataFrame()
        self.shopify_extractor.extract_file.return_value = pd.DataFrame(
            [
                {
                    "id": "SHP-001",
                    "transaction_code": "TXN-001",
                    "customer": {"id": 1001},
                    "created_at": "2026-04-01T10:00:00Z",
                    "status": "completed",
                    "financial_status": "paid",
                    "amount_vnd": 150000,
                    "amount_usd": 6.0,
                    "line_items": [
                        {
                            "variant_id": 2001,
                            "quantity": 2,
                            "price_vnd": 50000,
                        },
                        {
                            "variant_id": 2002,
                            "qty": 1,
                            "price": 50000,
                            "line_total": 50000,
                        },
                    ],
                }
            ]
        )
        self.online_orders_extractor.extract_orders.return_value = pd.DataFrame(
            [
                {
                    "code": "ONL-001",
                    "txn_id": "TXN-002",
                    "customer_id": 1002,
                    "created_on": "2026-04-03T12:00:00Z",
                    "shippingAddress": {
                        "provinceName": "Can Tho",
                        "address1": "45 Le Loi",
                    },
                    "order_status": "pending",
                    "payment_status": "unpaid",
                    "grand_total_vnd": 275000,
                    "grand_total_usd": 11.0,
                    "line_items": [
                        {
                            "item_id": 2001,
                            "qty": 1,
                            "price": 275000,
                            "line_total": 275000,
                        }
                    ],
                }
            ]
        )
        self.payment_extractor.payment_mercury_extract.return_value = pd.DataFrame()
        self.payment_extractor.payment_momo_extract.return_value = pd.DataFrame(
            [
                {
                    "txn_id": "PAY-001",
                    "reference_id": "SHP-001",
                    "customer": {"id": 1001},
                    "gateway": "momo",
                    "method": "wallet",
                    "amount": 150000,
                    "status": "paid",
                    "paid_at": "2026-04-02T08:00:00Z",
                }
            ]
        )
        self.payment_extractor.payment_odoo_extract.return_value = pd.DataFrame()
        self.payment_extractor.payment_paypal_extract.return_value = pd.DataFrame()
        self.payment_extractor.payment_sapo_extract.return_value = pd.DataFrame()
        self.payment_extractor.payment_zalopay_extract.return_value = pd.DataFrame()
        self.cart_tracking_extractor.extract_file.return_value = pd.DataFrame(
            [
                {
                    "id": "EVT-001",
                    "session": "SES-001",
                    "customer_id": 1001,
                    "type": "add_to_cart",
                    "timestamp": "2026-04-04T09:00:00Z",
                    "item_id": 2001,
                    "channel": "ads",
                    "device_type": "mobile",
                    "browser": "Chrome",
                    "utm_source": "facebook",
                    "utm_campaign": "launch",
                }
            ]
        )

        orchestrator = PipelineOrchestrator(
            bucket_name="bucket-test",
            dataset_id="dataset_test",
            project_id="test-project",
            location="US",
            start_date="2026-04-01T00:00:00Z",
            end_date="2026-04-06T23:59:59Z",
        )

        summary = orchestrator.run()

        fact_orders_load = next(
            call for call in self.loader.load_dataframe.call_args_list
            if call.args[2] == "fact_orders"
        )
        fact_orders_df = fact_orders_load.args[0]
        online_order = fact_orders_df.loc[fact_orders_df["order_id"] == "ONL-001"].iloc[0]

        loaded_tables = [call.args[2] for call in self.loader.load_dataframe.call_args_list]
        self.assertEqual(
            [
                "dim_customers",
                "dim_products",
                "dim_date",
                "fact_orders",
                "fact_order_items",
                "fact_payments",
                "fact_cart_events",
            ],
            loaded_tables,
        )
        self.assertEqual("Can Tho", online_order["shipping_city"])
        self.assertEqual("45 Le Loi", online_order["shipping_address"])
        self.assertEqual(2, summary["dim_customers"])
        self.assertEqual(2, summary["dim_products"])
        self.assertEqual(6, summary["dim_date"])
        self.assertEqual(2, summary["fact_orders"])
        self.assertEqual(3, summary["fact_order_items"])
        self.assertEqual(1, summary["fact_payments"])
        self.assertEqual(1, summary["fact_cart_events"])
        self.assertEqual(2, summary["dim_customers_aggregate_updates"])
        self.loader.client.query.assert_called_once()


if __name__ == "__main__":
    unittest.main()
