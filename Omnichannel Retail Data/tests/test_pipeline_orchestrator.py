from __future__ import annotations

import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from orchestration.pipeline_orchestrator import PipelineOrchestrator


class TestPipelineOrchestrator(unittest.TestCase):
    def setUp(self) -> None:
        self.patchers = {
            "load_dotenv": patch("orchestration.pipeline_orchestrator.load_dotenv", return_value=None),
            "setup_logger": patch("orchestration.pipeline_orchestrator.setup_logger"),
            "loader": patch("orchestration.pipeline_orchestrator.BigQueryLoader"),
            "dimension_transformer": patch("orchestration.pipeline_orchestrator.DimensionTransformer"),
            "fact_transformer": patch("orchestration.pipeline_orchestrator.FactTransformer"),
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
            num_dml_affected_rows=4,
        )
        self.mocks["loader"].return_value = self.loader

        self.dimension_transformer = MagicMock()
        self.fact_transformer = MagicMock()
        self.mocks["dimension_transformer"].return_value = self.dimension_transformer
        self.mocks["fact_transformer"].return_value = self.fact_transformer

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

        self.orchestrator = PipelineOrchestrator(
            bucket_name="bucket-test",
            dataset_id="dataset_test",
            project_id="test-project",
            location="US",
            start_date="2026-04-01",
            end_date="2026-04-30",
        )

    def _stop_patchers(self) -> None:
        for patcher in self.patchers.values():
            patcher.stop()

    def test_run_loads_dimensions_then_facts_then_updates_aggregates(self) -> None:
        raw_sources = {"raw": "sources"}
        fact_tables = {
            "fact_orders": pd.DataFrame({"order_key": ["A_1"]}),
            "fact_order_items": pd.DataFrame(),
            "fact_payments": pd.DataFrame(),
            "fact_cart_events": pd.DataFrame(),
        }
        dimension_tables = {
            "dim_customers": pd.DataFrame({"customer_id": [1]}),
            "dim_products": pd.DataFrame(),
            "dim_date": pd.DataFrame(),
        }
        call_order: list[str] = []

        self.orchestrator._extract_sources = MagicMock(return_value=raw_sources)
        self.orchestrator._transform_facts = MagicMock(return_value=fact_tables)
        self.orchestrator._transform_dimensions = MagicMock(return_value=dimension_tables)
        self.orchestrator._run_data_quality_checks = MagicMock(
            side_effect=[dimension_tables, fact_tables]
        )
        self.orchestrator._load_dimensions = MagicMock(
            side_effect=lambda tables, summary: (
                call_order.append("dimensions"),
                summary.update({"dim_customers": 1, "dim_products": 0, "dim_date": 0}),
            )[-1]
        )
        self.orchestrator._load_facts = MagicMock(
            side_effect=lambda tables, summary: (
                call_order.append("facts"),
                summary.update(
                    {
                        "fact_orders": 1,
                        "fact_order_items": 0,
                        "fact_payments": 0,
                        "fact_cart_events": 0,
                    }
                ),
            )[-1]
        )
        self.orchestrator.update_customer_aggregates = MagicMock(
            side_effect=lambda: (call_order.append("aggregates"), 7)[1]
        )

        summary = self.orchestrator.run()

        self.assertEqual(["dimensions", "facts", "aggregates"], call_order)
        self.assertEqual(7, summary["dim_customers_aggregate_updates"])
        self.assertEqual(1, summary["dim_customers"])
        self.assertEqual(1, summary["fact_orders"])
        self.orchestrator.update_customer_aggregates.assert_called_once()

    def test_run_skips_aggregate_update_when_fact_orders_or_dim_customers_empty(self) -> None:
        raw_sources = {"raw": "sources"}
        fact_tables = {
            "fact_orders": pd.DataFrame(),
            "fact_order_items": pd.DataFrame(),
            "fact_payments": pd.DataFrame(),
            "fact_cart_events": pd.DataFrame(),
        }
        dimension_tables = {
            "dim_customers": pd.DataFrame({"customer_id": [1]}),
            "dim_products": pd.DataFrame(),
            "dim_date": pd.DataFrame(),
        }

        self.orchestrator._extract_sources = MagicMock(return_value=raw_sources)
        self.orchestrator._transform_facts = MagicMock(return_value=fact_tables)
        self.orchestrator._transform_dimensions = MagicMock(return_value=dimension_tables)
        self.orchestrator._run_data_quality_checks = MagicMock(
            side_effect=[dimension_tables, fact_tables]
        )
        self.orchestrator._load_dimensions = MagicMock()
        self.orchestrator._load_facts = MagicMock()
        self.orchestrator.update_customer_aggregates = MagicMock()

        summary = self.orchestrator.run()

        self.assertEqual(0, summary["dim_customers_aggregate_updates"])
        self.orchestrator.update_customer_aggregates.assert_not_called()

    def test_extract_payment_sources_calls_each_gateway_extractor(self) -> None:
        self.payment_extractor.payment_mercury_extract.return_value = pd.DataFrame(
            {"id": ["mercury"]}
        )
        self.payment_extractor.payment_momo_extract.return_value = pd.DataFrame(
            {"id": ["momo"]}
        )
        self.payment_extractor.payment_odoo_extract.return_value = pd.DataFrame(
            {"id": ["odoo"]}
        )
        self.payment_extractor.payment_paypal_extract.return_value = pd.DataFrame(
            {"id": ["paypal"]}
        )
        self.payment_extractor.payment_sapo_extract.return_value = pd.DataFrame(
            {"id": ["sapo"]}
        )
        self.payment_extractor.payment_zalopay_extract.return_value = pd.DataFrame(
            {"id": ["zalopay"]}
        )

        payment_sources = self.orchestrator._extract_payment_sources()

        self.assertEqual(
            ["momo", "odoo", "paypal", "sapo", "zalopay"],
            list(payment_sources.keys()),
        )
        self.payment_extractor.payment_momo_extract.assert_called_once()
        self.payment_extractor.payment_odoo_extract.assert_called_once()
        self.payment_extractor.payment_paypal_extract.assert_called_once()
        self.payment_extractor.payment_sapo_extract.assert_called_once()
        self.payment_extractor.payment_zalopay_extract.assert_called_once()

    def test_update_customer_aggregates_executes_merge_query(self) -> None:
        affected_rows = self.orchestrator.update_customer_aggregates()

        self.assertEqual(4, affected_rows)
        self.loader.client.query.assert_called_once()
        query_text = self.loader.client.query.call_args.args[0]
        self.assertIn("MERGE `test-project.dataset_test.dim_customers`", query_text)
        self.assertIn("FROM `test-project.dataset_test.fact_orders`", query_text)
        self.loader.client.query.assert_called_with(query_text, location="US")

    def test_build_date_dimension_falls_back_to_existing_warehouse_facts(self) -> None:
        self.orchestrator.start_date = None
        self.orchestrator.end_date = None
        self.loader.execute_query.return_value = [
            {
                "overall_min_date": pd.Timestamp("2025-01-06").date(),
                "overall_max_date": pd.Timestamp("2026-01-06").date(),
            }
        ]
        expected_dim_date = pd.DataFrame(
            [
                {
                    "date_key": 20250106,
                    "full_date": pd.Timestamp("2025-01-06").date(),
                    "year": 2025,
                    "quarter": 1,
                    "month": 1,
                    "month_name": "January",
                    "week": 2,
                    "day_of_month": 6,
                    "day_of_week": 1,
                    "day_name": "Monday",
                    "is_weekend": False,
                    "is_holiday": False,
                    "fiscal_year": 2025,
                    "fiscal_quarter": 1,
                }
            ]
        )
        self.dimension_transformer.transform_date.return_value = expected_dim_date

        fact_tables = {
            "fact_orders": pd.DataFrame(columns=["order_date"]),
            "fact_order_items": pd.DataFrame(),
            "fact_payments": pd.DataFrame(columns=["payment_date"]),
            "fact_cart_events": pd.DataFrame(columns=["event_timestamp"]),
            "fact_bank_transactions": pd.DataFrame(columns=["transaction_date"]),
        }

        dim_date = self.orchestrator._build_date_dimension(fact_tables)

        self.dimension_transformer.transform_date.assert_called_once_with(
            "2025-01-06",
            "2026-01-06",
            holidays=[],
        )
        self.assertEqual(expected_dim_date.to_dict("records"), dim_date.to_dict("records"))


if __name__ == "__main__":
    unittest.main()
