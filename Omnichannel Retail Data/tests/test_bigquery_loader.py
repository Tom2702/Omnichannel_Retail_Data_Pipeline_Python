from __future__ import annotations

import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from loaders.bigquery_loader import BigQueryLoader


class TestBigQueryLoader(unittest.TestCase):
    def setUp(self) -> None:
        client_patcher = patch("loaders.bigquery_loader.bigquery.Client")
        credentials_patcher = patch.object(
            BigQueryLoader,
            "_configure_credentials",
            return_value=None,
        )
        dotenv_patcher = patch("loaders.bigquery_loader.load_dotenv", return_value=None)

        self.addCleanup(client_patcher.stop)
        self.addCleanup(credentials_patcher.stop)
        self.addCleanup(dotenv_patcher.stop)

        self.mock_client_cls = client_patcher.start()
        credentials_patcher.start()
        dotenv_patcher.start()

        self.mock_client = MagicMock()
        self.mock_client.project = "test-project"
        self.mock_client_cls.return_value = self.mock_client

        self.loader = BigQueryLoader(project_id="test-project", location="US")

    def test_create_dataset_if_not_exists_returns_existing_dataset(self) -> None:
        existing_dataset = SimpleNamespace(full_dataset_id="test-project:analytics")
        self.mock_client.get_dataset.return_value = existing_dataset

        result = self.loader.create_dataset_if_not_exists("analytics")

        self.assertEqual(existing_dataset, result)
        self.mock_client.get_dataset.assert_called_once_with("test-project.analytics")
        self.mock_client.create_dataset.assert_not_called()

    def test_create_dataset_if_not_exists_creates_missing_dataset(self) -> None:
        self.mock_client.get_dataset.side_effect = NotFound("missing dataset")
        created_dataset = SimpleNamespace(
            full_dataset_id="test-project:analytics",
            location="US",
        )
        self.mock_client.create_dataset.return_value = created_dataset

        result = self.loader.create_dataset_if_not_exists("analytics")

        self.assertEqual(created_dataset, result)
        self.mock_client.create_dataset.assert_called_once()
        created_arg = self.mock_client.create_dataset.call_args.args[0]
        self.assertEqual("test-project", created_arg.project)
        self.assertEqual("analytics", created_arg.dataset_id)
        self.assertEqual("US", created_arg.location)

    def test_load_dataframe_applies_partitioning_and_clustering(self) -> None:
        df = pd.DataFrame(
            {
                "order_date": pd.to_datetime(["2026-04-01", "2026-04-02"]),
                "customer_id": [1, 2],
                "channel": ["shopify", "online"],
                "total_vnd": [100000, 200000],
            }
        )
        load_job = MagicMock()
        self.mock_client.load_table_from_dataframe.return_value = load_job
        self.mock_client.get_table.return_value = SimpleNamespace(num_rows=2)
        self.loader.create_dataset_if_not_exists = MagicMock()

        result = self.loader.load_dataframe(
            df,
            "analytics",
            "fact_orders",
            write_disposition="WRITE_TRUNCATE",
            partition_by="order_date",
            cluster_by=["customer_id", "channel"],
        )

        self.assertEqual(load_job, result)
        self.loader.create_dataset_if_not_exists.assert_called_once_with("analytics")
        self.mock_client.load_table_from_dataframe.assert_called_once()
        args, kwargs = self.mock_client.load_table_from_dataframe.call_args
        self.assertTrue(args[0].equals(df))
        self.assertEqual("test-project.analytics.fact_orders", args[1])
        self.assertEqual("US", kwargs["location"])

        job_config = kwargs["job_config"]
        self.assertEqual(bigquery.WriteDisposition.WRITE_TRUNCATE, job_config.write_disposition)
        self.assertEqual(["customer_id", "channel"], job_config.clustering_fields)
        self.assertIsNotNone(job_config.time_partitioning)
        self.assertEqual("order_date", job_config.time_partitioning.field)
        load_job.result.assert_called_once()

    def test_load_dataframe_rejects_empty_dataframe(self) -> None:
        with self.assertRaises(ValueError):
            self.loader.load_dataframe(
                pd.DataFrame(),
                "analytics",
                "fact_orders",
            )

        self.mock_client.load_table_from_dataframe.assert_not_called()

    def test_load_dataframe_rejects_invalid_write_disposition(self) -> None:
        df = pd.DataFrame({"value": [1]})

        with self.assertRaises(ValueError):
            self.loader.load_dataframe(
                df,
                "analytics",
                "fact_orders",
                write_disposition="INVALID_MODE",
            )

    def test_load_dataframe_rejects_missing_partition_or_cluster_columns(self) -> None:
        df = pd.DataFrame({"order_date": pd.to_datetime(["2026-04-01"])})

        with self.assertRaises(ValueError):
            self.loader.load_dataframe(
                df,
                "analytics",
                "fact_orders",
                partition_by="missing_date",
            )

        with self.assertRaises(ValueError):
            self.loader.load_dataframe(
                df,
                "analytics",
                "fact_orders",
                cluster_by=["customer_id"],
            )

    def test_execute_query_returns_rows_or_dataframe(self) -> None:
        row_results = [SimpleNamespace(customer_id=1), SimpleNamespace(customer_id=2)]
        row_job = MagicMock()
        row_job.result.return_value = row_results
        row_job.total_bytes_processed = 1234
        self.mock_client.query.return_value = row_job

        rows = self.loader.execute_query("SELECT customer_id FROM table")

        self.assertEqual(row_results, rows)
        self.mock_client.query.assert_called_with(
            "SELECT customer_id FROM table",
            job_config=unittest.mock.ANY,
            location="US",
        )

        dataframe_job = MagicMock()
        dataframe_results = MagicMock()
        dataframe_results.to_dataframe.return_value = pd.DataFrame(
            {"customer_id": [1, 2]}
        )
        dataframe_job.result.return_value = dataframe_results
        dataframe_job.total_bytes_processed = 5678
        self.mock_client.query.return_value = dataframe_job

        dataframe = self.loader.execute_query(
            "SELECT customer_id FROM table",
            to_dataframe=True,
        )

        self.assertTrue(dataframe.equals(pd.DataFrame({"customer_id": [1, 2]})))


if __name__ == "__main__":
    unittest.main()
