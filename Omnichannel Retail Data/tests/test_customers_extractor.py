from __future__ import annotations

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from extractors.base_extractor import BaseExtractor
from extractors.customers_extractor import CustomersExtractor


class TestCustomersExtractor(unittest.TestCase):
    def setUp(self) -> None:
        fixture_path = os.path.join(CURRENT_DIR, "fixtures", "customers.json")
        with open(fixture_path, "r", encoding="utf-8") as fixture_file:
            self.customers_payload = json.load(fixture_file)

    @patch.object(BaseExtractor, "__init__", return_value=None)
    def test_extract_customers_filters_customer_batches_and_combines_rows(
        self,
        _mock_base_init: MagicMock,
    ) -> None:
        extractor = CustomersExtractor("dummy-bucket")
        extractor.logger = MagicMock()
        extractor.list_files = MagicMock(
            return_value=[
                "shared/customers/customer_batch_001.json.gz",
                "shared/customers/customer_batch_002.json.gz",
                "shared/customers/readme.txt",
                "shared/customers/orders_batch_001.json.gz",
            ]
        )

        second_payload = {
            "customers": [
                {
                    "id": 1005,
                    "email": "extra.customer@example.com",
                    "name": "Extra Customer",
                }
            ]
        }
        extractor.extract_json_file = MagicMock(
            side_effect=[self.customers_payload, second_payload]
        )

        result = extractor.extract_customers()

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(5, len(result))
        self.assertEqual(
            [1001, 1002, 1003, 1004, 1005],
            result["id"].tolist(),
        )
        extractor.list_files.assert_called_once_with("shared/customers/")
        self.assertEqual(2, extractor.extract_json_file.call_count)

    @patch.object(BaseExtractor, "__init__", return_value=None)
    def test_extract_customers_continues_when_a_file_fails(
        self,
        _mock_base_init: MagicMock,
    ) -> None:
        extractor = CustomersExtractor("dummy-bucket")
        extractor.logger = MagicMock()
        extractor.list_files = MagicMock(
            return_value=[
                "shared/customers/customer_batch_001.json.gz",
                "shared/customers/customer_batch_002.json.gz",
            ]
        )
        extractor.extract_json_file = MagicMock(
            side_effect=[
                self.customers_payload,
                RuntimeError("broken gzip"),
            ]
        )

        result = extractor.extract_customers()

        self.assertEqual(4, len(result))
        self.assertEqual(
            [1001, 1002, 1003, 1004],
            result["id"].tolist(),
        )
        extractor.logger.error.assert_called_once()

    @patch.object(BaseExtractor, "__init__", return_value=None)
    def test_extract_customers_falls_back_to_all_json_files_when_names_do_not_match(
        self,
        _mock_base_init: MagicMock,
    ) -> None:
        extractor = CustomersExtractor("dummy-bucket")
        extractor.logger = MagicMock()
        extractor.list_files = MagicMock(
            side_effect=[
                [
                    "shared/customers/batch_001.json.gz",
                    "shared/customers/batch_002.json.gz",
                ],
                [
                    "shared/customers/batch_001.json.gz",
                    "shared/customers/batch_002.json.gz",
                ],
            ]
        )
        extractor.extract_json_file = MagicMock(
            side_effect=[
                self.customers_payload,
                {"customers": [{"id": 1005, "email": "fallback@example.com", "name": "Fallback"}]},
            ]
        )

        result = extractor.extract_customers()

        self.assertEqual(5, len(result))
        self.assertEqual(2, extractor.list_files.call_count)
        extractor.logger.warning.assert_called_once()


if __name__ == "__main__":
    unittest.main()
