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

from extractors.base_extractor import BaseExtractor
from extractors.shopify_extractor import ShopifyExtractor


class TestShopifyExtractor(unittest.TestCase):
    @patch.object(BaseExtractor, "__init__", return_value=None)
    def test_extract_file_filters_order_batches_and_combines_orders(
        self,
        _mock_base_init: MagicMock,
    ) -> None:
        extractor = ShopifyExtractor("dummy-bucket")
        extractor.logger = MagicMock()
        extractor.list_files = MagicMock(
            return_value=[
                "shopify/orders_batch_001.json.gz",
                "shopify/orders_batch_002.json.gz",
                "shopify/customers_batch_001.json.gz",
            ]
        )
        extractor.extract_json_file = MagicMock(
            side_effect=[
                {
                    "orders": [
                        {"id": "SHP-001", "transaction_code": "TXN-001"},
                        {"id": "SHP-002", "transaction_code": "TXN-002"},
                    ]
                },
                [
                    {"id": "SHP-003", "transaction_code": "TXN-003"},
                ],
            ]
        )

        result = extractor.extract_file()

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(3, len(result))
        self.assertEqual(
            ["SHP-001", "SHP-002", "SHP-003"],
            result["id"].tolist(),
        )
        extractor.list_files.assert_called_once_with("shopify/")
        self.assertEqual(2, extractor.extract_json_file.call_count)

    @patch.object(BaseExtractor, "__init__", return_value=None)
    def test_extract_file_continues_when_one_batch_fails(
        self,
        _mock_base_init: MagicMock,
    ) -> None:
        extractor = ShopifyExtractor("dummy-bucket")
        extractor.logger = MagicMock()
        extractor.list_files = MagicMock(
            return_value=[
                "shopify/orders_batch_001.json.gz",
                "shopify/orders_batch_002.json.gz",
            ]
        )
        extractor.extract_json_file = MagicMock(
            side_effect=[
                {"orders": [{"id": "SHP-001"}]},
                RuntimeError("corrupted payload"),
            ]
        )

        result = extractor.extract_file()

        self.assertEqual(1, len(result))
        self.assertEqual(["SHP-001"], result["id"].tolist())
        extractor.logger.error.assert_called_once()


if __name__ == "__main__":
    unittest.main()
