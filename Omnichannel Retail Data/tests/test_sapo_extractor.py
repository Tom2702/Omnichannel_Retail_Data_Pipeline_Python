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
from extractors.sapo_extractor import SapoExtractor


class TestSapoExtractor(unittest.TestCase):
    @patch.object(BaseExtractor, "__init__", return_value=None)
    def test_extract_orders_accepts_non_batch_order_filenames(self, _mock_base_init: MagicMock) -> None:
        extractor = SapoExtractor("dummy-bucket")
        extractor.logger = MagicMock()
        extractor.list_files = MagicMock(
            return_value=[
                "sapo/orders.json.gz",
                "sapo/transactions.json.gz",
                "sapo/locations.json.gz",
            ]
        )
        extractor.extract_json_file = MagicMock(
            return_value={"orders": [{"id": "SAPO-001"}, {"id": "SAPO-002"}]}
        )

        result = extractor.extract_orders()

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(2, len(result))
        self.assertEqual(["SAPO-001", "SAPO-002"], result["id"].tolist())
        extractor.extract_json_file.assert_called_once_with("sapo/orders.json.gz")


if __name__ == "__main__":
    unittest.main()
