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
from extractors.payment_extractor import PaymentExtractor


class TestPaymentExtractor(unittest.TestCase):
    @patch.object(BaseExtractor, "__init__", return_value=None)
    def test_payment_momo_extract_reads_transactions_key(
        self,
        _mock_base_init: MagicMock,
    ) -> None:
        extractor = PaymentExtractor("dummy-bucket")
        extractor.logger = MagicMock()
        extractor.extract_json_file = MagicMock(
            return_value={
                "transactions": [
                    {"txn_id": "MOMO-001", "amount": 150000},
                    {"txn_id": "MOMO-002", "amount": 275000},
                ]
            }
        )

        result = extractor.payment_momo_extract()

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(2, len(result))
        self.assertEqual(["MOMO-001", "MOMO-002"], result["txn_id"].tolist())
        extractor.extract_json_file.assert_called_once_with("momo/transactions.json.gz")

    @patch.object(BaseExtractor, "__init__", return_value=None)
    def test_payment_zalopay_extract_handles_list_payload(
        self,
        _mock_base_init: MagicMock,
    ) -> None:
        extractor = PaymentExtractor("dummy-bucket")
        extractor.logger = MagicMock()
        extractor.extract_json_file = MagicMock(
            return_value=[
                {"txn_id": "ZALO-001", "amount": 99000},
                {"txn_id": "ZALO-002", "amount": 199000},
            ]
        )

        result = extractor.payment_zalopay_extract()

        self.assertEqual(2, len(result))
        self.assertEqual(["ZALO-001", "ZALO-002"], result["txn_id"].tolist())
        extractor.extract_json_file.assert_called_once_with("zalopay/transactions.json.gz")

    @patch.object(BaseExtractor, "__init__", return_value=None)
    def test_payment_mercury_extract_returns_empty_dataframe_on_error(
        self,
        _mock_base_init: MagicMock,
    ) -> None:
        extractor = PaymentExtractor("dummy-bucket")
        extractor.logger = MagicMock()
        extractor.extract_json_file = MagicMock(
            side_effect=RuntimeError("cannot read payload")
        )

        result = extractor.payment_mercury_extract()

        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(result.empty)
        extractor.logger.error.assert_called_once()


if __name__ == "__main__":
    unittest.main()
