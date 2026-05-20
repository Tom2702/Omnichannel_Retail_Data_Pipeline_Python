from __future__ import annotations

import os
import sys

import pandas as pd

try:
    from extractors.base_extractor import BaseExtractor
except ModuleNotFoundError:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    if project_root not in sys.path:
        sys.path.append(project_root)

    from extractors.base_extractor import BaseExtractor


class SapoExtractor(BaseExtractor):
    """Extractor for Sapo POS data."""

    def __init__(self, bucket_name: str) -> None:
        super().__init__(bucket_name)

    def extract_orders(self, prefix: str = "sapo/") -> pd.DataFrame:
        """
        Extract Sapo order batches from GCS.
        """
        files = self.list_files(prefix)
        files = [
            file_path
            for file_path in files
            if "order" in os.path.basename(file_path).lower()
            and "transaction" not in os.path.basename(file_path).lower()
            and "location" not in os.path.basename(file_path).lower()
        ]

        if not files:
            self.logger.warning(
                "No Sapo order files matched the expected naming pattern under '%s'.",
                prefix,
            )

        all_orders: list[dict] = []
        for file_path in files:
            try:
                data = self.extract_json_file(file_path)
                if isinstance(data, dict):
                    if "orders" in data:
                        all_orders.extend(data["orders"])
                    else:
                        all_orders.append(data)
                elif isinstance(data, list):
                    all_orders.extend(data)
                else:
                    self.logger.warning("Unexpected format in file '%s'.", file_path)
            except Exception as e:
                self.logger.error("Error extracting '%s': %s", file_path, e)
                continue

        return pd.DataFrame(all_orders)

    def extract_locations(self, prefix: str = "shared/") -> pd.DataFrame:
        """
        Extract Sapo locations metadata from GCS.
        """
        file_path = f"{prefix}sapo_locations.json.gz"
        df = pd.DataFrame()

        try:
            data = self.extract_json_file(file_path)
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                if "locations" in data:
                    df = pd.DataFrame(data["locations"])
                else:
                    df = pd.DataFrame([data])
            else:
                self.logger.warning("Unexpected data format in '%s'.", file_path)
        except Exception as e:
            self.logger.error("Error extracting '%s': %s", file_path, e)

        return df
