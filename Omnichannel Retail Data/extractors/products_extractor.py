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

class ProductsExtractor(BaseExtractor):
    """Extractor for product data from GCS."""

    def __init__(self, bucket_name: str) -> None:
        super().__init__(bucket_name)
    
    def extract_file(self, prefix: str = "shared/") -> pd.DataFrame:
        """
        Extract product infomation from GCS.

        Returns:
            pd.DataFrame: Transactions data
        """

        file_path = f"{prefix}products.json.gz"
    
        df = pd.DataFrame()

        try:   
            data = self.extract_json_file(file_path)

            # Handle format
            if isinstance(data, list):
                df = pd.DataFrame(data)

            elif isinstance(data, dict):
                if "products" in data:
                    df = pd.DataFrame(data["products"])
                else:
                    df = pd.DataFrame([data])

            else:
                self.logger.warning("Unexpected data format in '%s'.", file_path)

        except Exception as e: 
            self.logger.error("Error extracting '%s': %s", file_path, e)

        return df
