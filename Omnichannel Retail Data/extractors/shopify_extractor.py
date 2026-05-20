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


class ShopifyExtractor(BaseExtractor):
    """Extractor for Shopify data."""
    
    def __init__(self, bucket_name: str) -> None: 
        super().__init__(bucket_name)

    def extract_file(self, prefix: str = "shopify/") -> pd.DataFrame:
        """
        Extract Shopify orders from GCS and return as DataFrame.
        
        Args: 
            prefix(str): Folder path in GCS

        Returns:
            pd.DataFrame: Flattened orders data
        """

        # List all files
        files = self.list_files(prefix)

        # Filter only order batch files
        files = [
            f for f in files
            if "orders_batch" in f
        ]

        all_orders = []

         # Iterate over each file
        for file_path in files: 
            try: 
                data = self.extract_json_file(file_path)

                # Handle format
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
    
        df = pd.DataFrame(all_orders)

        return df

    def extract_staff(self, prefix: str = "shopify/") -> pd.DataFrame:
        """
        Extract Shopify staff data from a single gzipped JSON file.
        """
        file_path = f"{prefix}staff.json.gz"
        df = pd.DataFrame()

        try:
            data = self.extract_json_file(file_path)
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                if "staff" in data:
                    df = pd.DataFrame(data["staff"])
                else:
                    df = pd.DataFrame([data])
            else:
                self.logger.warning("Unexpected format in file '%s'.", file_path)

        except Exception as e:
            self.logger.error("Error extracting '%s': %s", file_path, e)

        return df
