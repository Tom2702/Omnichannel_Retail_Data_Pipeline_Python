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

class CustomersExtractor(BaseExtractor):
    """Extract for shared customer data."""

    def __init__(self, bucket_name: str) -> None:
        super().__init__(bucket_name)

    def extract_customers(self, prefix: str = "shared/customers/") -> pd.DataFrame:
        """
        Extract customers from batch files in GCS

        Returns: 
            pd.DataFrame: Customers data
        """

        # List all files
        files = self.list_files(prefix)

        # Filter only customer batch files
        files = [
            file_path
            for file_path in files
            if any(
                token in os.path.basename(file_path).lower()
                for token in ["customer_batch", "customers_batch", "customer", "customers"]
            )
        ]

        if not files:
            self.logger.warning(
                "No customer batch naming pattern matched under '%s'. Falling back to all JSON files.",
                prefix,
            )
            files = self.list_files(prefix)

        self.logger.info("Found %s customer batch files", len(files))

        all_customer = []

        # Iterate over each file
        for file_path in files:
            try: 
                data = self.extract_json_file(file_path)

                # Handle format
                if isinstance(data, list):
                    all_customer.extend(data)
                
                elif isinstance(data, dict):
                    if "customers" in data: 
                        all_customer.extend(data["customers"])
                    else: 
                        all_customer.append(data)

                else:    
                    self.logger.warning("Unexpected format in file '%s'.", file_path)
            
            except Exception as e: 
                self.logger.error("Error extracting '%s': %s", file_path, e)
                continue

        df =pd.DataFrame(all_customer)

        return df
