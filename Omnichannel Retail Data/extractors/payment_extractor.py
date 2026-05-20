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

class PaymentExtractor(BaseExtractor):
    """Extractor for payment transaction data. """

    def __init__(self, bucket_name: str) -> None:
        super().__init__(bucket_name)

    def payment_mercury_extract(self, prefix: str = "mercury/") -> pd.DataFrame:
        """
        Extract Mercury bank transaction from GCS.

        Returns:
            pd.DataFrame: Transactions data
        """

        file_path = f"{prefix}transactions.json.gz"
    
        df = pd.DataFrame()

        try:   
            data = self.extract_json_file(file_path)

            # Handle format
            if isinstance(data, list):
                df = pd.DataFrame(data)

            elif isinstance(data, dict):
                if "transactions" in data:
                    df = pd.DataFrame(data["transactions"])
                else:
                    df = pd.DataFrame([data])

            else:
                self.logger.warning("Unexpected data format in '%s'.", file_path)

        except Exception as e: 
            self.logger.error("Error extracting '%s': %s", file_path, e)

        return df
    
    def payment_momo_extract(self, prefix: str = "momo/") -> pd.DataFrame:
        """
        Extract Momo transaction from GCS.

        Returns:
            pd.DataFrame: Transactions data
        """

        file_path = f"{prefix}transactions.json.gz"
    
        df = pd.DataFrame()

        try:   
            data = self.extract_json_file(file_path)

            # Handle format
            if isinstance(data, list):
                df = pd.DataFrame(data)

            elif isinstance(data, dict):
                if "transactions" in data:
                    df = pd.DataFrame(data["transactions"])
                else:
                    df = pd.DataFrame([data])

            else:
                self.logger.warning("Unexpected data format in '%s'.", file_path)

        except Exception as e: 
            self.logger.error("Error extracting '%s': %s", file_path, e)

        return df
    
    def payment_odoo_extract(self, prefix: str = "odoo/") -> pd.DataFrame:
        """
        Extract Odoo transaction from GCS.

        Returns:
            pd.DataFrame: Transactions data
        """

        file_path = f"{prefix}transactions.json.gz"
    
        df = pd.DataFrame()

        try:   
            data = self.extract_json_file(file_path)

            # Handle format
            if isinstance(data, list):
                df = pd.DataFrame(data)

            elif isinstance(data, dict):
                if "transactions" in data:
                    df = pd.DataFrame(data["transactions"])
                else:
                    df = pd.DataFrame([data])

            else:
                self.logger.warning("Unexpected data format in '%s'.", file_path)

        except Exception as e: 
            self.logger.error("Error extracting '%s': %s", file_path, e)

        return df
    
    def payment_paypal_extract(self, prefix: str = "paypal/") -> pd.DataFrame:
        """
        Extract Paypal transaction from GCS.

        Returns:
            pd.DataFrame: Transactions data
        """

        file_path = f"{prefix}transactions.json.gz"
    
        df = pd.DataFrame()

        try:   
            data = self.extract_json_file(file_path)

            # Handle format
            if isinstance(data, list):
                df = pd.DataFrame(data)

            elif isinstance(data, dict):
                if "transactions" in data:
                    df = pd.DataFrame(data["transactions"])
                else:
                    df = pd.DataFrame([data])

            else:
                self.logger.warning("Unexpected data format in '%s'.", file_path)

        except Exception as e: 
            self.logger.error("Error extracting '%s': %s", file_path, e)

        return df
    
    def payment_sapo_extract(self, prefix: str = "sapo/") -> pd.DataFrame:
        """
        Extract Sapo transaction from GCS.

        Returns:
            pd.DataFrame: Transactions data
        """

        file_path = f"{prefix}transactions.json.gz"
    
        df = pd.DataFrame()

        try:   
            data = self.extract_json_file(file_path)

            # Handle format
            if isinstance(data, list):
                df = pd.DataFrame(data)

            elif isinstance(data, dict):
                if "transactions" in data:
                    df = pd.DataFrame(data["transactions"])
                else:
                    df = pd.DataFrame([data])

            else:
                self.logger.warning("Unexpected data format in '%s'.", file_path)

        except Exception as e: 
            self.logger.error("Error extracting '%s': %s", file_path, e)

        return df
    
    def payment_zalopay_extract(self, prefix: str = "zalopay/") -> pd.DataFrame:
        """
        Extract ZaloPay transaction from GCS.

        Returns:
            pd.DataFrame: Transactions data
        """

        file_path = f"{prefix}transactions.json.gz"
    
        df = pd.DataFrame()

        try:   
            data = self.extract_json_file(file_path)

            # Handle format
            if isinstance(data, list):
                df = pd.DataFrame(data)

            elif isinstance(data, dict):
                if "transactions" in data:
                    df = pd.DataFrame(data["transactions"])
                else:
                    df = pd.DataFrame([data])

            else:
                self.logger.warning("Unexpected data format in '%s'.", file_path)

        except Exception as e: 
            self.logger.error("Error extracting '%s': %s", file_path, e)

        return df
