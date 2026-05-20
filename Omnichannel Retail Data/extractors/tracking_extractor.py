import pandas as pd
from extractors.base_extractor import BaseExtractor

class CartTrackingExtractor(BaseExtractor):
    """Extractor for Cart tracking data from GCS."""

    def __init__ (self, bucket_name: str) -> None:
        super().__init__(bucket_name)
    
    def extract_file(self, prefix: str = "cart_tracking/") -> pd.DataFrame:
        """
        Extract Cart tracking infomation from GCS.

        Returns:
            pd.DataFrame: Transactions data
        """

        file_path = f"{prefix}cart_events.json.gz"
    
        df = pd.DataFrame()

        try:   
            data = self.extract_json_file(file_path)

            # Handle format
            if isinstance(data, list):
                df = pd.DataFrame(data)

            elif isinstance(data, dict):
                if "events" in data:
                    df = pd.DataFrame(data["events"])
                else:
                    df = pd.DataFrame([data])

            else:
                self.logger.warning("Unexpected data format in '%s'.", file_path)

        except Exception as e: 
            self.logger.error("Error extracting '%s': %s", file_path, e)

        return df

    
