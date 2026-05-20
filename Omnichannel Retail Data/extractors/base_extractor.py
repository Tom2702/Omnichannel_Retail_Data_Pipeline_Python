from __future__ import annotations

import gzip
import io
import json
import os
import sys

from dotenv import load_dotenv
from google.cloud import storage

try:
    from utils.logger import setup_logger
except ModuleNotFoundError:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    if project_root not in sys.path:
        sys.path.append(project_root)

    from utils.logger import setup_logger



class BaseExtractor:
    """Base class for extracting data from Google Cloud Storage (GCS)
    
    This class provides common funtionality for:
    - Connecting to a GCS bucket
    - Listing files in a bucket
    - Extracting and parsing .json.gz files

    Child extractor classes (e.g., ShopifyExtractor, SapoExtractor)
    should inherit from this class to reuse core extraction logic    
    """

    def __init__(self, bucket_name: str) -> None:

        """Initialize the BaseExtractor.
        
        Args:
            bucket_name (str): Name of the GCS bucket.

        Raises:
            ValueError: If the specified bucket does not exist.
        """
        # Load environment variables (for credentials, configs, etc.)
        load_dotenv()

        # Initialize logger
        self.logger = setup_logger(__name__)

        # Initialize GCS client
        self.client = storage.Client()

        # Get bucket reference
        self.bucket = self.client.bucket(bucket_name)

        # Validate bucket existence
        if not self.bucket.exists():
            raise ValueError(f"Bucket '{bucket_name}' does not exist.")
        
        self.logger.info(f'Successfully connected to GCS bucket: {bucket_name}')
    
    def list_files(self, prefix: str) -> list[str]:
        """List files in the GCS bucket.
        
        Args: 
            prefix: Filter files by prefix (e.g., 'shopify/').
        
        Returns:
            List[str]: List of file path in the bucket
        """
        blobs = self.bucket.list_blobs(prefix=prefix)

        # List comprehension
        file_list = [blob.name for blob in blobs if blob.name.endswith(".json.gz")]

        self.logger.info("Found %d files with prefix '%s'", len(file_list), prefix)

        return file_list

    def extract_json_file(self, blob_path: str):

        # Get file
        blob = self.bucket.blob(blob_path)

        with blob.open("rb") as blob_stream:
            with gzip.GzipFile(fileobj=blob_stream, mode="rb") as gzip_stream:
                with io.TextIOWrapper(gzip_stream, encoding="utf-8") as text_stream:
                    data = json.load(text_stream)
        
        self.logger.info("Extracted file: %s", blob_path)

        return data
      
