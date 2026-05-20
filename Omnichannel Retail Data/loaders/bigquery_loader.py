from __future__ import annotations

import os
import sys
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from google.api_core.exceptions import BadRequest, GoogleAPICallError, NotFound
from google.cloud import bigquery

try:
    from utils.logger import setup_logger
except ModuleNotFoundError:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    if project_root not in sys.path:
        sys.path.append(project_root)

    from utils.logger import setup_logger


class BigQueryLoader:
    """
    Helper class for loading transformed data into BigQuery.

    Supports:
    - creating datasets if they do not exist
    - loading pandas DataFrames into tables
    - executing SQL queries
    """

    VALID_WRITE_DISPOSITIONS = {
        "WRITE_TRUNCATE",
        "WRITE_APPEND",
        "WRITE_EMPTY",
    }

    def __init__(self, project_id: str | None = None, location: str | None = None) -> None:
        load_dotenv()
        self._configure_credentials()

        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        self.location = location or os.getenv("BIGQUERY_LOCATION")
        self.logger = setup_logger(__name__)

        try:
            self.client = bigquery.Client(project=self.project_id)
            self.logger.info(
                "Initialized BigQuery client for project '%s'.",
                self.client.project,
            )
        except Exception as e:
            self.logger.error("Failed to initialize BigQuery client: %s", e)
            raise

    def create_dataset_if_not_exists(
        self, dataset_id: str, location: str | None = None
    ) -> bigquery.Dataset:
        """
        Create a dataset if it does not already exist.
        """
        dataset_ref = self._build_dataset_id(dataset_id)
        dataset_location = location or self.location

        try:
            dataset = self.client.get_dataset(dataset_ref)
            self.logger.info("Dataset '%s' already exists.", dataset.full_dataset_id)
            return dataset
        except NotFound:
            self.logger.info("Dataset '%s' not found. Creating it now.", dataset_ref)

        try:
            dataset = bigquery.Dataset(dataset_ref)
            if dataset_location:
                dataset.location = dataset_location

            created_dataset = self.client.create_dataset(dataset, exists_ok=True)
            self.logger.info(
                "Created dataset '%s' in location '%s'.",
                created_dataset.full_dataset_id,
                created_dataset.location,
            )
            return created_dataset

        except (BadRequest, GoogleAPICallError) as e:
            self.logger.error("Failed to create dataset '%s': %s", dataset_ref, e)
            raise

    def load_dataframe(
        self,
        df: pd.DataFrame,
        dataset_id: str,
        table_id: str,
        write_disposition: str = "WRITE_APPEND",
        schema: list[bigquery.SchemaField] | None = None,
        autodetect: bool = True,
        partition_by: str | None = None,
        cluster_by: list[str] | None = None,
    ) -> bigquery.LoadJob:
        """
        Load a pandas DataFrame into a BigQuery table.

        Optional table optimizations:
        - `partition_by`: date/datetime/timestamp column for daily partitioning
        - `cluster_by`: up to 4 columns used for clustering
        """
        normalized_write_disposition = self._validate_write_disposition(
            write_disposition
        )

        if df.empty:
            self.logger.warning(
                "DataFrame for table '%s.%s' is empty. Load job skipped.",
                dataset_id,
                table_id,
            )
            raise ValueError("Cannot load an empty DataFrame into BigQuery.")

        self.create_dataset_if_not_exists(dataset_id)
        table_ref = self._build_table_id(dataset_id, table_id)

        job_config = bigquery.LoadJobConfig(
            write_disposition=normalized_write_disposition,
            autodetect=autodetect,
        )

        if schema:
            job_config.schema = schema
            job_config.autodetect = False

        if partition_by:
            if partition_by not in df.columns:
                raise ValueError(
                    f"Partition column '{partition_by}' not found in DataFrame."
                )

            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_by,
            )

        if cluster_by:
            missing_cluster_columns = [column for column in cluster_by if column not in df.columns]
            if missing_cluster_columns:
                raise ValueError(
                    "Cluster columns not found in DataFrame: "
                    f"{missing_cluster_columns}"
                )

            job_config.clustering_fields = cluster_by

        try:
            self.logger.info(
                "Starting load job for table '%s' with write_disposition='%s'.",
                table_ref,
                normalized_write_disposition,
            )
            if partition_by:
                self.logger.info(
                    "Applying daily partitioning on column '%s'.",
                    partition_by,
                )
            if cluster_by:
                self.logger.info(
                    "Applying clustering on columns: %s",
                    cluster_by,
                )
            load_job = self.client.load_table_from_dataframe(
                df,
                table_ref,
                job_config=job_config,
                location=self.location,
            )
            load_job.result()

            destination_table = self.client.get_table(table_ref)
            self.logger.info(
                "Loaded %s rows into '%s'.",
                destination_table.num_rows,
                table_ref,
            )
            return load_job

        except (BadRequest, GoogleAPICallError) as e:
            self.logger.error("Failed to load DataFrame into '%s': %s", table_ref, e)
            raise

    def execute_query(
        self,
        query: str,
        query_parameters: list[bigquery.ScalarQueryParameter] | None = None,
        job_config: bigquery.QueryJobConfig | None = None,
        to_dataframe: bool = False,
    ) -> Any:
        """
        Execute a SQL query in BigQuery.

        Returns a DataFrame when `to_dataframe=True`, otherwise returns query rows.
        """
        final_job_config = job_config or bigquery.QueryJobConfig()
        if query_parameters:
            final_job_config.query_parameters = query_parameters

        try:
            self.logger.info("Executing BigQuery query.")
            query_job = self.client.query(
                query,
                job_config=final_job_config,
                location=self.location,
            )
            results = query_job.result()

            self.logger.info(
                "Query completed successfully. Bytes processed: %s",
                getattr(query_job, "total_bytes_processed", "unknown"),
            )

            if to_dataframe:
                return results.to_dataframe()

            return list(results)

        except (BadRequest, GoogleAPICallError) as e:
            self.logger.error("Failed to execute query: %s", e)
            raise

    def _build_dataset_id(self, dataset_id: str) -> str:
        """
        Return a fully-qualified dataset id.
        """
        if "." in dataset_id:
            return dataset_id
        return f"{self.client.project}.{dataset_id}"

    def _build_table_id(self, dataset_id: str, table_id: str) -> str:
        """
        Return a fully-qualified table id.
        """
        normalized_dataset_id = self._build_dataset_id(dataset_id)
        return f"{normalized_dataset_id}.{table_id}"

    def _validate_write_disposition(self, write_disposition: str) -> str:
        """
        Validate write disposition and normalize casing.
        """
        normalized = write_disposition.upper().strip()
        if normalized not in self.VALID_WRITE_DISPOSITIONS:
            raise ValueError(
                "Invalid write_disposition. Supported values are: "
                f"{sorted(self.VALID_WRITE_DISPOSITIONS)}"
            )
        return normalized

    def _configure_credentials(self) -> None:
        """
        Normalize relative credentials path from .env into an absolute path.
        """
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not credentials_path:
            return

        normalized_path = credentials_path.strip().strip('"').strip("'")
        if os.path.isabs(normalized_path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = normalized_path
            return

        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        absolute_credentials_path = os.path.abspath(
            os.path.join(project_root, normalized_path)
        )
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = absolute_credentials_path
