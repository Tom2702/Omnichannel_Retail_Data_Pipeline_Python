from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound

try:
    from extractors.customers_extractor import CustomersExtractor
    from extractors.online_orders_extractor import OnlineOrdersExtractor
    from extractors.payment_extractor import PaymentExtractor
    from extractors.products_extractor import ProductsExtractor
    from extractors.sapo_extractor import SapoExtractor
    from extractors.shopify_extractor import ShopifyExtractor
    from extractors.tracking_extractor import CartTrackingExtractor
    from loaders.bigquery_loader import BigQueryLoader
    from transformers.dimension_transformer import DimensionTransformer
    from transformers.fact_transformer import FactTransformer
    from utils.logger import setup_logger
except ModuleNotFoundError:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    if project_root not in sys.path:
        sys.path.append(project_root)

    from extractors.customers_extractor import CustomersExtractor
    from extractors.online_orders_extractor import OnlineOrdersExtractor
    from extractors.payment_extractor import PaymentExtractor
    from extractors.products_extractor import ProductsExtractor
    from extractors.sapo_extractor import SapoExtractor
    from extractors.shopify_extractor import ShopifyExtractor
    from extractors.tracking_extractor import CartTrackingExtractor
    from loaders.bigquery_loader import BigQueryLoader
    from transformers.dimension_transformer import DimensionTransformer
    from transformers.fact_transformer import FactTransformer
    from utils.logger import setup_logger


class PipelineOrchestrator:
    """Coordinate the full ELT pipeline from extraction to aggregate updates."""

    DIM_CUSTOMERS_COLUMNS = [
        "customer_id",
        "email",
        "full_name",
        "phone",
        "city",
        "country",
        "created_at",
        "customer_segment",
        "lifetime_value_vnd",
        "total_orders",
        "first_order_date",
        "last_order_date",
    ]
    DIM_PRODUCTS_COLUMNS = [
        "product_id",
        "product_name",
        "sku",
        "barcode",
        "category",
        "brand",
        "price_vnd",
        "price_usd",
        "stock_quantity",
        "is_active",
    ]
    DIM_LOCATIONS_COLUMNS = [
        "location_id",
        "location_code",
        "location_name",
        "location_type",
        "city",
        "address",
        "phone",
        "is_active",
    ]
    DIM_DATE_COLUMNS = [
        "date_key",
        "full_date",
        "year",
        "quarter",
        "month",
        "month_name",
        "week",
        "day_of_month",
        "day_of_week",
        "day_name",
        "is_weekend",
        "is_holiday",
        "fiscal_year",
        "fiscal_quarter",
    ]
    FACT_ORDERS_COLUMNS = [
        "order_key",
        "order_id",
        "transaction_id",
        "customer_id",
        "order_date_key",
        "order_date",
        "channel",
        "source",
        "shipping_city",
        "shipping_address",
        "status",
        "payment_status",
        "total_vnd",
        "total_usd",
    ]
    FACT_ORDER_ITEMS_COLUMNS = [
        "order_item_key",
        "order_key",
        "transaction_id",
        "product_id",
        "order_date_key",
        "quantity",
        "unit_price_vnd",
        "line_total_vnd",
    ]
    FACT_PAYMENTS_COLUMNS = [
        "payment_key",
        "transaction_id",
        "order_id",
        "customer_id",
        "payment_gateway",
        "payment_method",
        "amount_vnd",
        "payment_status",
        "payment_date_key",
        "payment_date",
    ]
    FACT_CART_EVENTS_COLUMNS = [
        "event_key",
        "event_id",
        "session_id",
        "customer_id",
        "event_type",
        "event_date_key",
        "event_timestamp",
        "product_id",
        "source",
        "device",
        "browser",
        "utm_source",
        "utm_campaign",
    ]
    FACT_BANK_TRANSACTIONS_COLUMNS = [
        "transaction_key",
        "transaction_id",
        "account_id",
        "transaction_type",
        "amount_vnd",
        "status",
        "transaction_date_key",
        "transaction_date",
    ]

    def __init__(
        self,
        bucket_name: str | None = None,
        dataset_id: str | None = None,
        project_id: str | None = None,
        location: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        holidays: list[str] | None = None,
        write_disposition: str = "WRITE_TRUNCATE",
    ) -> None:
        load_dotenv()

        self.logger = setup_logger(__name__)
        self.bucket_name = (
            bucket_name
            or os.getenv("GCS_BUCKET_NAME")
            or os.getenv("BUCKET_NAME")
            or "minpy"
        )
        self.dataset_id = (
            dataset_id
            or os.getenv("BIGQUERY_DATASET")
            or os.getenv("BIGQUERY_DATASET_ID")
            or "end_to_end_project"
        )
        self.start_date = start_date or os.getenv("PIPELINE_START_DATE")
        self.end_date = end_date or os.getenv("PIPELINE_END_DATE")
        self.holidays = holidays or self._parse_holidays(os.getenv("PIPELINE_HOLIDAYS"))
        self.write_disposition = write_disposition

        self.loader = BigQueryLoader(project_id=project_id, location=location)
        self.dimension_transformer = DimensionTransformer()
        self.fact_transformer = FactTransformer()

        self.customers_extractor = CustomersExtractor(self.bucket_name)
        self.products_extractor = ProductsExtractor(self.bucket_name)
        self.sapo_extractor = SapoExtractor(self.bucket_name)
        self.shopify_extractor = ShopifyExtractor(self.bucket_name)
        self.online_orders_extractor = OnlineOrdersExtractor(self.bucket_name)
        self.payment_extractor = PaymentExtractor(self.bucket_name)
        self.cart_tracking_extractor = CartTrackingExtractor(self.bucket_name)

    def run(self) -> dict[str, int]:
        """Run the full pipeline end-to-end."""
        self.logger.info(
            "Starting pipeline orchestration for bucket '%s' and dataset '%s'.",
            self.bucket_name,
            self.dataset_id,
        )

        summary: dict[str, int] = {}

        try:
            raw_sources = self._run_step("extract source data", self._extract_sources)
            fact_tables = self._run_step(
                "transform fact tables",
                lambda: self._transform_facts(raw_sources),
            )
            dimension_tables = self._run_step(
                "transform dimension tables",
                lambda: self._transform_dimensions(raw_sources, fact_tables),
            )

            dimension_tables = self._run_step(
                "run data quality checks for dimensions",
                lambda: self._run_data_quality_checks(dimension_tables),
            )
            fact_tables = self._run_step(
                "run data quality checks for facts",
                lambda: self._run_data_quality_checks(fact_tables),
            )

            self._run_step(
                "load dimension tables",
                lambda: self._load_dimensions(dimension_tables, summary),
            )
            self._run_step(
                "load fact tables",
                lambda: self._load_facts(fact_tables, summary),
            )

            if dimension_tables["dim_customers"].empty or fact_tables["fact_orders"].empty:
                self.logger.warning(
                    "Skipping aggregate update because dim_customers or fact_orders is empty."
                )
                summary["dim_customers_aggregate_updates"] = 0
            else:
                summary["dim_customers_aggregate_updates"] = self._run_step(
                    "update customer aggregates",
                    self.update_customer_aggregates,
                )

            summary["analysis_views"] = self._run_step(
                "create analysis views",
                self.create_analysis_views,
            )

            self.logger.info("Pipeline completed successfully. Summary: %s", summary)
            return summary

        except Exception:
            self.logger.exception("Pipeline orchestration failed.")
            raise

    def update_customer_aggregates(self) -> int:
        """Update customer lifetime metrics after facts have been loaded."""
        dim_customers_table = self._table_ref("dim_customers")
        fact_orders_table = self._table_ref("fact_orders")

        sql = f"""
        MERGE `{dim_customers_table}` AS target
        USING (
            SELECT
                customer_id,
                SUM(COALESCE(total_vnd, 0)) AS lifetime_value_vnd,
                COUNT(DISTINCT order_key) AS total_orders,
                MIN(order_date) AS first_order_date,
                MAX(order_date) AS last_order_date
            FROM `{fact_orders_table}`
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id
        ) AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN
        UPDATE SET
            target.lifetime_value_vnd = source.lifetime_value_vnd,
            target.total_orders = source.total_orders,
            target.first_order_date = source.first_order_date,
            target.last_order_date = source.last_order_date,
            target.customer_segment = CASE
                WHEN source.total_orders >= 10 OR source.lifetime_value_vnd >= 10000000 THEN 'vip'
                WHEN source.total_orders >= 5 OR source.lifetime_value_vnd >= 5000000 THEN 'loyal'
                WHEN source.total_orders >= 1 THEN 'active'
                ELSE COALESCE(target.customer_segment, 'unknown')
            END
        """

        query_job = self.loader.client.query(sql, location=self.loader.location)
        query_job.result()

        affected_rows = int(query_job.num_dml_affected_rows or 0)
        self.logger.info(
            "Updated customer aggregates in '%s'. Rows affected: %s",
            dim_customers_table,
            affected_rows,
        )
        return affected_rows

    def _extract_sources(self) -> dict[str, Any]:
        raw_sources: dict[str, Any] = {
            "customers": self.customers_extractor.extract_customers(),
            "products": self.products_extractor.extract_file(),
            "sapo_locations": self.sapo_extractor.extract_locations(),
            "shopify_orders": self.shopify_extractor.extract_file(),
            "sapo_orders": self.sapo_extractor.extract_orders(),
            "online_orders": self.online_orders_extractor.extract_orders(),
            "cart_events": self.cart_tracking_extractor.extract_file(),
            "bank_transactions": self.payment_extractor.payment_mercury_extract(),
            "payments_by_gateway": self._extract_payment_sources(),
        }

        for source_name, dataframe in raw_sources.items():
            if isinstance(dataframe, dict):
                for nested_name, nested_df in dataframe.items():
                    self._log_row_count(f"{source_name}.{nested_name}", nested_df)
            else:
                self._log_row_count(source_name, dataframe)

        return raw_sources

    def _extract_payment_sources(self) -> dict[str, pd.DataFrame]:
        payment_extractors: dict[str, Callable[[], pd.DataFrame]] = {
            "momo": self.payment_extractor.payment_momo_extract,
            "odoo": self.payment_extractor.payment_odoo_extract,
            "paypal": self.payment_extractor.payment_paypal_extract,
            "sapo": self.payment_extractor.payment_sapo_extract,
            "zalopay": self.payment_extractor.payment_zalopay_extract,
        }

        payment_sources: dict[str, pd.DataFrame] = {}
        for gateway, extractor in payment_extractors.items():
            payment_sources[gateway] = extractor()
        return payment_sources

    def _transform_dimensions(
        self,
        raw_sources: dict[str, Any],
        fact_tables: dict[str, pd.DataFrame],
    ) -> dict[str, pd.DataFrame]:
        dim_customers = self._safe_transform(
            "dim_customers",
            raw_sources["customers"],
            self.dimension_transformer.transform_customers,
            self.DIM_CUSTOMERS_COLUMNS,
        )
        dim_products = self._safe_transform(
            "dim_products",
            raw_sources["products"],
            self.dimension_transformer.transform_products,
            self.DIM_PRODUCTS_COLUMNS,
        )
        dim_locations = self._safe_transform(
            "dim_locations",
            self._combine_frames(
                [
                    raw_sources["sapo_locations"],
                    self._extract_locations_from_orders(raw_sources["online_orders"]),
                ],
                self.DIM_LOCATIONS_COLUMNS,
                transformer=self.dimension_transformer,
                preserve_columns=True,
            ),
            self.dimension_transformer.transform_locations,
            self.DIM_LOCATIONS_COLUMNS,
        )
        dim_date = self._build_date_dimension(fact_tables)

        return {
            "dim_customers": dim_customers,
            "dim_products": dim_products,
            "dim_locations": dim_locations,
            "dim_date": dim_date,
        }

    def _transform_facts(
        self,
        raw_sources: dict[str, Any],
    ) -> dict[str, pd.DataFrame]:
        fact_orders_shopify = self._safe_transform(
            "fact_orders_shopify",
            raw_sources["shopify_orders"],
            lambda df: self.fact_transformer.transform_orders(
                df,
                channel="shopify",
                source="shopify",
            ),
            self.FACT_ORDERS_COLUMNS,
        )
        fact_orders_sapo = self._safe_transform(
            "fact_orders_sapo",
            raw_sources["sapo_orders"],
            lambda df: self.fact_transformer.transform_orders(
                df,
                channel="sapo_pos",
                source="sapo",
            ),
            self.FACT_ORDERS_COLUMNS,
        )
        fact_orders_online = self._safe_transform(
            "fact_orders_online",
            raw_sources["online_orders"],
            lambda df: self.fact_transformer.transform_orders(
                df,
                channel="online",
                source="online_orders",
            ),
            self.FACT_ORDERS_COLUMNS,
        )
        fact_orders = self._combine_frames(
            [fact_orders_shopify, fact_orders_sapo, fact_orders_online],
            self.FACT_ORDERS_COLUMNS,
            dedupe_subset=["order_key"],
        )

        fact_order_items_shopify = self._safe_transform(
            "fact_order_items_shopify",
            raw_sources["shopify_orders"],
            self.fact_transformer.transform_order_items,
            self.FACT_ORDER_ITEMS_COLUMNS,
        )
        fact_order_items_sapo = self._safe_transform(
            "fact_order_items_sapo",
            raw_sources["sapo_orders"],
            self.fact_transformer.transform_order_items,
            self.FACT_ORDER_ITEMS_COLUMNS,
        )
        fact_order_items_online = self._safe_transform(
            "fact_order_items_online",
            raw_sources["online_orders"],
            self.fact_transformer.transform_order_items,
            self.FACT_ORDER_ITEMS_COLUMNS,
        )
        fact_order_items = self._combine_frames(
            [fact_order_items_shopify, fact_order_items_sapo, fact_order_items_online],
            self.FACT_ORDER_ITEMS_COLUMNS,
            dedupe_subset=["order_item_key"],
        )

        payment_frames = [
            self._safe_transform(
                f"fact_payments_{gateway}",
                payment_df,
                lambda df, gateway_name=gateway: self.fact_transformer.transform_payments(
                    df,
                    payment_gateway=gateway_name,
                ),
                self.FACT_PAYMENTS_COLUMNS,
            )
            for gateway, payment_df in raw_sources["payments_by_gateway"].items()
        ]
        fact_payments = self._combine_frames(
            payment_frames,
            self.FACT_PAYMENTS_COLUMNS,
            dedupe_subset=["payment_key"],
        )

        fact_cart_events = self._safe_transform(
            "fact_cart_events",
            raw_sources["cart_events"],
            self.fact_transformer.transform_cart_events,
            self.FACT_CART_EVENTS_COLUMNS,
        )
        fact_bank_transactions = self._safe_transform(
            "fact_bank_transactions",
            raw_sources["bank_transactions"],
            self.fact_transformer.transform_bank_transactions,
            self.FACT_BANK_TRANSACTIONS_COLUMNS,
        )

        return {
            "fact_orders": fact_orders,
            "fact_order_items": fact_order_items,
            "fact_payments": fact_payments,
            "fact_cart_events": fact_cart_events,
            "fact_bank_transactions": fact_bank_transactions,
        }

    def _run_data_quality_checks(
        self,
        tables: dict[str, pd.DataFrame],
    ) -> dict[str, pd.DataFrame]:
        validated_tables: dict[str, pd.DataFrame] = {}

        for table_name, df in tables.items():
            if df.empty:
                self.logger.warning(
                    "Skipping data quality checks for '%s' because it is empty.",
                    table_name,
                )
                validated_tables[table_name] = df
                continue

            rules = self._data_quality_rules(table_name)
            transformer = (
                self.fact_transformer
                if table_name.startswith("fact_")
                else self.dimension_transformer
            )

            cleaned_df, report = transformer.data_quality_check(
                df,
                table_name=table_name,
                **rules,
            )
            self.logger.info(
                "Data quality summary for '%s': %s",
                table_name,
                report,
            )
            validated_tables[table_name] = cleaned_df

        return validated_tables

    def _load_dimensions(
        self,
        dimension_tables: dict[str, pd.DataFrame],
        summary: dict[str, int],
    ) -> None:
        self._load_table(
            "dim_customers",
            dimension_tables["dim_customers"],
            summary,
            partition_by="created_at",
        )
        self._load_table("dim_products", dimension_tables["dim_products"], summary)
        self._load_table("dim_locations", dimension_tables["dim_locations"], summary)
        self._load_table("dim_date", dimension_tables["dim_date"], summary)

    def _load_facts(
        self,
        fact_tables: dict[str, pd.DataFrame],
        summary: dict[str, int],
    ) -> None:
        self._load_table(
            "fact_orders",
            fact_tables["fact_orders"],
            summary,
            partition_by="order_date_key",
            cluster_by=["customer_id", "channel"],
        )
        self._load_table(
            "fact_order_items",
            fact_tables["fact_order_items"],
            summary,
            partition_by="order_date_key",
            cluster_by=["product_id"],
        )
        self._load_table(
            "fact_payments",
            fact_tables["fact_payments"],
            summary,
            partition_by="payment_date_key",
            cluster_by=["customer_id", "payment_gateway"],
        )
        self._load_table(
            "fact_cart_events",
            fact_tables["fact_cart_events"],
            summary,
            partition_by="event_date_key",
            cluster_by=["customer_id", "session_id", "event_type"],
        )
        self._load_table(
            "fact_bank_transactions",
            fact_tables["fact_bank_transactions"],
            summary,
            partition_by="transaction_date_key",
        )

    def _load_table(
        self,
        table_name: str,
        df: pd.DataFrame,
        summary: dict[str, int],
        partition_by: str | None = None,
        cluster_by: list[str] | None = None,
    ) -> None:
        if df.empty:
            self.logger.warning("Skipping load for '%s' because it is empty.", table_name)
            summary[table_name] = 0
            return

        self.loader.load_dataframe(
            df,
            self.dataset_id,
            table_name,
            write_disposition=self.write_disposition,
            partition_by=partition_by,
            cluster_by=cluster_by,
        )
        summary[table_name] = int(len(df))

    def _build_date_dimension(
        self,
        fact_tables: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        candidate_ranges: list[tuple[pd.Timestamp, pd.Timestamp]] = []
        date_candidates = [
            ("fact_orders", "order_date"),
            ("fact_payments", "payment_date"),
            ("fact_cart_events", "event_timestamp"),
            ("fact_bank_transactions", "transaction_date"),
        ]

        for table_name, column_name in date_candidates:
            df = fact_tables[table_name]
            if df.empty or column_name not in df.columns:
                continue

            valid_dates = (
                pd.to_datetime(df[column_name], errors="coerce", utc=True)
                .dropna()
                .dt.tz_convert(None)
            )
            if valid_dates.empty:
                continue

            candidate_ranges.append((valid_dates.min(), valid_dates.max()))

        today = pd.Timestamp.today().normalize()
        if candidate_ranges:
            derived_start = min(value[0] for value in candidate_ranges)
            derived_end = max(value[1] for value in candidate_ranges)
        else:
            warehouse_range = self._warehouse_fact_date_range()
            if warehouse_range is not None:
                derived_start, derived_end = warehouse_range
                self.logger.info(
                    "Derived dim_date range from warehouse facts: %s to %s.",
                    derived_start.date(),
                    derived_end.date(),
                )
            else:
                derived_start = pd.Timestamp(today.year, 1, 1)
                derived_end = pd.Timestamp(today.year, 12, 31)
                self.logger.warning(
                    "Could not derive a date range from extracted facts or warehouse facts. "
                    "Falling back to the current calendar year: %s to %s.",
                    derived_start.date(),
                    derived_end.date(),
                )

        final_start = self._normalize_timestamp(self.start_date) if self.start_date else derived_start
        final_end = self._normalize_timestamp(self.end_date) if self.end_date else derived_end

        if final_start > final_end:
            raise ValueError("Pipeline start_date must be earlier than or equal to end_date.")

        dim_date = self.dimension_transformer.transform_date(
            final_start.strftime("%Y-%m-%d"),
            final_end.strftime("%Y-%m-%d"),
            holidays=self.holidays,
        )
        self._log_row_count("dim_date", dim_date)
        return dim_date[self.DIM_DATE_COLUMNS]

    def _warehouse_fact_date_range(self) -> tuple[pd.Timestamp, pd.Timestamp] | None:
        date_sources = [
            ("fact_orders", "order_date"),
            ("fact_payments", "payment_date"),
            ("fact_cart_events", "event_timestamp"),
            ("fact_bank_transactions", "transaction_date"),
        ]
        available_sources = [
            (table_name, column_name)
            for table_name, column_name in date_sources
            if self._table_exists(table_name)
        ]
        if not available_sources:
            return None

        union_queries = [
            f"""
            SELECT
                MIN(DATE({column_name})) AS min_date,
                MAX(DATE({column_name})) AS max_date
            FROM `{self._table_ref(table_name)}`
            """
            for table_name, column_name in available_sources
        ]
        sql = f"""
        WITH ranges AS (
            {" UNION ALL ".join(union_queries)}
        )
        SELECT
            MIN(min_date) AS overall_min_date,
            MAX(max_date) AS overall_max_date
        FROM ranges
        WHERE min_date IS NOT NULL AND max_date IS NOT NULL
        """

        rows = self.loader.execute_query(sql)
        if not rows:
            return None

        result = rows[0]
        min_date = result["overall_min_date"]
        max_date = result["overall_max_date"]
        if min_date is None or max_date is None:
            return None

        return pd.Timestamp(min_date), pd.Timestamp(max_date)

    def create_analysis_views(self) -> int:
        """
        Create or replace analysis views required by the project spec.
        """
        view_specs = [
            (
                "vw_customer_journey",
                self._build_customer_journey_view_sql(),
                ["fact_cart_events", "fact_orders"],
            ),
            (
                "vw_customer_journey_sankey",
                self._build_customer_journey_sankey_view_sql(),
                ["fact_cart_events", "fact_orders"],
            ),
            (
                "vw_cashflow_daily",
                self._build_cashflow_daily_view_sql(),
                ["fact_orders", "fact_payments", "fact_bank_transactions"],
            ),
            (
                "vw_payment_status",
                self._build_payment_status_view_sql(),
                ["fact_orders", "fact_payments"],
            ),
        ]
        created_views = 0
        for view_name, query, required_tables in view_specs:
            missing_tables = [
                table_name
                for table_name in required_tables
                if not self._table_exists(table_name)
            ]
            if missing_tables:
                self.logger.warning(
                    "Skipping view '%s' because required tables are missing: %s",
                    view_name,
                    missing_tables,
                )
                continue

            self.loader.execute_query(query)
            created_views += 1

        return created_views

    def _safe_transform(
        self,
        table_name: str,
        df: pd.DataFrame,
        transform_fn: Callable[[pd.DataFrame], pd.DataFrame],
        expected_columns: list[str],
    ) -> pd.DataFrame:
        if df.empty:
            self.logger.warning(
                "Source DataFrame for '%s' is empty. Returning an empty table shape.",
                table_name,
            )
            return pd.DataFrame(columns=expected_columns)

        transformed_df = transform_fn(df)
        transformed_df = transformed_df.reindex(columns=expected_columns)
        self._log_row_count(table_name, transformed_df)
        return transformed_df

    def _combine_frames(
        self,
        frames: list[pd.DataFrame],
        expected_columns: list[str],
        dedupe_subset: list[str] | None = None,
        transformer: Any | None = None,
        preserve_columns: bool = False,
    ) -> pd.DataFrame:
        non_empty_frames = [frame for frame in frames if not frame.empty]
        if not non_empty_frames:
            return pd.DataFrame(columns=expected_columns)

        combined_df = pd.concat(non_empty_frames, ignore_index=True)
        if dedupe_subset:
            missing_subset = [
                column for column in dedupe_subset if column not in combined_df.columns
            ]
            if missing_subset:
                self.logger.warning(
                    "Skipping duplicate check because columns are missing: %s",
                    missing_subset,
                )
            else:
                active_transformer = transformer or self.fact_transformer
                combined_df, _ = active_transformer.check_duplicates(
                    combined_df,
                    subset=dedupe_subset,
                    remove_duplicates=True,
                )

        if preserve_columns:
            return combined_df

        return combined_df.reindex(columns=expected_columns)

    def _data_quality_rules(self, table_name: str) -> dict[str, Any]:
        rules: dict[str, Any] = {
            "critical_columns": None,
            "duplicate_subset": None,
            "remove_duplicates": True,
            "duplicate_flag_column": None,
            "date_columns": None,
            "amount_columns": None,
            "allow_negative_amounts": False,
            "min_date": self.start_date,
            "max_date": self.end_date,
            "allow_future_dates": False,
        }

        if table_name == "dim_customers":
            rules["critical_columns"] = ["customer_id", "email"]
            rules["duplicate_subset"] = ["customer_id"]
        elif table_name == "dim_products":
            rules["critical_columns"] = ["product_id", "product_name"]
            rules["duplicate_subset"] = ["product_id"]
        elif table_name == "dim_locations":
            rules["critical_columns"] = ["location_id", "location_name"]
            rules["duplicate_subset"] = ["location_id"]
        elif table_name == "dim_date":
            rules["critical_columns"] = ["date_key"]
            rules["duplicate_subset"] = ["date_key"]
        elif table_name == "fact_orders":
            rules["critical_columns"] = ["order_key", "order_id"]
            rules["duplicate_subset"] = ["order_key"]
            rules["date_columns"] = ["order_date"]
            rules["amount_columns"] = ["total_vnd", "total_usd"]
        elif table_name == "fact_order_items":
            rules["critical_columns"] = ["order_item_key", "order_key"]
            rules["duplicate_subset"] = ["order_item_key"]
            rules["amount_columns"] = ["unit_price_vnd", "line_total_vnd"]
        elif table_name == "fact_payments":
            rules["critical_columns"] = ["payment_key", "transaction_id"]
            rules["duplicate_subset"] = ["payment_key"]
            rules["date_columns"] = ["payment_date"]
            rules["amount_columns"] = ["amount_vnd"]
        elif table_name == "fact_cart_events":
            rules["critical_columns"] = ["event_key", "event_id"]
            rules["duplicate_subset"] = ["event_key"]
            rules["date_columns"] = ["event_timestamp"]
        elif table_name == "fact_bank_transactions":
            rules["critical_columns"] = ["transaction_key", "transaction_id"]
            rules["duplicate_subset"] = ["transaction_key"]
            rules["date_columns"] = ["transaction_date"]
            rules["amount_columns"] = ["amount_vnd"]
            rules["allow_negative_amounts"] = True

        return rules

    def _run_step(self, step_name: str, operation: Callable[[], Any]) -> Any:
        self.logger.info("Starting step: %s", step_name)
        start_time = time.perf_counter()

        try:
            result = operation()
        except Exception:
            self.logger.exception("Step failed: %s", step_name)
            raise

        duration_seconds = time.perf_counter() - start_time
        self.logger.info("Completed step: %s (%.2fs)", step_name, duration_seconds)
        return result

    def _log_row_count(self, name: str, df: pd.DataFrame) -> None:
        self.logger.info(
            "Prepared '%s' with %s rows and %s columns.",
            name,
            len(df),
            len(df.columns),
        )

    def _table_ref(self, table_name: str) -> str:
        return self.loader._build_table_id(self.dataset_id, table_name)

    def _table_exists(self, table_name: str) -> bool:
        try:
            self.loader.client.get_table(self._table_ref(table_name))
            return True
        except NotFound:
            return False

    @staticmethod
    def _normalize_timestamp(value: str | pd.Timestamp) -> pd.Timestamp:
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is not None:
            return timestamp.tz_convert("UTC").tz_localize(None)
        return timestamp

    def _extract_locations_from_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=self.DIM_LOCATIONS_COLUMNS)

        standardized = self.fact_transformer.standardize_column_names(df)
        source_columns = [
            "location_id",
            "location_code",
            "location_name",
            "location_city",
            "location_address",
            "location_phone",
        ]
        if not any(column in standardized.columns for column in source_columns):
            return pd.DataFrame(columns=self.DIM_LOCATIONS_COLUMNS)

        location_df = pd.DataFrame(index=standardized.index)
        location_df["location_id"] = (
            standardized["location_id"]
            if "location_id" in standardized.columns
            else pd.NA
        )
        location_df["location_code"] = (
            standardized["location_code"]
            if "location_code" in standardized.columns
            else pd.NA
        )
        location_df["location_name"] = (
            standardized["location_name"]
            if "location_name" in standardized.columns
            else pd.NA
        )
        location_df["location_type"] = "store"
        location_df["city"] = (
            standardized["location_city"]
            if "location_city" in standardized.columns
            else pd.NA
        )
        location_df["address"] = (
            standardized["location_address"]
            if "location_address" in standardized.columns
            else pd.NA
        )
        location_df["phone"] = (
            standardized["location_phone"]
            if "location_phone" in standardized.columns
            else pd.NA
        )
        location_df["is_active"] = True

        return location_df.dropna(
            subset=["location_id", "location_code", "location_name", "city", "address", "phone"],
            how="all",
        ).reset_index(drop=True)

    def _build_customer_journey_view_sql(self) -> str:
        dataset = self.dataset_id
        project = self.loader.client.project
        return f"""
        CREATE OR REPLACE VIEW `{project}.{dataset}.vw_customer_journey` AS
        WITH touchpoints AS (
            SELECT
                customer_id,
                event_timestamp AS touchpoint_time,
                source AS touchpoint_source,
                event_type
            FROM `{project}.{dataset}.fact_cart_events`
            WHERE customer_id IS NOT NULL
        ),
        purchase_summary AS (
            SELECT
                customer_id,
                MIN(order_date) AS first_purchase_time,
                MAX(order_date) AS last_purchase_time,
                COUNT(DISTINCT order_key) AS total_orders,
                SUM(COALESCE(total_vnd, 0)) AS total_revenue_vnd
            FROM `{project}.{dataset}.fact_orders`
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id
        )
        SELECT
            t.customer_id,
            STRING_AGG(
                CONCAT(COALESCE(t.touchpoint_source, 'unknown'), ':', COALESCE(t.event_type, 'unknown')),
                ' > '
                ORDER BY t.touchpoint_time
            ) AS touchpoint_sequence,
            MIN(t.touchpoint_time) AS first_touch_time,
            MAX(t.touchpoint_time) AS last_touch_time,
            p.first_purchase_time,
            p.last_purchase_time,
            p.total_orders,
            p.total_revenue_vnd
        FROM touchpoints AS t
        LEFT JOIN purchase_summary AS p
            ON t.customer_id = p.customer_id
        GROUP BY
            t.customer_id,
            p.first_purchase_time,
            p.last_purchase_time,
            p.total_orders,
            p.total_revenue_vnd
        """

    def _build_customer_journey_sankey_view_sql(self) -> str:
        dataset = self.dataset_id
        project = self.loader.client.project
        return f"""
        CREATE OR REPLACE VIEW `{project}.{dataset}.vw_customer_journey_sankey` AS
        WITH ordered_touchpoints AS (
            SELECT
                customer_id,
                event_timestamp AS touchpoint_time,
                COALESCE(source, 'unknown') AS touchpoint_channel,
                COALESCE(event_type, 'unknown') AS event_type,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_id
                    ORDER BY event_timestamp, COALESCE(source, 'unknown'), COALESCE(event_type, 'unknown')
                ) AS touchpoint_number,
                LEAD(COALESCE(source, 'unknown')) OVER (
                    PARTITION BY customer_id
                    ORDER BY event_timestamp, COALESCE(source, 'unknown'), COALESCE(event_type, 'unknown')
                ) AS next_touchpoint_channel,
                COUNT(*) OVER (PARTITION BY customer_id) AS total_touchpoints
            FROM `{project}.{dataset}.fact_cart_events`
            WHERE customer_id IS NOT NULL
        ),
        purchase_summary AS (
            SELECT
                customer_id,
                MIN(order_date) AS first_purchase_time
            FROM `{project}.{dataset}.fact_orders`
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id
        ),
        customer_edges AS (
            SELECT
                t.customer_id,
                t.source_stage_number,
                CONCAT('Touch ', LPAD(CAST(t.source_stage_number AS STRING), 2, '0')) AS source_stage_label,
                t.target_stage_number,
                CONCAT('Touch ', LPAD(CAST(t.target_stage_number AS STRING), 2, '0')) AS target_stage_label,
                CONCAT('Touch ', LPAD(CAST(t.source_stage_number AS STRING), 2, '0'), ' - ', t.source_channel) AS source_node,
                CONCAT('Touch ', LPAD(CAST(t.target_stage_number AS STRING), 2, '0'), ' - ', t.target_channel) AS target_node,
                t.source_channel,
                t.target_channel,
                t.edge_type
            FROM (
                SELECT
                    customer_id,
                    touchpoint_number AS source_stage_number,
                    touchpoint_number + 1 AS target_stage_number,
                    touchpoint_channel AS source_channel,
                    next_touchpoint_channel AS target_channel,
                    'touchpoint' AS edge_type
                FROM ordered_touchpoints
                WHERE next_touchpoint_channel IS NOT NULL

                UNION ALL

                SELECT
                    t.customer_id,
                    t.total_touchpoints AS source_stage_number,
                    t.total_touchpoints + 1 AS target_stage_number,
                    t.touchpoint_channel AS source_channel,
                    'purchase' AS target_channel,
                    'purchase' AS edge_type
                FROM ordered_touchpoints AS t
                INNER JOIN purchase_summary AS p
                    ON t.customer_id = p.customer_id
                WHERE t.touchpoint_number = t.total_touchpoints
                    AND p.first_purchase_time IS NOT NULL
                    AND p.first_purchase_time >= t.touchpoint_time

                UNION ALL

                SELECT
                    t.customer_id,
                    t.total_touchpoints AS source_stage_number,
                    t.total_touchpoints + 1 AS target_stage_number,
                    t.touchpoint_channel AS source_channel,
                    'drop_off' AS target_channel,
                    'drop_off' AS edge_type
                FROM ordered_touchpoints AS t
                LEFT JOIN purchase_summary AS p
                    ON t.customer_id = p.customer_id
                WHERE t.touchpoint_number = t.total_touchpoints
                    AND (
                        p.first_purchase_time IS NULL
                        OR p.first_purchase_time < t.touchpoint_time
                    )
            ) AS t
        ),
        source_stage_totals AS (
            SELECT
                source_stage_number,
                source_node,
                COUNT(DISTINCT customer_id) AS source_stage_customer_count
            FROM customer_edges
            GROUP BY source_stage_number, source_node
        ),
        total_touch_customers AS (
            SELECT COUNT(DISTINCT customer_id) AS total_customers
            FROM ordered_touchpoints
        )
        SELECT
            e.source_stage_number,
            e.source_stage_label,
            e.target_stage_number,
            e.target_stage_label,
            e.source_node,
            e.target_node,
            e.source_channel,
            e.target_channel,
            e.edge_type,
            COUNT(DISTINCT e.customer_id) AS customer_count,
            s.source_stage_customer_count,
            t.total_customers,
            SAFE_DIVIDE(COUNT(DISTINCT e.customer_id), s.source_stage_customer_count) AS pct_of_source_stage,
            SAFE_DIVIDE(COUNT(DISTINCT e.customer_id), t.total_customers) AS pct_of_total_customers
        FROM customer_edges AS e
        INNER JOIN source_stage_totals AS s
            ON e.source_stage_number = s.source_stage_number
            AND e.source_node = s.source_node
        CROSS JOIN total_touch_customers AS t
        GROUP BY
            e.source_stage_number,
            e.source_stage_label,
            e.target_stage_number,
            e.target_stage_label,
            e.source_node,
            e.target_node,
            e.source_channel,
            e.target_channel,
            e.edge_type,
            s.source_stage_customer_count,
            t.total_customers
        """

    def _build_cashflow_daily_view_sql(self) -> str:
        dataset = self.dataset_id
        project = self.loader.client.project
        return f"""
        CREATE OR REPLACE VIEW `{project}.{dataset}.vw_cashflow_daily` AS
        WITH order_revenue AS (
            SELECT
                order_date_key AS activity_date,
                SUM(COALESCE(total_vnd, 0)) AS sales_revenue_vnd
            FROM `{project}.{dataset}.fact_orders`
            GROUP BY order_date_key
        ),
        payment_received AS (
            SELECT
                payment_date_key AS activity_date,
                SUM(COALESCE(amount_vnd, 0)) AS payment_received_vnd
            FROM `{project}.{dataset}.fact_payments`
            GROUP BY payment_date_key
        ),
        bank_cashflow AS (
            SELECT
                transaction_date_key AS activity_date,
                SUM(CASE WHEN COALESCE(amount_vnd, 0) > 0 THEN amount_vnd ELSE 0 END) AS bank_inflow_vnd,
                SUM(CASE WHEN COALESCE(amount_vnd, 0) < 0 THEN amount_vnd ELSE 0 END) AS bank_outflow_vnd
            FROM `{project}.{dataset}.fact_bank_transactions`
            GROUP BY transaction_date_key
        ),
        all_dates AS (
            SELECT activity_date FROM order_revenue
            UNION DISTINCT
            SELECT activity_date FROM payment_received
            UNION DISTINCT
            SELECT activity_date FROM bank_cashflow
        )
        SELECT
            d.activity_date,
            COALESCE(o.sales_revenue_vnd, 0) AS sales_revenue_vnd,
            COALESCE(p.payment_received_vnd, 0) AS payment_received_vnd,
            COALESCE(b.bank_inflow_vnd, 0) AS bank_inflow_vnd,
            COALESCE(b.bank_outflow_vnd, 0) AS bank_outflow_vnd,
            COALESCE(p.payment_received_vnd, 0) + COALESCE(b.bank_inflow_vnd, 0) + COALESCE(b.bank_outflow_vnd, 0) AS net_cashflow_vnd
        FROM all_dates AS d
        LEFT JOIN order_revenue AS o
            ON d.activity_date = o.activity_date
        LEFT JOIN payment_received AS p
            ON d.activity_date = p.activity_date
        LEFT JOIN bank_cashflow AS b
            ON d.activity_date = b.activity_date
        """

    def _build_payment_status_view_sql(self) -> str:
        dataset = self.dataset_id
        project = self.loader.client.project
        return f"""
        CREATE OR REPLACE VIEW `{project}.{dataset}.vw_payment_status` AS
        SELECT
            o.order_key,
            o.order_id,
            o.transaction_id,
            o.customer_id,
            o.order_date,
            o.status AS order_status,
            o.total_vnd AS order_total_vnd,
            p.payment_key,
            p.payment_gateway,
            p.payment_method,
            p.payment_status,
            p.payment_date,
            p.amount_vnd AS payment_amount_vnd,
            TIMESTAMP_DIFF(
                TIMESTAMP(p.payment_date),
                TIMESTAMP(o.order_date),
                HOUR
            ) AS payment_delay_hours,
            GREATEST(COALESCE(o.total_vnd, 0) - COALESCE(p.amount_vnd, 0), 0) AS outstanding_amount,
            CASE
                WHEN LOWER(COALESCE(p.payment_status, '')) IN ('paid', 'completed', 'success') THEN 'Paid'
                WHEN COALESCE(p.payment_date, NULL) IS NULL AND DATE(o.order_date) < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN 'Overdue'
                WHEN LOWER(COALESCE(p.payment_status, '')) IN ('pending', 'processing') THEN 'Pending'
                WHEN LOWER(COALESCE(p.payment_status, '')) IN ('failed', 'error') THEN 'Failed'
                ELSE 'Pending'
            END AS payment_status_category
        FROM `{project}.{dataset}.fact_orders` AS o
        LEFT JOIN `{project}.{dataset}.fact_payments` AS p
            ON o.transaction_id = p.transaction_id
        """

    @staticmethod
    def _parse_holidays(holiday_config: str | None) -> list[str]:
        if not holiday_config:
            return []
        return [value.strip() for value in holiday_config.split(",") if value.strip()]
