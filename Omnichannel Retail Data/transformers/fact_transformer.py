from __future__ import annotations

import os
import sys
from typing import Any

import pandas as pd

try:
    from transformers.base_transformer import BaseTransformer
    from utils.gcs_helper import to_snake_case
except ModuleNotFoundError:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    if project_root not in sys.path:
        sys.path.append(project_root)

    from transformers.base_transformer import BaseTransformer
    from utils.gcs_helper import to_snake_case


class FactTransformer(BaseTransformer):
    """Transformer for fact tables in the analytics warehouse."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.standardize_column_names(df)

    def transform_orders(
        self,
        df: pd.DataFrame,
        channel: str | None = None,
        source: str | None = None,
    ) -> pd.DataFrame:
        transformed_df = self.standardize_column_names(df)

        transformed_df["order_id"] = self._coalesce_columns(
            transformed_df, ["order_id", "id", "code", "order_number"]
        )
        transformed_df["transaction_id"] = self._coalesce_columns(
            transformed_df, ["transaction_id", "txn_id", "transaction_code"]
        )
        transformed_df["customer_id"] = self._coalesce_columns(
            transformed_df,
            ["customer_id"],
            nested_sources=[("customer", "id"), ("customer", "customer_id")],
        )
        transformed_df["order_date"] = self._coalesce_columns(
            transformed_df,
            ["order_date", "created_at", "created_on", "timestamp", "response_time_iso"],
        )
        transformed_df["shipping_city"] = self._coalesce_columns(
            transformed_df,
            [
                "shipping_city",
                "delivery_city",
                "shipping_province",
                "delivery_province",
                "shipping_province_name",
                "delivery_province_name",
            ],
            nested_sources=[
                ("shipping_address", "city"),
                ("shipping_address", "city_name"),
                ("shipping_address", "province"),
                ("shipping_address", "province_name"),
                ("delivery_address", "city"),
                ("delivery_address", "city_name"),
                ("delivery_address", "province"),
                ("delivery_address", "province_name"),
            ],
        )
        transformed_df["shipping_address"] = self._coalesce_columns(
            transformed_df,
            [
                "shipping_address_1",
                "shipping_address1",
                "delivery_address_1",
                "delivery_address1",
                "shipping_street",
                "delivery_street",
                "shipping_line_1",
                "delivery_line_1",
                "shipping_full_address",
                "delivery_full_address",
                "shipping_address_text",
                "delivery_address_text",
            ],
            nested_sources=[
                ("shipping_address", "address"),
                ("shipping_address", "address1"),
                ("shipping_address", "line1"),
                ("shipping_address", "street"),
                ("shipping_address", "full_address"),
                ("delivery_address", "address"),
                ("delivery_address", "address1"),
                ("delivery_address", "line1"),
                ("delivery_address", "street"),
                ("delivery_address", "full_address"),
            ],
        )
        transformed_df["status"] = self._coalesce_columns(
            transformed_df, ["status", "order_status"]
        )
        transformed_df["payment_status"] = self._coalesce_columns(
            transformed_df, ["payment_status", "financial_status"]
        )
        transformed_df["total_vnd"] = self._coalesce_columns(
            transformed_df,
            ["total_vnd", "amount_vnd", "grand_total_vnd", "total", "transaction_amount_vnd"],
        )
        transformed_df["total_usd"] = self._coalesce_columns(
            transformed_df,
            ["total_usd", "amount_usd", "grand_total_usd"]
        )

        if "channel" not in transformed_df.columns:
            transformed_df["channel"] = channel if channel is not None else pd.NA
        elif channel is not None:
            transformed_df["channel"] = transformed_df["channel"].fillna(channel)

        if "source" not in transformed_df.columns:
            transformed_df["source"] = source if source is not None else pd.NA
        elif source is not None:
            transformed_df["source"] = transformed_df["source"].fillna(source)

        required_columns = [
            "order_id",
            "transaction_id",
            "customer_id",
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

        transformed_df = self._ensure_columns(transformed_df, required_columns)
        transformed_df = self.cast_columns(
            transformed_df,
            {
                "order_id": "string",
                "transaction_id": "string",
                "customer_id": "int",
                "order_date": "datetime",
                "channel": "string",
                "source": "string",
                "shipping_city": "string",
                "shipping_address": "string",
                "status": "string",
                "payment_status": "string",
                "total_vnd": "numeric",
                "total_usd": "numeric",
            },
        )
        transformed_df = self.handle_missing_value(
            transformed_df,
            {
                "status": "unknown",
                "payment_status": "unknown",
                "total_vnd": 0,
                "total_usd": 0,
            },
        )
        transformed_df = self.create_date_key(
            transformed_df, "order_date", "order_date_key"
        )
        transformed_df = self.create_surrogate_key(
            transformed_df,
            ["order_id", "transaction_id"],
            "order_key",
        )

        transformed_df, _ = self.check_duplicates(
            transformed_df,
            subset=["order_key"],
            remove_duplicates=True,
        )

        output_columns = [
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
        return transformed_df[output_columns]

    def transform_order_items(self, df: pd.DataFrame) -> pd.DataFrame:
        transformed_df = self.standardize_column_names(df)
        transformed_df["order_id"] = self._coalesce_columns(
            transformed_df, ["order_id", "id", "code", "order_number"]
        )
        transformed_df["transaction_id"] = self._coalesce_columns(
            transformed_df, ["transaction_id", "txn_id", "transaction_code"]
        )
        transformed_df["order_date"] = self._coalesce_columns(
            transformed_df, ["order_date", "created_at", "created_on", "timestamp"]
        )

        transformed_df = self.create_surrogate_key(
            transformed_df,
            ["order_id", "transaction_id"],
            "order_key",
        )

        items_df = self.unflatten_list(
            transformed_df,
            "line_items",
            ["order_key", "order_id", "transaction_id", "order_date"],
            meta_prefix="parent_",
        )
        if items_df.empty:
            return items_df

        items_df = self.standardize_column_names(items_df)
        items_df["order_key"] = self._coalesce_columns(
            items_df, ["order_key", "parent_order_key"]
        )
        items_df["transaction_id"] = self._coalesce_columns(
            items_df, ["transaction_id", "parent_transaction_id"]
        )
        items_df["order_date"] = self._coalesce_columns(
            items_df, ["order_date", "parent_order_date"]
        )
        items_df["product_id"] = self._coalesce_columns(
            items_df, ["product_id", "id", "item_id", "variant_id"]
        )
        items_df["quantity"] = self._coalesce_columns(items_df, ["quantity", "qty"])
        items_df["unit_price_vnd"] = self._coalesce_columns(
            items_df, ["unit_price_vnd", "price_vnd", "unit_price", "price"]
        )
        items_df["line_total_vnd"] = self._coalesce_columns(
            items_df, ["line_total_vnd", "total_vnd", "line_total", "amount_vnd"]
        )

        required_columns = [
            "order_key",
            "transaction_id",
            "product_id",
            "quantity",
            "unit_price_vnd",
            "line_total_vnd",
            "order_date",
        ]
        items_df = self._ensure_columns(items_df, required_columns)
        items_df = self.cast_columns(
            items_df,
            {
                "order_key": "string",
                "transaction_id": "string",
                "product_id": "int",
                "quantity": "numeric",
                "unit_price_vnd": "numeric",
                "line_total_vnd": "numeric",
                "order_date": "datetime",
            },
        )

        needs_line_total = items_df["line_total_vnd"].isna()
        items_df.loc[needs_line_total, "line_total_vnd"] = (
            items_df.loc[needs_line_total, "quantity"].fillna(0)
            * items_df.loc[needs_line_total, "unit_price_vnd"].fillna(0)
        )
        items_df = self.handle_missing_value(
            items_df,
            {
                "quantity": 0,
                "unit_price_vnd": 0,
                "line_total_vnd": 0,
            },
        )
        items_df = self.create_date_key(items_df, "order_date", "order_date_key")
        items_df = self.create_surrogate_key(
            items_df,
            ["order_key", "product_id", "transaction_id"],
            "order_item_key",
        )

        items_df, _ = self.check_duplicates(
            items_df,
            subset=["order_item_key"],
            remove_duplicates=True,
        )

        output_columns = [
            "order_item_key",
            "order_key",
            "transaction_id",
            "product_id",
            "order_date_key",
            "quantity",
            "unit_price_vnd",
            "line_total_vnd",
        ]
        return items_df[output_columns]

    def transform_payments(
        self,
        df: pd.DataFrame,
        payment_gateway: str | None = None,
    ) -> pd.DataFrame:
        transformed_df = self.standardize_column_names(df)

        transformed_df["transaction_id"] = self._coalesce_columns(
            transformed_df,
            [
                "transaction_id",
                "txn_id",
                "id",
                "paypal_transaction_id",
                "app_trans_id",
            ],
        )
        transformed_df["order_id"] = self._coalesce_columns(
            transformed_df,
            ["order_id", "reference_id", "order_code", "orderid"],
        )
        transformed_df["customer_id"] = self._coalesce_columns(
            transformed_df,
            ["customer_id"],
            nested_sources=[("customer", "id"), ("customer", "customer_id")],
        )
        transformed_df["payment_gateway"] = self._coalesce_columns(
            transformed_df, ["payment_gateway", "gateway", "provider", "source"]
        )
        if payment_gateway is not None:
            transformed_df["payment_gateway"] = transformed_df[
                "payment_gateway"
            ].fillna(payment_gateway)
        transformed_df["payment_method"] = self._coalesce_columns(
            transformed_df, ["payment_method", "method", "pay_type", "channel"]
        )
        transformed_df["amount_vnd"] = self._coalesce_columns(
            transformed_df,
            ["amount_vnd", "transaction_amount_vnd", "amount", "total_vnd"],
        )
        transformed_df["payment_status"] = self._coalesce_columns(
            transformed_df, ["payment_status", "status", "transaction_status", "message"]
        )
        transformed_df["payment_date"] = self._coalesce_columns(
            transformed_df,
            [
                "payment_date",
                "created_at",
                "paid_at",
                "timestamp",
                "transaction_initiation_date",
                "response_time_iso",
                "server_time_iso",
                "created_at",
                "createdat",
            ],
        )
        transformed_df["payment_status"] = self._derive_payment_status(transformed_df)

        required_columns = [
            "transaction_id",
            "order_id",
            "customer_id",
            "payment_gateway",
            "payment_method",
            "amount_vnd",
            "payment_status",
            "payment_date",
        ]
        transformed_df = self._ensure_columns(transformed_df, required_columns)
        transformed_df = self.cast_columns(
            transformed_df,
            {
                "transaction_id": "string",
                "order_id": "string",
                "customer_id": "int",
                "payment_gateway": "string",
                "payment_method": "string",
                "amount_vnd": "numeric",
                "payment_status": "string",
                "payment_date": "datetime",
            },
        )
        transformed_df = self.handle_missing_value(
            transformed_df,
            {
                "payment_gateway": payment_gateway or "unknown",
                "payment_method": "unknown",
                "payment_status": "unknown",
                "amount_vnd": 0,
            },
        )
        transformed_df = self.create_date_key(
            transformed_df, "payment_date", "payment_date_key"
        )
        transformed_df = self.create_surrogate_key(
            transformed_df,
            ["transaction_id", "payment_gateway", "order_id"],
            "payment_key",
        )
        transformed_df, _ = self.check_duplicates(
            transformed_df,
            subset=["payment_key"],
            remove_duplicates=True,
        )

        output_columns = [
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
        return transformed_df[output_columns]

    def transform_bank_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        transformed_df = self.standardize_column_names(df)

        transformed_df["transaction_id"] = self._coalesce_columns(
            transformed_df, ["transaction_id", "id"]
        )
        transformed_df["account_id"] = self._coalesce_columns(
            transformed_df, ["account_id", "accountid"]
        )
        transformed_df["transaction_type"] = self._coalesce_columns(
            transformed_df, ["transaction_type", "kind", "type"]
        )
        transformed_df["amount_vnd"] = self._coalesce_columns(
            transformed_df, ["amount_vnd", "amount"]
        )
        transformed_df["status"] = self._coalesce_columns(
            transformed_df, ["status"]
        )
        transformed_df["transaction_date"] = self._coalesce_columns(
            transformed_df,
            ["transaction_date", "created_at", "createdat", "timestamp"],
        )

        required_columns = [
            "transaction_id",
            "account_id",
            "transaction_type",
            "amount_vnd",
            "status",
            "transaction_date",
        ]
        transformed_df = self._ensure_columns(transformed_df, required_columns)
        transformed_df = self.cast_columns(
            transformed_df,
            {
                "transaction_id": "string",
                "account_id": "string",
                "transaction_type": "string",
                "amount_vnd": "numeric",
                "status": "string",
                "transaction_date": "datetime",
            },
        )
        transformed_df = self.handle_missing_value(
            transformed_df,
            {
                "transaction_type": "unknown",
                "status": "unknown",
                "amount_vnd": 0,
            },
        )
        transformed_df = self.create_date_key(
            transformed_df,
            "transaction_date",
            "transaction_date_key",
        )
        transformed_df = self.create_surrogate_key(
            transformed_df,
            ["transaction_id", "account_id"],
            "transaction_key",
        )
        transformed_df, _ = self.check_duplicates(
            transformed_df,
            subset=["transaction_key"],
            remove_duplicates=True,
        )

        output_columns = [
            "transaction_key",
            "transaction_id",
            "account_id",
            "transaction_type",
            "amount_vnd",
            "status",
            "transaction_date_key",
            "transaction_date",
        ]
        return transformed_df[output_columns]

    def transform_cart_events(self, df: pd.DataFrame) -> pd.DataFrame:
        transformed_df = self.standardize_column_names(df)

        transformed_df["event_id"] = self._coalesce_columns(
            transformed_df, ["event_id", "id"]
        )
        transformed_df["session_id"] = self._coalesce_columns(
            transformed_df, ["session_id", "session"]
        )
        transformed_df["customer_id"] = self._coalesce_columns(
            transformed_df, ["customer_id"]
        )
        transformed_df["event_type"] = self._coalesce_columns(
            transformed_df, ["event_type", "type"]
        )
        transformed_df["event_timestamp"] = self._coalesce_columns(
            transformed_df, ["event_timestamp", "timestamp", "created_at"]
        )
        transformed_df["product_id"] = self._coalesce_columns(
            transformed_df, ["product_id", "id_item", "item_id"]
        )
        transformed_df["source"] = self._coalesce_columns(
            transformed_df, ["source", "channel"]
        )
        transformed_df["device"] = self._coalesce_columns(
            transformed_df, ["device", "device_type"]
        )
        transformed_df["browser"] = self._coalesce_columns(
            transformed_df, ["browser"]
        )
        transformed_df["utm_source"] = self._coalesce_columns(
            transformed_df, ["utm_source"]
        )
        transformed_df["utm_campaign"] = self._coalesce_columns(
            transformed_df, ["utm_campaign"]
        )

        required_columns = [
            "event_id",
            "session_id",
            "customer_id",
            "event_type",
            "event_timestamp",
            "product_id",
            "source",
            "device",
            "browser",
            "utm_source",
            "utm_campaign",
        ]
        transformed_df = self._ensure_columns(transformed_df, required_columns)
        transformed_df = self.cast_columns(
            transformed_df,
            {
                "event_id": "string",
                "session_id": "string",
                "customer_id": "int",
                "event_type": "string",
                "event_timestamp": "datetime",
                "product_id": "int",
                "source": "string",
                "device": "string",
                "browser": "string",
                "utm_source": "string",
                "utm_campaign": "string",
            },
        )
        transformed_df = self.create_date_key(
            transformed_df, "event_timestamp", "event_date_key"
        )
        transformed_df = self.create_surrogate_key(
            transformed_df,
            ["event_id", "session_id"],
            "event_key",
        )
        transformed_df, _ = self.check_duplicates(
            transformed_df,
            subset=["event_key"],
            remove_duplicates=True,
        )

        output_columns = [
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
        return transformed_df[output_columns]

    def _ensure_columns(
        self, df: pd.DataFrame, required_columns: list[str]
    ) -> pd.DataFrame:
        transformed_df = df.copy()
        for column in required_columns:
            if column not in transformed_df.columns:
                transformed_df[column] = pd.NA
                self.logger.warning(
                    "Column '%s' missing. Added with null default.", column
                )
        return transformed_df

    def _coalesce_columns(
        self,
        df: pd.DataFrame,
        columns: list[str],
        nested_sources: list[tuple[str, str]] | None = None,
    ) -> pd.Series:
        result = pd.Series(pd.NA, index=df.index, dtype="object")

        for column in columns:
            if column in df.columns:
                result = result.combine_first(df[column])

        for source_column, nested_key in nested_sources or []:
            if source_column in df.columns:
                nested_series = df[source_column].apply(
                    lambda value: self._extract_nested_value(value, nested_key)
                )
                result = result.combine_first(nested_series)

        return result

    @staticmethod
    def _extract_nested_value(value: Any, key: str) -> Any:
        if not isinstance(value, dict):
            return pd.NA

        current: Any = value
        for key_part in key.split("."):
            if not isinstance(current, dict):
                return pd.NA

            normalized = {
                to_snake_case(str(candidate_key)): candidate_value
                for candidate_key, candidate_value in current.items()
            }
            normalized_key = to_snake_case(key_part)
            if normalized_key not in normalized:
                return pd.NA
            current = normalized[normalized_key]

        if isinstance(current, (dict, list, tuple, set)):
            return pd.NA
        return current

    def _derive_payment_status(self, df: pd.DataFrame) -> pd.Series:
        payment_status = self._coalesce_columns(
            df,
            ["payment_status", "status", "transaction_status", "message"],
        ).astype("string")

        normalized = payment_status.str.strip().str.lower()

        if "result_code" in df.columns:
            normalized = normalized.mask(df["result_code"].eq(0), "paid")
            normalized = normalized.mask(df["result_code"].notna() & df["result_code"].ne(0), "failed")

        if "return_code" in df.columns:
            normalized = normalized.mask(df["return_code"].eq(1), "paid")
            normalized = normalized.mask(df["return_code"].notna() & df["return_code"].ne(1), "failed")

        return normalized.replace(
            {
                "success": "paid",
                "completed": "paid",
                "succeeded": "paid",
                "authorized": "paid",
                "pending": "pending",
                "processing": "pending",
                "failed": "failed",
                "error": "failed",
            }
        ).astype("string")
