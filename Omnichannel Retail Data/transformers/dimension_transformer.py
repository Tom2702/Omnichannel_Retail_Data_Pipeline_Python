from __future__ import annotations

import os
import sys
from typing import Any

import pandas as pd

try:
    from transformers.base_transformer import BaseTransformer
except ModuleNotFoundError:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    if project_root not in sys.path:
        sys.path.append(project_root)

    from transformers.base_transformer import BaseTransformer


class DimensionTransformer(BaseTransformer):
    """Transformer for all dimension tables."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.standardize_column_names(df)

    def transform_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        transformed_df = self.standardize_column_names(df)
        transformed_df = transformed_df.rename(
            columns={"id": "customer_id", "name": "full_name"}
        )

        required_columns = [
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

        transformed_df = self._ensure_columns(transformed_df, required_columns)
        transformed_df = self.cast_columns(
            transformed_df,
            {
                "customer_id": "int",
                "email": "string",
                "full_name": "string",
                "phone": "string",
                "city": "string",
                "country": "string",
                "created_at": "datetime",
                "customer_segment": "string",
                "lifetime_value_vnd": "numeric",
                "total_orders": "int",
                "first_order_date": "datetime",
                "last_order_date": "datetime",
            },
        )
        transformed_df = self.handle_missing_value(
            transformed_df,
            {
                "customer_segment": "unknown",
                "lifetime_value_vnd": 0,
                "total_orders": 0,
            },
        )
        return transformed_df[required_columns].drop_duplicates(subset=["customer_id"])

    def transform_products(self, df: pd.DataFrame) -> pd.DataFrame:
        transformed_df = self.standardize_column_names(df)
        transformed_df = transformed_df.rename(
            columns={"id": "product_id", "name": "product_name"}
        )

        required_columns = [
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

        transformed_df = self._ensure_columns(transformed_df, required_columns)
        transformed_df = self.cast_columns(
            transformed_df,
            {
                "product_id": "int",
                "product_name": "string",
                "sku": "string",
                "barcode": "string",
                "category": "string",
                "brand": "string",
                "price_vnd": "numeric",
                "price_usd": "numeric",
                "stock_quantity": "int",
                "is_active": "bool",
            },
        )
        transformed_df = self.handle_missing_value(
            transformed_df,
            {
                "price_vnd": 0,
                "price_usd": 0,
                "stock_quantity": 0,
                "is_active": True,
            },
        )
        return transformed_df[required_columns].drop_duplicates(subset=["product_id"])

    def transform_locations(self, df: pd.DataFrame) -> pd.DataFrame:
        transformed_df = self.standardize_column_names(df)

        transformed_df["location_id"] = self._coalesce_columns(
            transformed_df, ["location_id", "id"]
        )
        transformed_df["location_code"] = self._coalesce_columns(
            transformed_df, ["location_code", "code"]
        )
        transformed_df["location_name"] = self._coalesce_columns(
            transformed_df, ["location_name", "name"]
        )
        transformed_df["location_type"] = self._coalesce_columns(
            transformed_df, ["location_type", "type"]
        )
        transformed_df["city"] = self._coalesce_columns(
            transformed_df, ["city"]
        )
        transformed_df["address"] = self._coalesce_columns(
            transformed_df, ["address"]
        )
        transformed_df["phone"] = self._coalesce_columns(
            transformed_df, ["phone"]
        )
        transformed_df["is_active"] = self._coalesce_columns(
            transformed_df, ["is_active", "active"]
        )

        required_columns = [
            "location_id",
            "location_code",
            "location_name",
            "location_type",
            "city",
            "address",
            "phone",
            "is_active",
        ]

        transformed_df = self._ensure_columns(transformed_df, required_columns)
        transformed_df = self.cast_columns(
            transformed_df,
            {
                "location_id": "int",
                "location_code": "string",
                "location_name": "string",
                "location_type": "string",
                "city": "string",
                "address": "string",
                "phone": "string",
                "is_active": "bool",
            },
        )
        transformed_df = self.handle_missing_value(
            transformed_df,
            {
                "location_type": "store",
                "is_active": True,
            },
        )
        return transformed_df[required_columns].drop_duplicates(subset=["location_id"])

    def transform_staff(self, df: pd.DataFrame) -> pd.DataFrame:
        transformed_df = self.standardize_column_names(df)

        transformed_df["staff_id"] = self._coalesce_columns(
            transformed_df, ["staff_id", "id"]
        )
        transformed_df["staff_code"] = self._coalesce_columns(
            transformed_df, ["staff_code", "code"]
        )
        transformed_df["full_name"] = self._coalesce_columns(
            transformed_df, ["full_name", "name"]
        )
        transformed_df["position"] = self._coalesce_columns(
            transformed_df, ["position", "role", "title"]
        )
        transformed_df["email"] = self._coalesce_columns(
            transformed_df, ["email"]
        )
        transformed_df["phone"] = self._coalesce_columns(
            transformed_df, ["phone"]
        )
        transformed_df["location_id"] = self._coalesce_columns(
            transformed_df,
            ["location_id", "store_id"],
            nested_sources=[("location", "id")],
        )
        transformed_df["hire_date"] = self._coalesce_columns(
            transformed_df, ["hire_date", "created_at", "joined_at"]
        )
        transformed_df["is_active"] = self._coalesce_columns(
            transformed_df, ["is_active", "active"]
        )

        required_columns = [
            "staff_id",
            "staff_code",
            "full_name",
            "position",
            "email",
            "phone",
            "location_id",
            "hire_date",
            "is_active",
        ]

        transformed_df = self._ensure_columns(transformed_df, required_columns)
        transformed_df = self.cast_columns(
            transformed_df,
            {
                "staff_id": "int",
                "staff_code": "string",
                "full_name": "string",
                "position": "string",
                "email": "string",
                "phone": "string",
                "location_id": "int",
                "hire_date": "datetime",
                "is_active": "bool",
            },
        )
        transformed_df = self.handle_missing_value(
            transformed_df,
            {
                "position": "unknown",
                "is_active": True,
            },
        )
        return transformed_df[required_columns].drop_duplicates(subset=["staff_id"])

    def transform_date(
        self,
        start_date: str,
        end_date: str,
        holidays: list[str] | None = None,
    ) -> pd.DataFrame:
        date_range = pd.date_range(start=start_date, end=end_date, freq="D")
        holiday_set = {
            pd.Timestamp(day).date() for day in (holidays or [])
        }

        calendar_dates = pd.Series(date_range)
        dim_date = pd.DataFrame({"full_date": calendar_dates.dt.date})
        dim_date["date_key"] = calendar_dates.dt.strftime("%Y%m%d").astype(int)
        dim_date["year"] = calendar_dates.dt.year
        dim_date["quarter"] = calendar_dates.dt.quarter
        dim_date["month"] = calendar_dates.dt.month
        dim_date["month_name"] = calendar_dates.dt.month_name()
        dim_date["week"] = calendar_dates.dt.isocalendar().week.astype(int)
        dim_date["day_of_month"] = calendar_dates.dt.day
        dim_date["day_of_week"] = calendar_dates.dt.dayofweek + 1
        dim_date["day_name"] = calendar_dates.dt.day_name()
        dim_date["is_weekend"] = calendar_dates.dt.dayofweek >= 5
        dim_date["is_holiday"] = dim_date["full_date"].isin(holiday_set)
        dim_date["fiscal_year"] = dim_date["year"]
        dim_date["fiscal_quarter"] = dim_date["quarter"]

        return dim_date[
            [
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
        ]

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
        if isinstance(value, dict):
            return value.get(key)
        return pd.NA
