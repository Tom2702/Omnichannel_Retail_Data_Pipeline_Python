from __future__ import annotations

import io
import os
import sys
from typing import Any

import pandas as pd

try:
    from utils.gcs_helper import to_snake_case
    from utils.logger import setup_logger
except ModuleNotFoundError:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    if project_root not in sys.path:
        sys.path.append(project_root)

    from utils.gcs_helper import to_snake_case
    from utils.logger import setup_logger


class BaseTransformer:
    """Base class for shared transformation and validation helpers."""

    def __init__(self) -> None:
        self.logger = setup_logger(__name__)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("Child transformers must implement `transform()`.")

    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        transformed_df = df.copy()
        transformed_df.columns = [to_snake_case(col) for col in transformed_df.columns]
        return transformed_df

    def to_date(self, df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        transformed_df = df.copy()
        for column in columns:
            if column in transformed_df.columns:
                transformed_df[column] = pd.to_datetime(
                    transformed_df[column], errors="coerce"
                )
            else:
                self.logger.warning("Column '%s' not found in DataFrame.", column)
        return transformed_df

    def cast_columns(
        self, df: pd.DataFrame, dtype_mapping: dict[str, str]
    ) -> pd.DataFrame:
        transformed_df = df.copy()

        for column, dtype_name in dtype_mapping.items():
            if column not in transformed_df.columns:
                self.logger.warning(
                    "Column '%s' not found for type casting.", column
                )
                continue

            normalized_dtype = dtype_name.lower()

            if normalized_dtype in {"datetime", "datetime64[ns]"}:
                transformed_df[column] = pd.to_datetime(
                    transformed_df[column], errors="coerce"
                )
            elif normalized_dtype == "date":
                transformed_df[column] = pd.to_datetime(
                    transformed_df[column], errors="coerce"
                ).dt.date
            elif normalized_dtype in {"int", "int64", "integer"}:
                transformed_df[column] = pd.to_numeric(
                    transformed_df[column], errors="coerce"
                ).astype("Int64")
            elif normalized_dtype in {"float", "float64"}:
                transformed_df[column] = pd.to_numeric(
                    transformed_df[column], errors="coerce"
                ).astype(float)
            elif normalized_dtype in {"numeric", "number"}:
                transformed_df[column] = pd.to_numeric(
                    transformed_df[column], errors="coerce"
                )
            elif normalized_dtype in {"string", "str"}:
                transformed_df[column] = transformed_df[column].astype("string")
            elif normalized_dtype in {"bool", "boolean"}:
                transformed_df[column] = (
                    transformed_df[column]
                    .astype("string")
                    .str.strip()
                    .str.lower()
                    .map(
                        {
                            "true": True,
                            "false": False,
                            "1": True,
                            "0": False,
                            "yes": True,
                            "no": False,
                        }
                    )
                    .astype("boolean")
                )
            else:
                transformed_df[column] = transformed_df[column].astype(
                    dtype_name, errors="ignore"
                )

        return transformed_df

    def create_date_key(
        self,
        df: pd.DataFrame,
        date_column: str,
        key_date_name: str = "date_key",
    ) -> pd.DataFrame:
        transformed_df = df.copy()
        if date_column in transformed_df.columns:
            transformed_df[date_column] = pd.to_datetime(
                transformed_df[date_column], errors="coerce"
            )
            transformed_df[key_date_name] = transformed_df[date_column].dt.date
        else:
            self.logger.warning(
                "Source column '%s' not found. Cannot create date key.",
                date_column,
            )
        return transformed_df

    def create_surrogate_key(
        self,
        df: pd.DataFrame,
        selected_cols: list[str],
        new_key_name: str = "surrogate_key",
        separator: str = "_",
    ) -> pd.DataFrame:
        transformed_df = df.copy()
        missing_cols = [
            column for column in selected_cols if column not in transformed_df.columns
        ]
        if missing_cols:
            self.logger.error(
                "Columns %s not found in DataFrame. Cannot create surrogate key.",
                missing_cols,
            )
            return transformed_df

        combined_col = transformed_df[selected_cols[0]].fillna("").astype(str)
        for column in selected_cols[1:]:
            combined_col = (
                combined_col + separator + transformed_df[column].fillna("").astype(str)
            )

        transformed_df[new_key_name] = combined_col
        self.logger.info("Successfully created surrogate key '%s'.", new_key_name)
        return transformed_df

    def unflatten_list(
        self,
        df: pd.DataFrame,
        list_col: str,
        col_to_keep: list[str],
        meta_prefix: str | None = None,
    ) -> pd.DataFrame:
        if list_col not in df.columns:
            self.logger.warning("List column '%s' not found in DataFrame.", list_col)
            return pd.DataFrame(columns=col_to_keep)

        records = df.to_dict(orient="records")
        output_df = pd.json_normalize(
            records,
            record_path=list_col,
            meta=col_to_keep,
            meta_prefix=meta_prefix,
            errors="ignore",
        )
        self.logger.info("Unflattening complete. Output shape: %s", output_df.shape)
        return output_df

    def handle_missing_value(
        self, df: pd.DataFrame, fill_cols: dict[str, Any] | None = None
    ) -> pd.DataFrame:
        transformed_df = df.copy()
        if fill_cols:
            for col, value in fill_cols.items():
                if col not in transformed_df.columns:
                    self.logger.warning("Column '%s' not found in DataFrame.", col)
                    continue

                if transformed_df[col].isnull().sum() > 0:
                    transformed_df[col] = transformed_df[col].fillna(value)
        return transformed_df

    def check_null_values(
        self, df: pd.DataFrame, critical_columns: list[str] | None = None
    ) -> dict[str, int]:
        if critical_columns:
            existing_columns = [col for col in critical_columns if col in df.columns]
            null_counts = (
                df[existing_columns].isnull().sum()
                if existing_columns
                else pd.Series(dtype="int64")
            )
        else:
            null_counts = df.isnull().sum()

        result = {
            column: int(count)
            for column, count in null_counts[null_counts > 0].to_dict().items()
        }
        if result:
            self.logger.warning("Found NULL values in columns:\n%s", pd.Series(result))
        return result

    def check_duplicates(
        self,
        df: pd.DataFrame,
        subset: list[str] | None = None,
        remove_duplicates: bool = False,
        flag_column: str | None = None,
    ) -> tuple[pd.DataFrame, int]:
        transformed_df = df.copy()
        duplicate_mask = (
            transformed_df.duplicated(subset=subset, keep="first")
            if subset
            else transformed_df.duplicated(keep="first")
        )

        duplicate_count = int(duplicate_mask.sum())
        if flag_column:
            transformed_df[flag_column] = duplicate_mask
        if remove_duplicates:
            transformed_df = transformed_df.loc[~duplicate_mask].copy()

        return transformed_df, duplicate_count

    def validate_date_ranges(
        self,
        df: pd.DataFrame,
        date_columns: list[str],
        min_date: str | None = None,
        max_date: str | None = None,
        allow_future_dates: bool = False,
    ) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        lower_bound = pd.Timestamp(min_date) if min_date else None
        upper_bound = pd.Timestamp(max_date) if max_date else pd.Timestamp.now()

        for col in date_columns:
            if col not in df.columns:
                continue

            valid_dates = pd.to_datetime(df[col], errors="coerce").dropna()
            if len(valid_dates) == 0:
                result[col] = {
                    "min_date": None,
                    "max_date": None,
                    "future_dates": 0,
                    "below_min_date": 0,
                    "above_max_date": 0,
                }
                continue

            column_is_tz_aware = getattr(valid_dates.dt, "tz", None) is not None
            if column_is_tz_aware:
                lower_bound = (
                    pd.Timestamp(min_date).tz_localize("UTC")
                    if min_date and pd.Timestamp(min_date).tzinfo is None
                    else pd.Timestamp(min_date)
                    if min_date
                    else None
                )
                upper_bound = (
                    pd.Timestamp(max_date).tz_localize("UTC")
                    if max_date and pd.Timestamp(max_date).tzinfo is None
                    else pd.Timestamp(max_date)
                    if max_date
                    else pd.Timestamp.now(tz="UTC")
                )
            else:
                lower_bound = (
                    pd.Timestamp(min_date).tz_localize(None)
                    if min_date and pd.Timestamp(min_date).tzinfo is not None
                    else pd.Timestamp(min_date)
                    if min_date
                    else None
                )
                upper_bound = (
                    pd.Timestamp(max_date).tz_localize(None)
                    if max_date and pd.Timestamp(max_date).tzinfo is not None
                    else pd.Timestamp(max_date)
                    if max_date
                    else pd.Timestamp.now()
                )

            below_min_count = (
                int((valid_dates < lower_bound).sum()) if lower_bound is not None else 0
            )
            above_max_count = (
                int((valid_dates > upper_bound).sum()) if upper_bound is not None else 0
            )
            future_count = 0 if allow_future_dates else above_max_count

            result[col] = {
                "min_date": valid_dates.min(),
                "max_date": valid_dates.max(),
                "future_dates": future_count,
                "below_min_date": below_min_count,
                "above_max_date": above_max_count,
            }

        return result

    def validate_amounts(
        self,
        df: pd.DataFrame,
        amount_columns: list[str],
        allow_negative_amounts: bool = False,
    ) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}

        for col in amount_columns:
            if col not in df.columns:
                continue

            valid_amounts = pd.to_numeric(df[col], errors="coerce").dropna()
            if len(valid_amounts) == 0:
                result[col] = {
                    "negative_count": 0,
                    "outlier_count": 0,
                    "mean": None,
                    "median": None,
                    "std_dev": None,
                    "min": None,
                    "max": None,
                }
                continue

            negative_count = int((valid_amounts < 0).sum())
            q1 = valid_amounts.quantile(0.25)
            q3 = valid_amounts.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            outliers = valid_amounts[
                (valid_amounts < lower_bound) | (valid_amounts > upper_bound)
            ]

            if negative_count > 0 and not allow_negative_amounts:
                self.logger.warning(
                    "Column '%s' has %s negative amounts.", col, negative_count
                )

            result[col] = {
                "negative_count": negative_count,
                "outlier_count": int(len(outliers)),
                "mean": float(valid_amounts.mean()),
                "median": float(valid_amounts.median()),
                "std_dev": float(valid_amounts.std())
                if pd.notna(valid_amounts.std())
                else 0.0,
                "min": float(valid_amounts.min()),
                "max": float(valid_amounts.max()),
            }

        return result

    def data_quality_check(
        self,
        df: pd.DataFrame,
        table_name: str,
        critical_columns: list[str] | None = None,
        duplicate_subset: list[str] | None = None,
        remove_duplicates: bool = False,
        duplicate_flag_column: str | None = None,
        date_columns: list[str] | None = None,
        amount_columns: list[str] | None = None,
        allow_negative_amounts: bool = False,
        min_date: str | None = None,
        max_date: str | None = None,
        allow_future_dates: bool = False,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        transformed_df = df.copy()
        self.logger.info("--- Data Quality check: %s ---", table_name)
        self.logger.info(
            "Shape: rows=%s, columns=%s",
            transformed_df.shape[0],
            transformed_df.shape[1],
        )

        buffer = io.StringIO()
        transformed_df.info(buf=buffer)
        self.logger.info("DataFrame info:\n%s", buffer.getvalue())

        null_report = self.check_null_values(
            transformed_df, critical_columns=critical_columns
        )
        transformed_df, duplicate_count = self.check_duplicates(
            transformed_df,
            subset=duplicate_subset,
            remove_duplicates=remove_duplicates,
            flag_column=duplicate_flag_column,
        )

        date_report = (
            self.validate_date_ranges(
                transformed_df,
                date_columns=date_columns,
                min_date=min_date,
                max_date=max_date,
                allow_future_dates=allow_future_dates,
            )
            if date_columns
            else {}
        )
        amount_report = (
            self.validate_amounts(
                transformed_df,
                amount_columns=amount_columns,
                allow_negative_amounts=allow_negative_amounts,
            )
            if amount_columns
            else {}
        )

        report = {
            "table_name": table_name,
            "row_count": int(transformed_df.shape[0]),
            "column_count": int(transformed_df.shape[1]),
            "null_counts": null_report,
            "duplicate_count": duplicate_count,
            "date_validation": date_report,
            "amount_validation": amount_report,
        }
        return transformed_df, report
