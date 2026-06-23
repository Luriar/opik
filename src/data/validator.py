"""Validation helpers for market data inputs."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


OHLCV_COLUMNS: set[str] = {
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
}

MACRO_COLUMNS: set[str] = {
    "date",
    "nasdaq_close",
    "sox_close",
    "sp500_close",
    "vix_close",
    "usdkrw",
    "wti_close",
}

UNIVERSE_COLUMNS: set[str] = {
    "date",
    "ticker",
    "market",
    "security_type",
    "trading_value_ma20",
}

PRICE_COLUMNS: tuple[str, ...] = ("open", "high", "low", "close")


class DataValidationError(ValueError):
    """Raised when input market data violates required contracts."""


def validate_required_columns(
    df: pd.DataFrame,
    required_columns: Iterable[str],
) -> None:
    """Raise if any required columns are missing."""
    required = set(required_columns)
    missing = required - set(df.columns)
    if missing:
        raise DataValidationError(f"Missing required columns: {sorted(missing)}")


def validate_datetime_column(df: pd.DataFrame, column: str = "date") -> None:
    """Raise if a column is not datetime typed."""
    validate_required_columns(df, {column})
    if not pd.api.types.is_datetime64_any_dtype(df[column]):
        raise DataValidationError(f"{column} must be datetime typed")


def validate_string_column(df: pd.DataFrame, column: str = "ticker") -> None:
    """Raise if a column contains non-string values."""
    validate_required_columns(df, {column})
    if not df[column].map(lambda value: isinstance(value, str)).all():
        raise DataValidationError(f"{column} must contain string values")


def validate_no_duplicate_keys(
    df: pd.DataFrame,
    key_columns: tuple[str, ...] = ("date", "ticker"),
) -> None:
    """Raise if duplicate key rows exist."""
    validate_required_columns(df, key_columns)
    duplicated = df.duplicated(subset=list(key_columns))
    if duplicated.any():
        raise DataValidationError(f"Duplicate rows for keys: {key_columns}")


def validate_ohlcv(df: pd.DataFrame) -> None:
    """Validate an OHLCV DataFrame."""
    validate_required_columns(df, OHLCV_COLUMNS)
    validate_datetime_column(df, "date")
    validate_string_column(df, "ticker")
    validate_no_duplicate_keys(df)

    for column in PRICE_COLUMNS:
        if not (df[column] > 0).all():
            raise DataValidationError(f"{column} must be positive")

    if not (df["volume"] >= 0).all():
        raise DataValidationError("volume must be non-negative")

    if not (df["high"] >= df["low"]).all():
        raise DataValidationError("high must be greater than or equal to low")

    if not (df["high"] >= df["open"]).all():
        raise DataValidationError("high must be greater than or equal to open")

    if not (df["high"] >= df["close"]).all():
        raise DataValidationError("high must be greater than or equal to close")

    if not (df["low"] <= df["open"]).all():
        raise DataValidationError("low must be less than or equal to open")

    if not (df["low"] <= df["close"]).all():
        raise DataValidationError("low must be less than or equal to close")


def validate_macro(df: pd.DataFrame) -> None:
    """Validate macro market data."""
    validate_required_columns(df, MACRO_COLUMNS)
    validate_datetime_column(df, "date")

    value_columns = MACRO_COLUMNS - {"date"}
    for column in value_columns:
        if not (df[column] > 0).all():
            raise DataValidationError(f"{column} must be positive")


def validate_universe_input(df: pd.DataFrame) -> None:
    """Validate input columns required for daily universe generation."""
    validate_required_columns(df, UNIVERSE_COLUMNS)
    validate_datetime_column(df, "date")
    validate_string_column(df, "ticker")
    validate_no_duplicate_keys(df)

    if not (df["trading_value_ma20"] >= 0).all():
        raise DataValidationError("trading_value_ma20 must be non-negative")
