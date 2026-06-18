"""Reusable market data loaders for Phase 1."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd

from src.model.data.validator import validate_macro, validate_ohlcv


DataFormat = Literal["csv", "parquet"]


class DataLoadError(ValueError):
    """Raised when a data file cannot be loaded safely."""


def _infer_format(path: Path) -> DataFormat:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix in {".parquet", ".pq"}:
        return "parquet"
    raise DataLoadError(f"Unsupported data file format: {path.suffix}")


def _read_table(path: str | Path, file_format: DataFormat | None = None) -> pd.DataFrame:
    data_path = Path(path)
    if not data_path.exists():
        raise DataLoadError(f"Data file does not exist: {data_path}")

    resolved_format = file_format or _infer_format(data_path)
    if resolved_format == "csv":
        return pd.read_csv(data_path)
    if resolved_format == "parquet":
        return pd.read_parquet(data_path)

    raise DataLoadError(f"Unsupported data format: {resolved_format}")


def normalize_date_column(df: pd.DataFrame, column: str = "date") -> pd.DataFrame:
    """Return a copy with a datetime date column."""
    result = df.copy()
    result[column] = pd.to_datetime(result[column])
    return result


def normalize_ticker_column(df: pd.DataFrame, column: str = "ticker") -> pd.DataFrame:
    """Return a copy with ticker values converted to strings."""
    result = df.copy()
    result[column] = result[column].astype(str)
    return result


def sort_by_ticker_date(df: pd.DataFrame) -> pd.DataFrame:
    """Return rows sorted by ticker and date."""
    return df.sort_values(["ticker", "date"]).reset_index(drop=True)


def load_ohlcv(path: str | Path, file_format: DataFormat | None = None) -> pd.DataFrame:
    """Load and validate Korean OHLCV data."""
    df = _read_table(path, file_format)
    df = normalize_date_column(df)
    df = normalize_ticker_column(df)
    df = sort_by_ticker_date(df)
    validate_ohlcv(df)
    return df


def load_korean_ohlcv(
    path: str | Path,
    file_format: DataFormat | None = None,
) -> pd.DataFrame:
    """Load Korean stock OHLCV data."""
    return load_ohlcv(path, file_format)


def load_macro_data(
    path: str | Path,
    file_format: DataFormat | None = None,
) -> pd.DataFrame:
    """Load and validate macro data."""
    df = _read_table(path, file_format)
    df = normalize_date_column(df)
    df = df.sort_values("date").reset_index(drop=True)
    validate_macro(df)
    return df


def load_us_market_data(
    path: str | Path,
    file_format: DataFormat | None = None,
) -> pd.DataFrame:
    """Load US market data table without imposing a feature schema."""
    df = _read_table(path, file_format)
    df = normalize_date_column(df)
    return df.sort_values("date").reset_index(drop=True)


def load_fx_data(path: str | Path, file_format: DataFormat | None = None) -> pd.DataFrame:
    """Load FX data table."""
    df = _read_table(path, file_format)
    df = normalize_date_column(df)
    return df.sort_values("date").reset_index(drop=True)


def load_commodity_data(
    path: str | Path,
    file_format: DataFormat | None = None,
) -> pd.DataFrame:
    """Load commodity data table."""
    df = _read_table(path, file_format)
    df = normalize_date_column(df)
    return df.sort_values("date").reset_index(drop=True)


def load_metadata(
    path: str | Path,
    file_format: DataFormat | None = None,
) -> pd.DataFrame:
    """Load stock metadata or identity data."""
    df = _read_table(path, file_format)
    if "ticker" in df.columns:
        df = normalize_ticker_column(df)
    return df.reset_index(drop=True)
