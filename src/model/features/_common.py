"""Shared helpers for leakage-safe feature generation."""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd


EPSILON = 1e-8


def sort_by_ticker_date(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy sorted by ticker and date."""
    required = {"date", "ticker"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    result = df.copy()
    result["date"] = pd.to_datetime(result["date"])
    result["ticker"] = result["ticker"].astype(str)
    return result.sort_values(["ticker", "date"]).reset_index(drop=True)


def require_columns(df: pd.DataFrame, columns: Iterable[str]) -> None:
    """Raise when required columns are absent."""
    missing = set(columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")


def lagged(df: pd.DataFrame, column: str, periods: int = 1) -> pd.Series:
    """Return ticker-level lagged values for one column."""
    require_columns(df, [column])
    return df.groupby("ticker", sort=False)[column].shift(periods)


def lagged_return(df: pd.DataFrame, column: str, period: int) -> pd.Series:
    """Return period return ending at T-1 for each ticker."""
    require_columns(df, [column])
    group = df.groupby("ticker", sort=False)[column]
    return group.shift(1) / group.shift(period + 1) - 1


def rank_by_date(df: pd.DataFrame, source: str, output: str | None = None) -> pd.DataFrame:
    """Add a percent rank computed independently within each date."""
    require_columns(df, ["date", source])
    target = output or f"{source}_rank_pct"
    result = df.copy()
    result[target] = result.groupby("date", sort=False)[source].rank(pct=True)
    return result


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Divide while treating zero denominators as missing values."""
    return numerator / denominator.replace(0, np.nan)

