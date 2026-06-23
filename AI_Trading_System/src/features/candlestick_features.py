"""Candlestick feature generation from previous-day OHLC."""

from __future__ import annotations

import pandas as pd

from src.features._common import EPSILON, lagged, sort_by_ticker_date


def add_candlestick_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add candlestick shape features using T-1 OHLC values."""
    result = sort_by_ticker_date(df)
    open_lag = lagged(result, "open")
    high_lag = lagged(result, "high")
    low_lag = lagged(result, "low")
    close_lag = lagged(result, "close")

    result["body"] = (close_lag - open_lag) / (open_lag + EPSILON)
    result["upper_shadow"] = (high_lag - pd.concat([open_lag, close_lag], axis=1).max(axis=1)) / (
        open_lag + EPSILON
    )
    result["lower_shadow"] = (pd.concat([open_lag, close_lag], axis=1).min(axis=1) - low_lag) / (
        open_lag + EPSILON
    )
    result["body_ratio"] = (close_lag - open_lag).abs() / (high_lag - low_lag + EPSILON)
    result["close_position"] = (close_lag - low_lag) / (high_lag - low_lag + EPSILON)
    return result

