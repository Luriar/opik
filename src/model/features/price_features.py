"""Leakage-safe price feature generation."""

from __future__ import annotations

import pandas as pd

from src.model.features._common import lagged, lagged_return, safe_divide, sort_by_ticker_date


def add_price_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add price ratio and return features using data available at T-1."""
    result = sort_by_ticker_date(df)
    group = result.groupby("ticker", sort=False)

    for period in (1, 3, 5, 20, 60):
        result[f"return_{period}d"] = lagged_return(result, "close", period)

    close_lag = lagged(result, "close")
    for window in (5, 20, 60):
        moving_average = group["close"].transform(
            lambda series, window=window: series.shift(1).rolling(window).mean()
        )
        result[f"close_ma{window}_ratio"] = safe_divide(close_lag, moving_average) - 1

    return result
