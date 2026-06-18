"""Breakout feature generation."""

from __future__ import annotations

import pandas as pd

from src.model.features._common import EPSILON, lagged, rank_by_date, sort_by_ticker_date


def add_breakout_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add shifted high/low breakout features."""
    result = sort_by_ticker_date(df)
    group = result.groupby("ticker", sort=False)
    close_lag = lagged(result, "close")

    result["high_20d"] = group["high"].transform(lambda series: series.shift(1).rolling(20).max())
    result["low_20d"] = group["low"].transform(lambda series: series.shift(1).rolling(20).min())
    result["close_to_20d_high"] = close_lag / result["high_20d"] - 1
    result["close_to_20d_low"] = close_lag / result["low_20d"] - 1
    result["breakout_strength"] = (
        (close_lag - result["low_20d"]) / (result["high_20d"] - result["low_20d"] + EPSILON)
    )
    result = rank_by_date(result, "breakout_strength", "breakout_rank_pct")
    return result

