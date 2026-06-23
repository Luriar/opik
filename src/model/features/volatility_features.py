"""Volatility feature generation."""

from __future__ import annotations

import pandas as pd

from src.model.features._common import EPSILON, lagged, rank_by_date, sort_by_ticker_date


def add_volatility_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add volatility features using returns and ranges known by T-1."""
    result = sort_by_ticker_date(df)
    group = result.groupby("ticker", sort=False)

    returns = group["close"].pct_change()
    result["volatility_5d"] = returns.groupby(result["ticker"], sort=False).transform(
        lambda series: series.shift(1).rolling(5).std()
    )
    result["volatility_20d"] = returns.groupby(result["ticker"], sort=False).transform(
        lambda series: series.shift(1).rolling(20).std()
    )

    intraday_range = (result["high"] - result["low"]) / result["close"].replace(0, pd.NA)
    result["intraday_range_5d"] = intraday_range.groupby(result["ticker"], sort=False).transform(
        lambda series: series.shift(1).rolling(5).mean()
    )

    prev_close = lagged(result, "close")
    true_range = pd.concat(
        [
            result["high"] - result["low"],
            (result["high"] - prev_close).abs(),
            (result["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr14 = true_range.groupby(result["ticker"], sort=False).transform(
        lambda series: series.shift(1).rolling(14).mean()
    )
    result["atr_percent"] = atr14 / (prev_close + EPSILON)
    result = rank_by_date(result, "volatility_20d", "volatility_rank_pct")
    return result

