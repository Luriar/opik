"""Volume and trading-value feature generation."""

from __future__ import annotations

import pandas as pd

from src.features._common import lagged, rank_by_date, safe_divide, sort_by_ticker_date


def add_volume_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add volume features without using current-day volume."""
    result = sort_by_ticker_date(df)
    group = result.groupby("ticker", sort=False)

    volume_lag = lagged(result, "volume")
    result["volume_change_1d"] = volume_lag / lagged(result, "volume", 2) - 1

    trading_value = lagged(result, "close") * volume_lag
    trading_value_ma20 = group["close"].transform(lambda series: series.shift(1)) * group[
        "volume"
    ].transform(lambda series: series.shift(1))
    trading_value_ma20 = trading_value_ma20.groupby(result["ticker"], sort=False).transform(
        lambda series: series.rolling(20).mean()
    )

    result["relative_trading_value"] = safe_divide(trading_value, trading_value_ma20)
    result = rank_by_date(result, "relative_trading_value", "trading_value_rank_pct")
    return result

