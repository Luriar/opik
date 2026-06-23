"""Momentum and relative-return feature generation."""

from __future__ import annotations

import pandas as pd

from src.features._common import lagged_return, rank_by_date, sort_by_ticker_date


def add_momentum_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add momentum features using lagged returns only."""
    result = sort_by_ticker_date(df)

    if "return_5d" not in result.columns:
        result["return_5d"] = lagged_return(result, "close", 5)
    if "return_20d" not in result.columns:
        result["return_20d"] = lagged_return(result, "close", 20)

    result["momentum_5d"] = result["return_5d"]
    result["momentum_20d"] = result["return_20d"]
    result["momentum_diff"] = result["momentum_5d"] - result["momentum_20d"]
    result["momentum_accel"] = result.groupby("ticker", sort=False)["momentum_5d"].diff()

    market_return_5d = result.groupby("date", sort=False)["return_5d"].transform("mean")
    market_return_20d = result.groupby("date", sort=False)["return_20d"].transform("mean")
    result["relative_return_5d_vs_market"] = result["return_5d"] - market_return_5d
    result["relative_return_20d_vs_market"] = result["return_20d"] - market_return_20d

    if "sector" in result.columns:
        sector_return_20d = result.groupby(["date", "sector"], sort=False)["return_20d"].transform("mean")
        result["relative_return_20d_vs_sector"] = result["return_20d"] - sector_return_20d
    else:
        result["relative_return_20d_vs_sector"] = result["relative_return_20d_vs_market"]

    result = rank_by_date(result, "momentum_20d", "momentum_rank_pct")
    return result
