"""Cross-sectional feature generation."""

from __future__ import annotations

import pandas as pd

from src.model.features._common import rank_by_date, sort_by_ticker_date


RANK_SOURCES: dict[str, str] = {
    "return_5d_rank_pct": "return_5d",
    "return_20d_rank_pct": "return_20d",
    "momentum_rank_pct": "momentum_20d",
    "momentum_20d_rank_pct": "momentum_20d",
    "momentum_diff_rank_pct": "momentum_diff",
    "trading_value_rank_pct": "relative_trading_value",
    "volume_change_rank_pct": "volume_change_1d",
    "volatility_rank_pct": "volatility_20d",
    "atr_rank_pct": "atr_percent",
    "bb_width_rank_pct": "bb_width",
    "breakout_rank_pct": "breakout_strength",
    "low_rebound_rank_pct": "close_to_20d_low",
    "relative_return_5d_rank_pct": "relative_return_5d_vs_market",
    "sector_relative_rank_pct": "relative_return_20d_vs_sector",
}


def add_cross_sectional_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add percent ranks grouped by date only."""
    result = sort_by_ticker_date(df)
    for output, source in RANK_SOURCES.items():
        if source in result.columns:
            result = rank_by_date(result, source, output)
    return result

