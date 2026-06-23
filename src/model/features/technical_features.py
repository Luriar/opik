"""Technical indicator feature generation."""

from __future__ import annotations

import pandas as pd

from src.model.features._common import EPSILON, lagged, rank_by_date, sort_by_ticker_date


def _rsi_from_lagged_close(close: pd.Series, period: int = 14) -> pd.Series:
    """Compute RSI from close shifted to T-1."""
    close_lag = close.shift(1)
    delta = close_lag.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    relative_strength = avg_gain / (avg_loss + EPSILON)
    return 100 - 100 / (1 + relative_strength)


def add_technical_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add RSI, MACD, Bollinger, and rank technical features."""
    result = sort_by_ticker_date(df)
    group = result.groupby("ticker", sort=False)

    result["rsi14"] = group["close"].transform(_rsi_from_lagged_close)
    result["rsi_change_5d"] = result.groupby("ticker", sort=False)["rsi14"].diff(5)

    close_lag = lagged(result, "close")
    ema12 = group["close"].transform(lambda series: series.shift(1).ewm(span=12, adjust=False).mean())
    ema26 = group["close"].transform(lambda series: series.shift(1).ewm(span=26, adjust=False).mean())
    macd = ema12 - ema26
    signal = macd.groupby(result["ticker"], sort=False).transform(
        lambda series: series.ewm(span=9, adjust=False).mean()
    )
    result["macd_hist_ratio"] = (macd - signal) / (close_lag + EPSILON)

    ma20 = group["close"].transform(lambda series: series.shift(1).rolling(20).mean())
    std20 = group["close"].transform(lambda series: series.shift(1).rolling(20).std())
    upper = ma20 + 2 * std20
    lower = ma20 - 2 * std20
    result["bb_position"] = (close_lag - lower) / (upper - lower + EPSILON)
    result["bb_width"] = (upper - lower) / (ma20 + EPSILON)
    result["bb_position_change_5d"] = result.groupby("ticker", sort=False)["bb_position"].diff(5)

    if "atr_percent" not in result.columns:
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

    result = rank_by_date(result, "rsi14", "rsi_rank_pct")
    result = rank_by_date(result, "macd_hist_ratio", "macd_rank_pct")
    result = rank_by_date(result, "bb_position", "bb_position_rank_pct")
    result = rank_by_date(result, "atr_percent", "atr_rank_pct")
    return result

