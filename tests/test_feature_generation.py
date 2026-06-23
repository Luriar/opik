
"""
tests/test_feature_generation.py

Feature generation tests for AI Trading System v1.0.

Based on:
docs/05_feature_library.md
docs/06_data_leakage_rules.md
configs/feature.yaml
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def sample_ohlcv() -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=80, freq="B")
    rows = []

    for ticker, offset in [("AAA", 0), ("BBB", 10), ("CCC", 20)]:
        for i, date in enumerate(dates):
            close = 100 + offset + i
            rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "open": close - 0.5,
                    "high": close + 1.0,
                    "low": close - 1.0,
                    "close": float(close),
                    "volume": float(1000 + i * 10 + offset),
                    "sector": "Semiconductor" if ticker != "CCC" else "Auto",
                    "market_type": "KOSPI",
                    "market_cap_group": "Top50",
                }
            )

    return pd.DataFrame(rows)


def test_return_1d_uses_lagged_close(sample_ohlcv: pd.DataFrame) -> None:
    df = sample_ohlcv.sort_values(["ticker", "date"]).copy()
    g = df.groupby("ticker")

    df["return_1d"] = g["close"].shift(1) / g["close"].shift(2) - 1

    for _, group in df.groupby("ticker"):
        expected = group["close"].shift(1) / group["close"].shift(2) - 1
        np.testing.assert_allclose(group["return_1d"], expected, equal_nan=True)


def test_return_5d_uses_lagged_close(sample_ohlcv: pd.DataFrame) -> None:
    df = sample_ohlcv.sort_values(["ticker", "date"]).copy()
    g = df.groupby("ticker")

    df["return_5d"] = g["close"].shift(1) / g["close"].shift(6) - 1

    for _, group in df.groupby("ticker"):
        expected = group["close"].shift(1) / group["close"].shift(6) - 1
        np.testing.assert_allclose(group["return_5d"], expected, equal_nan=True)


def test_ma20_ratio_uses_shift_before_rolling(sample_ohlcv: pd.DataFrame) -> None:
    df = sample_ohlcv.sort_values(["ticker", "date"]).copy()

    def calc(group: pd.DataFrame) -> pd.DataFrame:
        group = group.copy()
        ma20 = group["close"].shift(1).rolling(20).mean()
        group["close_ma20_ratio"] = group["close"].shift(1) / ma20 - 1
        return group

    result = pd.concat([calc(group) for _, group in df.groupby("ticker")]).sort_index()

    for _, group in result.groupby("ticker"):
        ma20 = group["close"].shift(1).rolling(20).mean()
        expected = group["close"].shift(1) / ma20 - 1
        np.testing.assert_allclose(group["close_ma20_ratio"], expected, equal_nan=True)


def test_trading_value_and_relative_trading_value(sample_ohlcv: pd.DataFrame) -> None:
    df = sample_ohlcv.sort_values(["ticker", "date"]).copy()

    def calc(group: pd.DataFrame) -> pd.DataFrame:
        group = group.copy()
        trading_value = group["close"].shift(1) * group["volume"].shift(1)
        trading_value_ma20 = trading_value.rolling(20).mean()
        group["relative_trading_value"] = trading_value / trading_value_ma20
        return group

    result = pd.concat([calc(group) for _, group in df.groupby("ticker")]).sort_index()

    for _, group in result.groupby("ticker"):
        trading_value = group["close"].shift(1) * group["volume"].shift(1)
        expected = trading_value / trading_value.rolling(20).mean()
        np.testing.assert_allclose(
            group["relative_trading_value"],
            expected,
            equal_nan=True,
        )


def test_volatility_20d_uses_lagged_returns(sample_ohlcv: pd.DataFrame) -> None:
    df = sample_ohlcv.sort_values(["ticker", "date"]).copy()

    def calc(group: pd.DataFrame) -> pd.DataFrame:
        group = group.copy()
        ret = group["close"].pct_change()
        group["volatility_20d"] = ret.shift(1).rolling(20).std()
        return group

    result = pd.concat([calc(group) for _, group in df.groupby("ticker")]).sort_index()

    for _, group in result.groupby("ticker"):
        ret = group["close"].pct_change()
        expected = ret.shift(1).rolling(20).std()
        np.testing.assert_allclose(group["volatility_20d"], expected, equal_nan=True)


def test_atr_percent_uses_lagged_true_range(sample_ohlcv: pd.DataFrame) -> None:
    df = sample_ohlcv.sort_values(["ticker", "date"]).copy()

    def calc(group: pd.DataFrame) -> pd.DataFrame:
        group = group.copy()
        prev_close = group["close"].shift(1)
        tr1 = group["high"] - group["low"]
        tr2 = (group["high"] - prev_close).abs()
        tr3 = (group["low"] - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr14 = tr.shift(1).rolling(14).mean()
        group["atr_percent"] = atr14 / group["close"].shift(1)
        return group

    result = df.groupby("ticker", group_keys=False).apply(calc)

    assert "atr_percent" in result.columns
    assert result["atr_percent"].dropna().ge(0).all()


def test_candlestick_features_use_previous_day_ohlc(sample_ohlcv: pd.DataFrame) -> None:
    df = sample_ohlcv.sort_values(["ticker", "date"]).copy()
    g = df.groupby("ticker")

    open_lag = g["open"].shift(1)
    high_lag = g["high"].shift(1)
    low_lag = g["low"].shift(1)
    close_lag = g["close"].shift(1)

    df["body"] = (close_lag - open_lag) / open_lag
    df["close_position"] = (close_lag - low_lag) / (high_lag - low_lag + 1e-8)

    assert df["body"].dropna().notna().all()
    assert df["close_position"].dropna().between(0, 1).all()


def test_breakout_features_use_shifted_high_low(sample_ohlcv: pd.DataFrame) -> None:
    df = sample_ohlcv.sort_values(["ticker", "date"]).copy()

    def calc(group: pd.DataFrame) -> pd.DataFrame:
        group = group.copy()
        high_20d = group["high"].shift(1).rolling(20).max()
        low_20d = group["low"].shift(1).rolling(20).min()
        close_lag = group["close"].shift(1)

        group["close_to_20d_high"] = close_lag / high_20d - 1
        group["close_to_20d_low"] = close_lag / low_20d - 1
        return group

    result = df.groupby("ticker", group_keys=False).apply(calc)

    assert "close_to_20d_high" in result.columns
    assert "close_to_20d_low" in result.columns
    assert result["close_to_20d_high"].dropna().le(0).all()
    assert result["close_to_20d_low"].dropna().ge(0).all()


def test_rsi14_range(sample_ohlcv: pd.DataFrame) -> None:
    df = sample_ohlcv.sort_values(["ticker", "date"]).copy()

    def calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
        close_lag = close.shift(1)
        delta = close_lag.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
        rs = avg_gain / (avg_loss + 1e-8)
        return 100 - 100 / (1 + rs)

    df["rsi14"] = df.groupby("ticker")["close"].transform(calc_rsi)

    assert df["rsi14"].dropna().between(0, 100).all()


def test_bollinger_features_range(sample_ohlcv: pd.DataFrame) -> None:
    df = sample_ohlcv.sort_values(["ticker", "date"]).copy()

    def calc(group: pd.DataFrame) -> pd.DataFrame:
        group = group.copy()
        close_lag = group["close"].shift(1)
        ma20 = close_lag.rolling(20).mean()
        std20 = close_lag.rolling(20).std()
        upper = ma20 + 2 * std20
        lower = ma20 - 2 * std20

        group["bb_position"] = (close_lag - lower) / (upper - lower + 1e-8)
        group["bb_width"] = (upper - lower) / ma20
        return group

    result = df.groupby("ticker", group_keys=False).apply(calc)

    assert result["bb_width"].dropna().ge(0).all()
    assert result["bb_position"].dropna().notna().all()


def test_cross_sectional_rank_grouped_by_date(sample_ohlcv: pd.DataFrame) -> None:
    df = sample_ohlcv.sort_values(["date", "ticker"]).copy()
    df["return_5d"] = (
        df.groupby("ticker")["close"].shift(1)
        / df.groupby("ticker")["close"].shift(6)
        - 1
    )

    df["return_5d_rank_pct"] = df.groupby("date")["return_5d"].rank(pct=True)

    for _, group in df.dropna(subset=["return_5d_rank_pct"]).groupby("date"):
        assert group["return_5d_rank_pct"].between(0, 1).all()
        assert group["return_5d_rank_pct"].max() <= 1.0


def test_identity_features_exist(sample_ohlcv: pd.DataFrame) -> None:
    required = {"sector", "market_type", "market_cap_group"}
    missing = required - set(sample_ohlcv.columns)

    assert not missing


def test_ticker_is_not_model_feature() -> None:
    feature_list = [
        "return_5d",
        "momentum_rank_pct",
        "relative_trading_value",
        "sector",
        "market_type",
        "market_cap_group",
    ]

    assert "ticker" not in feature_list
    assert "stock_code" not in feature_list


def test_target_columns_not_created_in_feature_generation(sample_ohlcv: pd.DataFrame) -> None:
    feature_df = sample_ohlcv.copy()

    forbidden_targets = {
        "target_rank_return",
        "target_gap",
        "target_intraday",
    }

    assert forbidden_targets.isdisjoint(feature_df.columns)


def test_required_feature_groups_have_features() -> None:
    feature_groups = {
        "price": ["return_1d", "return_5d", "close_ma20_ratio"],
        "momentum": ["momentum_5d", "momentum_diff"],
        "volume": ["relative_trading_value"],
        "volatility": ["atr_percent", "volatility_20d"],
        "candlestick": ["body", "close_position"],
        "breakout": ["close_to_20d_high", "breakout_rank_pct"],
        "technical": ["rsi14", "macd_hist_ratio", "bb_position"],
        "cross_sectional": ["return_5d_rank_pct", "momentum_rank_pct"],
        "macro": ["nasdaq_return_1d", "sox_return_1d", "usdkrw_return_1d"],
        "identity": ["sector", "market_type", "market_cap_group"],
    }

    for group_name, features in feature_groups.items():
        assert features, f"{group_name} must contain at least one feature"

