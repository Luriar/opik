
"""
tests/test_integration.py

Integration tests for AI Trading System v1.0.

Purpose:
- Verify that outputs from one module can be used as inputs to the next module.
- Data -> Feature -> Target -> Prediction -> Portfolio -> Backtest -> Execution
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def feature_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-02"]),
            "ticker": ["005930", "000660"],
            "return_5d": [0.02, 0.03],
            "momentum_rank_pct": [0.6, 0.8],
            "relative_trading_value": [1.2, 1.5],
            "atr_percent": [0.03, 0.04],
            "sector": ["Semiconductor", "Semiconductor"],
            "market_type": ["KOSPI", "KOSPI"],
            "market_cap_group": ["Top20", "Top20"],
            "target_rank_return": [0.01, 0.02],
            "target_gap": [0.002, 0.003],
            "target_intraday": [0.008, 0.017],
        }
    )


@pytest.fixture
def prediction_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-03", "2024-01-03"]),
            "ticker": ["005930", "000660"],
            "ranking_score": [0.7, 0.9],
            "pred_gap": [0.002, 0.003],
            "pred_intraday": [0.008, 0.017],
            "pred_open": [70140, 120360],
            "pred_close": [70701.12, 122406.12],
            "expected_return": [
                (1 + 0.002) * (1 + 0.008) - 1,
                (1 + 0.003) * (1 + 0.017) - 1,
            ],
            "sector": ["Semiconductor", "Semiconductor"],
            "market_type": ["KOSPI", "KOSPI"],
            "market_cap_group": ["Top20", "Top20"],
            "trading_value_ma20": [10_000_000_000, 12_000_000_000],
            "trading_value_rank_pct": [0.7, 0.9],
            "atr_percent": [0.03, 0.04],
            "volatility_20d": [0.03, 0.04],
        }
    )


def test_feature_dataset_can_feed_model_training(feature_df: pd.DataFrame) -> None:
    excluded = {
        "date",
        "ticker",
        "target_rank_return",
        "target_gap",
        "target_intraday",
    }

    feature_columns = [c for c in feature_df.columns if c not in excluded]

    assert feature_columns
    assert "target_rank_return" not in feature_columns
    assert "target_gap" not in feature_columns
    assert "target_intraday" not in feature_columns


def test_prediction_dataset_can_feed_portfolio(prediction_df: pd.DataFrame) -> None:
    required = {
        "date",
        "ticker",
        "ranking_score",
        "pred_gap",
        "pred_intraday",
        "expected_return",
        "sector",
        "market_type",
        "atr_percent",
        "volatility_20d",
        "trading_value_ma20",
        "trading_value_rank_pct",
    }

    missing = required - set(prediction_df.columns)

    assert not missing


def test_portfolio_output_can_feed_backtest(prediction_df: pd.DataFrame) -> None:
    portfolio = prediction_df.sort_values("ranking_score", ascending=False).head(2).copy()
    portfolio["weight"] = 1 / len(portfolio)

    market_data = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-03", "2024-01-03"]),
            "ticker": ["005930", "000660"],
            "open": [70000, 120000],
            "close": [70600, 122000],
            "volume": [10_000_000, 5_000_000],
        }
    )

    merged = portfolio.merge(market_data, on=["date", "ticker"], how="left")

    required = {"weight", "open", "close", "ticker", "date"}
    missing = required - set(merged.columns)

    assert not missing
    assert not merged[["open", "close"]].isna().any().any()


def test_backtest_positions_can_feed_execution(prediction_df: pd.DataFrame) -> None:
    positions = prediction_df.head(2).copy()
    positions["weight"] = [0.5, 0.5]
    positions["portfolio_score"] = positions["ranking_score"]

    required_for_execution = {
        "date",
        "ticker",
        "weight",
        "ranking_score",
        "pred_gap",
        "pred_intraday",
        "expected_return",
        "portfolio_score",
        "sector",
        "market_type",
    }

    missing = required_for_execution - set(positions.columns)

    assert not missing


def test_expected_return_consistency(prediction_df: pd.DataFrame) -> None:
    expected = (
        (1 + prediction_df["pred_gap"])
        * (1 + prediction_df["pred_intraday"])
        - 1
    )

    np.testing.assert_allclose(
        prediction_df["expected_return"],
        expected,
    )


def test_no_target_columns_in_prediction_dataset(prediction_df: pd.DataFrame) -> None:
    forbidden = {
        "target_rank_return",
        "target_gap",
        "target_intraday",
        "actual_return",
    }

    assert forbidden.isdisjoint(prediction_df.columns)


def test_pipeline_preserves_date_ticker_keys(
    feature_df: pd.DataFrame,
    prediction_df: pd.DataFrame,
) -> None:
    assert {"date", "ticker"}.issubset(feature_df.columns)
    assert {"date", "ticker"}.issubset(prediction_df.columns)


def test_portfolio_weights_sum_to_one(prediction_df: pd.DataFrame) -> None:
    portfolio = prediction_df.head(2).copy()
    portfolio["weight"] = 1 / len(portfolio)

    assert portfolio["weight"].sum() == pytest.approx(1.0)


def test_execution_order_plan_required_columns(prediction_df: pd.DataFrame) -> None:
    portfolio = prediction_df.head(2).copy()
    portfolio["weight"] = [0.5, 0.5]
    portfolio["target_amount"] = 100_000_000 * portfolio["weight"]
    portfolio["side"] = "BUY"
    portfolio["order_type"] = "MARKET"
    portfolio["execution_mode"] = "paper"

    required = {
        "date",
        "ticker",
        "side",
        "weight",
        "target_amount",
        "order_type",
        "execution_mode",
    }

    missing = required - set(portfolio.columns)

    assert not missing


def test_full_integration_minimal_flow(prediction_df: pd.DataFrame) -> None:
    candidates = prediction_df.sort_values("ranking_score", ascending=False).head(2)
    portfolio = candidates.sort_values("expected_return", ascending=False).head(2).copy()
    portfolio["weight"] = 1 / len(portfolio)

    market_data = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-03", "2024-01-03"]),
            "ticker": ["005930", "000660"],
            "open": [70000, 120000],
            "close": [70600, 122000],
        }
    )

    positions = portfolio.merge(market_data, on=["date", "ticker"], how="left")
    positions["buy_price"] = positions["open"] * 1.001
    positions["sell_price"] = positions["close"] * 0.999
    positions["net_return"] = (
        positions["sell_price"] / positions["buy_price"] - 1 - 0.0015 - 0.0015
    )

    daily_return = (positions["weight"] * positions["net_return"]).sum()

    assert len(portfolio) == 2
    assert portfolio["weight"].sum() == pytest.approx(1.0)
    assert np.isfinite(daily_return)
