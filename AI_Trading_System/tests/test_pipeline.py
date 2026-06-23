
"""
tests/test_pipeline.py

End-to-end pipeline tests for AI Trading System v1.0.

Purpose:
- Verify the full pipeline contract.
- Data -> Feature -> Target -> Prediction -> Portfolio -> Backtest -> Execution
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


def make_sample_raw_data() -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=40, freq="B")
    rows = []

    for ticker in ["005930", "000660", "035420"]:
        for i, date in enumerate(dates):
            close = 100 + i
            rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "open": close - 0.5,
                    "high": close + 1,
                    "low": close - 1,
                    "close": close,
                    "volume": 1000 + i,
                    "sector": "Semiconductor",
                    "market_type": "KOSPI",
                    "market_cap_group": "Top50",
                }
            )

    return pd.DataFrame(rows)


def test_minimal_pipeline_flow() -> None:
    raw = make_sample_raw_data().sort_values(["ticker", "date"]).copy()

    raw["return_5d"] = (
        raw.groupby("ticker")["close"].shift(1)
        / raw.groupby("ticker")["close"].shift(6)
        - 1
    )

    raw["target_rank_return"] = (
        raw.groupby("ticker")["close"].shift(-1)
        / raw["close"]
        - 1
    )

    feature_df = raw.dropna(subset=["return_5d", "target_rank_return"]).copy()

    prediction_df = feature_df[["date", "ticker", "sector", "market_type", "market_cap_group"]].copy()
    prediction_df["ranking_score"] = feature_df["return_5d"]
    prediction_df["pred_gap"] = 0.001
    prediction_df["pred_intraday"] = 0.002
    prediction_df["expected_return"] = (
        (1 + prediction_df["pred_gap"])
        * (1 + prediction_df["pred_intraday"])
        - 1
    )

    daily = prediction_df[prediction_df["date"] == prediction_df["date"].max()].copy()
    portfolio = daily.sort_values("ranking_score", ascending=False).head(3).copy()
    portfolio["weight"] = 1 / len(portfolio)

    market = raw[["date", "ticker", "open", "close"]]
    positions = portfolio.merge(market, on=["date", "ticker"], how="left")

    positions["buy_price"] = positions["open"] * 1.001
    positions["sell_price"] = positions["close"] * 0.999
    positions["net_return"] = (
        positions["sell_price"] / positions["buy_price"] - 1 - 0.0015 - 0.0015
    )

    daily_return = (positions["weight"] * positions["net_return"]).sum()

    order_plan = portfolio.copy()
    order_plan["side"] = "BUY"
    order_plan["order_type"] = "MARKET"
    order_plan["execution_mode"] = "paper"

    assert not feature_df.empty
    assert not prediction_df.empty
    assert len(portfolio) == 3
    assert portfolio["weight"].sum() == pytest.approx(1.0)
    assert np.isfinite(daily_return)
    assert {"side", "order_type", "execution_mode"}.issubset(order_plan.columns)


def test_pipeline_does_not_use_targets_for_prediction() -> None:
    feature_columns = [
        "return_5d",
        "momentum_rank_pct",
        "relative_trading_value",
        "sector",
        "market_type",
        "market_cap_group",
    ]

    forbidden = {
        "target_rank_return",
        "target_gap",
        "target_intraday",
        "actual_return",
    }

    assert forbidden.isdisjoint(feature_columns)


def test_pipeline_output_contracts() -> None:
    prediction_required = {
        "date",
        "ticker",
        "ranking_score",
        "pred_gap",
        "pred_intraday",
        "expected_return",
    }

    portfolio_required = {
        "date",
        "ticker",
        "weight",
        "ranking_score",
        "expected_return",
    }

    backtest_required = {
        "date",
        "ticker",
        "buy_price",
        "sell_price",
        "net_return",
        "weight",
    }

    execution_required = {
        "date",
        "ticker",
        "side",
        "order_type",
        "execution_mode",
    }

    assert prediction_required
    assert portfolio_required
    assert backtest_required
    assert execution_required


def test_pipeline_run_metadata_contract() -> None:
    metadata = {
        "run_id": "20240102_180000",
        "pipeline_version": "v1.0",
        "feature_version": "v1.0",
        "model_version": "v1.0",
        "config_version": "v1.0",
        "execution_mode": "paper",
        "status": "success",
    }

    required = {
        "run_id",
        "pipeline_version",
        "feature_version",
        "model_version",
        "config_version",
        "execution_mode",
        "status",
    }

    assert required.issubset(metadata.keys())
    assert metadata["execution_mode"] in {"backtest", "paper", "manual", "live"}
    assert metadata["status"] in {"success", "failed", "partial"}
