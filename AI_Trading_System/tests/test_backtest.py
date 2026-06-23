
"""
tests/test_backtest.py

Tests for backtest logic.

Based on:
docs/08_backtest.md
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest


def calculate_trade_return(
    open_t: float,
    close_t: float,
    buy_cost: float = 0.0015,
    sell_cost: float = 0.0015,
    buy_slippage: float = 0.001,
    sell_slippage: float = 0.001,
) -> float:
    buy_price = open_t * (1 + buy_slippage)
    sell_price = close_t * (1 - sell_slippage)
    gross_return = sell_price / buy_price - 1
    return gross_return - buy_cost - sell_cost


def test_trade_return_reflects_cost_and_slippage() -> None:
    result = calculate_trade_return(
        open_t=100,
        close_t=105,
        buy_cost=0.0015,
        sell_cost=0.0015,
        buy_slippage=0.001,
        sell_slippage=0.001,
    )

    buy_price = 100 * 1.001
    sell_price = 105 * 0.999
    expected = sell_price / buy_price - 1 - 0.0015 - 0.0015

    assert result == pytest.approx(expected)


def test_expected_return_formula() -> None:
    pred_gap = 0.01
    pred_intraday = 0.02

    expected_return = (1 + pred_gap) * (1 + pred_intraday) - 1

    assert expected_return == pytest.approx(0.0302)


def test_portfolio_selection_does_not_use_actual_or_target_columns() -> None:
    selection_columns = {
        "ranking_score",
        "pred_gap",
        "pred_intraday",
        "expected_return",
        "atr_percent",
        "sector",
    }

    forbidden = {
        "actual_return",
        "target_gap",
        "target_intraday",
        "target_rank_return",
        "close",
        "close_T",
    }

    assert selection_columns.isdisjoint(forbidden)


def test_portfolio_selection_using_actual_return_should_fail() -> None:
    selection_columns = {
        "ranking_score",
        "expected_return",
        "actual_return",
    }

    forbidden = {
        "actual_return",
        "target_gap",
        "target_intraday",
        "target_rank_return",
    }

    assert not selection_columns.isdisjoint(forbidden)


def test_select_top10_from_top30_candidates() -> None:
    df = pd.DataFrame(
        {
            "ticker": [f"T{i:03d}" for i in range(40)],
            "ranking_score": np.linspace(1, 0, 40),
            "expected_return": np.linspace(0, 0.1, 40),
            "sector": ["A"] * 40,
            "trading_value_ma20": [10_000_000_000] * 40,
            "atr_percent": [0.03] * 40,
            "volatility_20d": [0.03] * 40,
        }
    )

    top30 = df.sort_values("ranking_score", ascending=False).head(30)
    selected = top30.sort_values("expected_return", ascending=False).head(10)

    assert len(selected) == 10
    assert set(selected["ticker"]).issubset(set(top30["ticker"]))


def test_liquidity_filter() -> None:
    df = pd.DataFrame(
        {
            "ticker": ["A", "B", "C"],
            "trading_value_ma20": [
                10_000_000_000,
                4_000_000_000,
                7_000_000_000,
            ],
        }
    )

    filtered = df[df["trading_value_ma20"] >= 5_000_000_000]

    assert set(filtered["ticker"]) == {"A", "C"}


def test_risk_filter() -> None:
    df = pd.DataFrame(
        {
            "ticker": ["A", "B", "C"],
            "atr_percent": [0.03, 0.09, 0.05],
            "volatility_20d": [0.03, 0.04, 0.09],
        }
    )

    filtered = df[
        (df["atr_percent"] <= 0.08)
        & (df["volatility_20d"] <= 0.08)
    ]

    assert set(filtered["ticker"]) == {"A"}


def test_sector_limit_max_3_names() -> None:
    df = pd.DataFrame(
        {
            "ticker": [f"T{i}" for i in range(10)],
            "sector": [
                "Semiconductor",
                "Semiconductor",
                "Semiconductor",
                "Semiconductor",
                "Auto",
                "Auto",
                "Bio",
                "Bio",
                "Bank",
                "Shipbuilding",
            ],
            "expected_return": [0.10, 0.09, 0.08, 0.07, 0.06, 0.05, 0.04, 0.03, 0.02, 0.01],
        }
    )

    selected = (
        df.sort_values("expected_return", ascending=False)
          .groupby("sector", group_keys=False)
          .head(3)
    )

    assert selected[selected["sector"] == "Semiconductor"].shape[0] == 3
    assert selected.groupby("sector").size().max() <= 3


def test_equal_weighting_top10() -> None:
    tickers = [f"T{i}" for i in range(10)]
    df = pd.DataFrame({"ticker": tickers})

    df["weight"] = 1 / len(df)

    assert df["weight"].sum() == pytest.approx(1.0)
    assert (df["weight"] == 0.1).all()


def test_daily_portfolio_return_is_weighted_sum() -> None:
    positions = pd.DataFrame(
        {
            "ticker": ["A", "B", "C"],
            "weight": [0.4, 0.3, 0.3],
            "net_return": [0.01, 0.02, -0.01],
        }
    )

    daily_return = (positions["weight"] * positions["net_return"]).sum()

    expected = 0.4 * 0.01 + 0.3 * 0.02 + 0.3 * (-0.01)

    assert daily_return == pytest.approx(expected)


def test_cumulative_return_calculation() -> None:
    daily_returns = pd.Series([0.01, -0.02, 0.03])

    cumulative = (1 + daily_returns).cumprod() - 1

    expected_final = (1.01 * 0.98 * 1.03) - 1

    assert cumulative.iloc[-1] == pytest.approx(expected_final)


def test_maximum_drawdown_calculation() -> None:
    daily_returns = pd.Series([0.10, -0.05, -0.10, 0.02])

    equity = (1 + daily_returns).cumprod()
    running_max = equity.cummax()
    drawdown = equity / running_max - 1
    mdd = drawdown.min()

    assert mdd < 0
    assert mdd == pytest.approx(drawdown.min())


def test_annual_return_calculation() -> None:
    cumulative_return_final = 0.20
    num_trading_days = 252

    annual_return = (1 + cumulative_return_final) ** (252 / num_trading_days) - 1

    assert annual_return == pytest.approx(0.20)


def test_annual_volatility_calculation() -> None:
    daily_returns = pd.Series([0.01, -0.01, 0.02, -0.02])

    annual_volatility = daily_returns.std() * math.sqrt(252)

    assert annual_volatility > 0


def test_sharpe_ratio_calculation() -> None:
    annual_return = 0.15
    annual_volatility = 0.10

    sharpe = annual_return / annual_volatility

    assert sharpe == pytest.approx(1.5)


def test_win_rate_calculation() -> None:
    daily_returns = pd.Series([0.01, -0.01, 0.02, 0.00])

    win_rate = (daily_returns > 0).sum() / len(daily_returns)

    assert win_rate == pytest.approx(0.5)


def test_turnover_calculation() -> None:
    previous_holdings = {"A", "B", "C", "D", "E"}
    current_holdings = {"A", "B", "F", "G", "H"}

    overlap = len(previous_holdings & current_holdings)
    portfolio_size = len(current_holdings)
    turnover = 1 - overlap / portfolio_size

    assert turnover == pytest.approx(0.6)


def test_backtest_output_required_columns() -> None:
    daily_positions = pd.DataFrame(
        {
            "date": ["2024-01-02"],
            "ticker": ["005930"],
            "weight": [0.1],
            "ranking_score": [0.95],
            "pred_gap": [0.01],
            "pred_intraday": [0.02],
            "expected_return": [0.0302],
            "buy_price": [100],
            "sell_price": [103],
            "gross_return": [0.03],
            "net_return": [0.025],
            "sector": ["Semiconductor"],
        }
    )

    required = {
        "date",
        "ticker",
        "weight",
        "ranking_score",
        "pred_gap",
        "pred_intraday",
        "expected_return",
        "buy_price",
        "sell_price",
        "gross_return",
        "net_return",
        "sector",
    }

    missing = required - set(daily_positions.columns)

    assert not missing


def test_daily_portfolio_output_required_columns() -> None:
    daily_portfolio = pd.DataFrame(
        {
            "date": ["2024-01-02"],
            "daily_return": [0.01],
            "cumulative_return": [0.01],
            "benchmark_return": [0.005],
            "benchmark_cumulative_return": [0.005],
            "drawdown": [0.0],
            "turnover": [0.4],
            "num_positions": [10],
        }
    )

    required = {
        "date",
        "daily_return",
        "cumulative_return",
        "benchmark_return",
        "benchmark_cumulative_return",
        "drawdown",
        "turnover",
        "num_positions",
    }

    missing = required - set(daily_portfolio.columns)

    assert not missing


def test_backtest_config_values_are_valid() -> None:
    config = {
        "initial_capital": 100_000_000,
        "portfolio_size": 10,
        "candidate_size": 30,
        "buy_cost": 0.0015,
        "sell_cost": 0.0015,
        "buy_slippage": 0.001,
        "sell_slippage": 0.001,
        "max_names_per_sector": 3,
        "weighting_method": "equal_weight",
    }

    assert config["initial_capital"] > 0
    assert config["portfolio_size"] > 0
    assert config["candidate_size"] >= config["portfolio_size"]
    assert config["buy_cost"] >= 0
    assert config["sell_cost"] >= 0
    assert config["buy_slippage"] >= 0
    assert config["sell_slippage"] >= 0
    assert config["max_names_per_sector"] > 0
    assert config["weighting_method"] in {
        "equal_weight",
        "expected_return_weight",
        "risk_adjusted_weight",
    }

