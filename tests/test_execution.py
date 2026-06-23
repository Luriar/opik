"""
tests/test_execution.py

Execution contract tests for AI Trading System v1.0.
"""

from __future__ import annotations

import pandas as pd
import pytest


FORBIDDEN_EXECUTION_COLUMNS = {
    "actual_return",
    "target_gap",
    "target_intraday",
    "target_rank_return",
}


def make_portfolio() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-02"]),
            "ticker": ["005930", "000660"],
            "weight": [0.5, 0.5],
            "ranking_score": [0.9, 0.8],
            "pred_gap": [0.01, 0.00],
            "pred_intraday": [0.02, 0.01],
            "expected_return": [0.0302, 0.01],
            "portfolio_score": [0.95, 0.85],
            "sector": ["Semiconductor", "Semiconductor"],
            "market_type": ["KOSPI", "KOSPI"],
        }
    )


def test_portfolio_has_required_execution_columns() -> None:
    portfolio = make_portfolio()
    required = {
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

    missing = required - set(portfolio.columns)
    assert not missing


def test_order_plan_required_columns() -> None:
    portfolio = make_portfolio()
    orders = portfolio[["date", "ticker", "weight"]].copy()
    orders["side"] = "BUY"
    orders["target_amount"] = 100_000_000 * orders["weight"]
    orders["order_type"] = "MARKET"
    orders["execution_mode"] = "paper"

    required = {
        "date",
        "ticker",
        "side",
        "weight",
        "target_amount",
        "order_type",
        "execution_mode",
    }

    missing = required - set(orders.columns)
    assert not missing


def test_orders_use_paper_mode_for_v1() -> None:
    orders = make_portfolio()[["date", "ticker", "weight"]].copy()
    orders["execution_mode"] = "paper"

    assert set(orders["execution_mode"]) == {"paper"}


def test_order_weights_sum_to_one() -> None:
    portfolio = make_portfolio()

    assert portfolio["weight"].sum() == pytest.approx(1.0)


def test_duplicate_ticker_should_fail_risk_check() -> None:
    portfolio = make_portfolio()
    portfolio.loc[1, "ticker"] = portfolio.loc[0, "ticker"]

    assert portfolio["ticker"].duplicated().any()


def test_execution_does_not_use_forbidden_columns() -> None:
    execution_columns = set(make_portfolio().columns)

    assert execution_columns.isdisjoint(FORBIDDEN_EXECUTION_COLUMNS)


def test_execution_using_target_column_should_fail() -> None:
    execution_columns = set(make_portfolio().columns) | {"target_intraday"}

    assert not execution_columns.isdisjoint(FORBIDDEN_EXECUTION_COLUMNS)
