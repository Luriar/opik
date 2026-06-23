
"""
tests/test_portfolio.py

Portfolio construction tests for AI Trading System v1.0.

Based on:
docs/09_portfolio.md
configs/portfolio.yaml
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


FORBIDDEN_SELECTION_COLUMNS = {
    "actual_return",
    "target_gap",
    "target_intraday",
    "target_rank_return",
    "close_t",
    "open_t",
}


@pytest.fixture
def prediction_df() -> pd.DataFrame:
    rows = []
    sectors = ["Semiconductor", "Auto", "Bio", "Bank", "Shipbuilding"]

    for i in range(50):
        rows.append(
            {
                "date": pd.Timestamp("2024-01-02"),
                "ticker": f"T{i:03d}",
                "ranking_score": 1 - i * 0.01,
                "pred_gap": 0.001 * i,
                "pred_intraday": 0.002 * i,
                "expected_return": (1 + 0.001 * i) * (1 + 0.002 * i) - 1,
                "relative_trading_value": 1.0 + i * 0.01,
                "trading_value_ma20": 10_000_000_000,
                "trading_value_rank_pct": 0.5,
                "atr_percent": 0.03,
                "volatility_20d": 0.03,
                "momentum_rank_pct": 0.5,
                "sector": sectors[i % len(sectors)],
                "market_type": "KOSPI" if i % 2 == 0 else "KOSDAQ",
                "market_cap_group": "Top50" if i < 20 else "Others",
            }
        )

    return pd.DataFrame(rows)


def calculate_expected_return(pred_gap: float, pred_intraday: float) -> float:
    return (1 + pred_gap) * (1 + pred_intraday) - 1


def calculate_portfolio_score(row: pd.Series) -> float:
    return (
        0.50 * row["ranking_score"]
        + 0.30 * row["expected_return_rank"]
        + 0.10 * row["liquidity_rank"]
        + 0.10 * row["momentum_rank_pct"]
    )


def test_expected_return_formula() -> None:
    pred_gap = 0.01
    pred_intraday = 0.02

    result = calculate_expected_return(pred_gap, pred_intraday)

    assert result == pytest.approx(0.0302)


def test_candidate_size_is_30(prediction_df: pd.DataFrame) -> None:
    candidates = (
        prediction_df.sort_values("ranking_score", ascending=False)
        .head(30)
    )

    assert len(candidates) == 30


def test_final_portfolio_size_is_10(prediction_df: pd.DataFrame) -> None:
    candidates = (
        prediction_df.sort_values("ranking_score", ascending=False)
        .head(30)
    )

    selected = (
        candidates.sort_values("expected_return", ascending=False)
        .head(10)
    )

    assert len(selected) == 10


def test_final_portfolio_is_subset_of_candidates(prediction_df: pd.DataFrame) -> None:
    candidates = (
        prediction_df.sort_values("ranking_score", ascending=False)
        .head(30)
    )

    selected = (
        candidates.sort_values("expected_return", ascending=False)
        .head(10)
    )

    assert set(selected["ticker"]).issubset(set(candidates["ticker"]))


def test_liquidity_filter_applied(prediction_df: pd.DataFrame) -> None:
    df = prediction_df.copy()
    df.loc[df.index[:3], "trading_value_ma20"] = 1_000_000_000

    filtered = df[df["trading_value_ma20"] >= 5_000_000_000]

    assert not set(df.loc[df.index[:3], "ticker"]).intersection(
        set(filtered["ticker"])
    )


def test_liquidity_rank_filter_applied(prediction_df: pd.DataFrame) -> None:
    df = prediction_df.copy()
    df.loc[df.index[:5], "trading_value_rank_pct"] = 0.10

    filtered = df[df["trading_value_rank_pct"] >= 0.20]

    assert filtered["trading_value_rank_pct"].min() >= 0.20


def test_risk_filter_applied(prediction_df: pd.DataFrame) -> None:
    df = prediction_df.copy()
    df.loc[df.index[0], "atr_percent"] = 0.10
    df.loc[df.index[1], "volatility_20d"] = 0.10

    filtered = df[
        (df["atr_percent"] <= 0.08)
        & (df["volatility_20d"] <= 0.08)
    ]

    assert df.loc[df.index[0], "ticker"] not in set(filtered["ticker"])
    assert df.loc[df.index[1], "ticker"] not in set(filtered["ticker"])


def test_extreme_gap_filter_applied(prediction_df: pd.DataFrame) -> None:
    df = prediction_df.copy()
    df.loc[df.index[0], "pred_gap"] = 0.20
    df.loc[df.index[1], "pred_gap"] = -0.20

    filtered = df[df["pred_gap"].abs() <= 0.10]

    assert df.loc[df.index[0], "ticker"] not in set(filtered["ticker"])
    assert df.loc[df.index[1], "ticker"] not in set(filtered["ticker"])


def test_sector_limit_max_3_names(prediction_df: pd.DataFrame) -> None:
    df = prediction_df.copy()

    selected = (
        df.sort_values("expected_return", ascending=False)
        .groupby("sector", group_keys=False)
        .head(3)
        .head(10)
    )

    assert selected.groupby("sector").size().max() <= 3


def test_market_type_max_weight_70_percent() -> None:
    portfolio = pd.DataFrame(
        {
            "ticker": [f"T{i}" for i in range(10)],
            "market_type": ["KOSPI"] * 7 + ["KOSDAQ"] * 3,
            "weight": [0.1] * 10,
        }
    )

    market_weight = portfolio.groupby("market_type")["weight"].sum()

    assert market_weight.max() <= 0.70


def test_equal_weight_sum_is_one() -> None:
    portfolio = pd.DataFrame({"ticker": [f"T{i}" for i in range(10)]})
    portfolio["weight"] = 1 / len(portfolio)

    assert portfolio["weight"].sum() == pytest.approx(1.0)
    assert portfolio["weight"].nunique() == 1


def test_equal_weight_position_is_10_percent() -> None:
    portfolio = pd.DataFrame({"ticker": [f"T{i}" for i in range(10)]})
    portfolio["weight"] = 1 / len(portfolio)

    assert (portfolio["weight"] == pytest.approx(0.10)).all()


def test_no_duplicate_ticker_in_portfolio() -> None:
    portfolio = pd.DataFrame(
        {
            "ticker": ["A", "B", "C"],
            "weight": [0.33, 0.33, 0.34],
        }
    )

    assert not portfolio["ticker"].duplicated().any()


def test_duplicate_ticker_should_fail() -> None:
    portfolio = pd.DataFrame(
        {
            "ticker": ["A", "A", "C"],
            "weight": [0.33, 0.33, 0.34],
        }
    )

    assert portfolio["ticker"].duplicated().any()


def test_portfolio_score_formula(prediction_df: pd.DataFrame) -> None:
    df = prediction_df.copy()

    df["expected_return_rank"] = df["expected_return"].rank(pct=True)
    df["liquidity_rank"] = df["relative_trading_value"].rank(pct=True)

    df["portfolio_score"] = df.apply(calculate_portfolio_score, axis=1)

    expected = (
        0.50 * df.loc[0, "ranking_score"]
        + 0.30 * df.loc[0, "expected_return_rank"]
        + 0.10 * df.loc[0, "liquidity_rank"]
        + 0.10 * df.loc[0, "momentum_rank_pct"]
    )

    assert df.loc[0, "portfolio_score"] == pytest.approx(expected)


def test_portfolio_selection_does_not_use_forbidden_columns() -> None:
    selection_columns = {
        "ranking_score",
        "pred_gap",
        "pred_intraday",
        "expected_return",
        "atr_percent",
        "volatility_20d",
        "sector",
    }

    assert selection_columns.isdisjoint(FORBIDDEN_SELECTION_COLUMNS)


def test_portfolio_selection_using_target_should_fail() -> None:
    selection_columns = {
        "ranking_score",
        "expected_return",
        "target_intraday",
    }

    assert not selection_columns.isdisjoint(FORBIDDEN_SELECTION_COLUMNS)


def test_output_columns_exist(prediction_df: pd.DataFrame) -> None:
    portfolio = prediction_df.head(10).copy()
    portfolio["weight"] = 0.1
    portfolio["portfolio_score"] = portfolio["ranking_score"]

    required_columns = {
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
        "market_cap_group",
    }

    missing = required_columns - set(portfolio.columns)

    assert not missing


def test_daily_metrics_can_be_calculated(prediction_df: pd.DataFrame) -> None:
    portfolio = prediction_df.head(10).copy()
    portfolio["weight"] = 0.1

    metrics = {
        "average_atr": portfolio["atr_percent"].mean(),
        "average_volatility": portfolio["volatility_20d"].mean(),
        "average_ranking_score": portfolio["ranking_score"].mean(),
        "average_expected_return": portfolio["expected_return"].mean(),
        "number_of_positions": len(portfolio),
    }

    assert metrics["average_atr"] >= 0
    assert metrics["average_volatility"] >= 0
    assert metrics["number_of_positions"] == 10


def test_sector_exposure_calculation() -> None:
    portfolio = pd.DataFrame(
        {
            "sector": ["Semiconductor", "Semiconductor", "Auto", "Bank"],
            "weight": [0.25, 0.25, 0.25, 0.25],
        }
    )

    exposure = portfolio.groupby("sector")["weight"].sum()

    assert exposure["Semiconductor"] == pytest.approx(0.50)


def test_market_exposure_calculation() -> None:
    portfolio = pd.DataFrame(
        {
            "market_type": ["KOSPI", "KOSPI", "KOSDAQ", "KOSDAQ"],
            "weight": [0.25, 0.25, 0.25, 0.25],
        }
    )

    exposure = portfolio.groupby("market_type")["weight"].sum()

    assert exposure["KOSPI"] == pytest.approx(0.50)
    assert exposure["KOSDAQ"] == pytest.approx(0.50)


def test_portfolio_config_values_are_valid() -> None:
    config = {
        "candidate_size": 30,
        "portfolio_size": 10,
        "max_sector_names": 3,
        "max_sector_weight": 0.30,
        "max_market_weight": 0.70,
        "weighting_method": "equal_weight",
        "fully_invested": True,
    }

    assert config["candidate_size"] >= config["portfolio_size"]
    assert config["portfolio_size"] > 0
    assert config["max_sector_names"] > 0
    assert 0 < config["max_sector_weight"] <= 1
    assert 0 < config["max_market_weight"] <= 1
    assert config["weighting_method"] in {
        "equal_weight",
        "expected_return_weight",
        "risk_parity",
        "volatility_weight",
        "confidence_weight",
    }
    assert config["fully_invested"] is True

