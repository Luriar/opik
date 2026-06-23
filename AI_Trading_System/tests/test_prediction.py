
"""
tests/test_prediction.py

Prediction output tests for AI Trading System v1.0.

Based on:
docs/04_models.md
configs/model.yaml
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


def test_expected_return_formula() -> None:
    pred_gap = pd.Series([0.01, -0.01, 0.02])
    pred_intraday = pd.Series([0.02, 0.01, -0.005])

    expected_return = (1 + pred_gap) * (1 + pred_intraday) - 1

    assert expected_return.iloc[0] == pytest.approx(0.0302)
    assert expected_return.notna().all()


def test_pred_open_formula() -> None:
    close_t_minus_1 = pd.Series([100, 200])
    pred_gap = pd.Series([0.01, -0.02])

    pred_open = close_t_minus_1 * (1 + pred_gap)

    assert pred_open.iloc[0] == pytest.approx(101)
    assert pred_open.iloc[1] == pytest.approx(196)


def test_pred_close_formula() -> None:
    pred_open = pd.Series([101, 196])
    pred_intraday = pd.Series([0.02, -0.01])

    pred_close = pred_open * (1 + pred_intraday)

    assert pred_close.iloc[0] == pytest.approx(103.02)
    assert pred_close.iloc[1] == pytest.approx(194.04)


def test_prediction_output_required_columns() -> None:
    prediction_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02"]),
            "ticker": ["005930"],
            "ranking_score": [0.9],
            "pred_gap": [0.01],
            "pred_intraday": [0.02],
            "pred_open": [101],
            "pred_close": [103.02],
            "expected_return": [0.0302],
            "model_version": ["v1.0"],
        }
    )

    required = {
        "date",
        "ticker",
        "ranking_score",
        "pred_gap",
        "pred_intraday",
        "pred_open",
        "pred_close",
        "expected_return",
        "model_version",
    }

    missing = required - set(prediction_df.columns)
    assert not missing


def test_prediction_values_are_finite() -> None:
    prediction_df = pd.DataFrame(
        {
            "ranking_score": [0.8, 0.7],
            "pred_gap": [0.01, -0.01],
            "pred_intraday": [0.02, 0.00],
            "pred_open": [101, 99],
            "pred_close": [103.02, 99],
            "expected_return": [0.0302, -0.01],
        }
    )

    numeric_cols = [
        "ranking_score",
        "pred_gap",
        "pred_intraday",
        "pred_open",
        "pred_close",
        "expected_return",
    ]

    assert np.isfinite(prediction_df[numeric_cols].to_numpy()).all()


def test_prediction_has_no_target_columns() -> None:
    prediction_columns = {
        "date",
        "ticker",
        "ranking_score",
        "pred_gap",
        "pred_intraday",
        "pred_open",
        "pred_close",
        "expected_return",
    }

    forbidden = {
        "target_rank_return",
        "target_gap",
        "target_intraday",
        "actual_return",
    }

    assert prediction_columns.isdisjoint(forbidden)


def test_prediction_has_unique_date_ticker() -> None:
    prediction_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-02"]),
            "ticker": ["005930", "000660"],
            "ranking_score": [0.9, 0.8],
        }
    )

    assert not prediction_df.duplicated(subset=["date", "ticker"]).any()


def test_duplicate_prediction_date_ticker_should_fail() -> None:
    prediction_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-02"]),
            "ticker": ["005930", "005930"],
            "ranking_score": [0.9, 0.8],
        }
    )

    assert prediction_df.duplicated(subset=["date", "ticker"]).any()


def test_prediction_dates_are_datetime() -> None:
    prediction_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02"]),
            "ticker": ["005930"],
            "ranking_score": [0.9],
        }
    )

    assert pd.api.types.is_datetime64_any_dtype(prediction_df["date"])


def test_prediction_ticker_is_string() -> None:
    prediction_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02"]),
            "ticker": ["005930"],
            "ranking_score": [0.9],
        }
    )

    assert prediction_df["ticker"].map(lambda x: isinstance(x, str)).all()


def test_prediction_model_version_exists() -> None:
    prediction_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02"]),
            "ticker": ["005930"],
            "model_version": ["v1.0"],
        }
    )

    assert prediction_df["model_version"].notna().all()


def test_ranking_score_can_be_sorted() -> None:
    prediction_df = pd.DataFrame(
        {
            "ticker": ["A", "B", "C"],
            "ranking_score": [0.2, 0.9, 0.5],
        }
    )

    sorted_df = prediction_df.sort_values("ranking_score", ascending=False)

    assert sorted_df.iloc[0]["ticker"] == "B"


def test_prediction_output_can_feed_portfolio() -> None:
    prediction_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02"]),
            "ticker": ["005930"],
            "ranking_score": [0.9],
            "pred_gap": [0.01],
            "pred_intraday": [0.02],
            "expected_return": [0.0302],
            "sector": ["Semiconductor"],
            "market_type": ["KOSPI"],
            "market_cap_group": ["Top20"],
            "trading_value_ma20": [10_000_000_000],
            "trading_value_rank_pct": [0.8],
            "atr_percent": [0.03],
            "volatility_20d": [0.03],
        }
    )

    required_for_portfolio = {
        "date",
        "ticker",
        "ranking_score",
        "pred_gap",
        "pred_intraday",
        "expected_return",
        "sector",
        "market_type",
        "market_cap_group",
        "trading_value_ma20",
        "trading_value_rank_pct",
        "atr_percent",
        "volatility_20d",
    }

    missing = required_for_portfolio - set(prediction_df.columns)
    assert not missing


def test_prediction_output_has_audit_dates() -> None:
    prediction_df = pd.DataFrame(
        {
            "feature_date": pd.to_datetime(["2024-01-01"]),
            "target_date": pd.to_datetime(["2024-01-02"]),
        }
    )

    assert (prediction_df["feature_date"] < prediction_df["target_date"]).all()
