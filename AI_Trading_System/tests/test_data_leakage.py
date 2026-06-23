
# tests/test_data_leakage.py

# Data leakage tests for AI Trading System v1.0.

# These tests are based on: docs/06_data_leakage_rules.md

# Core principle:    Feature date = T-1    Target date  = T

# No feature may use Open(T), High(T), Low(T), Close(T), Volume(T), target values, actual returns, or any future information.


from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

TARGET_COLUMNS = {
    "target_gap",
    "target_intraday",
    "target_rank_return",
}

FORBIDDEN_FEATURE_PATTERNS = [
    r"^target_",
    r"future",
    r"next_",
    r"_future",
    r"_next",
    r"actual",
    r"realized",
    r"label",
    r"y_",
]

FORBIDDEN_T_COLUMNS = {
    "open_t",
    "high_t",
    "low_t",
    "close_t",
    "volume_t",
    "trading_value_t",
}


# ---------------------------------------------------------------------
# Mock fixtures
# Replace these fixtures with real project functions when implemented.
# ---------------------------------------------------------------------

@pytest.fixture
def sample_feature_list() -> list[str]:
    return [
        "return_5d",
        "return_20d",
        "close_ma20_ratio",
        "momentum_rank_pct",
        "relative_trading_value",
        "atr_percent",
        "bb_position",
        "breakout_rank_pct",
        "nasdaq_return_1d",
        "sox_return_1d",
        "usdkrw_return_1d",
        "sector",
        "market_type",
        "market_cap_group",
    ]


@pytest.fixture
def sample_dataset() -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=10, freq="B")

    rows = []
    for ticker in ["AAA", "BBB", "CCC"]:
        close = np.arange(100, 110, dtype=float)
        open_ = close - 0.5
        high = close + 1
        low = close - 1
        volume = np.arange(1000, 1010, dtype=float)

        for i, date in enumerate(dates):
            rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "open": open_[i],
                    "high": high[i],
                    "low": low[i],
                    "close": close[i],
                    "volume": volume[i],
                    "return_5d": np.nan if i <= 5 else close[i - 1] / close[i - 6] - 1,
                    "target_gap": np.nan if i == len(dates) - 1 else open_[i + 1] / close[i] - 1,
                    "target_intraday": np.nan if i == len(dates) - 1 else close[i + 1] / open_[i + 1] - 1,
                    "target_rank_return": np.nan if i == len(dates) - 1 else close[i + 1] / close[i] - 1,
                }
            )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------

def _matches_any_pattern(value: str, patterns: Iterable[str]) -> bool:
    value_lower = value.lower()
    return any(re.search(pattern, value_lower) for pattern in patterns)


def assert_no_forbidden_feature_names(feature_list: list[str]) -> None:
    forbidden = []

    for feature in feature_list:
        if feature in TARGET_COLUMNS:
            forbidden.append(feature)
        elif feature.lower() in FORBIDDEN_T_COLUMNS:
            forbidden.append(feature)
        elif _matches_any_pattern(feature, FORBIDDEN_FEATURE_PATTERNS):
            forbidden.append(feature)

    assert not forbidden, f"Forbidden feature names found: { forbidden }"


def assert_time_ordered_split(
    train: pd.DataFrame,
    valid: pd.DataFrame,
    test: pd.DataFrame,
    date_col: str = "date",
) -> None:
    train_max = train[date_col].max()
    valid_min = valid[date_col].min()
    valid_max = valid[date_col].max()
    test_min = test[date_col].min()

    assert train_max < valid_min, (
        f"Train max date {train_max} must be before validation min date {valid_min}"
    )
    assert valid_max < test_min, (
        f"Validation max date {valid_max} must be before test min date {test_min}"
    )


# ---------------------------------------------------------------------
# Test 1. Target columns must not be in feature list
# ---------------------------------------------------------------------

def test_target_columns_are_not_in_feature_list(sample_feature_list: list[str]) -> None:
    overlap = set(sample_feature_list) & TARGET_COLUMNS
    assert not overlap, f"Target columns must not be used as features: {overlap}"


# ---------------------------------------------------------------------
# Test 2. No future-looking feature names
# ---------------------------------------------------------------------

def test_no_future_like_feature_names(sample_feature_list: list[str]) -> None:
    assert_no_forbidden_feature_names(sample_feature_list)


# ---------------------------------------------------------------------
# Test 3. Feature columns must not contain explicit T-day OHLCV columns
# ---------------------------------------------------------------------

def test_no_t_day_ohlcv_columns_in_feature_list(sample_feature_list: list[str]) -> None:
    forbidden = set(sample_feature_list) & FORBIDDEN_T_COLUMNS
    assert not forbidden, f"T-day OHLCV columns are forbidden as features: {forbidden}"


# ---------------------------------------------------------------------
# Test 4. Target generation sanity check
# ---------------------------------------------------------------------

def test_target_generation_uses_next_day_values(sample_dataset: pd.DataFrame) -> None:
    df = sample_dataset.sort_values(["ticker", "date"]).copy()

    for _, g in df.groupby("ticker"):
        expected_gap = g["open"].shift(-1) / g["close"] - 1
        expected_intraday = g["close"].shift(-1) / g["open"].shift(-1) - 1
        expected_rank_return = g["close"].shift(-1) / g["close"] - 1

        np.testing.assert_allclose(
            g["target_gap"].values,
            expected_gap.values,
            equal_nan=True,
        )
        np.testing.assert_allclose(
            g["target_intraday"].values,
            expected_intraday.values,
            equal_nan=True,
        )
        np.testing.assert_allclose(
            g["target_rank_return"].values,
            expected_rank_return.values,
            equal_nan=True,
        )


# ---------------------------------------------------------------------
# Test 5. Rolling feature should use lagged data
# ---------------------------------------------------------------------

def test_return_5d_uses_lagged_data(sample_dataset: pd.DataFrame) -> None:
    df = sample_dataset.sort_values(["ticker", "date"]).copy()

    for _, g in df.groupby("ticker"):
        expected = g["close"].shift(1) / g["close"].shift(6) - 1

        np.testing.assert_allclose(
            g["return_5d"].values,
            expected.values,
            equal_nan=True,
        )


# ---------------------------------------------------------------------
# Test 6. Cross-sectional rank must be grouped by date
# ---------------------------------------------------------------------

def test_cross_sectional_rank_is_grouped_by_date() -> None:
    df = pd.DataFrame(
        {
            "date": [
                "2024-01-01",
                "2024-01-01",
                "2024-01-01",
                "2024-01-02",
                "2024-01-02",
                "2024-01-02",
            ],
            "ticker": ["A", "B", "C", "A", "B", "C"],
            "return_5d": [0.01, 0.03, 0.02, -0.01, 0.02, 0.04],
        }
    )

    df["return_5d_rank_pct"] = (
        df.groupby("date")["return_5d"].rank(pct=True)
    )

    for _, g in df.groupby("date"):
        assert g["return_5d_rank_pct"].between(0, 1).all()
        assert g["return_5d_rank_pct"].max() == 1.0


# ---------------------------------------------------------------------
# Test 7. Time ordered split
# ---------------------------------------------------------------------

def test_train_valid_test_split_is_time_ordered() -> None:
    dates = pd.date_range("2020-01-01", periods=100, freq="B")
    df = pd.DataFrame({"date": dates, "x": range(len(dates))})

    train = df[df["date"] < "2020-03-01"]
    valid = df[(df["date"] >= "2020-03-01") & (df["date"] < "2020-04-01")]
    test = df[df["date"] >= "2020-04-01"]

    assert_time_ordered_split(train, valid, test)


# ---------------------------------------------------------------------
# Test 8. Random shuffled split must be rejected
# ---------------------------------------------------------------------

def test_random_shuffled_split_is_not_time_ordered() -> None:
    dates = pd.date_range("2020-01-01", periods=100, freq="B")
    df = pd.DataFrame({"date": dates, "x": range(len(dates))})

    shuffled = df.sample(frac=1, random_state=42).reset_index(drop=True)

    train = shuffled.iloc[:60]
    valid = shuffled.iloc[60:80]
    test = shuffled.iloc[80:]

    with pytest.raises(AssertionError):
        assert_time_ordered_split(train, valid, test)


# ---------------------------------------------------------------------
# Test 9. Scaler must be fit on train only
# ---------------------------------------------------------------------

@dataclass
class MockScalerAudit:
    fit_start_date: pd.Timestamp
    fit_end_date: pd.Timestamp


def test_scaler_fit_period_must_not_exceed_train_period() -> None:
    train_end = pd.Timestamp("2022-12-31")

    scaler_audit = MockScalerAudit(
        fit_start_date=pd.Timestamp("2020-01-01"),
        fit_end_date=pd.Timestamp("2022-12-31"),
    )

    assert scaler_audit.fit_end_date <= train_end


def test_scaler_fit_on_full_dataset_should_fail() -> None:
    train_end = pd.Timestamp("2022-12-31")

    scaler_audit = MockScalerAudit(
        fit_start_date=pd.Timestamp("2020-01-01"),
        fit_end_date=pd.Timestamp("2024-12-31"),
    )

    assert scaler_audit.fit_end_date > train_end


# ---------------------------------------------------------------------
# Test 10. Portfolio selection must not use actual returns or targets
# ---------------------------------------------------------------------

def test_portfolio_selection_columns_do_not_include_actual_or_target() -> None:
    selection_columns = [
        "date",
        "ticker",
        "ranking_score",
        "pred_gap",
        "pred_intraday",
        "expected_return",
        "atr_percent",
        "sector",
    ]

    assert_no_forbidden_feature_names(selection_columns)


def test_portfolio_selection_using_target_should_fail() -> None:
    selection_columns = [
        "date",
        "ticker",
        "ranking_score",
        "target_intraday",
    ]

    with pytest.raises(AssertionError):
        assert_no_forbidden_feature_names(selection_columns)


# ---------------------------------------------------------------------
# Test 11. Feature date must be before target date
# ---------------------------------------------------------------------

def test_feature_date_is_before_target_date() -> None:
    df = pd.DataFrame(
        {
            "feature_date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "target_date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
        }
    )

    assert (df["feature_date"] < df["target_date"]).all()


def test_feature_date_equal_to_target_date_should_fail() -> None:
    df = pd.DataFrame(
        {
            "feature_date": pd.to_datetime(["2024-01-01"]),
            "target_date": pd.to_datetime(["2024-01-01"]),
        }
    )

    assert not (df["feature_date"] < df["target_date"]).all()


# ---------------------------------------------------------------------
# Test 12. Required leakage audit columns exist
# ---------------------------------------------------------------------

def test_prediction_dataset_has_audit_columns() -> None:
    prediction_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02"]),
            "ticker": ["AAA"],
            "feature_date": pd.to_datetime(["2024-01-01"]),
            "target_date": pd.to_datetime(["2024-01-02"]),
            "ranking_score": [0.8],
            "pred_gap": [0.01],
            "pred_intraday": [0.02],
        }
    )

    required = {"date", "ticker", "feature_date", "target_date"}
    missing = required - set(prediction_df.columns)

    assert not missing, f"Missing leakage audit columns: {missing}"
    assert (prediction_df["feature_date"] < prediction_df["target_date"]).all()

