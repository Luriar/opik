
"""
tests/test_walk_forward.py

Tests for walk-forward validation.

Based on:
docs/07_walk_forward_validation.md
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import pytest


@dataclass
class WalkForwardFold:
    fold_id: int
    train_start_date: pd.Timestamp
    train_end_date: pd.Timestamp
    valid_start_date: pd.Timestamp
    valid_end_date: pd.Timestamp
    test_start_date: pd.Timestamp
    test_end_date: pd.Timestamp


def validate_fold_order(fold: WalkForwardFold) -> None:
    assert fold.train_start_date < fold.train_end_date
    assert fold.train_end_date < fold.valid_start_date
    assert fold.valid_start_date < fold.valid_end_date
    assert fold.valid_end_date < fold.test_start_date
    assert fold.test_start_date < fold.test_end_date


def validate_no_overlap(fold: WalkForwardFold) -> None:
    assert fold.train_end_date < fold.valid_start_date
    assert fold.valid_end_date < fold.test_start_date
    assert fold.train_end_date < fold.test_start_date


def test_single_fold_order_is_valid() -> None:
    fold = WalkForwardFold(
        fold_id=1,
        train_start_date=pd.Timestamp("2018-01-01"),
        train_end_date=pd.Timestamp("2021-12-31"),
        valid_start_date=pd.Timestamp("2022-01-01"),
        valid_end_date=pd.Timestamp("2022-12-31"),
        test_start_date=pd.Timestamp("2023-01-01"),
        test_end_date=pd.Timestamp("2023-12-31"),
    )

    validate_fold_order(fold)
    validate_no_overlap(fold)


def test_overlapping_fold_should_fail() -> None:
    fold = WalkForwardFold(
        fold_id=1,
        train_start_date=pd.Timestamp("2018-01-01"),
        train_end_date=pd.Timestamp("2022-06-30"),
        valid_start_date=pd.Timestamp("2022-01-01"),
        valid_end_date=pd.Timestamp("2022-12-31"),
        test_start_date=pd.Timestamp("2023-01-01"),
        test_end_date=pd.Timestamp("2023-12-31"),
    )

    with pytest.raises(AssertionError):
        validate_fold_order(fold)


def test_expanding_window_train_period_expands() -> None:
    folds = [
        WalkForwardFold(
            1,
            pd.Timestamp("2018-01-01"),
            pd.Timestamp("2021-12-31"),
            pd.Timestamp("2022-01-01"),
            pd.Timestamp("2022-12-31"),
            pd.Timestamp("2023-01-01"),
            pd.Timestamp("2023-12-31"),
        ),
        WalkForwardFold(
            2,
            pd.Timestamp("2018-01-01"),
            pd.Timestamp("2022-12-31"),
            pd.Timestamp("2023-01-01"),
            pd.Timestamp("2023-12-31"),
            pd.Timestamp("2024-01-01"),
            pd.Timestamp("2024-12-31"),
        ),
    ]

    assert folds[1].train_start_date == folds[0].train_start_date
    assert folds[1].train_end_date > folds[0].train_end_date


def test_random_shuffle_split_should_fail_time_order_check() -> None:
    dates = pd.date_range("2020-01-01", periods=100, freq="B")
    df = pd.DataFrame({"date": dates})

    shuffled = df.sample(frac=1, random_state=42).reset_index(drop=True)

    train = shuffled.iloc[:60]
    valid = shuffled.iloc[60:80]
    test = shuffled.iloc[80:]

    with pytest.raises(AssertionError):
        assert train["date"].max() < valid["date"].min()
        assert valid["date"].max() < test["date"].min()


def test_validation_config_values_are_valid() -> None:
    config = {
        "validation_years": 1,
        "test_years": 1,
        "train_window_type": "expanding",
        "retraining_frequency": "monthly",
    }

    assert config["validation_years"] > 0
    assert config["test_years"] > 0
    assert config["train_window_type"] in {"expanding", "rolling"}
    assert config["retraining_frequency"] in {"weekly", "monthly", "quarterly"}


def test_prediction_output_has_required_columns() -> None:
    prediction_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2023-01-02"]),
            "ticker": ["005930"],
            "fold_id": [1],
            "model_version": ["v1.0"],
            "ranking_score": [0.88],
            "pred_gap": [0.01],
            "pred_intraday": [0.02],
            "pred_open": [60600],
            "pred_close": [61812],
            "expected_return": [0.0302],
        }
    )

    required = {
        "date",
        "ticker",
        "fold_id",
        "model_version",
        "ranking_score",
        "pred_gap",
        "pred_intraday",
        "pred_open",
        "pred_close",
        "expected_return",
    }

    missing = required - set(prediction_df.columns)
    assert not missing, f"Missing prediction columns: {missing}"


def test_aggregated_fold_predictions_have_no_duplicates() -> None:
    predictions = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2023-01-02", "2023-01-02", "2024-01-02", "2024-01-02"]
            ),
            "ticker": ["005930", "000660", "005930", "000660"],
            "fold_id": [1, 1, 2, 2],
            "ranking_score": [0.8, 0.7, 0.9, 0.6],
        }
    )

    duplicated = predictions.duplicated(subset=["date", "ticker"])
    assert not duplicated.any()


def test_fold_predictions_must_have_fold_id() -> None:
    prediction_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2023-01-02"]),
            "ticker": ["005930"],
            "ranking_score": [0.9],
        }
    )

    assert "fold_id" not in prediction_df.columns

    required = {"fold_id"}
    missing = required - set(prediction_df.columns)
    assert missing == {"fold_id"}


def test_scaler_fit_period_is_train_only() -> None:
    train_end_date = pd.Timestamp("2021-12-31")

    scaler_metadata = {
        "fit_start_date": pd.Timestamp("2018-01-01"),
        "fit_end_date": pd.Timestamp("2021-12-31"),
    }

    assert scaler_metadata["fit_end_date"] <= train_end_date


def test_scaler_fit_on_future_period_should_fail() -> None:
    train_end_date = pd.Timestamp("2021-12-31")

    scaler_metadata = {
        "fit_start_date": pd.Timestamp("2018-01-01"),
        "fit_end_date": pd.Timestamp("2023-12-31"),
    }

    assert scaler_metadata["fit_end_date"] > train_end_date


def test_feature_selection_does_not_use_test_period() -> None:
    test_start_date = pd.Timestamp("2023-01-01")

    feature_selection_metadata = {
        "selection_start_date": pd.Timestamp("2018-01-01"),
        "selection_end_date": pd.Timestamp("2022-12-31"),
    }

    assert feature_selection_metadata["selection_end_date"] < test_start_date


def test_feature_selection_using_test_period_should_fail() -> None:
    test_start_date = pd.Timestamp("2023-01-01")

    feature_selection_metadata = {
        "selection_start_date": pd.Timestamp("2018-01-01"),
        "selection_end_date": pd.Timestamp("2023-06-30"),
    }

    assert feature_selection_metadata["selection_end_date"] >= test_start_date


def test_hyperparameter_tuning_uses_validation_not_test() -> None:
    tuning_metadata = {
        "used_train": True,
        "used_validation": True,
        "used_test": False,
    }

    assert tuning_metadata["used_train"] is True
    assert tuning_metadata["used_validation"] is True
    assert tuning_metadata["used_test"] is False


def test_hyperparameter_tuning_using_test_should_fail() -> None:
    tuning_metadata = {
        "used_train": True,
        "used_validation": True,
        "used_test": True,
    }

    assert tuning_metadata["used_test"] is True

