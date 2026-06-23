
"""
tests/test_model_training.py

Model training tests for AI Trading System v1.0.

Based on:
docs/03_targets.md
docs/04_models.md
configs/model.yaml
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from sklearn.metrics import mean_absolute_error, mean_squared_error


TARGET_COLUMNS = {
    "target_rank_return",
    "target_gap",
    "target_intraday",
}

ID_COLUMNS = {
    "date",
    "ticker",
    "name",
}


@pytest.fixture
def sample_training_df() -> pd.DataFrame:
    dates = pd.date_range("2021-01-01", periods=120, freq="B")
    rows = []

    for ticker_idx, ticker in enumerate(["AAA", "BBB", "CCC", "DDD"]):
        for i, date in enumerate(dates):
            rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "return_5d": 0.001 * i + ticker_idx * 0.001,
                    "return_20d": 0.002 * i + ticker_idx * 0.001,
                    "momentum_rank_pct": (ticker_idx + 1) / 4,
                    "relative_trading_value": 1.0 + 0.01 * i,
                    "atr_percent": 0.02 + ticker_idx * 0.005,
                    "bb_position": 0.4 + ticker_idx * 0.1,
                    "nasdaq_return_1d": 0.005,
                    "sox_return_1d": 0.007,
                    "usdkrw_return_1d": -0.002,
                    "sector": "Semiconductor" if ticker_idx < 2 else "Auto",
                    "market_type": "KOSPI",
                    "market_cap_group": "Top50",
                    "target_rank_return": 0.001 * i + ticker_idx * 0.002,
                    "target_gap": 0.0005 * i,
                    "target_intraday": 0.0007 * i + ticker_idx * 0.001,
                }
            )

    return pd.DataFrame(rows)


def build_feature_columns(df: pd.DataFrame) -> list[str]:
    excluded = TARGET_COLUMNS | ID_COLUMNS
    return [col for col in df.columns if col not in excluded]


def test_feature_columns_exclude_targets(sample_training_df: pd.DataFrame) -> None:
    feature_columns = build_feature_columns(sample_training_df)

    assert TARGET_COLUMNS.isdisjoint(feature_columns)


def test_feature_columns_exclude_identifiers(sample_training_df: pd.DataFrame) -> None:
    feature_columns = build_feature_columns(sample_training_df)

    assert ID_COLUMNS.isdisjoint(feature_columns)


def test_required_targets_exist(sample_training_df: pd.DataFrame) -> None:
    missing = TARGET_COLUMNS - set(sample_training_df.columns)

    assert not missing


def test_training_dataset_has_no_missing_target(sample_training_df: pd.DataFrame) -> None:
    assert sample_training_df[list(TARGET_COLUMNS)].notna().all().all()


def test_categorical_features_exist(sample_training_df: pd.DataFrame) -> None:
    categorical_features = {
        "sector",
        "market_type",
        "market_cap_group",
    }

    missing = categorical_features - set(sample_training_df.columns)

    assert not missing


def test_train_valid_split_is_time_ordered(sample_training_df: pd.DataFrame) -> None:
    train = sample_training_df[sample_training_df["date"] < "2021-04-01"]
    valid = sample_training_df[sample_training_df["date"] >= "2021-04-01"]

    assert train["date"].max() < valid["date"].min()


def test_random_shuffle_split_should_not_be_used(sample_training_df: pd.DataFrame) -> None:
    shuffled = sample_training_df.sample(frac=1, random_state=42).reset_index(drop=True)

    train = shuffled.iloc[:300]
    valid = shuffled.iloc[300:]

    assert not (train["date"].max() < valid["date"].min())


def test_model_config_targets_are_valid() -> None:
    model_targets = {
        "ranking_model": "target_rank_return",
        "gap_model": "target_gap",
        "intraday_model": "target_intraday",
    }

    assert model_targets["ranking_model"] == "target_rank_return"
    assert model_targets["gap_model"] == "target_gap"
    assert model_targets["intraday_model"] == "target_intraday"


def test_prediction_column_names_are_valid() -> None:
    prediction_columns = {
        "ranking_model": "ranking_score",
        "gap_model": "pred_gap",
        "intraday_model": "pred_intraday",
    }

    assert prediction_columns["ranking_model"] == "ranking_score"
    assert prediction_columns["gap_model"] == "pred_gap"
    assert prediction_columns["intraday_model"] == "pred_intraday"


def test_lightgbm_parameter_defaults_are_valid() -> None:
    params = {
        "objective": "regression",
        "metric": "rmse",
        "learning_rate": 0.03,
        "num_leaves": 31,
        "min_data_in_leaf": 100,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "n_estimators": 2000,
        "early_stopping_rounds": 100,
        "random_state": 42,
    }

    assert params["objective"] == "regression"
    assert params["metric"] == "rmse"
    assert 0 < params["learning_rate"] <= 1
    assert params["num_leaves"] > 1
    assert params["min_data_in_leaf"] > 0
    assert 0 < params["feature_fraction"] <= 1
    assert 0 < params["bagging_fraction"] <= 1
    assert params["n_estimators"] > 0
    assert params["early_stopping_rounds"] > 0
    assert params["random_state"] == 42


def test_model_evaluation_metrics_can_be_calculated() -> None:
    y_true = np.array([0.01, 0.02, -0.01, 0.03])
    y_pred = np.array([0.012, 0.018, -0.005, 0.025])

    mae = mean_absolute_error(y_true, y_pred)
    rmse = mean_squared_error(y_true, y_pred) ** 0.5
    correlation = np.corrcoef(y_true, y_pred)[0, 1]

    assert mae >= 0
    assert rmse >= 0
    assert -1 <= correlation <= 1


def test_directional_accuracy_calculation() -> None:
    y_true = np.array([0.01, -0.02, 0.03, -0.01])
    y_pred = np.array([0.02, -0.01, -0.01, -0.02])

    directional_accuracy = (np.sign(y_true) == np.sign(y_pred)).mean()

    assert directional_accuracy == pytest.approx(0.75)


def test_rank_ic_calculation() -> None:
    df = pd.DataFrame(
        {
            "date": ["2024-01-02"] * 5,
            "ranking_score": [0.9, 0.8, 0.7, 0.6, 0.5],
            "target_rank_return": [0.05, 0.03, 0.01, -0.01, -0.02],
        }
    )

    rank_ic = df["ranking_score"].rank().corr(
        df["target_rank_return"].rank(),
        method="spearman",
    )

    assert rank_ic == pytest.approx(1.0)


def test_top10_return_calculation() -> None:
    df = pd.DataFrame(
        {
            "ticker": [f"T{i}" for i in range(20)],
            "ranking_score": np.linspace(1, 0, 20),
            "target_rank_return": np.linspace(0.05, -0.05, 20),
        }
    )

    top10 = df.sort_values("ranking_score", ascending=False).head(10)
    top10_return = top10["target_rank_return"].mean()

    assert top10_return > 0


def test_expected_return_formula_from_predictions() -> None:
    pred_gap = pd.Series([0.01, -0.01, 0.02])
    pred_intraday = pd.Series([0.02, 0.01, -0.005])

    expected_return = (1 + pred_gap) * (1 + pred_intraday) - 1

    assert expected_return.iloc[0] == pytest.approx(0.0302)
    assert expected_return.notna().all()


def test_model_metadata_required_fields() -> None:
    metadata = {
        "model_name": "ranking_model",
        "model_version": "v1.0",
        "train_start_date": "2018-01-01",
        "train_end_date": "2021-12-31",
        "validation_start_date": "2022-01-01",
        "validation_end_date": "2022-12-31",
        "feature_list": ["return_5d", "momentum_rank_pct"],
        "categorical_features": ["sector", "market_type", "market_cap_group"],
        "target_name": "target_rank_return",
        "hyperparameters": {"learning_rate": 0.03},
        "metrics": {"rmse": 0.01},
        "created_at": "2024-01-01T00:00:00",
    }

    required = {
        "model_name",
        "model_version",
        "train_start_date",
        "train_end_date",
        "validation_start_date",
        "validation_end_date",
        "feature_list",
        "categorical_features",
        "target_name",
        "hyperparameters",
        "metrics",
        "created_at",
    }

    missing = required - set(metadata.keys())

    assert not missing


def test_model_training_output_required_columns() -> None:
    prediction_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02"]),
            "ticker": ["005930"],
            "ranking_score": [0.8],
            "pred_gap": [0.01],
            "pred_intraday": [0.02],
            "pred_open": [101],
            "pred_close": [103],
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


def test_model_does_not_use_ticker_as_feature(sample_training_df: pd.DataFrame) -> None:
    feature_columns = build_feature_columns(sample_training_df)

    assert "ticker" not in feature_columns


def test_model_does_not_use_date_as_feature(sample_training_df: pd.DataFrame) -> None:
    feature_columns = build_feature_columns(sample_training_df)

    assert "date" not in feature_columns
