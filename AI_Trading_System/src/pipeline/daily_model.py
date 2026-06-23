"""Daily rolling-window model training helpers."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

import pandas as pd

from src.models.model_factory import (
    MODEL_KEYS,
    TrainedModel,
    build_model_spec,
    train_model,
)
from src.models.trainer import ModelBundle
from src.pipeline.config import DailyUpdateConfig
from src.pipeline.rolling_window import get_model_feature_columns, select_rolling_train_window


@dataclass(frozen=True)
class DailyModelTrainingResult:
    """Daily rolling model training result."""

    model_bundle: ModelBundle
    train_df: pd.DataFrame
    feature_columns: list[str]
    train_start_date: str
    train_end_date: str
    rolling_train_days: int
    rolling_train_rows: int
    model_output_dir: str
    model_paths: dict[str, str]


def train_daily_models(
    config: DailyUpdateConfig,
    prediction_date: str | pd.Timestamp,
) -> DailyModelTrainingResult:
    """Train all three models on a pure rolling daily window."""
    training_dataset = pd.read_parquet(config.resolve_path("training_dataset_file"))
    train_df, train_start, train_end, unique_dates, row_count = select_rolling_train_window(
        training_dataset,
        prediction_date,
        config.rolling_train_days,
    )
    feature_columns = get_model_feature_columns(train_df)
    if not feature_columns:
        raise ValueError("No model feature columns selected for daily model training")

    output_dir = config.resolve_path("daily_model_dir") / pd.Timestamp(prediction_date).strftime("%Y%m%d")
    output_dir.mkdir(parents=True, exist_ok=True)
    trained_models: dict[str, TrainedModel] = {}
    model_paths: dict[str, str] = {}
    for model_key in MODEL_KEYS:
        spec = build_daily_model_spec(model_key, train_df, config)
        train_input = train_df.loc[:, [*feature_columns, spec.target]].copy()
        trained_model = train_model(train_input, spec, valid_df=None)
        model_path = output_dir / f"{model_key}.txt"
        trained_model.model.booster_.save_model(str(model_path))
        trained_models[model_key] = trained_model
        model_paths[model_key] = str(model_path)

    return DailyModelTrainingResult(
        model_bundle=ModelBundle(
            ranking_model=trained_models["ranking_model"],
            gap_model=trained_models["gap_model"],
            intraday_model=trained_models["intraday_model"],
        ),
        train_df=train_df,
        feature_columns=feature_columns,
        train_start_date=train_start.date().isoformat(),
        train_end_date=train_end.date().isoformat(),
        rolling_train_days=len(unique_dates),
        rolling_train_rows=int(row_count),
        model_output_dir=str(output_dir),
        model_paths=model_paths,
    )


def build_daily_model_spec(model_key: str, train_df: pd.DataFrame, config: DailyUpdateConfig):
    """Build a capped daily model spec with local target-name compatibility."""
    spec = build_model_spec(model_key)
    target = spec.target
    if model_key == "ranking_model" and target not in train_df.columns and "target_ranking" in train_df.columns:
        target = "target_ranking"
    params = dict(spec.params)
    cap = int(config.values.get("model_n_estimators_cap", params.get("n_estimators", 200)))
    if "n_estimators" in params:
        params["n_estimators"] = min(int(params["n_estimators"]), cap)
    else:
        params["n_estimators"] = cap
    params.pop("early_stopping_rounds", None)
    excluded = set(spec.excluded_columns)
    excluded.update({"target_ranking", "target_rank_return", "target_gap", "target_intraday"})
    return replace(spec, target=target, params=params, excluded_columns=excluded)
