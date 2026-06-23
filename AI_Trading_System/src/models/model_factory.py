"""Model factory and shared LightGBM utilities."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.utils.config_loader import load_yaml_config
from src.utils.paths import build_project_paths


MODEL_KEYS: tuple[str, ...] = ("ranking_model", "gap_model", "intraday_model")


@dataclass(frozen=True)
class ModelSpec:
    """Configuration needed to train and predict one LightGBM model."""

    key: str
    model_name: str
    target: str
    prediction_column: str
    model_version: str
    params: dict[str, Any]
    categorical_features: list[str]
    excluded_columns: set[str]
    storage_dir: Path


@dataclass(frozen=True)
class TrainedModel:
    """A fitted model and its reproducibility metadata."""

    spec: ModelSpec
    model: Any
    feature_columns: list[str]
    categorical_features: list[str]
    metadata: dict[str, Any]


def load_model_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Load model.yaml and return the model config root."""
    path = Path(config_path) if config_path is not None else build_project_paths().configs / "model.yaml"
    data = load_yaml_config(path)
    if "model" not in data:
        raise ValueError("model.yaml must contain a 'model' root key")
    return data["model"]


def build_model_spec(model_key: str, config: dict[str, Any] | None = None) -> ModelSpec:
    """Build a model specification from config for one model key."""
    model_config = config or load_model_config()
    if model_key not in MODEL_KEYS:
        raise ValueError(f"Unsupported model key: {model_key}")
    section = model_config[model_key]
    if section.get("algorithm") != "lightgbm":
        raise ValueError(f"{model_key} must use LightGBM")

    storage = model_config.get("model_storage", {})
    storage_dir = build_project_paths().root / storage.get(
        f"{model_key}_dir",
        f"outputs/models/{model_key}",
    )
    common = model_config.get("common", {})
    return ModelSpec(
        key=model_key,
        model_name=section["model_name"],
        target=section["target"],
        prediction_column=section["prediction_column"],
        model_version=model_config.get("version", "unknown"),
        params=dict(section.get("lightgbm_params", {})),
        categorical_features=list(common.get("categorical_features", [])),
        excluded_columns=set(common.get("excluded_columns", [])),
        storage_dir=storage_dir,
    )


def get_feature_columns(df: pd.DataFrame, spec: ModelSpec) -> list[str]:
    """Return trainable feature columns, excluding ids, targets, and raw OHLCV."""
    excluded = set(spec.excluded_columns)
    excluded.update({"close_t_minus_1", "previous_close"})
    excluded.update({"model_version", "ranking_score", "pred_gap", "pred_intraday"})
    excluded.update({"pred_open", "pred_close", "expected_return"})
    return [column for column in df.columns if column not in excluded]


def prepare_training_data(
    df: pd.DataFrame,
    spec: ModelSpec,
) -> tuple[pd.DataFrame, pd.Series, list[str], list[str]]:
    """Prepare feature matrix and target vector for LightGBM training."""
    if spec.target not in df.columns:
        raise ValueError(f"Missing target column: {spec.target}")
    train_df = df.dropna(subset=[spec.target]).copy()
    if train_df.empty:
        raise ValueError(f"No rows available after dropping missing target: {spec.target}")

    feature_columns = get_feature_columns(train_df, spec)
    if not feature_columns:
        raise ValueError("No feature columns available for model training")

    x = train_df[feature_columns].copy()
    categorical = [column for column in spec.categorical_features if column in x.columns]
    for column in categorical:
        x[column] = x[column].astype("category")
    y = train_df[spec.target].astype(float)
    return x, y, feature_columns, categorical


def prepare_prediction_data(
    df: pd.DataFrame,
    feature_columns: list[str],
    categorical_features: list[str],
) -> pd.DataFrame:
    """Prepare prediction features in the same column order used for training."""
    missing = set(feature_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing prediction feature columns: {sorted(missing)}")
    x = df.loc[:, feature_columns].copy()
    for column in categorical_features:
        if column in x.columns:
            x[column] = x[column].astype("category")
    return x


def create_lightgbm_regressor(params: dict[str, Any]) -> Any:
    """Create an LGBMRegressor from config parameters."""
    try:
        from lightgbm import LGBMRegressor
    except ImportError as exc:
        raise ImportError(
            "LightGBM is required for Phase 3 model training. Install lightgbm>=4.5.0."
        ) from exc

    constructor_params = dict(params)
    constructor_params.pop("early_stopping_rounds", None)
    return LGBMRegressor(**constructor_params)


def train_model(
    df: pd.DataFrame,
    spec: ModelSpec,
    valid_df: pd.DataFrame | None = None,
) -> TrainedModel:
    """Train one LightGBM regressor and return the fitted model package."""
    x_train, y_train, feature_columns, categorical = prepare_training_data(df, spec)
    model = create_lightgbm_regressor(spec.params)

    fit_kwargs: dict[str, Any] = {"categorical_feature": categorical or "auto"}
    if valid_df is not None and not valid_df.empty:
        valid_clean = valid_df.dropna(subset=[spec.target]).copy()
        if not valid_clean.empty:
            x_valid = prepare_prediction_data(valid_clean, feature_columns, categorical)
            y_valid = valid_clean[spec.target].astype(float)
            fit_kwargs["eval_set"] = [(x_valid, y_valid)]
    model.fit(x_train, y_train, **fit_kwargs)

    metadata = build_model_metadata(spec, df, valid_df, feature_columns, categorical)
    return TrainedModel(
        spec=spec,
        model=model,
        feature_columns=feature_columns,
        categorical_features=categorical,
        metadata=metadata,
    )


def predict_model(trained_model: TrainedModel, df: pd.DataFrame) -> pd.Series:
    """Predict one model output as a float Series."""
    x = prepare_prediction_data(
        df,
        trained_model.feature_columns,
        trained_model.categorical_features,
    )
    predictions = trained_model.model.predict(x)
    return pd.Series(np.asarray(predictions, dtype=float), index=df.index)


def build_model_metadata(
    spec: ModelSpec,
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame | None,
    feature_columns: list[str],
    categorical_features: list[str],
) -> dict[str, Any]:
    """Build model metadata required for reproducibility."""
    train_dates = pd.to_datetime(train_df["date"]) if "date" in train_df.columns else pd.Series(dtype="datetime64[ns]")
    valid_dates = (
        pd.to_datetime(valid_df["date"])
        if valid_df is not None and "date" in valid_df.columns and not valid_df.empty
        else pd.Series(dtype="datetime64[ns]")
    )
    return {
        "model_name": spec.model_name,
        "model_version": spec.model_version,
        "train_start_date": str(train_dates.min()) if not train_dates.empty else None,
        "train_end_date": str(train_dates.max()) if not train_dates.empty else None,
        "validation_start_date": str(valid_dates.min()) if not valid_dates.empty else None,
        "validation_end_date": str(valid_dates.max()) if not valid_dates.empty else None,
        "feature_list": feature_columns,
        "categorical_features": categorical_features,
        "target_name": spec.target,
        "hyperparameters": spec.params,
        "metrics": {},
        "created_at": datetime.now(UTC).isoformat(),
    }


def save_trained_model(trained_model: TrainedModel) -> None:
    """Save a LightGBM model file and metadata JSON."""
    trained_model.spec.storage_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d")
    model_path = trained_model.spec.storage_dir / f"{trained_model.spec.model_name}_{timestamp}.txt"
    metadata_path = (
        trained_model.spec.storage_dir
        / f"{trained_model.spec.model_name}_metadata_{timestamp}.json"
    )
    trained_model.model.booster_.save_model(str(model_path))
    metadata_path.write_text(
        json.dumps(trained_model.metadata, indent=2),
        encoding="utf-8",
    )
