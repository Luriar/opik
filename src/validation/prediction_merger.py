"""Walk-forward prediction aggregation utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.paths import build_project_paths
from src.validation.fold_generator import load_validation_config


def add_fold_id(predictions: pd.DataFrame, fold_id: int) -> pd.DataFrame:
    """Attach fold_id to one fold prediction DataFrame."""
    result = predictions.copy()
    result["fold_id"] = int(fold_id)
    return result


def validate_prediction_columns(
    predictions: pd.DataFrame,
    required_columns: list[str] | None = None,
) -> None:
    """Validate walk-forward prediction columns and unique date/ticker rows."""
    required = required_columns or load_validation_config().get("required_prediction_columns", [])
    missing = set(required) - set(predictions.columns)
    if missing:
        raise ValueError(f"Missing walk-forward prediction columns: {sorted(missing)}")
    if predictions.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("Aggregated predictions contain duplicate date/ticker rows")


def aggregate_fold_predictions(
    fold_predictions: list[pd.DataFrame],
    required_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Concatenate fold predictions, sort by date/ticker, and reject duplicates."""
    if not fold_predictions:
        raise ValueError("At least one fold prediction DataFrame is required")
    result = pd.concat(fold_predictions, ignore_index=True)
    result["date"] = pd.to_datetime(result["date"])
    result["ticker"] = result["ticker"].astype(str)
    result = result.sort_values(["date", "ticker", "fold_id"]).reset_index(drop=True)
    validate_prediction_columns(result, required_columns)
    return result


def save_predictions(
    predictions: pd.DataFrame,
    output_path: str | Path | None = None,
) -> Path:
    """Save aggregated predictions to parquet."""
    path = Path(output_path) if output_path is not None else _default_aggregated_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    predictions.to_parquet(path, index=False)
    return path


def _default_aggregated_path() -> Path:
    config = load_validation_config()
    return build_project_paths().root / config["output"]["aggregated_predictions_file"]


def required_prediction_columns(config: dict[str, Any] | None = None) -> list[str]:
    """Return required walk-forward prediction columns from config."""
    validation_config = config or load_validation_config()
    return list(validation_config.get("required_prediction_columns", []))

