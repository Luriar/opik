"""Walk-forward validation runner."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from src.utils.logger import get_logger
from src.utils.paths import build_project_paths
from src.validation.fold_generator import (
    WalkForwardFold,
    generate_walk_forward_folds,
    load_validation_config,
    save_fold_metadata,
)
from src.validation.prediction_merger import aggregate_fold_predictions, save_predictions
from src.validation.retrainer import FoldRunResult, retrain_for_fold


def run_walk_forward_training(
    df: pd.DataFrame,
    folds: list[WalkForwardFold] | None = None,
    save_outputs: bool = True,
    previous_close_column: str = "close_t_minus_1",
) -> pd.DataFrame:
    """Run expanding-window walk-forward training and aggregate predictions."""
    logger = get_logger(__name__)
    validation_config = load_validation_config()
    run_folds = folds or generate_walk_forward_folds(validation_config)
    fold_results: list[FoldRunResult] = []

    if save_outputs:
        save_fold_metadata(run_folds, build_project_paths().root / validation_config["output"]["fold_file"])

    for fold in run_folds:
        logger.info(
            "walk_forward_fold_started",
            extra={"step": f"fold_{fold.fold_id}", "status": "started"},
        )
        fold_result = retrain_for_fold(
            df,
            fold,
            date_column=validation_config.get("date_column", "date"),
            previous_close_column=previous_close_column,
        )
        fold_results.append(fold_result)
        if save_outputs and validation_config["output"].get("save_fold_predictions", True):
            _save_fold_predictions(fold_result, validation_config)
        logger.info(
            "walk_forward_fold_completed",
            extra={"step": f"fold_{fold.fold_id}", "status": "success"},
        )

    aggregated = aggregate_fold_predictions([result.predictions for result in fold_results])
    if save_outputs and validation_config["output"].get("save_aggregated_predictions", True):
        save_predictions(
            aggregated,
            build_project_paths().root / validation_config["output"]["aggregated_predictions_file"],
        )
        _save_run_metadata(fold_results, validation_config)
    return aggregated


def _save_fold_predictions(
    fold_result: FoldRunResult,
    validation_config: dict[str, object],
) -> Path:
    output = validation_config["output"]
    if not isinstance(output, dict):
        raise ValueError("validation output config must be a mapping")
    predictions_dir = build_project_paths().root / str(output["predictions_dir"])
    predictions_dir.mkdir(parents=True, exist_ok=True)
    path = predictions_dir / f"fold_{fold_result.fold.fold_id:03d}_predictions.parquet"
    fold_result.predictions.to_parquet(path, index=False)
    return path


def _save_run_metadata(
    fold_results: list[FoldRunResult],
    validation_config: dict[str, object],
) -> Path:
    output = validation_config["output"]
    if not isinstance(output, dict):
        raise ValueError("validation output config must be a mapping")
    metrics_dir = build_project_paths().root / str(output["metrics_dir"])
    metrics_dir.mkdir(parents=True, exist_ok=True)
    path = metrics_dir / "walk_forward_metadata.json"
    metadata = {
        "fold_count": len(fold_results),
        "folds": [asdict(result.fold) for result in fold_results],
    }
    path.write_text(json.dumps(metadata, default=str, indent=2), encoding="utf-8")
    return path

