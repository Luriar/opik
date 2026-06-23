"""Fold-level model retraining utilities."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.model.models.predictor import build_prediction_frame
from src.model.models.trainer import ModelBundle, train_all_models
from src.model.validation.fold_generator import WalkForwardFold, split_by_fold
from src.model.validation.prediction_merger import add_fold_id


@dataclass(frozen=True)
class FoldRunResult:
    """Output from one walk-forward fold run."""

    fold: WalkForwardFold
    model_bundle: ModelBundle
    predictions: pd.DataFrame


def retrain_for_fold(
    df: pd.DataFrame,
    fold: WalkForwardFold,
    date_column: str = "date",
    previous_close_column: str = "close_t_minus_1",
    save_models: bool = False,
) -> FoldRunResult:
    """Train models on one fold and predict the fold test period."""
    train_df, valid_df, test_df = split_by_fold(df, fold, date_column=date_column)
    model_bundle = train_all_models(train_df, valid_df=valid_df, save_models=save_models)
    predictions = build_prediction_frame(
        test_df,
        model_bundle,
        previous_close_column=previous_close_column,
    )
    predictions = _attach_targets(add_fold_id(predictions, fold.fold_id), test_df)
    return FoldRunResult(fold=fold, model_bundle=model_bundle, predictions=predictions)


def _attach_targets(predictions: pd.DataFrame, test_df: pd.DataFrame) -> pd.DataFrame:
    result = predictions.copy()
    target_columns = ["target_rank_return", "target_gap", "target_intraday"]
    for column in target_columns:
        if column in test_df.columns:
            result[column] = test_df[column].to_numpy()
    return result

