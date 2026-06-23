"""Intraday model wrapper for target_intraday."""

from __future__ import annotations

import pandas as pd

from src.models.model_factory import TrainedModel, build_model_spec, predict_model, train_model


MODEL_KEY = "intraday_model"


def train_intraday_model(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame | None = None,
) -> TrainedModel:
    """Train the LightGBM intraday regressor."""
    return train_model(train_df, build_model_spec(MODEL_KEY), valid_df)


def predict_intraday(model: TrainedModel, feature_df: pd.DataFrame) -> pd.Series:
    """Predict pred_intraday for each row."""
    return predict_model(model, feature_df)

