"""Gap model wrapper for target_gap."""

from __future__ import annotations

import pandas as pd

from src.models.model_factory import TrainedModel, build_model_spec, predict_model, train_model


MODEL_KEY = "gap_model"


def train_gap_model(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame | None = None,
) -> TrainedModel:
    """Train the LightGBM gap regressor."""
    return train_model(train_df, build_model_spec(MODEL_KEY), valid_df)


def predict_gap(model: TrainedModel, feature_df: pd.DataFrame) -> pd.Series:
    """Predict pred_gap for each row."""
    return predict_model(model, feature_df)

