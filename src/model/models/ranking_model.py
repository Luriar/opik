"""Ranking model wrapper for target_rank_return."""

from __future__ import annotations

import pandas as pd

from src.model.models.model_factory import TrainedModel, build_model_spec, predict_model, train_model


MODEL_KEY = "ranking_model"


def train_ranking_model(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame | None = None,
) -> TrainedModel:
    """Train the LightGBM ranking regressor."""
    return train_model(train_df, build_model_spec(MODEL_KEY), valid_df)


def predict_ranking_score(model: TrainedModel, feature_df: pd.DataFrame) -> pd.Series:
    """Predict ranking_score for each row."""
    return predict_model(model, feature_df)

