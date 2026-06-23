"""Training orchestration for the three Phase 3 models."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.model.models.model_factory import MODEL_KEYS, TrainedModel, build_model_spec, save_trained_model, train_model
from src.model.utils.logger import get_logger


@dataclass(frozen=True)
class ModelBundle:
    """Container for the three fitted Phase 3 models."""

    ranking_model: TrainedModel
    gap_model: TrainedModel
    intraday_model: TrainedModel


def train_all_models(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame | None = None,
    save_models: bool = False,
) -> ModelBundle:
    """Train ranking, gap, and intraday LightGBM models."""
    logger = get_logger(__name__)
    trained: dict[str, TrainedModel] = {}
    for model_key in MODEL_KEYS:
        spec = build_model_spec(model_key)
        logger.info(
            "model_training_started",
            extra={"step": model_key, "status": "started"},
        )
        trained_model = train_model(train_df, spec, valid_df)
        if save_models:
            save_trained_model(trained_model)
        trained[model_key] = trained_model
        logger.info(
            "model_training_completed",
            extra={"step": model_key, "status": "success"},
        )

    return ModelBundle(
        ranking_model=trained["ranking_model"],
        gap_model=trained["gap_model"],
        intraday_model=trained["intraday_model"],
    )

