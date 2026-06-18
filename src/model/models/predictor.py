"""Prediction merge utilities for the three Phase 3 models."""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.model.models.model_factory import predict_model
from src.model.models.trainer import ModelBundle


PREDICTION_COLUMNS: tuple[str, ...] = (
    "date",
    "ticker",
    "ranking_score",
    "pred_gap",
    "pred_intraday",
    "pred_open",
    "pred_close",
    "expected_return",
    "model_version",
)


def build_prediction_frame(
    feature_df: pd.DataFrame,
    model_bundle: ModelBundle,
    previous_close_column: str = "close_t_minus_1",
) -> pd.DataFrame:
    """Generate merged Phase 3 prediction output for portfolio input."""
    required = {"date", "ticker", previous_close_column}
    missing = required - set(feature_df.columns)
    if missing:
        raise ValueError(f"Missing prediction input columns: {sorted(missing)}")

    result = feature_df.loc[:, ["date", "ticker", previous_close_column]].copy()
    result["date"] = pd.to_datetime(result["date"])
    result["ticker"] = result["ticker"].astype(str)
    result["ranking_score"] = predict_model(model_bundle.ranking_model, feature_df)
    result["pred_gap"] = predict_model(model_bundle.gap_model, feature_df)
    result["pred_intraday"] = predict_model(model_bundle.intraday_model, feature_df)

    close_t_minus_1 = result[previous_close_column].astype(float)
    result["pred_open"] = close_t_minus_1 * (1 + result["pred_gap"])
    result["pred_close"] = result["pred_open"] * (1 + result["pred_intraday"])
    result["expected_return"] = (1 + result["pred_gap"]) * (1 + result["pred_intraday"]) - 1
    result["model_version"] = model_bundle.ranking_model.spec.model_version

    numeric_cols = [
        "ranking_score",
        "pred_gap",
        "pred_intraday",
        "pred_open",
        "pred_close",
        "expected_return",
    ]
    if not np.isfinite(result[numeric_cols].to_numpy()).all():
        raise ValueError("Prediction output contains non-finite values")
    if result.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("Prediction output contains duplicate date/ticker rows")

    return result.loc[:, PREDICTION_COLUMNS].copy()
