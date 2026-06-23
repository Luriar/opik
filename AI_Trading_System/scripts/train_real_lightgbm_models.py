"""Train real LightGBM models from the production training dataset."""

from __future__ import annotations

import json
import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.model_factory import (  # noqa: E402
    MODEL_KEYS,
    ModelSpec,
    TrainedModel,
    build_model_spec,
    prepare_training_data,
    train_model,
)
from src.models.predictor import build_prediction_frame  # noqa: E402
from src.models.trainer import ModelBundle  # noqa: E402


INPUT_PATH = PROJECT_ROOT / "data" / "processed" / "real_training_dataset.parquet"
MODEL_DIR = PROJECT_ROOT / "outputs" / "models" / "real"
PREDICTION_DIR = PROJECT_ROOT / "outputs" / "predictions" / "real"
METRICS_DIR = PROJECT_ROOT / "outputs" / "metrics"
EXPORT_DIR = PROJECT_ROOT / "data" / "features" / "model_training_real"
REPORT_DIR = PROJECT_ROOT / "reports"

MODEL_PATHS = {
    "ranking_model": MODEL_DIR / "ranking_model.txt",
    "gap_model": MODEL_DIR / "gap_model.txt",
    "intraday_model": MODEL_DIR / "intraday_model.txt",
}
TARGET_BY_MODEL = {
    "ranking_model": "target_ranking",
    "gap_model": "target_gap",
    "intraday_model": "target_intraday",
}
AUDIT_COLUMNS = {"date", "ticker", "feature_date", "target_date", "prediction_horizon", "prev_close"}
TARGET_COLUMNS = {"target_ranking", "target_gap", "target_intraday", "target_rank_return"}
FORBIDDEN_FEATURE_COLUMNS = AUDIT_COLUMNS | TARGET_COLUMNS


def load_training_dataset() -> pd.DataFrame:
    """Load and normalize the real training dataset."""
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Missing real training dataset: {INPUT_PATH}")

    df = pd.read_parquet(INPUT_PATH)
    for column in ["date", "feature_date", "target_date"]:
        df[column] = pd.to_datetime(df[column])
    df["ticker"] = df["ticker"].astype(str).str.zfill(6)
    return df.sort_values(["date", "ticker"]).reset_index(drop=True)


def chronological_split(df: pd.DataFrame, train_ratio: float = 0.8) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split by sorted dates without randomization or shuffling."""
    unique_dates = pd.Series(df["date"].drop_duplicates().sort_values().to_numpy())
    split_index = int(len(unique_dates) * train_ratio)
    if split_index <= 0 or split_index >= len(unique_dates):
        raise ValueError("Chronological split cannot create non-empty train and validation sets")

    cutoff_date = unique_dates.iloc[split_index - 1]
    train_df = df[df["date"] <= cutoff_date].copy()
    valid_df = df[df["date"] > cutoff_date].copy()
    if train_df.empty or valid_df.empty:
        raise ValueError("Chronological split produced an empty partition")
    if train_df["date"].max() >= valid_df["date"].min():
        raise ValueError("Chronological split is not time ordered")
    return train_df, valid_df


def build_real_model_spec(model_key: str) -> ModelSpec:
    """Build a Phase 3 model spec adapted to the real target column names."""
    spec = build_model_spec(model_key)
    excluded = set(spec.excluded_columns) | FORBIDDEN_FEATURE_COLUMNS
    return replace(
        spec,
        target=TARGET_BY_MODEL[model_key],
        excluded_columns=excluded,
        storage_dir=MODEL_DIR,
    )


def save_model_file(trained_model: TrainedModel) -> None:
    """Save a trained LightGBM booster to the exact requested model path."""
    path = MODEL_PATHS[trained_model.spec.key]
    path.parent.mkdir(parents=True, exist_ok=True)
    trained_model.model.booster_.save_model(str(path))


def export_training_inputs(
    train_df: pd.DataFrame,
    specs: dict[str, ModelSpec],
) -> dict[str, dict[str, Any]]:
    """Export exact X/y matrices created by the shared Phase 3 preparation path."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    metadata: dict[str, dict[str, Any]] = {}
    audit_frames = []

    for model_key, spec in specs.items():
        x_train, y_train, feature_columns, categorical = prepare_training_data(train_df, spec)
        model_name = spec.model_name
        x_train.to_csv(EXPORT_DIR / f"{model_name}_X_train.csv", index=False)
        y_train.to_frame(spec.target).to_csv(EXPORT_DIR / f"{model_name}_y_train.csv", index=False)

        audit = train_df.loc[x_train.index, ["date", "ticker"]].copy()
        audit["fold_id"] = "real_train"
        audit["model_name"] = model_name
        audit["target_name"] = spec.target
        audit_frames.append(audit)

        metadata[model_key] = {
            "model_name": model_name,
            "target_name": spec.target,
            "feature_columns": feature_columns,
            "categorical_features": categorical,
            "train_start_date": str(train_df["date"].min().date()),
            "train_end_date": str(train_df["date"].max().date()),
            "row_count": int(x_train.shape[0]),
            "column_count": int(x_train.shape[1]),
            "forbidden_columns_in_X_train": sorted(set(x_train.columns) & FORBIDDEN_FEATURE_COLUMNS),
        }

    pd.concat(audit_frames, ignore_index=True).to_csv(
        EXPORT_DIR / "training_audit.csv",
        index=False,
    )
    (EXPORT_DIR / "feature_metadata.json").write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )
    return metadata


def train_models(train_df: pd.DataFrame, valid_df: pd.DataFrame) -> tuple[ModelBundle, dict[str, ModelSpec]]:
    """Train all real-data models with the existing Phase 3 trainer utility."""
    specs = {model_key: build_real_model_spec(model_key) for model_key in MODEL_KEYS}
    trained: dict[str, TrainedModel] = {}

    for model_key, spec in specs.items():
        trained_model = train_model(train_df, spec, valid_df)
        save_model_file(trained_model)
        trained[model_key] = trained_model

    bundle = ModelBundle(
        ranking_model=trained["ranking_model"],
        gap_model=trained["gap_model"],
        intraday_model=trained["intraday_model"],
    )
    return bundle, specs


def build_validation_predictions(valid_df: pd.DataFrame, bundle: ModelBundle) -> pd.DataFrame:
    """Generate validation predictions using the shared Phase 3 prediction merger."""
    prediction_input = valid_df.copy()
    prediction_input["close_t_minus_1"] = 1.0
    predictions = build_prediction_frame(prediction_input, bundle)
    PREDICTION_DIR.mkdir(parents=True, exist_ok=True)
    predictions.to_parquet(PREDICTION_DIR / "validation_predictions.parquet", index=False)
    predictions.to_csv(PREDICTION_DIR / "validation_predictions.csv", index=False)
    return predictions


def rmse(y_true: pd.Series, y_pred: pd.Series) -> float:
    """Calculate root mean squared error."""
    values = np.asarray(y_true, dtype=float) - np.asarray(y_pred, dtype=float)
    return float(np.sqrt(np.mean(values**2)))


def mae(y_true: pd.Series, y_pred: pd.Series) -> float:
    """Calculate mean absolute error."""
    return float(np.mean(np.abs(np.asarray(y_true, dtype=float) - np.asarray(y_pred, dtype=float))))


def directional_accuracy(y_true: pd.Series, y_pred: pd.Series) -> float:
    """Calculate sign agreement between actual and predicted returns."""
    return float((np.sign(y_true.to_numpy(dtype=float)) == np.sign(y_pred.to_numpy(dtype=float))).mean())


def calculate_metrics(valid_df: pd.DataFrame, predictions: pd.DataFrame) -> dict[str, Any]:
    """Calculate validation metrics for the three model outputs."""
    merged = valid_df[["date", "ticker", "target_ranking", "target_gap", "target_intraday"]].merge(
        predictions,
        on=["date", "ticker"],
        how="inner",
        validate="one_to_one",
    )
    return {
        "ranking_model": {
            "rank_ic_spearman": float(
                merged["ranking_score"].rank().corr(merged["target_ranking"].rank(), method="spearman")
            ),
        },
        "gap_model": {
            "rmse": rmse(merged["target_gap"], merged["pred_gap"]),
            "mae": mae(merged["target_gap"], merged["pred_gap"]),
            "directional_accuracy": directional_accuracy(merged["target_gap"], merged["pred_gap"]),
        },
        "intraday_model": {
            "rmse": rmse(merged["target_intraday"], merged["pred_intraday"]),
            "mae": mae(merged["target_intraday"], merged["pred_intraday"]),
            "directional_accuracy": directional_accuracy(
                merged["target_intraday"],
                merged["pred_intraday"],
            ),
        },
        "prediction_row_count": int(len(predictions)),
    }


def build_report(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    feature_metadata: dict[str, dict[str, Any]],
    metrics: dict[str, Any],
) -> str:
    """Render the Markdown report for real model training."""
    feature_count = feature_metadata["ranking_model"]["column_count"]
    model_paths = "\n".join(f"- `{path.relative_to(PROJECT_ROOT)}`" for path in MODEL_PATHS.values())
    return "\n".join(
        [
            "# Real Model Training Report",
            "",
            f"Created at: {datetime.now(UTC).isoformat()}",
            "",
            "## Dataset",
            f"- Train date range: {train_df['date'].min().date()} to {train_df['date'].max().date()}",
            f"- Validation date range: {valid_df['date'].min().date()} to {valid_df['date'].max().date()}",
            f"- Train rows: {len(train_df)}",
            f"- Validation rows: {len(valid_df)}",
            f"- Train unique tickers: {train_df['ticker'].nunique()}",
            f"- Validation unique tickers: {valid_df['ticker'].nunique()}",
            f"- Feature count: {feature_count}",
            f"- 005930 included: {bool((train_df['ticker'].eq('005930')).any() or (valid_df['ticker'].eq('005930')).any())}",
            "",
            "## Metrics",
            f"- Ranking Rank IC/Spearman: {metrics['ranking_model']['rank_ic_spearman']:.8f}",
            f"- Gap RMSE: {metrics['gap_model']['rmse']:.8f}",
            f"- Gap MAE: {metrics['gap_model']['mae']:.8f}",
            f"- Gap directional accuracy: {metrics['gap_model']['directional_accuracy']:.8f}",
            f"- Intraday RMSE: {metrics['intraday_model']['rmse']:.8f}",
            f"- Intraday MAE: {metrics['intraday_model']['mae']:.8f}",
            f"- Intraday directional accuracy: {metrics['intraday_model']['directional_accuracy']:.8f}",
            f"- Prediction row count: {metrics['prediction_row_count']}",
            "",
            "## Model Files",
            model_paths,
            "",
            "## Audit",
            "- Chronological split only; no random split and no shuffle.",
            "- date, ticker, feature_date, target_date, prediction_horizon, and target columns are excluded from X_train.",
            "- Prediction pred_open and pred_close are normalized to previous close = 1.0 because the optimized training dataset does not store raw previous close prices.",
            "",
        ]
    )


def main() -> None:
    """Train real models, export matrices, predictions, metrics, and report."""
    print("Loading real training dataset...")
    dataset = load_training_dataset()
    train_df, valid_df = chronological_split(dataset)
    print(f"Train shape: {train_df.shape}")
    print(f"Validation shape: {valid_df.shape}")

    print("Preparing model specs and exporting exact training matrices...")
    specs = {model_key: build_real_model_spec(model_key) for model_key in MODEL_KEYS}
    feature_metadata = export_training_inputs(train_df, specs)
    forbidden = {
        key: value["forbidden_columns_in_X_train"]
        for key, value in feature_metadata.items()
        if value["forbidden_columns_in_X_train"]
    }
    if forbidden:
        raise ValueError(f"Forbidden columns found in X_train: {forbidden}")

    print("Training real LightGBM models...")
    bundle, _ = train_models(train_df, valid_df)

    print("Generating validation predictions...")
    predictions = build_validation_predictions(valid_df, bundle)
    metrics = calculate_metrics(valid_df, predictions)

    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    metrics_payload = {
        "created_at": datetime.now(UTC).isoformat(),
        "train_date_range": [str(train_df["date"].min().date()), str(train_df["date"].max().date())],
        "validation_date_range": [str(valid_df["date"].min().date()), str(valid_df["date"].max().date())],
        "train_rows": int(len(train_df)),
        "validation_rows": int(len(valid_df)),
        "feature_count": int(feature_metadata["ranking_model"]["column_count"]),
        "unique_tickers": int(dataset["ticker"].nunique()),
        "ticker_005930_included": bool(dataset["ticker"].eq("005930").any()),
        "model_files": {key: str(path) for key, path in MODEL_PATHS.items()},
        "metrics": metrics,
    }
    (METRICS_DIR / "real_model_metrics.json").write_text(
        json.dumps(metrics_payload, indent=2),
        encoding="utf-8",
    )
    (REPORT_DIR / "real_model_training_report.md").write_text(
        build_report(train_df, valid_df, feature_metadata, metrics),
        encoding="utf-8",
    )

    print("Real LightGBM training complete")
    print(f"Ranking X_train shape: ({feature_metadata['ranking_model']['row_count']}, {feature_metadata['ranking_model']['column_count']})")
    print(f"Gap X_train shape: ({feature_metadata['gap_model']['row_count']}, {feature_metadata['gap_model']['column_count']})")
    print(f"Intraday X_train shape: ({feature_metadata['intraday_model']['row_count']}, {feature_metadata['intraday_model']['column_count']})")
    print(f"Validation prediction shape: {predictions.shape}")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
