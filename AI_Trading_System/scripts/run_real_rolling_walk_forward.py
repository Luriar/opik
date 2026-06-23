"""Run real rolling-window daily walk-forward validation."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = PROJECT_ROOT / ".cache" / "matplotlib"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(CACHE_DIR))
os.environ.setdefault("LOKY_MAX_CPU_COUNT", "4")

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.model_factory import MODEL_KEYS, ModelSpec, build_model_spec, predict_model, train_model  # noqa: E402


INPUT_PATH = PROJECT_ROOT / "data" / "processed" / "real_training_dataset.parquet"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "walk_forward_real_rolling"
REPORT_PATH = PROJECT_ROOT / "reports" / "real_rolling_walk_forward_report.md"

TRAIN_START_DATE = pd.Timestamp("2024-07-01")
PREDICTION_START_POLICY_DATE = pd.Timestamp("2026-03-01")
TARGET_BY_MODEL = {
    "ranking_model": "target_ranking",
    "gap_model": "target_gap",
    "intraday_model": "target_intraday",
}
AUDIT_COLUMNS = {"date", "ticker", "feature_date", "target_date", "prediction_horizon", "prev_close"}
TARGET_COLUMNS = {"target_ranking", "target_gap", "target_intraday", "target_rank_return"}
FORBIDDEN_FEATURE_COLUMNS = AUDIT_COLUMNS | TARGET_COLUMNS

# Daily retraining is intentionally expensive. This keeps the real walk-forward
# run practical while still using the Phase 3 LightGBM training path.
MAX_N_ESTIMATORS = int(os.environ.get("REAL_ROLLING_WF_MAX_N_ESTIMATORS", "300"))


def load_dataset() -> pd.DataFrame:
    """Load and normalize the real training dataset."""
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Missing real training dataset: {INPUT_PATH}")
    df = pd.read_parquet(INPUT_PATH)
    for column in ["date", "feature_date", "target_date"]:
        df[column] = pd.to_datetime(df[column])
    df["ticker"] = df["ticker"].astype(str).str.zfill(6)
    if "prev_close" not in df.columns:
        raise ValueError("real_training_dataset.parquet must include prev_close")
    return df.sort_values(["feature_date", "ticker"]).reset_index(drop=True)


def build_real_model_spec(model_key: str) -> ModelSpec:
    """Build a Phase 3 model spec for real rolling targets."""
    spec = build_model_spec(model_key)
    params = dict(spec.params)
    if MAX_N_ESTIMATORS > 0:
        params["n_estimators"] = min(int(params.get("n_estimators", MAX_N_ESTIMATORS)), MAX_N_ESTIMATORS)
    excluded = set(spec.excluded_columns) | FORBIDDEN_FEATURE_COLUMNS
    return replace(
        spec,
        target=TARGET_BY_MODEL[model_key],
        params=params,
        excluded_columns=excluded,
        storage_dir=OUTPUT_DIR / "models",
    )


def validation_dates(df: pd.DataFrame) -> list[pd.Timestamp]:
    """Return available validation feature dates from the policy start date onward."""
    dates = pd.Series(df["feature_date"].drop_duplicates().sort_values().to_numpy())
    selected = dates[dates >= PREDICTION_START_POLICY_DATE]
    if selected.empty:
        raise ValueError("No validation feature dates found at or after 2026-03-01")
    return [pd.Timestamp(value) for value in selected]


def feature_checks(specs: dict[str, ModelSpec], train_df: pd.DataFrame) -> dict[str, Any]:
    """Validate that forbidden audit and target fields are not model features."""
    from src.models.model_factory import prepare_training_data

    checks: dict[str, Any] = {}
    for model_key, spec in specs.items():
        x_train, _, feature_columns, categorical = prepare_training_data(train_df, spec)
        forbidden = sorted(set(feature_columns) & FORBIDDEN_FEATURE_COLUMNS)
        checks[model_key] = {
            "feature_count": len(feature_columns),
            "categorical_features": categorical,
            "forbidden_columns": forbidden,
            "prev_close_in_features": "prev_close" in feature_columns,
            "target_columns_in_features": sorted(set(feature_columns) & TARGET_COLUMNS),
            "x_train_shape": [int(x_train.shape[0]), int(x_train.shape[1])],
        }
    return checks


def train_and_predict_day(
    df: pd.DataFrame,
    prediction_date: pd.Timestamp,
    specs: dict[str, ModelSpec],
    fold_id: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Train all models with prior data and predict one validation feature date."""
    train_df = df[
        (df["feature_date"] >= TRAIN_START_DATE)
        & (df["feature_date"] < prediction_date)
    ].copy()
    predict_df = df[df["feature_date"] == prediction_date].copy()
    if train_df.empty:
        raise ValueError(f"No train rows for prediction date {prediction_date.date()}")
    if predict_df.empty:
        raise ValueError(f"No prediction rows for prediction date {prediction_date.date()}")

    checks = feature_checks(specs, train_df)
    forbidden = {
        model_key: check["forbidden_columns"]
        for model_key, check in checks.items()
        if check["forbidden_columns"]
    }
    if forbidden:
        raise ValueError(f"Forbidden model feature columns for {prediction_date.date()}: {forbidden}")

    trained = {model_key: train_model(train_df, spec, valid_df=None) for model_key, spec in specs.items()}
    predictions = predict_df.loc[
        :,
        [
            "date",
            "ticker",
            "feature_date",
            "target_date",
            "prev_close",
            "target_ranking",
            "target_gap",
            "target_intraday",
        ],
    ].copy()
    predictions["ranking_score"] = predict_model(trained["ranking_model"], predict_df)
    predictions["pred_gap"] = predict_model(trained["gap_model"], predict_df)
    predictions["pred_intraday"] = predict_model(trained["intraday_model"], predict_df)
    predictions["expected_return"] = (1 + predictions["pred_gap"]) * (1 + predictions["pred_intraday"]) - 1
    predictions["pred_open_price"] = predictions["prev_close"] * (1 + predictions["pred_gap"])
    predictions["pred_close_price"] = predictions["pred_open_price"] * (1 + predictions["pred_intraday"])
    predictions["model_version"] = trained["ranking_model"].spec.model_version
    predictions["train_start_date"] = train_df["feature_date"].min()
    predictions["train_end_date"] = train_df["feature_date"].max()
    predictions["prediction_date"] = prediction_date
    predictions["fold_id"] = fold_id

    leakage_ok = bool(train_df["feature_date"].max() < prediction_date)
    metadata = {
        "fold_id": fold_id,
        "prediction_date": prediction_date.date().isoformat(),
        "target_date_min": predictions["target_date"].min().date().isoformat(),
        "target_date_max": predictions["target_date"].max().date().isoformat(),
        "train_start_date": train_df["feature_date"].min().date().isoformat(),
        "train_end_date": train_df["feature_date"].max().date().isoformat(),
        "train_rows": int(len(train_df)),
        "prediction_rows": int(len(predictions)),
        "unique_tickers": int(predictions["ticker"].nunique()),
        "max_train_feature_date_lt_prediction_date": leakage_ok,
        "max_train_feature_date": train_df["feature_date"].max().date().isoformat(),
        "feature_checks": checks,
    }
    return predictions, metadata


def rmse(y_true: pd.Series, y_pred: pd.Series) -> float:
    """Calculate root mean squared error."""
    delta = np.asarray(y_true, dtype=float) - np.asarray(y_pred, dtype=float)
    return float(np.sqrt(np.mean(delta**2)))


def mae(y_true: pd.Series, y_pred: pd.Series) -> float:
    """Calculate mean absolute error."""
    return float(np.mean(np.abs(np.asarray(y_true, dtype=float) - np.asarray(y_pred, dtype=float))))


def directional_accuracy(y_true: pd.Series, y_pred: pd.Series) -> float:
    """Calculate sign agreement."""
    return float((np.sign(y_true.to_numpy(dtype=float)) == np.sign(y_pred.to_numpy(dtype=float))).mean())


def rank_ic(y_true: pd.Series, y_score: pd.Series) -> float:
    """Calculate Spearman rank correlation."""
    if y_true.nunique(dropna=True) < 2 or y_score.nunique(dropna=True) < 2:
        return float("nan")
    return float(y_score.rank().corr(y_true.rank(), method="spearman"))


def metrics_for_frame(df: pd.DataFrame) -> dict[str, float]:
    """Calculate ranking, gap, intraday, and expected-return metrics."""
    return {
        "ranking_rank_ic_spearman": rank_ic(df["target_ranking"], df["ranking_score"]),
        "gap_rmse": rmse(df["target_gap"], df["pred_gap"]),
        "gap_mae": mae(df["target_gap"], df["pred_gap"]),
        "gap_directional_accuracy": directional_accuracy(df["target_gap"], df["pred_gap"]),
        "intraday_rmse": rmse(df["target_intraday"], df["pred_intraday"]),
        "intraday_mae": mae(df["target_intraday"], df["pred_intraday"]),
        "intraday_directional_accuracy": directional_accuracy(df["target_intraday"], df["pred_intraday"]),
        "expected_return_rank_ic_spearman": rank_ic(df["target_ranking"], df["expected_return"]),
    }


def build_daily_metrics(predictions: pd.DataFrame) -> pd.DataFrame:
    """Calculate one metrics row per prediction date."""
    rows = []
    for prediction_date, group in predictions.groupby("prediction_date", sort=True):
        row = {
            "prediction_date": pd.Timestamp(prediction_date).date().isoformat(),
            "prediction_rows": int(len(group)),
            "unique_tickers": int(group["ticker"].nunique()),
        }
        row.update(metrics_for_frame(group))
        rows.append(row)
    return pd.DataFrame(rows)


def build_overall_metrics(
    predictions: pd.DataFrame,
    daily_metrics: pd.DataFrame,
    fold_metadata: pd.DataFrame,
) -> dict[str, Any]:
    """Build serializable overall metrics and leakage audit."""
    duplicated = int(predictions.duplicated(subset=["date", "ticker"]).sum())
    no_forbidden_features = all(
        not check["forbidden_columns"]
        for fold_checks in fold_metadata["feature_checks"]
        for check in fold_checks.values()
    )
    leakage_ok = bool(fold_metadata["max_train_feature_date_lt_prediction_date"].all())
    metrics = metrics_for_frame(predictions)
    metrics.update(
        {
            "number_of_prediction_dates": int(predictions["prediction_date"].nunique()),
            "total_prediction_rows": int(len(predictions)),
            "unique_tickers": int(predictions["ticker"].nunique()),
            "ticker_005930_exists": bool(predictions["ticker"].eq("005930").any()),
            "actual_first_prediction_date": str(predictions["prediction_date"].min().date()),
            "actual_last_prediction_date": str(predictions["prediction_date"].max().date()),
            "duplicate_date_ticker_predictions": duplicated,
            "leakage_checks": {
                "max_train_feature_date_lt_prediction_date": leakage_ok,
                "no_duplicated_date_ticker_predictions": duplicated == 0,
                "prev_close_not_in_model_features": no_forbidden_features,
                "target_columns_not_in_model_features": no_forbidden_features,
            },
            "daily_metrics_mean": {
                column: float(daily_metrics[column].mean())
                for column in daily_metrics.columns
                if column not in {"prediction_date"}
            },
        }
    )
    return metrics


def prediction_columns() -> list[str]:
    """Return final prediction output column order."""
    return [
        "date",
        "ticker",
        "ranking_score",
        "pred_gap",
        "pred_intraday",
        "expected_return",
        "prev_close",
        "pred_open_price",
        "pred_close_price",
        "target_ranking",
        "target_gap",
        "target_intraday",
        "model_version",
        "train_start_date",
        "train_end_date",
        "prediction_date",
        "feature_date",
        "target_date",
        "fold_id",
    ]


def render_report(
    metrics: dict[str, Any],
    fold_metadata: pd.DataFrame,
    max_n_estimators: int,
) -> str:
    """Render Markdown summary for the rolling real walk-forward run."""
    leakage = metrics["leakage_checks"]
    return "\n".join(
        [
            "# Real Rolling Walk-Forward Report",
            "",
            f"Created at: {datetime.now(UTC).isoformat()}",
            "",
            "## Date Policy",
            f"- Rolling train window start: {TRAIN_START_DATE.date()}",
            "- Rolling train window end for first prediction: last available feature_date before first prediction",
            f"- Validation policy start: {PREDICTION_START_POLICY_DATE.date()}",
            f"- Actual first prediction date: {metrics['actual_first_prediction_date']}",
            f"- Actual last prediction date: {metrics['actual_last_prediction_date']}",
            f"- Daily retrains: {metrics['number_of_prediction_dates']}",
            f"- Runtime LightGBM n_estimators cap: {max_n_estimators}",
            "",
            "## Output Shape",
            f"- Prediction rows: {metrics['total_prediction_rows']}",
            f"- Unique tickers: {metrics['unique_tickers']}",
            f"- 005930 exists: {metrics['ticker_005930_exists']}",
            "",
            "## Overall Metrics",
            f"- Ranking Rank IC / Spearman: {metrics['ranking_rank_ic_spearman']:.8f}",
            f"- Gap RMSE: {metrics['gap_rmse']:.8f}",
            f"- Gap MAE: {metrics['gap_mae']:.8f}",
            f"- Gap directional accuracy: {metrics['gap_directional_accuracy']:.8f}",
            f"- Intraday RMSE: {metrics['intraday_rmse']:.8f}",
            f"- Intraday MAE: {metrics['intraday_mae']:.8f}",
            f"- Intraday directional accuracy: {metrics['intraday_directional_accuracy']:.8f}",
            f"- Expected return Rank IC / Spearman: {metrics['expected_return_rank_ic_spearman']:.8f}",
            "",
            "## Leakage Checks",
            f"- max(train feature_date) < prediction_date: {leakage['max_train_feature_date_lt_prediction_date']}",
            f"- no duplicated date/ticker predictions: {leakage['no_duplicated_date_ticker_predictions']}",
            f"- prev_close not in model features: {leakage['prev_close_not_in_model_features']}",
            f"- target columns not in model features: {leakage['target_columns_not_in_model_features']}",
            "",
            "## Files",
            "- `outputs/walk_forward_real_rolling/predictions.csv`",
            "- `outputs/walk_forward_real_rolling/predictions.parquet`",
            "- `outputs/walk_forward_real_rolling/metrics.json`",
            "- `outputs/walk_forward_real_rolling/daily_metrics.csv`",
            "- `outputs/walk_forward_real_rolling/fold_metadata.csv`",
            "",
            "## Fold Metadata Preview",
            "",
            fold_metadata.drop(columns=["feature_checks"]).head(10).to_markdown(index=False),
            "",
        ]
    )


def main() -> None:
    """Run daily rolling real-data walk-forward validation."""
    print("Loading real training dataset...")
    df = load_dataset()
    dates = validation_dates(df)
    specs = {model_key: build_real_model_spec(model_key) for model_key in MODEL_KEYS}
    print(f"Prediction dates: {len(dates)}")
    print(f"First prediction date: {dates[0].date()}")
    print(f"Last prediction date: {dates[-1].date()}")
    print(f"LightGBM n_estimators cap: {MAX_N_ESTIMATORS}")

    prediction_frames = []
    metadata_rows = []
    for fold_id, prediction_date in enumerate(dates, start=1):
        print(f"[{fold_id}/{len(dates)}] training through prior date for {prediction_date.date()}...")
        predictions, metadata = train_and_predict_day(df, prediction_date, specs, fold_id)
        prediction_frames.append(predictions)
        metadata_rows.append(metadata)

    predictions = pd.concat(prediction_frames, ignore_index=True)
    predictions = predictions.loc[:, prediction_columns()].sort_values(["prediction_date", "ticker"])
    if predictions.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("Duplicate date/ticker predictions found")

    fold_metadata = pd.DataFrame(metadata_rows)
    daily_metrics = build_daily_metrics(predictions)
    metrics = build_overall_metrics(predictions, daily_metrics, fold_metadata)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(OUTPUT_DIR / "predictions.csv", index=False)
    predictions.to_parquet(OUTPUT_DIR / "predictions.parquet", index=False)
    daily_metrics.to_csv(OUTPUT_DIR / "daily_metrics.csv", index=False)
    fold_metadata.to_csv(OUTPUT_DIR / "fold_metadata.csv", index=False)
    (OUTPUT_DIR / "metrics.json").write_text(
        json.dumps(
            {
                "created_at": datetime.now(UTC).isoformat(),
                "train_start_date_policy": TRAIN_START_DATE.date().isoformat(),
                "validation_start_policy": PREDICTION_START_POLICY_DATE.date().isoformat(),
                "max_n_estimators": MAX_N_ESTIMATORS,
                "metrics": metrics,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    REPORT_PATH.write_text(render_report(metrics, fold_metadata, MAX_N_ESTIMATORS), encoding="utf-8")

    print("Real rolling walk-forward complete")
    print(f"Prediction shape: {predictions.shape}")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
