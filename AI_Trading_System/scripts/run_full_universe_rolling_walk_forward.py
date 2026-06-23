"""Run full-universe daily rolling walk-forward validation and reports."""

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


INPUT_PATH = PROJECT_ROOT / "data" / "processed" / "full_universe_training_dataset.parquet"
TICKER_NAMES_PATH = PROJECT_ROOT / "data" / "metadata" / "ticker_names.csv"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "walk_forward_full_universe_rolling"
REPORT_PATH = PROJECT_ROOT / "reports" / "full_universe_rolling_walk_forward_report.md"
READABLE_CSV = PROJECT_ROOT / "reports" / "full_universe_validation_predictions_readable.csv"
READABLE_XLSX = PROJECT_ROOT / "reports" / "full_universe_validation_predictions_readable.xlsx"
TOP10_CSV = PROJECT_ROOT / "reports" / "full_universe_validation_top10_by_date.csv"
TOP10_XLSX = PROJECT_ROOT / "reports" / "full_universe_validation_top10_by_date.xlsx"
SUMMARY_MD = PROJECT_ROOT / "reports" / "full_universe_validation_predictions_summary.md"

TRAIN_START_DATE = pd.Timestamp("2024-07-01")
VALIDATION_START_DATE = pd.Timestamp("2026-03-01")
VALIDATION_END_DATE = pd.Timestamp("2026-06-12")
SAMSUNG = "005930"
MAX_N_ESTIMATORS = int(os.environ.get("FULL_UNIVERSE_WF_MAX_N_ESTIMATORS", "200"))

TARGET_BY_MODEL = {
    "ranking_model": "target_ranking",
    "gap_model": "target_gap",
    "intraday_model": "target_intraday",
}
EXCLUDED_COLUMNS = {
    "date",
    "ticker",
    "ticker_name",
    "feature_date",
    "target_date",
    "prediction_horizon",
    "prev_close",
    "target_ranking",
    "target_gap",
    "target_intraday",
    "target_rank_return",
    "sector",
    "market_type",
    "market_cap_group",
}
TARGET_COLUMNS = {"target_ranking", "target_gap", "target_intraday", "target_rank_return"}
FINAL_PREDICTION_COLUMNS = [
    "date",
    "ticker",
    "ticker_name",
    "ranking_score",
    "expected_return",
    "pred_gap",
    "pred_intraday",
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
]
PCT_COLUMNS = {
    "ranking_score": "ranking_score_pct",
    "expected_return": "expected_return_pct",
    "pred_gap": "pred_gap_pct",
    "pred_intraday": "pred_intraday_pct",
}


def load_dataset() -> pd.DataFrame:
    """Load and normalize full-universe training data."""
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Missing full-universe training dataset: {INPUT_PATH}")
    df = pd.read_parquet(INPUT_PATH)
    for column in ["date", "feature_date", "target_date"]:
        df[column] = pd.to_datetime(df[column])
    df["ticker"] = df["ticker"].astype(str).str.zfill(6)
    required = {"date", "ticker", "feature_date", "target_date", "prev_close", *TARGET_BY_MODEL.values()}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Training dataset is missing columns: {sorted(missing)}")
    return df.sort_values(["feature_date", "ticker"]).reset_index(drop=True)


def load_ticker_names() -> dict[str, str]:
    """Load offline ticker name mapping."""
    if not TICKER_NAMES_PATH.exists():
        return {}
    mapping = pd.read_csv(TICKER_NAMES_PATH, dtype=str, encoding="utf-8-sig")
    mapping["ticker"] = mapping["ticker"].astype(str).str.zfill(6)
    mapping["ticker_name"] = mapping["ticker_name"].fillna("UNKNOWN").astype(str)
    return dict(zip(mapping["ticker"], mapping["ticker_name"], strict=False))


def add_ticker_names(df: pd.DataFrame, names: dict[str, str]) -> pd.DataFrame:
    """Attach ticker_name, using UNKNOWN for unmapped tickers."""
    result = df.copy()
    result["ticker_name"] = result["ticker"].map(names).fillna("UNKNOWN")
    return result


def build_spec(model_key: str) -> ModelSpec:
    """Build real full-universe model spec with strict excluded columns."""
    spec = build_model_spec(model_key)
    params = dict(spec.params)
    if MAX_N_ESTIMATORS > 0:
        params["n_estimators"] = min(int(params.get("n_estimators", MAX_N_ESTIMATORS)), MAX_N_ESTIMATORS)
    return replace(
        spec,
        target=TARGET_BY_MODEL[model_key],
        params=params,
        categorical_features=[],
        excluded_columns=set(spec.excluded_columns) | EXCLUDED_COLUMNS,
        storage_dir=OUTPUT_DIR / "models",
    )


def validation_dates(df: pd.DataFrame) -> list[pd.Timestamp]:
    """Return available prediction feature dates within the requested validation window."""
    dates = pd.Series(df["feature_date"].drop_duplicates().sort_values().to_numpy())
    selected = dates[(dates >= VALIDATION_START_DATE) & (dates <= VALIDATION_END_DATE)]
    if selected.empty:
        raise ValueError("No validation feature dates found in requested range")
    return [pd.Timestamp(value) for value in selected]


def feature_checks(train_df: pd.DataFrame, specs: dict[str, ModelSpec]) -> dict[str, Any]:
    """Verify forbidden columns are not used by Phase 3 model preparation."""
    from src.models.model_factory import prepare_training_data

    checks = {}
    for key, spec in specs.items():
        x_train, _, feature_columns, _ = prepare_training_data(train_df, spec)
        checks[key] = {
            "feature_count": len(feature_columns),
            "x_train_shape": [int(x_train.shape[0]), int(x_train.shape[1])],
            "forbidden_columns": sorted(set(feature_columns) & EXCLUDED_COLUMNS),
            "target_columns": sorted(set(feature_columns) & TARGET_COLUMNS),
            "prev_close_in_features": "prev_close" in feature_columns,
        }
    return checks


def train_predict_day(
    df: pd.DataFrame,
    prediction_date: pd.Timestamp,
    specs: dict[str, ModelSpec],
    fold_id: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Retrain the three models and predict one feature date."""
    train_df = df[(df["feature_date"] >= TRAIN_START_DATE) & (df["feature_date"] < prediction_date)].copy()
    predict_df = df[df["feature_date"] == prediction_date].copy()
    if train_df.empty or predict_df.empty:
        raise ValueError(f"Empty train or prediction data for {prediction_date.date()}")
    checks = feature_checks(train_df, specs)
    forbidden = {key: value for key, value in checks.items() if value["forbidden_columns"] or value["target_columns"]}
    if forbidden:
        raise ValueError(f"Forbidden model features for {prediction_date.date()}: {forbidden}")

    trained = {key: train_model(train_df, spec, valid_df=None) for key, spec in specs.items()}
    predictions = predict_df.loc[
        :,
        ["date", "ticker", "ticker_name", "prev_close", "target_ranking", "target_gap", "target_intraday"],
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

    metadata = {
        "fold_id": fold_id,
        "prediction_date": prediction_date.date().isoformat(),
        "train_start_date": train_df["feature_date"].min().date().isoformat(),
        "train_end_date": train_df["feature_date"].max().date().isoformat(),
        "train_rows": int(len(train_df)),
        "prediction_rows": int(len(predictions)),
        "unique_tickers": int(predictions["ticker"].nunique()),
        "max_train_feature_date_lt_prediction_date": bool(train_df["feature_date"].max() < prediction_date),
        "feature_checks": checks,
    }
    return predictions.loc[:, FINAL_PREDICTION_COLUMNS + ["fold_id"]], metadata


def rmse(y_true: pd.Series, y_pred: pd.Series) -> float:
    """Return RMSE."""
    diff = np.asarray(y_true, dtype=float) - np.asarray(y_pred, dtype=float)
    return float(np.sqrt(np.mean(diff**2)))


def mae(y_true: pd.Series, y_pred: pd.Series) -> float:
    """Return MAE."""
    return float(np.mean(np.abs(np.asarray(y_true, dtype=float) - np.asarray(y_pred, dtype=float))))


def directional_accuracy(y_true: pd.Series, y_pred: pd.Series) -> float:
    """Return sign agreement."""
    return float((np.sign(y_true.to_numpy(dtype=float)) == np.sign(y_pred.to_numpy(dtype=float))).mean())


def rank_ic(y_true: pd.Series, y_score: pd.Series) -> float:
    """Return Spearman rank IC."""
    if y_true.nunique(dropna=True) < 2 or y_score.nunique(dropna=True) < 2:
        return float("nan")
    return float(y_score.rank().corr(y_true.rank(), method="spearman"))


def metrics_for_frame(df: pd.DataFrame) -> dict[str, float]:
    """Calculate requested metrics for a prediction frame."""
    return {
        "ranking_rank_ic": rank_ic(df["target_ranking"], df["ranking_score"]),
        "expected_return_rank_ic": rank_ic(df["target_ranking"], df["expected_return"]),
        "gap_rmse": rmse(df["target_gap"], df["pred_gap"]),
        "gap_mae": mae(df["target_gap"], df["pred_gap"]),
        "gap_directional_accuracy": directional_accuracy(df["target_gap"], df["pred_gap"]),
        "intraday_rmse": rmse(df["target_intraday"], df["pred_intraday"]),
        "intraday_mae": mae(df["target_intraday"], df["pred_intraday"]),
        "intraday_directional_accuracy": directional_accuracy(df["target_intraday"], df["pred_intraday"]),
    }


def build_daily_metrics(predictions: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily metrics."""
    rows = []
    for date, group in predictions.groupby("prediction_date", sort=True):
        row = {
            "prediction_date": pd.Timestamp(date).date().isoformat(),
            "prediction_rows": int(len(group)),
            "unique_tickers": int(group["ticker"].nunique()),
        }
        row.update(metrics_for_frame(group))
        rows.append(row)
    return pd.DataFrame(rows)


def build_metrics(predictions: pd.DataFrame, daily_metrics: pd.DataFrame, fold_metadata: pd.DataFrame) -> dict[str, Any]:
    """Build overall metrics and leakage checks."""
    duplicated = int(predictions.duplicated(subset=["date", "ticker"]).sum())
    no_forbidden = all(
        not check["forbidden_columns"] and not check["target_columns"] and not check["prev_close_in_features"]
        for checks in fold_metadata["feature_checks"]
        for check in checks.values()
    )
    metrics = metrics_for_frame(predictions)
    metrics.update(
        {
            "prediction_dates": int(predictions["prediction_date"].nunique()),
            "prediction_rows": int(len(predictions)),
            "unique_tickers": int(predictions["ticker"].nunique()),
            "ticker_005930_exists": bool(predictions["ticker"].eq(SAMSUNG).any()),
            "first_prediction_date": str(predictions["prediction_date"].min().date()),
            "last_prediction_date": str(predictions["prediction_date"].max().date()),
            "duplicate_date_ticker_predictions": duplicated,
            "leakage_checks": {
                "max_train_feature_date_lt_prediction_date": bool(
                    fold_metadata["max_train_feature_date_lt_prediction_date"].all()
                ),
                "no_duplicate_date_ticker": duplicated == 0,
                "no_forbidden_model_features": no_forbidden,
            },
            "daily_metrics_mean": {
                column: float(daily_metrics[column].mean())
                for column in daily_metrics.columns
                if column != "prediction_date"
            },
        }
    )
    return metrics


def format_pct(value: float | int | None) -> str:
    """Format decimal as percentage string."""
    if pd.isna(value):
        return ""
    return f"{float(value) * 100:.2f}%"


def make_readable(predictions: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Create readable full and top10 prediction reports."""
    readable = predictions.loc[:, FINAL_PREDICTION_COLUMNS].copy()
    for source, target in PCT_COLUMNS.items():
        readable[target] = readable[source].map(format_pct)
    ordered = [
        "date",
        "ticker",
        "ticker_name",
        "ranking_score",
        "ranking_score_pct",
        "expected_return",
        "expected_return_pct",
        "pred_gap",
        "pred_gap_pct",
        "pred_intraday",
        "pred_intraday_pct",
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
    ]
    readable = readable.loc[:, ordered].sort_values(["date", "ranking_score"], ascending=[True, False])
    top10 = readable.copy()
    top10["rank"] = top10.groupby("date")["ranking_score"].rank(method="first", ascending=False).astype(int)
    top10 = top10[top10["rank"] <= 10].copy()
    top10 = top10.loc[:, ["date", "rank"] + [column for column in ordered if column != "date"]]
    mapping = {
        "mapped_unique_tickers": int(readable.loc[readable["ticker_name"].ne("UNKNOWN"), "ticker"].nunique()),
        "unknown_unique_tickers": int(readable.loc[readable["ticker_name"].eq("UNKNOWN"), "ticker"].nunique()),
        "unknown_rows": int(readable["ticker_name"].eq("UNKNOWN").sum()),
        "ticker_005930_name": (
            readable.loc[readable["ticker"].eq(SAMSUNG), "ticker_name"].iloc[0]
            if readable["ticker"].eq(SAMSUNG).any()
            else "MISSING"
        ),
    }
    return readable, top10, mapping


def save_xlsx(path: Path, df: pd.DataFrame, sheet_name: str) -> None:
    """Save a filterable Excel workbook."""
    with pd.ExcelWriter(path) as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        worksheet = writer.sheets[sheet_name]
        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = worksheet.dimensions
        for column_cells in worksheet.columns:
            header = str(column_cells[0].value)
            sample_lengths = [len(str(cell.value)) for cell in column_cells[:200] if cell.value is not None]
            worksheet.column_dimensions[column_cells[0].column_letter].width = min(
                max([len(header), *sample_lengths]) + 2,
                30,
            )


def render_report(metrics: dict[str, Any]) -> str:
    """Render walk-forward Markdown report."""
    checks = metrics["leakage_checks"]
    return "\n".join(
        [
            "# Full Universe Rolling Walk-Forward Report",
            "",
            f"- Created at: `{datetime.now(UTC).isoformat()}`",
            f"- Train start policy: `{TRAIN_START_DATE.date()}`",
            f"- Validation policy: `{VALIDATION_START_DATE.date()}` to `{VALIDATION_END_DATE.date()}`",
            f"- Actual prediction period: `{metrics['first_prediction_date']}` to `{metrics['last_prediction_date']}`",
            f"- Daily retrains: `{metrics['prediction_dates']}`",
            f"- Prediction rows: `{metrics['prediction_rows']}`",
            f"- Unique tickers: `{metrics['unique_tickers']}`",
            f"- 005930 exists: `{metrics['ticker_005930_exists']}`",
            "",
            "## Metrics",
            f"- Ranking Rank IC: `{metrics['ranking_rank_ic']:.8f}`",
            f"- Expected Return Rank IC: `{metrics['expected_return_rank_ic']:.8f}`",
            f"- Gap RMSE: `{metrics['gap_rmse']:.8f}`",
            f"- Gap MAE: `{metrics['gap_mae']:.8f}`",
            f"- Gap directional accuracy: `{metrics['gap_directional_accuracy']:.8f}`",
            f"- Intraday RMSE: `{metrics['intraday_rmse']:.8f}`",
            f"- Intraday MAE: `{metrics['intraday_mae']:.8f}`",
            f"- Intraday directional accuracy: `{metrics['intraday_directional_accuracy']:.8f}`",
            "",
            "## Leakage Checks",
            f"- max(train feature_date) < prediction_date: `{checks['max_train_feature_date_lt_prediction_date']}`",
            f"- no duplicate date/ticker: `{checks['no_duplicate_date_ticker']}`",
            f"- no forbidden model features: `{checks['no_forbidden_model_features']}`",
            "",
        ]
    )


def render_readable_summary(readable: pd.DataFrame, top10: pd.DataFrame, mapping: dict[str, Any], metrics: dict[str, Any]) -> str:
    """Render readable prediction summary."""
    top20 = (
        top10.groupby(["ticker", "ticker_name"])
        .size()
        .reset_index(name="top10_appearances")
        .sort_values(["top10_appearances", "ticker"], ascending=[False, True])
        .head(20)
    )
    return "\n".join(
        [
            "# Full Universe Validation Predictions Summary",
            "",
            f"- Total rows: `{len(readable)}`",
            f"- Top10 rows: `{len(top10)}`",
            f"- Date range: `{readable['date'].min().date()}` to `{readable['date'].max().date()}`",
            f"- Unique dates: `{readable['date'].nunique()}`",
            f"- Unique tickers: `{readable['ticker'].nunique()}`",
            f"- Mapped unique tickers: `{mapping['mapped_unique_tickers']}`",
            f"- Unknown unique tickers: `{mapping['unknown_unique_tickers']}`",
            f"- Unknown rows: `{mapping['unknown_rows']}`",
            f"- 005930 ticker_name: `{mapping['ticker_005930_name']}`",
            f"- Average Top10 expected_return: `{top10['expected_return'].mean():.8f}`",
            f"- Average Top10 ranking_score: `{top10['ranking_score'].mean():.8f}`",
            f"- Ranking Rank IC: `{metrics['ranking_rank_ic']:.8f}`",
            f"- Expected Return Rank IC: `{metrics['expected_return_rank_ic']:.8f}`",
            "",
            "## Top 20 Daily Top10 Tickers",
            "",
            top20.to_markdown(index=False),
            "",
        ]
    )


def main() -> None:
    """Run full-universe daily rolling validation and readable reports."""
    print("Loading full-universe training data...")
    names = load_ticker_names()
    df = add_ticker_names(load_dataset(), names)
    dates = validation_dates(df)
    specs = {key: build_spec(key) for key in MODEL_KEYS}
    print(f"Prediction dates: {len(dates)}")
    print(f"First prediction date: {dates[0].date()}")
    print(f"Last prediction date: {dates[-1].date()}")
    print(f"LightGBM n_estimators cap: {MAX_N_ESTIMATORS}")

    prediction_frames = []
    metadata_rows = []
    for fold_id, prediction_date in enumerate(dates, start=1):
        print(f"[{fold_id}/{len(dates)}] daily retrain for {prediction_date.date()}...")
        predictions, metadata = train_predict_day(df, prediction_date, specs, fold_id)
        prediction_frames.append(predictions)
        metadata_rows.append(metadata)

    predictions = pd.concat(prediction_frames, ignore_index=True)
    predictions = predictions.sort_values(["date", "ranking_score"], ascending=[True, False]).reset_index(drop=True)
    fold_metadata = pd.DataFrame(metadata_rows)
    daily_metrics = build_daily_metrics(predictions)
    metrics = build_metrics(predictions, daily_metrics, fold_metadata)
    readable, top10, mapping = make_readable(predictions)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    predictions.loc[:, FINAL_PREDICTION_COLUMNS].to_parquet(OUTPUT_DIR / "predictions.parquet", index=False)
    predictions.loc[:, FINAL_PREDICTION_COLUMNS].to_csv(OUTPUT_DIR / "predictions.csv", index=False, encoding="utf-8-sig")
    daily_metrics.to_csv(OUTPUT_DIR / "daily_metrics.csv", index=False, encoding="utf-8-sig")
    fold_metadata.to_csv(OUTPUT_DIR / "fold_metadata.csv", index=False, encoding="utf-8-sig")
    (OUTPUT_DIR / "metrics.json").write_text(
        json.dumps({"created_at": datetime.now(UTC).isoformat(), "max_n_estimators": MAX_N_ESTIMATORS, "metrics": metrics}, indent=2),
        encoding="utf-8",
    )
    REPORT_PATH.write_text(render_report(metrics), encoding="utf-8")

    readable.to_csv(READABLE_CSV, index=False, encoding="utf-8-sig")
    top10.to_csv(TOP10_CSV, index=False, encoding="utf-8-sig")
    save_xlsx(READABLE_XLSX, readable, "Predictions")
    save_xlsx(TOP10_XLSX, top10, "Top10ByDate")
    SUMMARY_MD.write_text(render_readable_summary(readable, top10, mapping, metrics), encoding="utf-8")

    print("Full-universe rolling walk-forward complete")
    print(f"Prediction shape: {tuple(predictions.loc[:, FINAL_PREDICTION_COLUMNS].shape)}")
    print(f"Ticker mapping: {mapping}")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
