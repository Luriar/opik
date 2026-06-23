"""Create the real LightGBM training dataset from optimized features."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


FEATURE_PATH = PROJECT_ROOT / "data" / "features" / "real_features_optimized.parquet"
OHLCV_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "kr_stock"
    / "ohlcv_clean_20230615_20260614.parquet"
)
OUTPUT_PARQUET = PROJECT_ROOT / "data" / "processed" / "real_training_dataset.parquet"
OUTPUT_CSV = PROJECT_ROOT / "data" / "processed" / "real_training_dataset.csv"
METADATA_JSON = PROJECT_ROOT / "data" / "processed" / "real_training_metadata.json"
SUMMARY_MD = PROJECT_ROOT / "reports" / "real_training_dataset_summary.md"

AUDIT_COLUMNS = ["date", "ticker", "feature_date", "target_date", "prediction_horizon"]
TARGET_COLUMNS = ["target_ranking", "target_gap", "target_intraday"]
LEGACY_TARGET_ALIAS = "target_rank_return"
FORBIDDEN_MODEL_FEATURES = set(AUDIT_COLUMNS) | set(TARGET_COLUMNS) | {LEGACY_TARGET_ALIAS}


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load optimized features and cleaned OHLCV."""
    features = pd.read_parquet(FEATURE_PATH)
    ohlcv = pd.read_parquet(OHLCV_PATH)
    for df in (features, ohlcv):
        df["date"] = pd.to_datetime(df["date"])
        df["ticker"] = df["ticker"].astype(str).str.zfill(6)
    return features, ohlcv


def build_target_frame(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """Calculate documented T targets from current day and previous close."""
    data = ohlcv.sort_values(["ticker", "date"]).reset_index(drop=True).copy()
    group = data.groupby("ticker", sort=False)
    data["feature_date"] = group["date"].shift(1)
    data["previous_close"] = group["close"].shift(1)
    data["target_date"] = data["date"]
    data["prediction_horizon"] = (
        data["target_date"] - data["feature_date"]
    ).dt.days
    data["target_ranking"] = data["close"] / data["previous_close"] - 1
    data["target_gap"] = data["open"] / data["previous_close"] - 1
    data["target_intraday"] = data["close"] / data["open"] - 1
    return data.loc[
        :,
        [
            "date",
            "ticker",
            "feature_date",
            "target_date",
            "prediction_horizon",
            *TARGET_COLUMNS,
        ],
    ]


def create_training_dataset(features: pd.DataFrame, targets: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Merge optimized features with targets and remove rows with missing targets."""
    merged = features.merge(targets, on=["date", "ticker"], how="left", validate="one_to_one")
    before = len(merged)
    required = ["feature_date", "target_date", "prediction_horizon", *TARGET_COLUMNS]
    cleaned = merged.dropna(subset=required).copy()
    cleaned = cleaned[cleaned["feature_date"] < cleaned["target_date"]].copy()
    removed = before - len(cleaned)
    cleaned = cleaned.sort_values(["ticker", "date"]).reset_index(drop=True)
    return cleaned, removed


def model_feature_columns(training_df: pd.DataFrame) -> list[str]:
    """Return model feature columns excluding audit and target columns."""
    return [column for column in training_df.columns if column not in FORBIDDEN_MODEL_FEATURES]


def build_metadata(training_df: pd.DataFrame, removed_rows: int) -> dict[str, Any]:
    """Build training dataset metadata and leakage checks."""
    features = model_feature_columns(training_df)
    leakage_violations = training_df[~(training_df["feature_date"] < training_df["target_date"])]
    nan_summary = {
        column: int(value)
        for column, value in training_df.isna().sum().sort_values(ascending=False).items()
        if int(value) > 0
    }
    return {
        "input_files": {
            "features": str(FEATURE_PATH),
            "ohlcv": str(OHLCV_PATH),
        },
        "output_files": {
            "parquet": str(OUTPUT_PARQUET),
            "csv": str(OUTPUT_CSV),
            "metadata": str(METADATA_JSON),
            "summary": str(SUMMARY_MD),
        },
        "rows": int(len(training_df)),
        "columns": int(training_df.shape[1]),
        "feature_count": len(features),
        "feature_columns": features,
        "target_count": len(TARGET_COLUMNS),
        "target_columns": TARGET_COLUMNS,
        "removed_rows": int(removed_rows),
        "unique_tickers": int(training_df["ticker"].nunique()),
        "unique_trading_dates": int(training_df["date"].nunique()),
        "min_feature_date": training_df["feature_date"].min().date().isoformat(),
        "max_feature_date": training_df["feature_date"].max().date().isoformat(),
        "min_target_date": training_df["target_date"].min().date().isoformat(),
        "max_target_date": training_df["target_date"].max().date().isoformat(),
        "ticker_005930_exists": bool((training_df["ticker"] == "005930").any()),
        "date_is_model_feature": "date" in features,
        "ticker_is_model_feature": "ticker" in features,
        "target_columns_are_model_features": sorted(set(TARGET_COLUMNS) & set(features)),
        "leakage_check": {
            "feature_date_lt_target_date": bool(leakage_violations.empty),
            "violation_count": int(len(leakage_violations)),
        },
        "nan_summary": nan_summary,
    }


def render_summary(metadata: dict[str, Any]) -> str:
    """Render Markdown summary report."""
    lines = [
        "# Real Training Dataset Summary",
        "",
        f"- Rows: `{metadata['rows']}`",
        f"- Columns: `{metadata['columns']}`",
        f"- Feature count: `{metadata['feature_count']}`",
        f"- Target count: `{metadata['target_count']}`",
        f"- Removed rows: `{metadata['removed_rows']}`",
        f"- Unique tickers: `{metadata['unique_tickers']}`",
        f"- Unique trading dates: `{metadata['unique_trading_dates']}`",
        f"- Min feature date: `{metadata['min_feature_date']}`",
        f"- Max feature date: `{metadata['max_feature_date']}`",
        f"- Min target date: `{metadata['min_target_date']}`",
        f"- Max target date: `{metadata['max_target_date']}`",
        f"- Ticker 005930 exists: `{metadata['ticker_005930_exists']}`",
        f"- Date is model feature: `{metadata['date_is_model_feature']}`",
        f"- Ticker is model feature: `{metadata['ticker_is_model_feature']}`",
        f"- Target columns as model features: `{metadata['target_columns_are_model_features']}`",
        f"- Leakage check passed: `{metadata['leakage_check']['feature_date_lt_target_date']}`",
        f"- Leakage violation count: `{metadata['leakage_check']['violation_count']}`",
        "",
        "## NaN Summary",
        "| Column | Missing Count |",
        "| --- | ---: |",
    ]
    if metadata["nan_summary"]:
        for column, count in metadata["nan_summary"].items():
            lines.append(f"| {column} | {count} |")
    else:
        lines.append("| None | 0 |")
    lines.extend(
        [
            "",
            "## Target Definitions",
            "- `target_ranking = close(T) / close(T-1) - 1`",
            "- `target_gap = open(T) / close(T-1) - 1`",
            "- `target_intraday = close(T) / open(T) - 1`",
            "",
            "No models were trained and no feature formulas were modified.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    """Create and save the real training dataset."""
    print("Loading optimized features and cleaned OHLCV...")
    features, ohlcv = load_inputs()
    print(f"Optimized feature shape: {features.shape}")
    print(f"Cleaned OHLCV shape: {ohlcv.shape}")

    print("Generating target columns from documented definitions...")
    targets = build_target_frame(ohlcv)
    training_df, removed_rows = create_training_dataset(features, targets)
    metadata = build_metadata(training_df, removed_rows)

    if not metadata["leakage_check"]["feature_date_lt_target_date"]:
        raise ValueError("Leakage check failed: feature_date must be before target_date")
    if metadata["date_is_model_feature"] or metadata["ticker_is_model_feature"]:
        raise ValueError("date/ticker must not be model features")
    if metadata["target_columns_are_model_features"]:
        raise ValueError("target columns must not be model features")

    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_MD.parent.mkdir(parents=True, exist_ok=True)
    training_df.to_parquet(OUTPUT_PARQUET, index=False)
    training_df.to_csv(OUTPUT_CSV, index=False)
    METADATA_JSON.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    SUMMARY_MD.write_text(render_summary(metadata), encoding="utf-8")

    print("Real training dataset created")
    print(f"Shape: {tuple(training_df.shape)}")
    print(f"Feature count: {metadata['feature_count']}")
    print(f"Target count: {metadata['target_count']}")
    print(f"Removed rows: {removed_rows}")
    print(f"Ticker 005930 exists: {metadata['ticker_005930_exists']}")
    print(f"Leakage check: {metadata['leakage_check']}")
    print(f"Saved parquet: {OUTPUT_PARQUET}")
    print(f"Saved CSV: {OUTPUT_CSV}")
    print(f"Saved metadata: {METADATA_JSON}")
    print(f"Saved summary: {SUMMARY_MD}")


if __name__ == "__main__":
    main()
