"""Add real previous-close price levels to validation predictions."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


TRAINING_PARQUET = PROJECT_ROOT / "data" / "processed" / "real_training_dataset.parquet"
TRAINING_CSV = PROJECT_ROOT / "data" / "processed" / "real_training_dataset.csv"
TRAINING_METADATA = PROJECT_ROOT / "data" / "processed" / "real_training_metadata.json"
OHLCV_PATH = PROJECT_ROOT / "data" / "processed" / "kr_stock" / "ohlcv_clean_20230615_20260614.parquet"
PREDICTION_PATH = PROJECT_ROOT / "outputs" / "predictions" / "real" / "validation_predictions.parquet"
OUTPUT_PARQUET = PROJECT_ROOT / "outputs" / "predictions" / "real" / "validation_predictions_with_prices.parquet"
OUTPUT_CSV = PROJECT_ROOT / "outputs" / "predictions" / "real" / "validation_predictions_with_prices.csv"
REPORT_PATH = PROJECT_ROOT / "reports" / "real_prediction_price_level_report.md"
FEATURE_METADATA_PATH = PROJECT_ROOT / "data" / "features" / "model_training_real" / "feature_metadata.json"


def normalize_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize date and ticker columns for one-to-one joins."""
    result = df.copy()
    result["date"] = pd.to_datetime(result["date"])
    result["ticker"] = result["ticker"].astype(str).str.zfill(6)
    return result


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load training data, cleaned OHLCV, and validation predictions."""
    missing = [path for path in [TRAINING_PARQUET, OHLCV_PATH, PREDICTION_PATH] if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required input files: {[str(path) for path in missing]}")

    training = normalize_keys(pd.read_parquet(TRAINING_PARQUET))
    training["feature_date"] = pd.to_datetime(training["feature_date"])
    training["target_date"] = pd.to_datetime(training["target_date"])

    ohlcv = normalize_keys(pd.read_parquet(OHLCV_PATH))
    predictions = normalize_keys(pd.read_parquet(PREDICTION_PATH))
    return training, ohlcv, predictions


def add_prev_close(training: pd.DataFrame, ohlcv: pd.DataFrame) -> pd.DataFrame:
    """Append previous close as an audit/pricing column using feature_date."""
    close_lookup = ohlcv.loc[:, ["date", "ticker", "close"]].rename(
        columns={"date": "feature_date", "close": "prev_close"},
    )
    close_lookup["feature_date"] = pd.to_datetime(close_lookup["feature_date"])
    close_lookup["ticker"] = close_lookup["ticker"].astype(str).str.zfill(6)

    without_existing = training.drop(columns=["prev_close"], errors="ignore")
    merged = without_existing.merge(
        close_lookup,
        on=["feature_date", "ticker"],
        how="left",
        validate="many_to_one",
    )
    if len(merged) != len(training):
        raise ValueError("Adding prev_close changed the real training dataset row count")
    return merged


def save_training_dataset(training: pd.DataFrame) -> None:
    """Persist the training dataset with prev_close as an audit column."""
    training.to_parquet(TRAINING_PARQUET, index=False)
    training.to_csv(TRAINING_CSV, index=False)

    if TRAINING_METADATA.exists():
        metadata = json.loads(TRAINING_METADATA.read_text(encoding="utf-8"))
    else:
        metadata = {}
    feature_columns = metadata.get("feature_columns", [])
    audit_columns = ["date", "ticker", "feature_date", "target_date", "prediction_horizon", "prev_close"]
    metadata.update(
        {
            "columns": int(training.shape[1]),
            "audit_columns": audit_columns,
            "prev_close_missing_count": int(training["prev_close"].isna().sum()),
            "prev_close_min": float(training["prev_close"].min()),
            "prev_close_max": float(training["prev_close"].max()),
            "prev_close_is_model_feature": "prev_close" in feature_columns,
            "updated_at": datetime.now(UTC).isoformat(),
        }
    )
    TRAINING_METADATA.write_text(json.dumps(metadata, indent=2), encoding="utf-8")


def build_priced_predictions(training: pd.DataFrame, predictions: pd.DataFrame) -> pd.DataFrame:
    """Join prev_close onto validation predictions and create actual price levels."""
    original_rows = len(predictions)
    priced = predictions.merge(
        training.loc[:, ["date", "ticker", "prev_close"]],
        on=["date", "ticker"],
        how="left",
        validate="one_to_one",
    )
    if len(priced) != original_rows:
        raise ValueError("Prediction row count changed while adding prev_close")

    priced["pred_open_price"] = priced["prev_close"] * (1 + priced["pred_gap"])
    priced["pred_close_price"] = priced["pred_open_price"] * (1 + priced["pred_intraday"])
    return priced


def load_feature_metadata() -> dict[str, Any]:
    """Load exported X_train metadata when available."""
    if not FEATURE_METADATA_PATH.exists():
        return {}
    return json.loads(FEATURE_METADATA_PATH.read_text(encoding="utf-8"))


def verify_model_features(feature_metadata: dict[str, Any]) -> dict[str, Any]:
    """Confirm prev_close was not part of exported real model feature lists."""
    result: dict[str, Any] = {}
    for model_key, metadata in feature_metadata.items():
        feature_columns = metadata.get("feature_columns", [])
        result[model_key] = {
            "prev_close_in_feature_list": "prev_close" in feature_columns,
            "x_train_shape": [metadata.get("row_count"), metadata.get("column_count")],
        }
    return result


def render_report(
    priced: pd.DataFrame,
    model_feature_check: dict[str, Any],
    original_prediction_rows: int,
) -> str:
    """Render a Markdown report for prediction price-level conversion."""
    sample_005930 = priced[priced["ticker"].eq("005930")].head(10)
    sample_table = (
        sample_005930.loc[
            :,
            [
                "date",
                "ticker",
                "prev_close",
                "pred_gap",
                "pred_intraday",
                "pred_open_price",
                "pred_close_price",
                "expected_return",
            ],
        ].to_markdown(index=False)
        if not sample_005930.empty
        else "No 005930 rows found in validation predictions."
    )
    x_shape_lines = [
        f"- {model_key}: X_train shape {tuple(values['x_train_shape'])}, "
        f"prev_close in features = {values['prev_close_in_feature_list']}"
        for model_key, values in model_feature_check.items()
    ]
    return "\n".join(
        [
            "# Real Prediction Price Level Report",
            "",
            f"Created at: {datetime.now(UTC).isoformat()}",
            "",
            "## Summary",
            f"- Prediction row count: {len(priced)}",
            f"- Original prediction row count: {original_prediction_rows}",
            f"- Row count unchanged: {len(priced) == original_prediction_rows}",
            f"- Missing prev_close count: {int(priced['prev_close'].isna().sum())}",
            f"- Min prev_close: {float(priced['prev_close'].min()):.6f}",
            f"- Max prev_close: {float(priced['prev_close'].max()):.6f}",
            f"- Min pred_open_price: {float(priced['pred_open_price'].min()):.6f}",
            f"- Max pred_open_price: {float(priced['pred_open_price'].max()):.6f}",
            f"- Min pred_close_price: {float(priced['pred_close_price'].min()):.6f}",
            f"- Max pred_close_price: {float(priced['pred_close_price'].max()):.6f}",
            f"- 005930 exists: {bool(priced['ticker'].eq('005930').any())}",
            "",
            "## Model Feature Check",
            *x_shape_lines,
            "",
            "## First 10 Rows For 005930",
            "",
            sample_table,
            "",
        ]
    )


def main() -> None:
    """Add prev_close audit values and write price-level prediction outputs."""
    print("Loading real training dataset, cleaned OHLCV, and validation predictions...")
    training, ohlcv, predictions = load_inputs()
    original_training_shape = training.shape
    original_prediction_rows = len(predictions)

    print("Adding prev_close as an audit/pricing column...")
    training_with_prev_close = add_prev_close(training, ohlcv)
    save_training_dataset(training_with_prev_close)

    print("Computing prediction price levels...")
    priced = build_priced_predictions(training_with_prev_close, predictions)
    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    priced.to_parquet(OUTPUT_PARQUET, index=False)
    priced.to_csv(OUTPUT_CSV, index=False)

    model_feature_check = verify_model_features(load_feature_metadata())
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        render_report(priced, model_feature_check, original_prediction_rows),
        encoding="utf-8",
    )

    print("Price-level prediction export complete")
    print(f"Training shape before: {original_training_shape}")
    print(f"Training shape after: {training_with_prev_close.shape}")
    print(f"Prediction rows: {len(priced)}")
    print(f"Missing prev_close count: {int(priced['prev_close'].isna().sum())}")
    print(f"005930 exists: {bool(priced['ticker'].eq('005930').any())}")
    print(f"Model feature check: {json.dumps(model_feature_check, indent=2)}")
    sample = priced[priced["ticker"].eq("005930")].head(10)
    if not sample.empty:
        print(
            sample.loc[
                :,
                ["date", "ticker", "prev_close", "pred_open_price", "pred_close_price"],
            ].to_string(index=False)
        )


if __name__ == "__main__":
    main()
