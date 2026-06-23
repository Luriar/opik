"""Daily prediction generation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from src.models.model_factory import predict_model
from src.models.trainer import ModelBundle
from src.pipeline.config import DailyUpdateConfig


PREDICTION_COLUMNS: tuple[str, ...] = (
    "prediction_date",
    "ticker",
    "ticker_name",
    "ranking_score",
    "expected_return",
    "pred_gap",
    "pred_intraday",
    "prev_close",
    "pred_open_price",
    "pred_close_price",
    "train_start_date",
    "train_end_date",
    "rolling_train_days",
)


@dataclass(frozen=True)
class DailyPredictionResult:
    """Daily prediction output summary."""

    prediction_df: pd.DataFrame
    prediction_output_csv: str
    prediction_output_parquet: str
    prediction_rows: int


def generate_daily_predictions(
    config: DailyUpdateConfig,
    model_bundle: ModelBundle,
    update_date: str | pd.Timestamp,
    prediction_date: str | pd.Timestamp,
    train_start_date: str,
    train_end_date: str,
    rolling_train_days: int,
) -> DailyPredictionResult:
    """Generate and save daily predictions from latest optimized features."""
    feature_rows = load_latest_feature_rows(config.resolve_path("feature_file"), update_date)
    prediction_input = attach_prev_close(feature_rows, config.resolve_path("clean_ohlcv_file"), update_date)
    prediction_input = attach_ticker_names(prediction_input, config.resolve_path("ticker_name_file"))
    predictions = build_daily_prediction_frame(
        prediction_input,
        model_bundle,
        prediction_date,
        train_start_date,
        train_end_date,
        rolling_train_days,
    )
    csv_path, parquet_path = write_daily_predictions(config, prediction_date, predictions)
    return DailyPredictionResult(
        prediction_df=predictions,
        prediction_output_csv=str(csv_path),
        prediction_output_parquet=str(parquet_path),
        prediction_rows=len(predictions),
    )


def load_latest_feature_rows(path: Path, update_date: str | pd.Timestamp) -> pd.DataFrame:
    """Load feature rows for update_date."""
    data = pd.read_parquet(path)
    data["date"] = pd.to_datetime(data["date"]).dt.normalize()
    data["ticker"] = data["ticker"].astype(str).str.zfill(6)
    update_ts = pd.Timestamp(update_date).normalize()
    rows = data[data["date"].eq(update_ts)].copy()
    if rows.empty:
        raise ValueError(f"No feature rows available for update_date {update_ts.date()}")
    return rows.sort_values(["date", "ticker"]).reset_index(drop=True)


def attach_prev_close(feature_rows: pd.DataFrame, clean_ohlcv_path: Path, update_date: str | pd.Timestamp) -> pd.DataFrame:
    """Attach update-date close as previous close for pricing audit."""
    ohlcv = pd.read_parquet(clean_ohlcv_path, columns=["date", "ticker", "close"])
    ohlcv["date"] = pd.to_datetime(ohlcv["date"]).dt.normalize()
    ohlcv["ticker"] = ohlcv["ticker"].astype(str).str.zfill(6)
    update_ts = pd.Timestamp(update_date).normalize()
    close_rows = ohlcv[ohlcv["date"].eq(update_ts)].loc[:, ["ticker", "close"]].rename(columns={"close": "prev_close"})
    merged = feature_rows.merge(close_rows, on="ticker", how="left", validate="one_to_one")
    if merged["prev_close"].isna().any():
        missing = sorted(merged.loc[merged["prev_close"].isna(), "ticker"].unique().tolist())
        raise ValueError(f"Missing prev_close for tickers: {missing[:10]}")
    return merged


def attach_ticker_names(feature_rows: pd.DataFrame, ticker_name_path: Path) -> pd.DataFrame:
    """Attach Korean ticker names from metadata if available."""
    result = feature_rows.copy()
    if not ticker_name_path.exists():
        result["ticker_name"] = "UNKNOWN"
        return result
    names = pd.read_csv(ticker_name_path, dtype={"ticker": str})
    names["ticker"] = names["ticker"].astype(str).str.zfill(6)
    if "ticker_name" not in names.columns:
        result["ticker_name"] = "UNKNOWN"
        return result
    result = result.merge(names[["ticker", "ticker_name"]], on="ticker", how="left")
    result["ticker_name"] = result["ticker_name"].fillna("UNKNOWN")
    return result


def build_daily_prediction_frame(
    prediction_input: pd.DataFrame,
    model_bundle: ModelBundle,
    prediction_date: str | pd.Timestamp,
    train_start_date: str,
    train_end_date: str,
    rolling_train_days: int,
) -> pd.DataFrame:
    """Build daily prediction DataFrame with price-level audit columns."""
    result = prediction_input.loc[:, ["ticker", "ticker_name", "prev_close"]].copy()
    result["prediction_date"] = pd.Timestamp(prediction_date).normalize()
    result["ranking_score"] = predict_model(model_bundle.ranking_model, prediction_input)
    result["pred_gap"] = predict_model(model_bundle.gap_model, prediction_input)
    result["pred_intraday"] = predict_model(model_bundle.intraday_model, prediction_input)
    result["expected_return"] = result["pred_gap"] + result["pred_intraday"]
    result["pred_open_price"] = result["prev_close"].astype(float) * (1 + result["pred_gap"])
    result["pred_close_price"] = result["pred_open_price"] * (1 + result["pred_intraday"])
    result["train_start_date"] = train_start_date
    result["train_end_date"] = train_end_date
    result["rolling_train_days"] = int(rolling_train_days)
    numeric = [
        "ranking_score",
        "expected_return",
        "pred_gap",
        "pred_intraday",
        "prev_close",
        "pred_open_price",
        "pred_close_price",
    ]
    if not np.isfinite(result[numeric].to_numpy()).all():
        raise ValueError("Daily prediction output contains non-finite values")
    if result.duplicated(subset=["prediction_date", "ticker"]).any():
        raise ValueError("Daily prediction output contains duplicate prediction_date/ticker rows")
    return result.loc[:, PREDICTION_COLUMNS].sort_values(["prediction_date", "ranking_score"], ascending=[True, False])


def write_daily_predictions(
    config: DailyUpdateConfig,
    prediction_date: str | pd.Timestamp,
    predictions: pd.DataFrame,
) -> tuple[Path, Path]:
    """Write daily prediction parquet and CSV outputs."""
    output_dir = config.resolve_path("daily_prediction_dir")
    output_dir.mkdir(parents=True, exist_ok=True)
    compact = pd.Timestamp(prediction_date).strftime("%Y%m%d")
    parquet_path = output_dir / f"predictions_{compact}.parquet"
    csv_path = output_dir / f"predictions_{compact}.csv"
    predictions.to_parquet(parquet_path, index=False)
    predictions.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return csv_path, parquet_path
