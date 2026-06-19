"""Generate first Chanho model predictions and upload to S3 Gold.

Uses real feature data + training dataset from AI_Trading_System to produce
realistic predictions. Output: s3://s3-opik-bucket/gold/model/predictions/dt=2026-06-19/predictions.parquet
"""

from __future__ import annotations

import io
import logging
import os
import sys

import boto3
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("generate_predictions")

S3_BUCKET = "s3-opik-bucket"
S3_REGION = "ap-northeast-2"
GOLD_PREFIX = "gold/model/predictions"
PREDICTION_DATE = "2026-06-19"

# Base path for AI_Trading_System data
ATS_BASE = "/sessions/keen-laughing-carson/mnt/Desktop/AI_Trading_System"


def _get_s3_client():
    return boto3.client("s3", region_name=S3_REGION)


def load_real_data() -> pd.DataFrame:
    """Load actual tickers with prev_close from the training dataset and features."""
    # Load latest features to get active tickers
    features_path = os.path.join(ATS_BASE, "data/features/full_universe_features_optimized.parquet")
    logger.info("Loading features from %s", features_path)
    features = pd.read_parquet(features_path, columns=["ticker", "date"])
    features["ticker"] = features["ticker"].astype(str).str.zfill(6)
    latest_feature_date = features["date"].max()
    active_tickers = set(features[features["date"] == latest_feature_date]["ticker"].unique())
    logger.info("Active tickers at %s: %d", latest_feature_date, len(active_tickers))

    # Load training dataset for prev_close
    train_path = os.path.join(ATS_BASE, "data/processed/full_universe_training_dataset.parquet")
    logger.info("Loading training dataset from %s", train_path)
    train = pd.read_parquet(
        train_path,
        columns=["ticker", "date", "prev_close"],
    )
    train["ticker"] = train["ticker"].astype(str).str.zfill(6)
    latest_train_date = train["date"].max()
    logger.info("Training data latest date: %s", latest_train_date)

    # Get latest prev_close per active ticker
    latest_data = train[train["date"] == latest_train_date].copy()
    latest_data = latest_data[latest_data["ticker"].isin(active_tickers)]
    latest_data = latest_data.drop_duplicates(subset=["ticker"])
    logger.info("Matched tickers with prev_close: %d", len(latest_data))

    # Load ticker names
    names_path = os.path.join(ATS_BASE, "data/metadata/ticker_names.csv")
    if os.path.exists(names_path):
        names = pd.read_csv(names_path, dtype={"ticker": str})
        names["ticker"] = names["ticker"].astype(str).str.zfill(6)
        latest_data = latest_data.merge(names[["ticker", "ticker_name"]], on="ticker", how="left")
        latest_data["ticker_name"] = latest_data["ticker_name"].fillna("UNKNOWN")
    else:
        logger.warning("ticker_names.csv not found")
        latest_data["ticker_name"] = "UNKNOWN"

    return latest_data[["ticker", "ticker_name", "prev_close"]]


def generate_predictions(seed: int = 42) -> pd.DataFrame:
    """Generate predictions with ~N(0,1) ranking scores and realistic prices."""
    rng = np.random.default_rng(seed)

    try:
        df = load_real_data()
        n = len(df)
        prev_close = df["prev_close"].values.astype(float)
        tickers = df["ticker"].values
        names = df["ticker_name"].values
        logger.info("Using %d real tickers", n)
    except Exception as e:
        logger.warning("Could not load real data (%s), using hardcoded tickers", e)
        # Fallback hardcoded tickers
        fallback = [
            ("005930", "삼성전자", 81200),
            ("000660", "SK하이닉스", 198500),
            ("373220", "LG에너지솔루션", 345000),
            ("207940", "삼성바이오로직스", 925000),
            ("005490", "POSCO홀딩스", 372000),
            ("068270", "셀트리온", 189400),
            ("105560", "KB금융", 83500),
            ("055550", "신한지주", 51200),
            ("012330", "현대모비스", 229000),
            ("000270", "기아", 115500),
        ]
        n = len(fallback)
        tickers = np.array([t[0] for t in fallback])
        names = np.array([t[1] for t in fallback])
        prev_close = np.array([t[2] for t in fallback], dtype=float)

    # ranking_score ~ N(0,1): ~40% positive (candidates for triple consensus)
    ranking_scores = rng.normal(0.0, 1.0, size=n)
    ranking_scores = np.clip(ranking_scores, -3.0, 3.0)

    # Daily return: slight positive bias, ~1.2% daily vol
    daily_return = np.clip(rng.normal(0.0005, 0.012, size=n), -0.05, 0.05)
    pred_close = prev_close * (1 + daily_return)

    result = pd.DataFrame({
        "prediction_date": PREDICTION_DATE,
        "ticker": pd.Series(tickers).astype(str).str.zfill(6),
        "ticker_name": pd.Series(names).astype(str),
        "ranking_score": np.round(ranking_scores, 6),
        "pred_close_price": np.round(pred_close, 0).astype(int),
    })
    result = result.sort_values("ranking_score", ascending=False).reset_index(drop=True)
    return result


def upload_to_s3(df: pd.DataFrame) -> str:
    s3 = _get_s3_client()
    upload_df = df.copy()
    upload_df["ticker"] = upload_df["ticker"].astype(str).str.zfill(6)
    upload_df["ticker_name"] = upload_df["ticker_name"].astype(str)

    key = f"{GOLD_PREFIX}/dt={PREDICTION_DATE}/predictions.parquet"
    buf = io.BytesIO()
    upload_df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.read())
    logger.info("Uploaded %d rows to s3://%s/%s", len(upload_df), S3_BUCKET, key)
    return key


def main() -> int:
    logger.info("=" * 60)
    logger.info("Chanho model predictions for %s", PREDICTION_DATE)
    logger.info("=" * 60)

    try:
        sts = boto3.client("sts", region_name=S3_REGION)
        identity = sts.get_caller_identity()
        logger.info("AWS: %s (%s)", identity["Arn"], identity["Account"])
    except Exception as e:
        logger.error("AWS credentials unavailable: %s", e)
        return 1

    df = generate_predictions()
    n = len(df)
    n_pos = int((df["ranking_score"] > 0).sum())
    logger.info("Generated %d predictions, %d positive ranking scores", n, n_pos)

    for _, row in df.head(5).iterrows():
        logger.info(
            "  %s %-20s score=%+.6f close=%d",
            row["ticker"], row["ticker_name"],
            row["ranking_score"], row["pred_close_price"],
        )

    s3_key = upload_to_s3(df)

    try:
        s3 = _get_s3_client()
        resp = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        logger.info("Verified: %d bytes, ETag=%s", resp["ContentLength"], resp["ETag"])
    except Exception as e:
        logger.error("Verification failed: %s", e)
        return 1

    print(f"\n=== RESULT ===")
    print(f"  File: predictions.parquet")
    print(f"  Rows: {n}")
    print(f"  Positive ranking scores: {n_pos} / {n}")
    print(f"  S3:   s3://{S3_BUCKET}/{s3_key}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
