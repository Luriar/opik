"""S3 upload for model Gold predictions — OPIK Phase 2 integration.

Uploads daily prediction parquet to s3://s3-opik-bucket/gold/model/predictions/
so the OPIK Briefing DAG can read it for triple consensus checking.

Usage (inside daily_prediction.py):
    from src.model.pipeline.s3_upload import upload_gold_predictions
    upload_gold_predictions(df, prediction_date)
"""

from __future__ import annotations

import io
import logging

import boto3
import pandas as pd

logger = logging.getLogger("model.s3_upload")

S3_BUCKET = "s3-opik-bucket"
S3_REGION = "ap-northeast-2"
GOLD_PREFIX = "gold/model/predictions"

# Columns exposed to OPIK Briefing DAG.
# Internal columns (pred_gap, pred_intraday, expected_return, train_*) stay local only.
GOLD_COLUMNS = [
    "prediction_date",
    "ticker",
    "ticker_name",
    "ranking_score",
    "pred_close_price",
]


def _get_s3_client():
    return boto3.client("s3", region_name=S3_REGION)


def upload_gold_predictions(
    df: pd.DataFrame,
    prediction_date: str,
    bucket: str = S3_BUCKET,
    prefix: str = GOLD_PREFIX,
) -> str:
    """Upload daily predictions to S3 Gold layer.

    Args:
        df: Full predictions dataframe (must contain GOLD_COLUMNS).
        prediction_date: YYYYMMDD string.
        bucket: S3 bucket name.
        prefix: S3 key prefix.

    Returns:
        S3 key of uploaded parquet.
    """
    s3 = _get_s3_client()

    gold_df = df[GOLD_COLUMNS].copy()
    gold_df["ticker"] = gold_df["ticker"].astype(str).str.zfill(6)
    gold_df["ticker_name"] = gold_df["ticker_name"].astype(str)

    # Gold parquet path: gold/model/predictions/dt={YYYY-MM-DD}/predictions.parquet
    dt = pd.Timestamp(prediction_date).strftime("%Y-%m-%d")
    key = f"{prefix}/dt={dt}/predictions.parquet"

    buf = io.BytesIO()
    gold_df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)

    s3.put_object(Bucket=bucket, Key=key, Body=buf.read())
    logger.info(
        "Uploaded %d predictions to s3://%s/%s",
        len(gold_df),
        bucket,
        key,
    )
    return key
