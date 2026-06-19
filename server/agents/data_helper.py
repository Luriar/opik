"""
Agent Data Helper — unified read path: Delta first, Parquet fallback.

All agents use this module to read OPIK data. The read preference is:
  1. Local Delta Lake tables (fast, no S3 egress)
  2. S3 Parquet (always available, zero-dependency fallback)
"""

import logging
import os
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq
import boto3

logger = logging.getLogger("opik.data_helper")

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
DELTA_BASE = os.environ.get("OPIK_DELTA_PATH", "/home/ec2-user/opik-server/delta")


def _read_parquet_s3(key: str, bucket: str = "s3-opik-bucket") -> Optional[pd.DataFrame]:
    """Read a Parquet file from S3."""
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        resp = s3.get_object(Bucket=bucket, Key=key)
        table = pq.read_table(resp["Body"])
        return table.to_pandas()
    except Exception as e:
        logger.debug("S3 Parquet read failed for %s: %s", key, e)
        return None


def _read_delta_local(table_path: str) -> Optional[pd.DataFrame]:
    """Read a Delta Lake table from local disk."""
    import importlib
    try:
        dt = importlib.import_module("deltalake")
        if os.path.isdir(table_path):
            return dt.DeltaTable(table_path).to_pandas()
    except (ImportError, Exception) as e:
        logger.debug("Delta read failed for %s: %s", table_path, e)
    return None


def read_gold_data(
    dataset: str,
    dt: Optional[str] = None,
    columns: Optional[list] = None,
) -> Optional[pd.DataFrame]:
    """Read Gold data: Delta first, then Parquet from S3.

    Args:
        dataset: e.g. "structured", "embeddings", "dart/disclosure_events"
        dt: date string in YYYY-MM or YYYY-MM-DD format for partition filter
        columns: optional column subset

    Returns:
        DataFrame or None if data not found in either path.
    """
    # ── 1. Try Delta ──
    delta_table = os.path.join(DELTA_BASE, "gold", dataset)
    df = _read_delta_local(delta_table)
    if df is not None and len(df) > 0:
        logger.info("Read %d rows from Delta: %s", len(df), delta_table)
        if dt and "dt" in df.columns:
            df = df[df["dt"].astype(str).str.startswith(dt)]
            logger.info("Delta filtered to dt=%s: %d rows", dt, len(df))
        if columns:
            df = df[columns]
        return df

    # ── 2. Fallback to S3 Parquet ──
    if dt:
        key = f"gold/{dataset}/dt={dt}/data.parquet"
    else:
        key = f"gold/{dataset}/data.parquet"

    df = _read_parquet_s3(key)
    if df is not None:
        logger.info("Read %d rows from S3: s3://s3-opik-bucket/%s", len(df), key)
        if columns and df is not None:
            existing = [c for c in columns if c in df.columns]
            df = df[existing]
        return df

    logger.warning("No data found for %s at dt=%s (Delta + S3)", dataset, dt)
    return None


# Compatibility aliases for existing agent code
read_gold_structured = lambda dt: read_gold_data("structured", dt=dt)
read_gold_embeddings = lambda dt: read_gold_data("embeddings", dt=dt)
read_dart_disclosures = lambda dt: read_gold_data("dart/disclosure_events", dt=dt)
