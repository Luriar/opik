"""
Agent Data Helper — unified read path: Delta from S3 first, Parquet fallback.

All agents use this module to read OPIK data. The read preference is:
  1. Delta Lake on S3 (fast, ACID, time-travel capable)
  2. S3 Parquet (always available, zero-dependency fallback)
"""

import logging
import os
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
import boto3

logger = logging.getLogger("opik.data_helper")

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
S3_BUCKET = os.environ.get("S3_BUCKET", "s3-opik-bucket")
DELTA_S3_BASE = f"s3://{S3_BUCKET}/delta/gold_db"


def _read_delta_s3(table_path: str) -> Optional[pd.DataFrame]:
    """Read a Delta Lake table directly from S3 via deltalake library."""
    try:
        from deltalake import DeltaTable
        import os
        # deltalake defaults to us-east-1; force correct region
        os.environ.setdefault("AWS_REGION", AWS_REGION)
        dt = DeltaTable(table_path)
        df = dt.to_pandas()
        logger.info("Delta read: %s -> %d rows", table_path, len(df))
        return df
    except ImportError:
        logger.debug("deltalake not installed, Delta read skipped")
        return None
    except Exception as e:
        logger.debug("Delta read failed for %s: %s", table_path, e)
        return None


def _read_parquet_s3(key: str) -> Optional[pd.DataFrame]:
    """Read a single Parquet file from S3."""
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        buf = BytesIO(resp["Body"].read()); table = pq.read_table(buf)
        return table.to_pandas()
    except Exception as e:
        logger.debug("S3 Parquet read failed for %s: %s", key, e)
        return None


def read_gold_data(
    dataset: str,
    dt: Optional[str] = None,
    columns: Optional[list] = None,
) -> Optional[pd.DataFrame]:
    """Read Gold data: Delta from S3 first, then Parquet from S3.

    Args:
        dataset: table name relative to delta/gold_db/ or gold/.
                 Examples: "structured", "embeddings", "dart/disclosure_events"
        dt: date string in YYYY-MM or YYYY-MM-DD format for pandas filter
        columns: optional column subset

    Returns:
        DataFrame or None if data not found in either path.
    """
    # -- 1. Try Delta from S3 --
    delta_path = f"{DELTA_S3_BASE}/{dataset}"
    df = _read_delta_s3(delta_path)
    if df is not None and len(df) > 0:
        if dt and "dt" in df.columns:
            df = df[df["dt"].astype(str).str.startswith(dt)]
            logger.info("Delta filtered to dt=%s: %d rows", dt, len(df))
        if columns:
            existing = [c for c in columns if c in df.columns]
            df = df[existing]
        return df

    # -- 2. Fallback: S3 Parquet (supports partitioned paths) --
    s3 = boto3.client("s3", region_name=AWS_REGION)

    # Try exact key first, then partition listing
    if dt:
        key = f"gold/{dataset}/dt={dt}/data.parquet"
    else:
        key = f"gold/{dataset}/data.parquet"

    df = _read_parquet_s3(key)
    if df is None and not dt:
        # No exact key — try to discover partition folders
        try:
            prefix = f"gold/{dataset}/"
            paginator = s3.get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
                for obj in page.get("Contents", []):
                    k = obj["Key"]
                    if k.endswith(".parquet"):
                        keys.append(k)
            if keys:
                keys = keys[:20]  # limit to avoid OOM
                logger.info("Parquet fallback: found %d partition files for %s", len(keys), dataset)
                dfs = []
                for k in keys:
                    part = _read_parquet_s3(k)
                    if part is not None:
                        dfs.append(part)
                if dfs:
                    df = pd.concat(dfs, ignore_index=True)
        except Exception:
            pass

    if df is not None:
        logger.info("Read %d rows from S3 Parquet: %s", len(df), dataset)
        if columns:
            existing = [c for c in columns if c in df.columns]
            df = df[existing]
        return df

    logger.warning("No data found for %s at dt=%s (Delta + S3)", dataset, dt)
    return None


# Compatibility aliases for existing agent code
read_gold_structured = lambda dt=None: read_gold_data("structured", dt=dt)
read_gold_embeddings = lambda dt=None: read_gold_data("embeddings", dt=dt)
read_dart_disclosures = lambda dt=None: read_gold_data("dart/disclosure_events", dt=dt)
