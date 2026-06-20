"""
Agent Data Helper — unified read path: Delta from S3 first, Parquet fallback.
Gold v3 DART integration (상용, 2026-06-19): report_registry + facts material_event.

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

# Gold v3 DART prefix (상용 Exisign/DartCollector, 2026-06-19)
DART_GOLD_PREFIX = "gold/dart"


def _read_delta_s3(table_path: str) -> Optional[pd.DataFrame]:
    """Read a Delta Lake table directly from S3 via deltalake library."""
    try:
        from deltalake import DeltaTable
        import os
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

    if dt:
        key = f"gold/{dataset}/dt={dt}/data.parquet"
    else:
        key = f"gold/{dataset}/data.parquet"

    df = _read_parquet_s3(key)
    if df is None and not dt:
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
                keys = keys[:20]
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


# ── Gold v3 DART (상용 Exisign/DartCollector) ──────────────────────────────

def _scan_parquet_partitions(s3_prefix: str, max_keys: int = 50) -> list[str]:
    """Scan S3 prefix for .parquet files and return up to max_keys paths."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    keys = []
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=s3_prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if k.endswith(".parquet"):
                    keys.append(k)
                    if len(keys) >= max_keys:
                        return keys
    except Exception as e:
        logger.debug("S3 scan failed for %s: %s", s3_prefix, e)
    return keys


def read_dart_report_registry(
    rcept_year: Optional[str] = None,
    rcept_month: Optional[str] = None,
    report_type: Optional[str] = None,
    max_keys: int = 50,
) -> Optional[pd.DataFrame]:
    """Read DART report_registry from Gold v3 Parquet partitions.

    Args:
        rcept_year: 접수연도 필터 (e.g. "2026"). None = all.
        rcept_month: 접수월 필터 (e.g. "06"). None = all.
        report_type: 보고서 유형 필터 (e.g. "REGULAR", "MATERIAL_EVENT"). None = all.
        max_keys: max partition files to scan.

    Returns:
        DataFrame with all report_registry columns or None.
    """
    # Build prefix path
    parts = [f"{DART_GOLD_PREFIX}/report_registry"]
    if rcept_year:
        parts.append(f"rcept_year={rcept_year}")
    if rcept_month:
        parts.append(f"rcept_month={rcept_month}")
    if report_type:
        parts.append(f"report_type={report_type}")
    prefix = "/".join(parts) + "/"

    keys = _scan_parquet_partitions(prefix, max_keys)
    if not keys:
        logger.warning("No report_registry parquet files at %s", prefix)
        return None

    dfs = []
    for k in keys:
        part = _read_parquet_s3(k)
        if part is not None:
            dfs.append(part)
    if not dfs:
        return None

    df = pd.concat(dfs, ignore_index=True)
    # Deduplicate by rcept_no (keep latest by is_latest or rcept_dt)
    if "is_latest" in df.columns:
        df = df.sort_values("is_latest", ascending=False)
    if "rcept_no" in df.columns:
        df = df.drop_duplicates(subset=["rcept_no"], keep="first")
    # Normalize rcept_dt to YYYYMMDD for backward compat
    if "rcept_dt" in df.columns:
        df["rcept_dt"] = df["rcept_dt"].astype(str).str.replace("-", "")
    logger.info("DART report_registry: %d rows from %d partitions", len(df), len(keys))
    return df


def read_dart_material_events(
    rcept_year: Optional[str] = None,
    rcept_month: Optional[str] = None,
    event_type: Optional[str] = None,
    max_keys: int = 30,
) -> Optional[pd.DataFrame]:
    """Read DART material_event facts from Gold v3 Parquet partitions.

    Args:
        rcept_year: 접수연도 필터 (e.g. "2026"). None = all recent.
        rcept_month: 접수월 필터 (e.g. "06"). None = all.
        event_type: 이벤트 유형 필터 (e.g. "bdwtnIssuDcrs").
        max_keys: max partition files to scan.

    Returns:
        DataFrame with material_event columns or None.
    """
    parts = [f"{DART_GOLD_PREFIX}/facts/material_event"]
    if event_type:
        parts.append(f"event_type={event_type}")
    if rcept_year:
        parts.append(f"rcept_year={rcept_year}")
    if rcept_month:
        parts.append(f"rcept_month={rcept_month}")
    prefix = "/".join(parts) + "/"

    keys = _scan_parquet_partitions(prefix, max_keys)
    if not keys:
        logger.warning("No material_event parquet files at %s", prefix)
        return None

    dfs = []
    for k in keys:
        part = _read_parquet_s3(k)
        if part is not None:
            dfs.append(part)
    if not dfs:
        return None

    df = pd.concat(dfs, ignore_index=True)
    if "rcept_no" in df.columns:
        df = df.drop_duplicates(subset=["rcept_no"], keep="first")
    if "rcept_dt" in df.columns:
        df["rcept_dt"] = df["rcept_dt"].astype(str).str.replace("-", "")
    logger.info("DART material_events: %d rows from %d partitions", len(df), len(keys))
    return df


def read_dart_latest_context(snapshot_date: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Read DART latest_company_context serving cache."""
    prefix = f"{DART_GOLD_PREFIX}/serving/latest_company_context/"
    if snapshot_date:
        prefix = f"{prefix}snapshot_date={snapshot_date}/"
    keys = _scan_parquet_partitions(prefix, max_keys=5)
    if not keys:
        return None
    dfs = [df for k in keys if (df := _read_parquet_s3(k)) is not None]
    return pd.concat(dfs, ignore_index=True) if dfs else None


# ── Compatibility aliases ──────────────────────────────────────────────────

read_gold_structured = lambda dt=None: read_gold_data("structured", dt=dt)
read_gold_embeddings = lambda dt=None: read_gold_data("embeddings", dt=dt)
read_dart_disclosures = lambda dt=None: read_gold_data("dart/disclosure_events", dt=dt)
