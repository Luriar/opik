"""
Gold embedding prefix fix — re-encodes existing embeddings with "passage:" prefix.

BUG: dags/gold/embedding.py line 843 passed raw _embedding_input to
SentenceTransformer.encode() without the required "passage:" prefix.
e5 models are trained with prefix-conditioned embeddings; without it,
document vectors live in a different space than query-prefixed search vectors.

This script reads existing Gold parquet, re-encodes with "passage:" prefix,
and overwrites in-place. No Bedrock LLM calls — text reuse only.

Usage on EC2:
    cd /home/ec2-user/airflow/opik
    PYTHONPATH=/home/ec2-user/airflow/opik:/opt/airflow/opik/server \
    python scripts/reembed_with_prefix.py --month 2026-06
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from io import BytesIO
from typing import Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("reembed")

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
S3_BUCKET = os.environ.get("S3_BUCKET", "s3-opik-bucket")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL_NAME", "intfloat/multilingual-e5-small")
EMBEDDING_DIM = int(os.environ.get("EMBEDDING_DIM", "384"))


def list_gold_parquet_keys(prefix: str, max_keys: int = 500) -> list[str]:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".parquet"):
                keys.append(k)
                if len(keys) >= max_keys:
                    return keys
    return keys


def read_parquet(key: str) -> Optional[pd.DataFrame]:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        buf = BytesIO(resp["Body"].read())
        return pq.read_table(buf).to_pandas()
    except Exception as e:
        logger.warning("Read failed: %s — %s", key, e)
        return None


def write_parquet(df: pd.DataFrame, key: str):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.read())
    logger.info("Overwritten: %s (%d rows)", key, len(df))


def reembed_month(year: int, month: int, dry_run: bool = False):
    """Re-encode all gold/embeddings parquet files for a given year+month."""
    prefix = f"gold/embeddings/year={year:04d}/month={month:02d}/"
    keys = list_gold_parquet_keys(prefix)
    logger.info("Found %d parquet files for %04d-%02d", len(keys), year, month)

    model = SentenceTransformer(EMBEDDING_MODEL)
    total_rows = 0
    total_files = 0

    for key in keys:
        df = read_parquet(key)
        if df is None:
            continue

        # Check if embedding column exists
        emb_col = None
        for col in ["embedding", "embeddings", "vector", "_embedding"]:
            if col in df.columns:
                emb_col = col
                break

        text_col = None
        for col in ["_embedding_input", "text", "content", "embedding_input"]:
            if col in df.columns:
                text_col = col
                break

        if text_col is None:
            logger.warning("No text column in %s, columns: %s", key, list(df.columns))
            continue

        # Re-encode with passage prefix
        texts = df[text_col].fillna(" ").tolist()
        prefixed = ["passage: " + str(t) for t in texts]
        logger.info("Re-encoding %d texts for %s (prefix added)", len(prefixed), key)

        if dry_run:
            logger.info("DRY-RUN: would overwrite %s", key)
            total_rows += len(prefixed)
            total_files += 1
            continue

        vectors = model.encode(prefixed, batch_size=16, normalize_embeddings=True, show_progress_bar=False)

        if emb_col and emb_col in df.columns:
            # Store as list of lists (pyarrow compatible)
            df["embedding"] = [v.tolist() for v in vectors]
        else:
            df["embedding"] = [v.tolist() for v in vectors]

        write_parquet(df, key)
        total_rows += len(prefixed)
        total_files += 1

    logger.info("%04d-%02d: %d files, %d rows re-embedded", year, month, total_files, total_rows)
    return total_files, total_rows


def main():
    parser = argparse.ArgumentParser(description="Re-embed Gold parquet with passage: prefix")
    parser.add_argument("--year", type=int, required=True, help="Year to process (e.g. 2026)")
    parser.add_argument("--month", type=int, required=True, help="Month to process (e.g. 6)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    files, rows = reembed_month(args.year, args.month, dry_run=args.dry_run)
    action = "Would re-embed" if args.dry_run else "Re-embedded"
    print(f"{action}: {files} files, {rows} rows")


if __name__ == "__main__":
    main()
