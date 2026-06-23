"""
Spark Delta Lake MERGE — daily Gold Parquet to Delta table sync.

Runs on EC2 r6g.large in Spark local mode via cron (07:00 KST, before briefing).
Merges three Gold tables:
  1. gold/structured/  -> delta/gold_db/structured/   (PK: report_id)
  2. gold/embeddings/  -> delta/gold_db/embeddings/   (PK: report_id)
  3. gold/dart/disclosure_events/ -> delta/gold_db/disclosure_events/ (PK: rcept_no)

Execution modes:
  Daily:   spark-submit --master 'local[4]' --driver-memory 6g spark_silver_to_delta.py --date 20260619
  Backfill: spark-submit --master 'local[4]' --driver-memory 6g spark_silver_to_delta.py --backfill

JVM cold-start ~15s. Daily MERGE <30s. Full backfill 2-3 minutes.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("spark_delta_merge")


S3_BUCKET = os.environ.get("S3_BUCKET", "s3-opik-bucket")
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
S3_BASE = f"s3a://{S3_BUCKET}"

GOLD_STRUCTURED_PREFIX = f"{S3_BASE}/gold/structured/"
GOLD_EMBEDDINGS_PREFIX = f"{S3_BASE}/gold/embeddings/"
GOLD_DISCLOSURE_PREFIX = f"{S3_BASE}/gold/dart/disclosure_events/"

DELTA_BASE = f"{S3_BASE}/delta/gold_db"
DELTA_STRUCTURED = f"{DELTA_BASE}/structured"
DELTA_EMBEDDINGS = f"{DELTA_BASE}/embeddings"
DELTA_DISCLOSURE = f"{DELTA_BASE}/disclosure_events"

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("OPIK-Delta-MERGE")
        .master("local[4]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

def _ensure_delta_table(spark: SparkSession, delta_path: str,
                         sample_df: DataFrame, pk_col: str) -> DeltaTable:
    if DeltaTable.isDeltaTable(spark, delta_path):
        logger.debug("Delta table exists: %s", delta_path)
        return DeltaTable.forPath(spark, delta_path)

    logger.info("Creating new Delta table: %s (pk=%s)", delta_path, pk_col)
    (sample_df.limit(0).write
     .format("delta")
     .mode("overwrite")
     .option("delta.enableChangeDataFeed", "false")
     .save(delta_path))
    return DeltaTable.forPath(spark, delta_path)

def _merge_by_key(spark: SparkSession, df: DataFrame, delta_path: str,
                   pk_col: str) -> int:
    row_count = df.count()
    if row_count == 0:
        return 0

    table = _ensure_delta_table(spark, delta_path, df, pk_col)

    merge_condition = f"target.{pk_col} = source.{pk_col}"
    (table.alias("target")
     .merge(df.alias("source"), merge_condition)
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())

    return row_count

def _load_monthly_partition(spark: SparkSession, prefix: str,
                              year: int, month: int) -> Optional[DataFrame]:
    path = f"{prefix}year={year}/month={month:02d}/"
    try:
        df = spark.read.parquet(path)
        return df
    except Exception as e:
        logger.debug("No parquet at %s: %s", path, e)
        return None

def _load_disclosure_partitions(spark: SparkSession, prefix: str,
                                  target_dt: datetime,
                                  lookback_days: int = 1) -> Optional[DataFrame]:
    start_dt = target_dt - timedelta(days=lookback_days)

    months = set()
    d = start_dt
    while d <= target_dt:
        months.add(d.strftime("%Y-%m"))
        d = datetime(d.year, d.month, 1) + timedelta(days=32)
        d = datetime(d.year, d.month, 1)

    dfs = []
    for ym in sorted(months):
        path = f"{prefix}dt={ym}/"
        try:
            df = spark.read.parquet(path)
            dfs.append(df)
            logger.info("  Loaded dt=%s: %d rows", ym, df.count())
        except Exception:
            logger.debug("  No partition dt=%s", ym)

    if not dfs:
        return None

    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.unionByName(df)

    start_str = start_dt.strftime("%Y%m%d")
    end_str = target_dt.strftime("%Y%m%d")
    if "rcept_dt" in combined.columns:
        combined = combined.filter(
            (combined["rcept_dt"] >= start_str) & (combined["rcept_dt"] <= end_str)
        )

    return combined

def merge_structured(spark: SparkSession, year: int, month: int) -> int:
    df = _load_monthly_partition(spark, GOLD_STRUCTURED_PREFIX, year, month)
    if df is None:
        return 0
    return _merge_by_key(spark, df, DELTA_STRUCTURED, "report_id")

def merge_embeddings(spark: SparkSession, year: int, month: int) -> int:
    df = _load_monthly_partition(spark, GOLD_EMBEDDINGS_PREFIX, year, month)
    if df is None:
        return 0
    return _merge_by_key(spark, df, DELTA_EMBEDDINGS, "report_id")

def merge_disclosure_events(spark: SparkSession, target_date: datetime) -> int:
    df = _load_disclosure_partitions(spark, GOLD_DISCLOSURE_PREFIX, target_date)
    if df is None:
        return 0
    return _merge_by_key(spark, df, DELTA_DISCLOSURE, "rcept_no")

def backfill_all(spark: SparkSession, start_year: int = 2020,
                  end_year: int = 2026, end_month: int = 6) -> dict:
    total_s = 0
    total_e = 0

    for year in range(start_year, end_year + 1):
        m_start = 1
        m_end = 12
        if year == end_year:
            m_end = end_month

        for month in range(m_start, m_end + 1):
            ns = merge_structured(spark, year, month)
            ne = merge_embeddings(spark, year, month)
            total_s += ns
            total_e += ne
            if ns + ne > 0:
                logger.info("  %04d-%02d: structured=%d embeddings=%d", year, month, ns, ne)

    total_d = 0
    for y in range(2024, 2027):
        for m in range(1, 13):
            if y == 2024 and m < 8:
                continue
            if y == 2026 and m > 3:
                break
            if m == 12:
                target_dt = datetime(y + 1, 1, 1) - timedelta(days=1)
            else:
                target_dt = datetime(y + 1, m + 1, 1) - timedelta(days=1)
            target_dt = target_dt + timedelta(days=31)
            target_dt = datetime(target_dt.year, target_dt.month, 1) - timedelta(days=1)

            nd = merge_disclosure_events(spark, target_dt)
            total_d += nd
            if nd > 0:
                logger.info("  disclosure dt=%04d-%02d: %d rows", y, m, nd)

    result = {"structured": total_s, "embeddings": total_e, "disclosure": total_d}
    logger.info("Backfill complete: %s", result)
    return result

def daily_merge(spark: SparkSession, date_str: str) -> dict:
    target_dt = datetime.strptime(date_str, "%Y%m%d")
    year = target_dt.year
    month = target_dt.month

    logger.info("=== Daily Delta MERGE: %s (year=%d month=%d) ===", date_str, year, month)

    ns = merge_structured(spark, year, month)
    ne = merge_embeddings(spark, year, month)
    nd = merge_disclosure_events(spark, target_dt)

    result = {
        "date": date_str,
        "structured": ns,
        "embeddings": ne,
        "disclosure": nd,
        "total": ns + ne + nd,
    }

    logger.info("Daily MERGE done: %s", result)
    return result

def main():
    parser = argparse.ArgumentParser(description="OPIK Spark Delta Lake MERGE")
    parser.add_argument("--date", help="Target date YYYYMMDD for daily merge")
    parser.add_argument("--backfill", action="store_true", help="Full backfill of all existing partitions")
    parser.add_argument("--backfill-start", type=int, default=2020)
    parser.add_argument("--backfill-end", type=int, default=2026)
    parser.add_argument("--backfill-end-month", type=int, default=6)
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        if args.backfill:
            logger.info("Starting FULL BACKFILL %d-%d", args.backfill_start, args.backfill_end)
            result = backfill_all(
                spark,
                start_year=args.backfill_start,
                end_year=args.backfill_end,
                end_month=args.backfill_end_month,
            )
            print(f"BACKFILL OK: {result}")

        elif args.date:
            result = daily_merge(spark, args.date)
            print(f"MERGE OK: {result}")

        else:
            now = datetime.now()
            date_str = now.strftime("%Y%m%d")
            logger.info("No date given -- using today: %s", date_str)
            result = daily_merge(spark, date_str)
            print(f"MERGE OK: {result}")

    except Exception as e:
        logger.exception("Delta MERGE fatal error: %s", e)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
