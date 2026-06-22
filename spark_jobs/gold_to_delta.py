"""
Spark Delta Lake MERGE — daily Gold Parquet to Delta table sync.

Runs on EC2 r6g.large in Spark local mode, dag_maintenance_delta_faiss(Dataset 트리거)에서 호출.
Merges six Gold tables:
  1. gold/structured/             -> delta/gold_db/structured/              (PK: report_id)
  2. gold/embeddings/             -> delta/gold_db/embeddings/              (PK: report_id)
  3. gold/dart/facts/material_event/ -> delta/gold_db/material_event/      (PK: event_id)
  4. gold/dart/facts/financial_statement/ -> delta/gold_db/dart_financial_statement/ (PK: fact_id)
  5. gold/dart/facts/ownership/   -> delta/gold_db/dart_ownership/         (PK: ownership_fact_id)
  6. gold/dart/facts/regular_structured/ -> delta/gold_db/dart_regular_structured/ (PK: fact_id)

Execution modes:
  Daily:   spark-submit --master 'local[4]' --driver-memory 6g gold_to_delta.py --date 20260619
  Backfill: spark-submit --master 'local[4]' --driver-memory 6g gold_to_delta.py --backfill

JVM cold-start ~15s. Daily MERGE <2min. Full backfill 3-5 minutes.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("spark_delta_merge")


S3_BUCKET = os.environ.get("S3_BUCKET", "s3-opik-bucket")
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
S3_BASE = f"s3a://{S3_BUCKET}"

GOLD_STRUCTURED_PREFIX = f"{S3_BASE}/gold/structured/"
GOLD_EMBEDDINGS_PREFIX = f"{S3_BASE}/gold/embeddings/"
# DartCollector 정본 경로(event_type=/rcept_year=/rcept_month= Hive 파티션). 구
# gold/dart/disclosure_events/dt= 경로는 신 파이프라인이 더 이상 쓰지 않아 폐기.
GOLD_MATERIAL_EVENT_PREFIX = f"{S3_BASE}/gold/dart/facts/material_event/"

DELTA_BASE = f"{S3_BASE}/delta/gold_db"
DELTA_STRUCTURED = f"{DELTA_BASE}/structured"
DELTA_EMBEDDINGS = f"{DELTA_BASE}/embeddings"
# material_event는 rcept_no당 다중 행(event_id=rcept_no:event_type:i) → PK=event_id.
# 구 disclosure_events(PK rcept_no, 1커밋)는 은퇴.
DELTA_MATERIAL_EVENT = f"{DELTA_BASE}/material_event"

# DART facts 3종 — all-string 개별파일 읽기로 schema drift (report_tp='약식', stkqy='515,679') 우회.
GOLD_DART_FINANCIAL_STATEMENT_PREFIX = f"{S3_BASE}/gold/dart/facts/financial_statement/"
GOLD_DART_OWNERSHIP_PREFIX = f"{S3_BASE}/gold/dart/facts/ownership/"
GOLD_DART_REGULAR_STRUCTURED_PREFIX = f"{S3_BASE}/gold/dart/facts/regular_structured/"

DELTA_DART_FINANCIAL_STATEMENT = f"{DELTA_BASE}/dart_financial_statement"
DELTA_DART_OWNERSHIP = f"{DELTA_BASE}/dart_ownership"
DELTA_DART_REGULAR_STRUCTURED = f"{DELTA_BASE}/dart_regular_structured"

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

def _load_dart_parquet_files(spark: SparkSession, s3_prefix: str) -> Optional[DataFrame]:
    """DART facts 개별 Parquet 파일을 all-string으로 읽어 union.

    compaction 파일 간 타입 불일치(INT32/BINARY/string, '약식'/'515,679' 등)를
    우회하기 위해 모든 컬럼을 string으로 cast. boto3로 S3 prefix 하위 .parquet
    전수 리스팅 → 개별 spark.read.parquet → 전 컬럼 .cast("string") → unionByName.
    """
    import boto3 as b3

    prefix = s3_prefix
    if prefix.startswith("s3a://"):
        prefix = prefix[6:]
    bucket = prefix.split("/", 1)[0]
    key_prefix = prefix.split("/", 1)[1] if "/" in prefix else ""

    s3c = b3.client("s3")
    file_list = []
    paginator = s3c.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                file_list.append(f"s3a://{bucket}/{obj['Key']}")

    if not file_list:
        logger.warning("No parquet files under %s", s3_prefix)
        return None

    logger.info("Loading %d parquet files from %s (all-string cast)...", len(file_list), s3_prefix)

    all_dfs = []
    failed = 0
    for i, fpath in enumerate(file_list):
        if i > 0 and i % 100 == 0:
            logger.info("  file %d/%d...", i, len(file_list))
        try:
            df_i = spark.read.parquet(fpath)
            for c in df_i.columns:
                df_i = df_i.withColumn(c, F.col(c).cast("string"))
            all_dfs.append(df_i)
        except Exception as e:
            failed += 1
            logger.debug("  skip %s: %s", fpath[-80:], e)

    if not all_dfs:
        logger.warning("All %d files failed to read from %s", len(file_list), s3_prefix)
        return None

    df = all_dfs[0]
    for df_i in all_dfs[1:]:
        df = df.unionByName(df_i, allowMissingColumns=True)

    logger.info("  %d rows, %d cols (ok=%d failed=%d)", df.count(), len(df.columns), len(all_dfs), failed)
    return df


def _load_material_event_partitions(spark: SparkSession, prefix: str,
                                    target_dt: datetime,
                                    lookback_days: int = 1) -> Optional[DataFrame]:
    """최근 월의 material_event 파티션을 읽는다.

    경로: facts/material_event/event_type={..}/rcept_year={Y}/rcept_month={M}/part-*.parquet
    event_type=* 글롭으로 한 월의 모든 이벤트 유형을 모은다. 파티션 컬럼 추론은 끄고
    (basePath 미지정) row 데이터 컬럼(event_id, rcept_dt 등)만 사용 → 컬럼 충돌 회피.
    """
    start_dt = target_dt - timedelta(days=lookback_days)

    months = set()
    d = start_dt
    while d <= target_dt:
        months.add((f"{d.year:04d}", f"{d.month:02d}"))
        d = datetime(d.year, d.month, 1) + timedelta(days=32)
        d = datetime(d.year, d.month, 1)

    dfs = []
    for y, m in sorted(months):
        path = f"{prefix}event_type=*/rcept_year={y}/rcept_month={m}/"
        try:
            df = spark.read.parquet(path)
            dfs.append(df)
            logger.info("  Loaded material_event %s-%s: %d rows", y, m, df.count())
        except Exception:
            logger.debug("  No material_event partition %s-%s", y, m)

    if not dfs:
        return None

    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    # rcept_dt 윈도우 정밀 필터(파티션은 월 단위라 경계 보정). 포맷(YYYYMMDD/YYYY-MM-DD)
    # 무관하게 '-' 제거 후 비교. MERGE가 event_id 멱등이라 경계가 느슨해도 안전.
    start_str = start_dt.strftime("%Y%m%d")
    end_str = target_dt.strftime("%Y%m%d")
    if "rcept_dt" in combined.columns:
        norm = F.regexp_replace(combined["rcept_dt"].cast("string"), "-", "")
        combined = combined.filter((norm >= start_str) & (norm <= end_str))

    return combined


def _load_all_material_event(spark: SparkSession, prefix: str) -> Optional[DataFrame]:
    """백필용 전체 적재. 개별 Parquet 파일을 all-string으로 읽어 union → 정규화.
    compaction 파일 간 타입 불일치(INT32/BINARY/string)를 우회한다.
    Daily MERGE는 _load_material_event_partitions로 단월 읽기 → 이 함수는 백필 전용."""
    import json
    try:
        import boto3 as b3
    except ImportError:
        b3 = None

    base = prefix
    if base.startswith("s3a://"):
        base = base[6:]  # s3a://bucket/prefix → bucket/prefix
    bucket = base.split("/", 1)[0]
    key_prefix = base.split("/", 1)[1] if "/" in base else ""

    file_list = []
    if b3:
        s3c = b3.client("s3")
        paginator = s3c.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    file_list.append(f"s3a://{bucket}/{obj['Key']}")
    else:
        # fallback: Hadoop FS listing
        hadoop = spark._jvm.org.apache.hadoop
        fs = hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration(),
        )
        path = hadoop.fs.Path(prefix)
        it = fs.listFiles(path, True)
        lst = []
        while it.hasNext():
            f = it.next().getPath().toString()
            if f.endswith(".parquet"):
                lst.append(f)
        file_list = lst

    if not file_list:
        logger.warning("material_event: no parquet files found under %s", prefix)
        return None

    logger.info("material_event: reading %d parquet files individually (schema-safe)...", len(file_list))

    all_dfs = []
    for i, fpath in enumerate(file_list):
        try:
            df_i = spark.read.parquet(fpath)
            for c in df_i.columns:
                df_i = df_i.withColumn(c, F.col(c).cast("string"))
            all_dfs.append(df_i)
        except Exception as e:
            logger.debug("material_event file %d skip: %s", i + 1, e)

    if not all_dfs:
        logger.warning("material_event: all %d files failed to read", len(file_list))
        return None

    df = all_dfs[0]
    for df_i in all_dfs[1:]:
        df = df.unionByName(df_i, allowMissingColumns=True)

    # 정규화: Delta target schema (amount=long, is_latest=boolean, 나머지 string)
    if "amount" in df.columns:
        df = df.withColumn("amount", F.col("amount").cast("long"))
    if "is_latest" in df.columns:
        df = df.withColumn(
            "is_latest",
            F.when(F.col("is_latest").isin("true", "True", "1"), True).otherwise(False)
        )

    logger.info("material_event full read: %d rows from %d/%d files", df.count(), len(all_dfs), len(file_list))
    return df

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

def merge_material_event(spark: SparkSession, target_date: datetime) -> int:
    df = _load_material_event_partitions(spark, GOLD_MATERIAL_EVENT_PREFIX, target_date)
    if df is None:
        return 0
    # MERGE는 source 키가 유일해야 한다(중복 시 "multiple source rows matched").
    # 재처리/컴팩션 전 part로 동일 event_id가 중복될 수 있어 source에서 선제 dedup.
    df = df.dropDuplicates(["event_id"])
    return _merge_by_key(spark, df, DELTA_MATERIAL_EVENT, "event_id")

def merge_dart_financial_statement(spark: SparkSession, year: int = None) -> int:
    """Merge dart_financial_statement: bsns_year 파티션 Delta MERGE. PK=fact_id.
    year 지정 시 bsns_year={year}/ 만 읽고, 없으면 전체(백필)."""
    prefix = GOLD_DART_FINANCIAL_STATEMENT_PREFIX
    if year is not None:
        prefix = f"{prefix}bsns_year={year}/"
    df = _load_dart_parquet_files(spark, prefix)
    if df is None:
        return 0
    df = df.dropDuplicates(["fact_id"])
    return _merge_by_key(spark, df, DELTA_DART_FINANCIAL_STATEMENT, "fact_id")

def merge_dart_ownership(spark: SparkSession, year: int = None,
                         month: int = None) -> int:
    """Merge dart_ownership: Delta MERGE. PK=ownership_fact_id.
    ownership은 50파일 내외로 작아 전체 로드 (year/month 무시)."""
    df = _load_dart_parquet_files(spark, GOLD_DART_OWNERSHIP_PREFIX)
    if df is None:
        return 0
    df = df.dropDuplicates(["ownership_fact_id"])
    return _merge_by_key(spark, df, DELTA_DART_OWNERSHIP, "ownership_fact_id")

def merge_dart_regular_structured(spark: SparkSession, year: int = None,
                                  month: int = None) -> int:
    """Merge dart_regular_structured: Delta MERGE. PK=fact_id.
    regular_structured는 200파일 내외로 작아 전체 로드 (year/month 무시)."""
    df = _load_dart_parquet_files(spark, GOLD_DART_REGULAR_STRUCTURED_PREFIX)
    if df is None:
        return 0
    df = df.dropDuplicates(["fact_id"])
    return _merge_by_key(spark, df, DELTA_DART_REGULAR_STRUCTURED, "fact_id")


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

    # material_event 백필: 전체 트리를 한 번에 읽어 event_id로 MERGE(멱등).
    total_d = 0
    df_me = _load_all_material_event(spark, GOLD_MATERIAL_EVENT_PREFIX)
    if df_me is not None:
        df_me = df_me.dropDuplicates(["event_id"])
        total_d = _merge_by_key(spark, df_me, DELTA_MATERIAL_EVENT, "event_id")
        logger.info("  material_event backfill: %d rows", total_d)

    # DART facts 3종 백필: 전체 Parquet 트리를 all-string으로 읽어 PK MERGE(멱등).
    nd_fs = merge_dart_financial_statement(spark)       # bsns_year 파티션, year=None=전체
    nd_ow = merge_dart_ownership(spark)                 # 50파일 내외, 전량
    nd_rs = merge_dart_regular_structured(spark)        # 200파일 내외, 전량
    logger.info("  dart_financial_statement backfill: %d rows", nd_fs)
    logger.info("  dart_ownership backfill: %d rows", nd_ow)
    logger.info("  dart_regular_structured backfill: %d rows", nd_rs)

    result = {
        "structured": total_s,
        "embeddings": total_e,
        "material_event": total_d,
        "dart_financial_statement": nd_fs,
        "dart_ownership": nd_ow,
        "dart_regular_structured": nd_rs,
    }
    logger.info("Backfill complete: %s", result)
    return result

def daily_merge(spark: SparkSession, date_str: str) -> dict:
    target_dt = datetime.strptime(date_str, "%Y%m%d")
    year = target_dt.year
    month = target_dt.month

    logger.info("=== Daily Delta MERGE: %s (year=%d month=%d) ===", date_str, year, month)

    ns = merge_structured(spark, year, month)
    ne = merge_embeddings(spark, year, month)
    nd = merge_material_event(spark, target_dt)

    # DART facts 3종: financial_statement는 올해 bsns_year= 필터링(연도 단위),
    # ownership(50파일)/regular_structured(200파일)은 소량이므로 전량 MERGE.
    nd_fs = merge_dart_financial_statement(spark, year=year)
    nd_ow = merge_dart_ownership(spark)
    nd_rs = merge_dart_regular_structured(spark)

    result = {
        "date": date_str,
        "structured": ns,
        "embeddings": ne,
        "material_event": nd,
        "dart_financial_statement": nd_fs,
        "dart_ownership": nd_ow,
        "dart_regular_structured": nd_rs,
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
