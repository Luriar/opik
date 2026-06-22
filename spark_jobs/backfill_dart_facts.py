"""Backfill 3 DART facts tables to Delta Lake.
financial_statement (933 files, PK=fact_id)
ownership (1,366 files, PK=ownership_fact_id)
regular_structured (4,288 files, PK=fact_id)

v1 — individual file read + all-string cast (survives compaction schema drift).
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, BooleanType, IntegerType
from delta.tables import DeltaTable
import boto3

spark = SparkSession.builder \
    .appName("dart_facts_backfill") \
    .master("local[4]") \
    .config("spark.driver.memory", "6g") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

s3c = boto3.client("s3")


def load_all_safe(s3_prefix, pk_col):
    """List all parquet files under s3_prefix, read individually with all-string cast, union, dedup."""
    bucket = "s3-opik-bucket"
    key_prefix = s3_prefix
    files = []
    paginator = s3c.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(f"s3a://{bucket}/{obj['Key']}")

    print(f"  [{s3_prefix}] Found {len(files)} parquet files")

    all_dfs = []
    failed = 0
    for i, fpath in enumerate(files):
        if i % 50 == 0:
            print(f"    file {i+1}/{len(files)}...")
        try:
            df_i = spark.read.parquet(fpath)
            for c in df_i.columns:
                df_i = df_i.withColumn(c, F.col(c).cast("string"))
            all_dfs.append(df_i)
        except Exception as e:
            failed += 1
            if failed <= 3:
                print(f"    FAIL {fpath[-80:]}: {e}")

    print(f"    Read {len(all_dfs)}/{len(files)} ok, {failed} failed")
    if not all_dfs:
        print(f"    ABORT - no files read")
        return None

    df = all_dfs[0]
    for df_i in all_dfs[1:]:
        df = df.unionByName(df_i, allowMissingColumns=True)

    before = df.count()
    print(f"    Union: {before} rows, {len(df.columns)} cols")

    # Dedup by PK
    df = df.dropDuplicates([pk_col])
    after = df.count()
    if before != after:
        print(f"    Dedup: {before} -> {after}")

    return df


def merge_table(df, delta_path, pk_col, table_name):
    """MERGE df into Delta table, creating if needed."""
    if df is None:
        return 0

    print(f"  [{table_name}] MERGE...")
    if DeltaTable.isDeltaTable(spark, delta_path):
        table = DeltaTable.forPath(spark, delta_path)
        table.alias("t").merge(
            df.alias("s"), f"t.{pk_col} = s.{pk_col}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)

    final = spark.read.format("delta").load(delta_path)
    cnt = final.count()
    print(f"  [{table_name}] DONE: {cnt} rows in Delta")
    return cnt


# ── Table 1: financial_statement ──
print("=" * 60)
print("1/3: financial_statement")
print("=" * 60)
df_fs = load_all_safe("gold/dart/facts/financial_statement/", "fact_id")
cnt_fs = merge_table(df_fs, "s3a://s3-opik-bucket/delta/gold_db/dart_financial_statement", "fact_id", "financial_statement")

# ── Table 2: ownership ──
print("=" * 60)
print("2/3: ownership")
print("=" * 60)
df_ow = load_all_safe("gold/dart/facts/ownership/", "ownership_fact_id")
# Cast back typed columns
if df_ow is not None:
    if "report_tp" in df_ow.columns:
        df_ow = df_ow.withColumn("report_tp", F.col("report_tp").cast(IntegerType()))
    if "stkqy" in df_ow.columns:
        df_ow = df_ow.withColumn("stkqy", F.col("stkqy").cast("long"))
    if "is_latest" in df_ow.columns:
        df_ow = df_ow.withColumn("is_latest", F.when(F.col("is_latest").isin("true", "True", "1"), True).otherwise(False))
cnt_ow = merge_table(df_ow, "s3a://s3-opik-bucket/delta/gold_db/dart_ownership", "ownership_fact_id", "ownership")

# ── Table 3: regular_structured ──
print("=" * 60)
print("3/3: regular_structured")
print("=" * 60)
df_rs = load_all_safe("gold/dart/facts/regular_structured/", "fact_id")
cnt_rs = merge_table(df_rs, "s3a://s3-opik-bucket/delta/gold_db/dart_regular_structured", "fact_id", "regular_structured")

# ── Summary ──
print("=" * 60)
print(f"BACKFILL COMPLETE")
print(f"  financial_statement: {cnt_fs} rows")
print(f"  ownership:           {cnt_ow} rows")
print(f"  regular_structured:  {cnt_rs} rows")
print(f"  TOTAL:               {cnt_fs + cnt_ow + cnt_rs} rows")
print("=" * 60)
spark.stop()
