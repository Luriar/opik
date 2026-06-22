"""Single-purpose backfill: material_event Delta only.
v5 — individual file read with all-string cast to survive compaction schema drift.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, BooleanType
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("material_event_backfill") \
    .master("local[4]") \
    .config("spark.driver.memory", "6g") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

src_prefix = "s3a://s3-opik-bucket/gold/dart/facts/material_event/"
delta_path = "s3a://s3-opik-bucket/delta/gold_db/material_event"

# List files via boto3 (avoids Hadoop FS s3a listing issues)
import boto3 as b3
s3c = b3.client("s3")
files = []
paginator = s3c.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket="s3-opik-bucket", Prefix="gold/dart/facts/material_event/"):
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            files.append(f"s3a://s3-opik-bucket/{obj['Key']}")

print(f"Found {len(files)} parquet files")

# Read each file individually, cast all columns to string, union
all_dfs = []
for i, fpath in enumerate(files):
    if i % 10 == 0:
        print(f"  file {i+1}/{len(files)}...")
    try:
        df_i = spark.read.parquet(fpath)
        for c in df_i.columns:
            df_i = df_i.withColumn(c, F.col(c).cast("string"))
        all_dfs.append(df_i)
    except Exception as e:
        print(f"  FAIL {fpath[-80:]}: {e}")

print(f"Read {len(all_dfs)}/{len(files)} files ok")

if not all_dfs:
    print("ABORT - no files read")
    spark.stop()
    exit(1)

df = all_dfs[0]
for df_i in all_dfs[1:]:
    df = df.unionByName(df_i, allowMissingColumns=True)

total = df.count()
print(f"Union: {total} rows, {len(df.columns)} cols")

# Cast to target types
if "amount" in df.columns:
    df = df.withColumn("amount", F.col("amount").cast(LongType()))
if "is_latest" in df.columns:
    df = df.withColumn("is_latest", F.when(F.col("is_latest").isin("true", "True", "1"), True).otherwise(False))

if "event_id" in df.columns:
    df = df.dropDuplicates(["event_id"])
    print(f"After dedup: {df.count()}")

print("MERGE...")
table = DeltaTable.forPath(spark, delta_path)
table.alias("t").merge(df.alias("s"), "t.event_id = s.event_id") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

print(f"MERGE done. {total} source rows processed")
final = spark.read.format("delta").load(delta_path)
print(f"SUCCESS: Delta material_event = {final.count()} rows")
spark.stop()
