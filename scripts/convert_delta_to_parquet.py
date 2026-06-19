"""
Convert Delta disclosure_events table to partitioned Parquet format.

Reads from:  delta/gold_db/disclosure_events/  (Delta Lake)
Writes to:   gold/dart/disclosure_events/dt=YYYY-MM/data.parquet  (partitioned Parquet)

Column dt is derived from rcept_dt column (YYYYMMDD format → YYYY-MM).
"""

import sys
import logging
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, col, concat, lit

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("convert_delta_to_parquet")

S3_BUCKET = "s3a://s3-opik-bucket"
DELTA_SRC = f"{S3_BUCKET}/delta/gold_db/disclosure_events"
PARQUET_DST = f"{S3_BUCKET}/gold/dart/disclosure_events"


def get_spark():
    return (
        SparkSession.builder
        .appName("OPIK-Delta-to-Parquet")
        .master("local[4]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def run():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Reading Delta table: %s", DELTA_SRC)
    df = spark.read.format("delta").load(DELTA_SRC)

    # Print schema
    log.info("Schema:")
    for f in df.schema.fields:
        log.info("  %s: %s", f.name, f.dataType)

    total_rows = df.count()
    log.info("Total rows in Delta table: %d", total_rows)

    # Check for rcept_dt column
    if "rcept_dt" not in df.columns:
        log.error("No rcept_dt column in Delta table!")
        log.info("Available columns: %s", df.columns)
        spark.stop()
        return 1

    # Show sample
    log.info("Sample rows (5):")
    df_sample = df.select(df.columns[:min(15, len(df.columns))]).limit(5)
    for row in df_sample.collect():
        log.info("  %s", row.asDict())

    # Derive dt partition column from rcept_dt (which is YYYYMMDD string)
    # dt = first 7 chars of rcept_dt → YYYY-MM
    log.info("Deriving dt=YYYY-MM from rcept_dt...")
    df_with_dt = df.withColumn("dt", substring(col("rcept_dt"), 1, 7))

    # Get distinct dt values
    distinct_dts = [r["dt"] for r in df_with_dt.select("dt").distinct().orderBy("dt").collect()]
    log.info("Distinct dt partitions found: %d", len(distinct_dts))
    log.info("First 10: %s", distinct_dts[:10])
    log.info("Last 10: %s", distinct_dts[-10:])

    # Filter to the expected range: 2024-08 through 2026-03
    allowed_dts = [d for d in distinct_dts if "2024-08" <= d <= "2026-03"]
    log.info("Partitions in range 2024-08 ~ 2026-03: %d", len(allowed_dts))
    if allowed_dts:
        df_filtered = df_with_dt.filter(col("dt").isin(allowed_dts))
        filtered_rows = df_filtered.count()
        log.info("Rows in range: %d", filtered_rows)

        # Write as partitioned Parquet — OVERWRITE mode (safe for first time)
        log.info("Writing to %s with partitionBy=dt ...", PARQUET_DST)
        df_filtered.write \
            .mode("overwrite") \
            .partitionBy("dt") \
            .option("compression", "snappy") \
            .parquet(PARQUET_DST)

        log.info("Write complete!")

        # Verify — list partitions
        log.info("Verifying partitions at %s ...", PARQUET_DST)
        df_verify = spark.read.parquet(PARQUET_DST)
        verify_count = df_verify.count()
        verify_dts = sorted([r["dt"] for r in df_verify.select("dt").distinct().collect()])
        log.info("Verified: %d total rows, %d partitions", verify_count, len(verify_dts))
        log.info("First 5 partitions: %s", verify_dts[:5])
        log.info("Last 5 partitions: %s", verify_dts[-5:])
    else:
        log.warning("No partitions in range 2024-08 ~ 2026-03 found!")

    spark.stop()
    result = {
        "total_rows_in_delta": total_rows,
        "partitions_found": len(distinct_dts),
        "partitions_in_range": len(allowed_dts),
        "first_partitions": distinct_dts[:5] if distinct_dts else [],
        "last_partitions": distinct_dts[-5:] if distinct_dts else [],
    }
    print(f"RESULT: {json.dumps(result)}")
    return 0


if __name__ == "__main__":
    sys.exit(run())
