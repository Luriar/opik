"""Spark Silver-to-Delta MERGE job.

Phase 2b — merges today's Gold Structured + LLM parquet files into Delta Lake tables.

Args:
    date: YYYY-MM-DD (from Airflow {{ ds }})

Delta tables:
    - gold_db.structured: report_id PK, opinion, TP, stock_code, ...
    - gold_db.llm:         report_id PK, reason, risks, keywords, embedding

MERGE semantics:
    - MATCHED: update existing rows (e.g., re-extracted same report)
    - NOT MATCHED: insert new rows
    - WHEN NOT MATCHED BY SOURCE: no-op (keep historical data)

Run:
    spark-submit spark_silver_to_delta.py 2026-06-12
"""

from pyspark.sql import SparkSession
import sys

S3_BUCKET = "s3-opik-bucket"
DELTA_PATH = f"s3a://{S3_BUCKET}/delta"


def merge_structured(spark, date):
    """Merge gold/structured/ parquet into gold_db.structured Delta table."""
    year, month, day = date.split("-")
    source_path = (
        f"s3a://{S3_BUCKET}/gold/structured/"
        f"year={year}/month={month}/data.parquet"
    )

    source = spark.read.parquet(source_path)
    source.createOrReplaceTempView("source_structured")

    spark.sql(f"""
        MERGE INTO delta.`{DELTA_PATH}/gold_db.structured` AS target
        USING source_structured AS source
        ON target.report_id = source.report_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    return source.count()


def merge_llm(spark, date):
    """Merge gold/embeddings/ parquet into gold_db.llm Delta table."""
    year, month, day = date.split("-")
    source_path = (
        f"s3a://{S3_BUCKET}/gold/embeddings/"
        f"year={year}/month={month}/data.parquet"
    )

    source = spark.read.parquet(source_path)
    source.createOrReplaceTempView("source_llm")

    spark.sql(f"""
        MERGE INTO delta.`{DELTA_PATH}/gold_db.llm` AS target
        USING source_llm AS source
        ON target.report_id = source.report_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    return source.count()


def main():
    if len(sys.argv) < 2:
        print("Usage: spark_silver_to_delta.py <YYYY-MM-DD>")
        sys.exit(1)

    date = sys.argv[1]

    spark = (
        SparkSession.builder
        .appName(f"opik-silver-to-delta-{date}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "6g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )

    try:
        n_struct = merge_structured(spark, date)
        n_llm = merge_llm(spark, date)
        print(f"Merged: {n_struct} structured + {n_llm} LLM rows")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
