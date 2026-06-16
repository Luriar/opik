"""Spark Score Computation job.

Phase 2b — 3-way JOIN + composite scoring.

Args:
    date: YYYY-MM-DD (from Airflow {{ ds }})

Input tables (Delta Lake):
    - gold_db.structured:  report-level opinion/TP/stock_code
    - gold_db.llm:          report-level reason/risks/keywords/embedding
    - predictions_db.daily: stock-level price predictions (Chanho)
    - dart_db.impact:       stock-level DART disclosure impact (Sangyong)

Scoring formula:
    composite_score = 0.4 * a_score(price) + 0.3 * b_score(dart) + 0.3 * c_score(report)

Output:
    - recommendations.daily_picks: stock-level recommendations with scores
    - Delta Lake ACID ensures atomic write

Run:
    spark-submit spark_compute_scores.py 2026-06-12
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import sys
import math

S3_BUCKET = "s3-opik-bucket"
DELTA_PATH = f"s3a://{S3_BUCKET}/delta"


def compute_c_score(spark):
    """Compute c_score (report sentiment) from gold_db tables.

    c_score components:
        - Opinion mapping: BUY=+1.0, HOLD=0.0, SELL=-1.0, NR/null=0.0
        - TP upside: (TP - CP) / CP, capped at [-1.0, +1.0]
        - Weighted average per stock per day

    Returns DataFrame with (종목코드, c_score).
    """
    structured = spark.read.format("delta").load(
        f"{DELTA_PATH}/gold_db.structured"
    )

    # Opinion score mapping
    opinion_score = F.when(F.col("투자의견") == "BUY", 1.0) \
        .when(F.col("투자의견") == "SELL", -1.0) \
        .otherwise(0.0)

    # TP upside score (capped)
    tp_upside = F.when(
        F.col("목표주가").isNotNull() & F.col("현재주가").isNotNull(),
        (F.col("목표주가") - F.col("현재주가")) / F.col("현재주가")
    ).otherwise(0.0)
    tp_score = F.when(tp_upside > 1.0, 1.0) \
        .when(tp_upside < -1.0, -1.0) \
        .otherwise(tp_upside)

    c_score = (0.5 * opinion_score + 0.5 * tp_score).alias("c_score")

    return structured.select(
        F.col("종목코드"),
        c_score,
        F.col("증권사"),
    ).groupBy("종목코드").agg(
        F.avg("c_score").alias("c_score"),
        F.count("*").alias("report_count"),
    )


def compute_composite_scores(spark, date, c_score_df):
    """Join c_score with partner data and compute composite.

    composite = 0.4 * a_score + 0.3 * b_score + 0.3 * c_score

    Partner data may be missing (FileSensor soft_fail) — use defaults.
    """

    # Try loading partner data; fall back to defaults if missing
    try:
        preds = spark.read.format("delta").load(
            f"{DELTA_PATH}/predictions_db.daily"
        ).filter(F.col("date") == date)
        has_preds = preds.count() > 0
    except Exception:
        has_preds = False

    try:
        dart = spark.read.format("delta").load(
            f"{DELTA_PATH}/dart_db.impact"
        ).filter(F.col("date") == date)
        has_dart = dart.count() > 0
    except Exception:
        has_dart = False

    # Build composite dataframe
    if has_preds:
        composite = c_score_df.join(
            preds.select("종목코드", F.col("score").alias("a_score")),
            on="종목코드",
            how="left",
        )
    else:
        composite = c_score_df.withColumn("a_score", F.lit(0.0))

    if has_dart:
        composite = composite.join(
            dart.select("종목코드", F.col("impact_score").alias("b_score")),
            on="종목코드",
            how="left",
        )
    else:
        composite = composite.withColumn("b_score", F.lit(0.0))

    # Fill missing scores with 0
    composite = composite.fillna({"a_score": 0.0, "b_score": 0.0})

    # Compute final composite score
    composite = composite.withColumn(
        "composite_score",
        0.4 * F.col("a_score") + 0.3 * F.col("b_score") + 0.3 * F.col("c_score"),
    )

    # Add date partition
    composite = composite.withColumn("date", F.lit(date))

    return composite.select(
        "date", "종목코드", "a_score", "b_score", "c_score",
        "composite_score", "report_count",
    )


def main():
    if len(sys.argv) < 2:
        print("Usage: spark_compute_scores.py <YYYY-MM-DD>")
        sys.exit(1)

    date = sys.argv[1]

    spark = (
        SparkSession.builder
        .appName(f"opik-compute-scores-{date}")
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
        c_score_df = compute_c_score(spark)
        recommendations = compute_composite_scores(spark, date, c_score_df)

        # Write to Delta Lake
        recommendations.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(f"{DELTA_PATH}/recommendations.daily_picks")

        count = recommendations.count()
        high = recommendations.filter(F.col("composite_score") > 0.5).count()
        print(f"Wrote {count} recommendations ({high} above threshold)")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
