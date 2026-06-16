"""OPIK Nightly Batch DAG

Phase 2a (current): collect -> silver -> gold_structured -> telegram
Phase 2b (pending): + LLM Gold + Spark Delta + scoring + partner wait

Schedule: 16:00 KST daily (after market close)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

import logging
import subprocess
import tempfile
import os as _os

logger = logging.getLogger("opik.dag")

SCRIPTS_DIR = "/opt/airflow/scripts"


# =============================================================================
# Callbacks
# =============================================================================

def rollback_delta_on_failure(context):
    """Spark job failure -> Delta Lake Time Travel rollback to last good version.

    Writes rollback code to a temp file and submits via spark-submit.
    Phase 2b only — no-op when Spark is not available.
    """
    table = "recommendations.daily_picks"
    delta_path = f"s3a://s3-opik-bucket/delta/{table}"

    rollback_code = f"""\
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
try:
    history = spark.sql(f"DESCRIBE HISTORY delta.`{{delta_path}}`")
    last_writes = history.filter("operation == 'WRITE'") \\
                         .orderBy("version", ascending=False).limit(2).collect()
    if len(last_writes) == 2:
        prev_version = last_writes[1]["version"]
        spark.sql(f"RESTORE TABLE delta.`{{delta_path}}` TO VERSION AS OF {{prev_version}}")
        print(f"[Rollback] {{table}} restored to version {{prev_version}}")
    else:
        print(f"[Rollback] No previous write version found for {{table}}")
except Exception as e:
    print(f"[Rollback] Failed: {{e}}")
finally:
    spark.stop()
"""
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", prefix="delta_rollback_", delete=False
        ) as f:
            f.write(rollback_code)
            tmp_path = f.name
        subprocess.run(
            ["spark-submit", tmp_path],
            check=False,
            timeout=60,
        )
        _os.unlink(tmp_path)
    except FileNotFoundError:
        logger.warning("[Rollback] spark-submit not found — skipping (Phase 2a)")
    except Exception as e:
        logger.error(f"[Rollback] Unexpected error: {e}")


def notify_telegram_failure(context):
    """Send failure notification via Telegram."""
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    exec_date = context["execution_date"]
    error_msg = (
        f"OPIK DAG Failure\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Date: {exec_date.strftime('%Y-%m-%d')}"
    )
    logger.error(error_msg)
    # Telegram notification would use environment-configured bot


# =============================================================================
# DAG definition
# =============================================================================

default_args = {
    "owner": "opik",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 15),
    "retries": 0,  # per-task retries defined individually
    "execution_timeout": timedelta(hours=4),
    "on_failure_callback": None,  # per-task override where needed
}

with DAG(
    dag_id="nightly_batch",
    default_args=default_args,
    schedule="0 16 * * *",       # 16:00 KST daily (market close + 30min)
    catchup=False,
    max_active_runs=1,
    tags=["opik", "phase2"],
    description="OPIK nightly: collect reports -> extract -> score -> telegram briefing",
) as dag:

    # =========================================================================
    # Task 1: Collect reports (independent, parallel)
    # Both run simultaneously. Naver failure does not block Koreainvest.
    # =========================================================================

    collect_naver = BashOperator(
        task_id="collect_naver",
        bash_command=(
            f"cd {SCRIPTS_DIR} && "
            "python collectors/naver.py --upload --date {{ ds }}"
        ),
        retries=3,
        retry_delay=timedelta(minutes=5),
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=15),
    )

    collect_koreainvest = BashOperator(
        task_id="collect_koreainvest",
        bash_command=(
            f"cd {SCRIPTS_DIR} && "
            "python collectors/koreainvest.py --upload --date {{ ds }}"
        ),
        retries=3,
        retry_delay=timedelta(minutes=5),
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=15),
    )

    # =========================================================================
    # Task 2: Silver extraction (PDF -> text)
    # Runs after BOTH collectors complete (at least one must succeed)
    # =========================================================================

    extract_silver = BashOperator(
        task_id="extract_silver",
        bash_command=(
            f"python {SCRIPTS_DIR}/extract_silver.py "
            "--start {{ ds }} --end {{ ds }} --workers 20"
        ),
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=10),
    )

    # =========================================================================
    # Task 3: Gold Structured extraction (regex-based)
    # =========================================================================

    extract_gold_structured = BashOperator(
        task_id="extract_gold_structured",
        bash_command=(
            f"python {SCRIPTS_DIR}/extract_gold_structured.py "
            "--start {{ ds }} --end {{ ds }} --workers 20 --force-refresh"
        ),
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=15),
    )

    # =========================================================================
    # Task 4: Gold LLM extraction (Haiku API)
    # Phase 2b — pending extract_gold_llm.py implementation
    # =========================================================================

    extract_gold_llm = BashOperator(
        task_id="extract_gold_llm",
        bash_command=(
            f"python {SCRIPTS_DIR}/extract_gold_llm.py "
            "--date {{ ds }} --workers 20"
        ),
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
        trigger_rule="all_done",  # LLM failure should not block telegram
    )

    # =========================================================================
    # Task 5: Silver -> Delta Lake MERGE (Spark)
    # Phase 2b — pending spark_silver_to_delta.py implementation
    # =========================================================================

    silver_to_delta = SparkSubmitOperator(
        task_id="silver_to_delta",
        application="/opt/airflow/spark_jobs/spark_silver_to_delta.py",
        application_args=["{{ ds }}"],
        conn_id="spark_local",
        conf={
            "spark.driver.memory": "6g",
            "spark.executor.memory": "4g",
        },
        retries=2,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=rollback_delta_on_failure,
        execution_timeout=timedelta(minutes=10),
    )

    # =========================================================================
    # Task 6: Wait for partner data (Chanho's predictions, Sangyong's DART)
    # Polls S3 for partner parquet files. Timeout after 6hr (22:00 KST).
    # soft_fail=True: proceed with c_score only if partners don't deliver.
    # Phase 2b — requires partner pipeline integration
    # =========================================================================

    wait_for_partners = FileSensor(
        task_id="wait_for_partners",
        filepath="predictions/{{ ds }}/stock_scores.parquet",
        fs_conn_id="s3_opik",
        poke_interval=timedelta(minutes=10),
        timeout=timedelta(hours=6),    # wait until 22:00 KST
        mode="reschedule",
        soft_fail=True,
    )

    # =========================================================================
    # Task 7: Compute recommendations (Spark 3-way JOIN + scoring)
    # Phase 2b — pending spark_compute_scores.py implementation
    # =========================================================================

    compute_recommendations = SparkSubmitOperator(
        task_id="compute_recommendations",
        application="/opt/airflow/spark_jobs/spark_compute_scores.py",
        application_args=["{{ ds }}"],
        conn_id="spark_local",
        conf={
            "spark.driver.memory": "6g",
            "spark.executor.memory": "4g",
        },
        retries=2,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=rollback_delta_on_failure,
        execution_timeout=timedelta(minutes=15),
    )

    # =========================================================================
    # Task 8: Telegram briefing delivery
    # Sends integrated briefing (structured + LLM Gold if available).
    # =========================================================================

    deliver_telegram = BashOperator(
        task_id="deliver_telegram",
        bash_command=(
            f"python {SCRIPTS_DIR}/telegram_briefing.py --date {{ ds }}"
        ),
        retries=3,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=5),
    )

    # =========================================================================
    # DAG dependency graph
    #
    # Phase 2a (current):
    #   [collect_naver, collect_koreainvest]
    #        -> extract_silver
    #        -> extract_gold_structured
    #        -> deliver_telegram
    #
    # Phase 2b (full):
    #   [collect_naver, collect_koreainvest]
    #        -> extract_silver
    #        -> extract_gold_structured
    #        -> extract_gold_llm
    #        -> silver_to_delta
    #        -> wait_for_partners
    #        -> compute_recommendations
    #        -> deliver_telegram
    # =========================================================================

    # Phase 2a chain (always active)
    [collect_naver, collec