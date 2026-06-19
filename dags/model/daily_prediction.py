"""OPIK Model Prediction DAG

Daily model training + prediction generation + S3 Gold upload.
Runs at 06:00 KST after US market close, before the Korean market opens at 09:00.

Output: s3://s3-opik-bucket/gold/model/predictions/dt={YYYY-MM-DD}/predictions.parquet
Consumed by: OPIK Briefing DAG (07:00 KST) for triple consensus checking.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

import logging

logger = logging.getLogger("opik.model_dag")

OPIK_ROOT = "/opt/airflow/opik"


def notify_telegram_failure(context):
    """Send failure notification via Telegram."""
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    exec_date = context["execution_date"]
    error_msg = (
        f"OPIK Model DAG Failure\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Date: {exec_date.strftime('%Y-%m-%d')}"
    )
    logger.error(error_msg)


default_args = {
    "owner": "opik",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 19),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": notify_telegram_failure,
}

with DAG(
    dag_id="model_daily_prediction",
    default_args=default_args,
    schedule="0 6 * * *",          # 06:00 KST daily -- after US market close
    catchup=False,
    max_active_runs=1,
    tags=["opik", "model", "phase2"],
    description="Daily LightGBM model training + prediction + S3 Gold upload (06:00 KST)",
) as dag:

    # -------------------------------------------------------------------------
    # Task 1: Model training + prediction + S3 upload
    # Single task -- the script handles all three steps sequentially.
    # -------------------------------------------------------------------------

    run_model_pipeline = BashOperator(
        task_id="run_model_pipeline",
        bash_command=(
            f"cd {OPIK_ROOT} && "
            "python scripts/model/run_prediction.py --date {{ ds_nodash }} --project-root /opt/airflow/opik"
        ),
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )

    # -------------------------------------------------------------------------
    # DAG graph: single task (extensible to multi-task in Phase 2b)
    #   run_model_pipeline
    # -------------------------------------------------------------------------

    run_model_pipeline
