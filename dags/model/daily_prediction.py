"""OPIK Model Prediction DAG

Daily model training + prediction generation + S3 Gold upload.
Runs at 06:30 KST after daily OHLCV collection (06:00), before the Korean market opens at 09:00.

Output: s3://s3-opik-bucket/gold/model/predictions/dt={YYYY-MM-DD}/predictions.parquet
Consumed by: OPIK Briefing DAG (07:00 KST) for triple consensus checking.

IMPORTANT - Date offset: OHLCV/feature data arrives with ~2 day lag (T-2).
The DAG uses `execution_date - 2 days` so training data is always available.
Runs Mon-Fri only (weekends have no market data).
"""

from datetime import timedelta
import pendulum
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
        "OPIK Model DAG Failure\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Date: {exec_date.strftime('%Y-%m-%d')}"
    )
    logger.error(error_msg)


default_args = {
    "owner": "opik",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": notify_telegram_failure,
}

with DAG(
    dag_id="model_daily_prediction",
    default_args=default_args,
    schedule="30 6 * * 1-5",
    start_date=pendulum.datetime(2026, 6, 18, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["opik", "model", "phase2"],
    description="Daily LightGBM model training + prediction + S3 Gold upload (06:30 KST, Mon-Fri)",
) as dag:

    run_model_pipeline = BashOperator(
        task_id="run_model_pipeline",
        bash_command=(
            "cd " + OPIK_ROOT + " && "
            "python scripts/model/run_prediction.py "
            "--date {{ (execution_date - macros.timedelta(days=2)).strftime('%Y%m%d') }} "
            "--project-root " + OPIK_ROOT
        ),
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )

    run_model_pipeline
