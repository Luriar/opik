"""
OPIK Daily Briefing DAG — Star/! Triple Consensus Telegram Briefing.

Schedule: 0 7 * * * (07:00 KST, before market open at 09:00)
Depends on:
  - opik_gold_structured + opik_gold_embeddings (report BUY signal, ~02:00-03:00)
  - model_daily_prediction (ranking_score > 0 signal, 06:00)
  - DART Gold (previous day 08:00 compaction, 1 business day lag)

Runs the full 9-step briefing pipeline via briefing_graph.py.
Uses BashOperator + scripts/run_briefing.py wrapper to avoid Jinja escaping issues.
"""

from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "opik",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="opik_briefing",
    default_args=default_args,
    description="OPIK Star/! Triple Consensus Daily Briefing -> Telegram",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["opik", "briefing", "telegram"],
) as dag:

    run_briefing = BashOperator(
        task_id="run_briefing",
        bash_command=(
            "set -a && source /opt/airflow/opik/.env 2>/dev/null || true && set +a && "
            "cd /opt/airflow/opik && "
            "PYTHONPATH=/opt/airflow/opik:$PYTHONPATH "
            "python3 scripts/run_briefing.py {{ ds_nodash }}"
        ),
    )
