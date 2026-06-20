"""
OPIK Daily Briefing DAG — ★/! Triple Consensus Telegram Briefing.

Schedule: 0 7 * * * (07:00 KST, before market open at 09:00)
Depends on:
  - opik_gold_structured + opik_gold_embeddings (report BUY signal, ~02:00-03:00)
  - model_daily_prediction (ranking_score > 0 signal, 06:00)
  - DART Gold (전일 08:00 compaction 기준, 1영업일 lag)

Runs the full 9-step briefing pipeline via briefing_graph.py.
Single PythonOperator — LangGraph handles internal orchestration.

Deployment: copy this file to /opt/airflow/dags/ on EC2.
Also needs server/agents/ accessible in PYTHONPATH.
"""

import logging
import os
import sys
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure server/agents is importable
# On EC2: /opt/airflow/opik/server/agents/
_PROJECT_ROOT = os.environ.get("OPIK_PROJECT_ROOT", "/opt/airflow/opik")
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

logger = logging.getLogger("opik.briefing_dag")

# ── DAG definition ──

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
    description="OPIK ★/! Triple Consensus Daily Briefing → Telegram",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["opik", "briefing", "telegram"],
) as dag:

    def _run_briefing(**context) -> dict:
        """Execute the briefing pipeline.

        Uses Airflow ds_nodash as the target date (KST data_interval_end - 1일).
        For a schedule of 0 7 * * *, the ds_nodash is the previous day's date
        (which is correct: 07:00 briefing on June 19 processes June 18 data).
        """
        from server.agents.briefing_graph import run_briefing_pipeline

        ds_nodash = context["ds_nodash"]
        logger.info("Starting OPIK briefing for date=%s", ds_nodash)

        result = run_briefing_pipeline(ds_nodash)

        logger.info(
            "Briefing complete: ★=%d !=%d reports=%d dart=%d error=%s",
            result["star_count"],
            result["exclamation_count"],
            result["report_count"],
            result["dart_count"],
            result["error"],
        )

        if result["error"]:
            # Telegram-only failures are non-fatal; briefing content was already composed.
            if "Telegram" in result["error"]:
                logger.warning("Briefing content ready but Telegram delivery failed: %s", result["error"])
          