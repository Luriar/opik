from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from dart_agent.gold.build import run_gold_backfill
from dart_agent.gold.context import run_gold_latest_context


# 역할: Silver _done 중 Gold _done이 없는 대상을 넓은 limit로 보정 적재한다.
# 기준: 매일 07:00 KST. dag_dart_raw_to_silver(05:30) 이후 누락분을 보정한다.
# 주의: run_gold_backfill도 Gold _done marker set-diff를 사용하므로 이미 적재된 보고서는 재임베딩하지 않는다.
with DAG(
    dag_id="dag_dart_gold_backfill",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule="0 7 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dart", "gold", "backfill"],
) as dag:
    build_gold = PythonOperator(
        task_id="gold_backfill",
        python_callable=run_gold_backfill,
    )
    build_latest_context = PythonOperator(
        task_id="build_latest_context",
        python_callable=run_gold_latest_context,
    )

    build_gold >> build_latest_context
