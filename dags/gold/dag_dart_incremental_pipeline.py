from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from dart_agent.gold.build import run_gold_incremental
from dart_agent.repositories.state import get_state
from dart_agent.workflows import (
    build_context,
    run_detail_collector,
    run_incremental_discovery,
    run_silver_incremental,
)


def backfill_completed() -> bool:
    context = build_context()
    with context.service_engine.begin() as conn:
        return get_state(conn, "backfill_status") == "COMPLETED"


# 역할: 운영 증분 수집의 Bronze -> Silver -> Gold 경로를 한 DAG run 안에서 순서대로 실행한다.
# 기준: 3분 간격(24/7). 신규 공시 감지 주기를 3분으로 둔다.
# 동시성: max_active_runs=1. 한 run의 task 런타임 예산 합이 ~9분(collect 300 + silver 120 + gold 120)이라
#         */3여도 직전 run이 끝나는 대로 연속 실행된다(사실상 상시 가동, run 중복 없음). quota는
#         collect_pending_detail_jobs의 batch_size와 rate limiter가 통제한다.
# 주의: 이 DAG를 운영 스케줄로 쓰면 개별 component DAG
#       (incremental_discovery/detail_collector/silver_incremental/gold_incremental)는 수동 보정용으로만 둔다.
with DAG(
    dag_id="dag_dart_incremental_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule="*/3 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dart", "incremental", "pipeline"],
) as dag:
    gate_backfill_completed = ShortCircuitOperator(
        task_id="gate_backfill_completed",
        python_callable=backfill_completed,
    )
    discover_incremental_disclosures = PythonOperator(
        task_id="discover_incremental_disclosures",
        python_callable=run_incremental_discovery,
    )
    collect_pending_detail_jobs = PythonOperator(
        task_id="collect_pending_detail_jobs",
        python_callable=run_detail_collector,
        op_kwargs={"batch_size": 270, "concurrency": 4, "max_runtime_seconds": 300},
    )
    silver_incremental = PythonOperator(
        task_id="silver_incremental",
        python_callable=run_silver_incremental,
        op_kwargs={"max_runtime_seconds": 120},
    )
    gold_incremental = PythonOperator(
        task_id="gold_incremental",
        python_callable=run_gold_incremental,
        op_kwargs={"limit": 1000, "chunk_size": 20, "max_runtime_seconds": 120},
    )

    (
        gate_backfill_completed
        >> discover_incremental_disclosures
        >> collect_pending_detail_jobs
        >> silver_incremental
        >> gold_incremental
    )
