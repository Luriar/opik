from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from dart_agent.gold.compaction import run_gold_compaction


# 역할: Gold incremental/backfill이 파티션마다 쌓은 작은 part-*.parquet를 파티션별 1파일로 병합한다.
# 기준: 매일 08:00 KST. dag_dart_gold_backfill(07:00, 보정 적재) 이후 실행해 그날 적재분까지 함께 병합한다.
# 동시성: max_active_runs=1. 병합 시작 시점에 list된 part만 제거하고, 이후 증분 part는 보존한다.
with DAG(
    dag_id="dag_dart_gold_compaction",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule="0 8 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dart", "gold", "compaction"],
) as dag:
    PythonOperator(
        task_id="gold_compaction",
        python_callable=run_gold_compaction,
    )
