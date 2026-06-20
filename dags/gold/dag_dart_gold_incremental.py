from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from dart_agent.gold.build import run_gold_incremental


# 역할: Bronze completion -> Silver 증분 처리 뒤 Gold facts/rag_chunk/embedding Parquet까지 이어 적재하는 수동 component DAG.
# 기준: 운영 자동 실행은 dag_dart_incremental_pipeline의 gold_incremental task가 담당한다.
# 멱등성: Silver _done과 Gold _done marker set-diff로 Gold 미처리 대상만 처리한다.
with DAG(
    dag_id="dag_dart_gold_incremental",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,  # component DAG: 운영 증분 스케줄은 dag_dart_incremental_pipeline이 담당
    catchup=False,
    max_active_runs=1,
    tags=["dart", "gold", "incremental"],
) as dag:
    PythonOperator(
        task_id="gold_incremental",
        python_callable=run_gold_incremental,
        op_kwargs={"limit": 2000, "chunk_size": 20, "max_runtime_seconds": 540},
    )
