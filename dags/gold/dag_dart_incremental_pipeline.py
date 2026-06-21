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


def operation_gate_open(**context) -> bool:
    """운영 게이트(ShortCircuit).

    - 스케줄 실행: 백필 완료 + 평일 07:20~20:10 KST 창에서만 통과. cron(`*/3 7-20 * * 1-5`)이 대략
      좁히고, cron으로 못 박는 분 경계(07:20 시작, 20:10 종료)를 이 게이트가 강제한다.
    - 수동 트리거(Trigger DAG): **시간/요일 창을 무시하고 통과** → 날짜·시각 무관 즉시 수집.
      (백필 완료 조건은 manual도 동일하게 확인 — 백필 단계에 discovery 경합을 막기 위함)
    """
    app_ctx = build_context()
    with app_ctx.service_engine.begin() as conn:
        if get_state(conn, "backfill_status") != "COMPLETED":
            return False
    dag_run = context.get("dag_run")
    is_manual = bool(dag_run and (str(getattr(dag_run, "run_type", "")) == "manual"
                                  or getattr(dag_run, "external_trigger", False)))
    if is_manual:
        return True  # 수동 트리거: 날짜/시간 창 무시
    now = pendulum.now("Asia/Seoul")
    if now.weekday() >= 5:  # 토(5)/일(6) 제외 — 평일만
        return False
    minutes = now.hour * 60 + now.minute
    return 7 * 60 + 20 <= minutes <= 20 * 60 + 10  # 07:20 ~ 20:10 KST


# 역할: 운영 증분 수집의 Bronze -> Silver -> Gold 경로를 한 DAG run 안에서 순서대로 실행한다.
# 기준: 평일 07:20~20:10 KST, 3분 간격. cron은 `*/3 7-20 * * 1-5`로 좁히고, 정확한 분 경계는
#       gate_operation_window(ShortCircuit)가 강제한다(창 밖이면 downstream skip).
# 수동: Trigger DAG(수동 실행)은 시간/요일 창을 무시하고 즉시 수집한다(날짜 무관). 백필 완료 조건만 동일 적용.
# 동시성: max_active_runs=1. 한 run의 task 런타임 예산 합이 ~9분(collect 300 + silver 120 + gold 120)이라
#         직전 run이 끝나는 대로 연속 실행된다(창 안에서 사실상 상시, run 중복 없음).
# 주의: 이 DAG를 운영 스케줄로 쓰면 개별 component DAG
#       (incremental_discovery/detail_collector/silver_incremental/gold_incremental)는 수동 보정용으로만 둔다.
with DAG(
    dag_id="dag_dart_incremental_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule="*/3 7-20 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["dart", "incremental", "pipeline"],
) as dag:
    gate_operation_window = ShortCircuitOperator(
        task_id="gate_operation_window",
        python_callable=operation_gate_open,
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
        # chunk_size 20→10: 임베딩은 배치 flush 단위라, 배치를 줄이면 budget(120s) 오버슈트가 작아진다
        # (한 배치 임베딩은 중단 불가). 산출물(임베딩 row)은 동일, part 파일만 잘게 나뉘고 compaction이 병합.
        op_kwargs={"limit": 1000, "chunk_size": 10, "max_runtime_seconds": 120},
    )

    (
        gate_operation_window
        >> discover_incremental_disclosures
        >> collect_pending_detail_jobs
        >> silver_incremental
        >> gold_incremental
    )
