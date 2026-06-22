from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from dart_agent.workflows import run_company_master


# 역할: OpenDART corpCode.xml(전체 고유번호/종목코드 마스터)을 받아 dart_corp_code에 upsert한다.
#       공시검색/재무 API가 쓰는 corp_code 기준을 맞추는 기반 데이터다.
# 기준: 평일 15:00 KST 1회 실행. 게이트 없음 — 매 실행 전체 갱신.
# 수집 범위·주기·조건 단일 출처: docs/pipeline/dag-matrix.md
with DAG(
    dag_id="dag_dart_company_master",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule="0 9,15 * * 1-5",  # 15:00 KST 평일
    catchup=False,
    tags=["dart", "master"],
) as dag:
    PythonOperator(
        task_id="collect_dart_corp_code",
        python_callable=run_company_master,
    )
