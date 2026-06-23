from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from dart_agent.workflows import run_collect_company_overviews


# 역할: 등록된 전체 기업의 기업개황(company_overview)을 OpenDART에서 수집한다.
#       DartCollector Gold processor가 company_overview 데이터를 사용한다.
# 기준: 주 1회 월요일 03:00 KST 실행. 게이트 없음.
with DAG(
    dag_id="dag_dart_company_overview",
    start_date=pendulum.datetime(2026, 6, 1, tz="Asia/Seoul"),
    schedule="0 3 * * 1",  # 월요일 03:00 KST
    catchup=False,
    tags=["dart", "company", "overview"],
) as dag:
    PythonOperator(
        task_id="collect_company_overviews",
        python_callable=run_collect_company_overviews,
    )
