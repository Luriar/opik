"""
Daily Delta MERGE + FAISS rebuild DAG — Dataset(data-aware) triggered.

두 gold 산출물이 모두 갱신되면 Airflow가 자동으로 1회 트리거한다(AND 조건):
  - opik_gold_structured  → Dataset s3://s3-opik-bucket/gold/structured/
  - opik_gold_embeddings  → Dataset s3://s3-opik-bucket/gold/embeddings/

이전에는 schedule=None + ExternalTaskSensor였으나, schedule=None DAG은 트리거가 없으면
DAG run 자체가 생성되지 않아 센서가 영영 poke되지 않았다(자동 실행 불가). Dataset 스케줄로
바꾸면서 logical_date를 upstream과 맞출 필요가 없어져 execution_date_fn 정합 문제도 제거된다.

sequence: (gold structured + gold embeddings 완료) -> Delta MERGE -> FAISS Rebuild -> Server Restart

CRITICAL — nsenter pattern: Airflow worker 컨테이너에는 spark-submit, Java, /home/ec2-user
마운트가 없다. 대신 /var/run/docker.sock이 마운트되어 있으므로, privileged alpine 컨테이너로
nsenter --target 1 (호스트 PID 1) 네임스페이스에 진입해 호스트의 spark-submit/python3/systemctl을
실행한다. alpine:latest는 최초 1회 pull 후 캐시된다(~7 MB).
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator

# Dataset URI — 두 gold DAG의 outlet과 정확히 일치해야 한다.
#   dags/gold/structured.py : GOLD_STRUCTURED_DATASET_URI
#   dags/gold/embedding.py  : GOLD_EMBEDDINGS_DATASET_URI
GOLD_STRUCTURED_DATASET = Dataset("s3://s3-opik-bucket/gold/structured/")
GOLD_EMBEDDINGS_DATASET = Dataset("s3://s3-opik-bucket/gold/embeddings/")

# 호스트 네임스페이스 진입 prefix — /var/run/docker.sock이 마운트된 worker에서 실행.
# privileged + pid=host로 호스트의 모든 네임스페이스(mount/uts/ipc/net)에 진입한다.
NSENTER = (
    "docker run --rm --privileged --pid=host "
    "alpine:latest nsenter --target 1 --mount --uts --ipc --net -- "
    "bash -c "
)

default_args = {
    "owner": "opik",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


with DAG(
    dag_id="dag_maintenance_delta_faiss",
    default_args=default_args,
    description="Gold Delta MERGE + FAISS rebuild, triggered when both gold Datasets update",
    # data-aware scheduling: 두 Dataset이 모두 갱신된 뒤 1회 트리거(AND).
    # 한쪽 gold가 실패해 Dataset을 갱신하지 않으면 트리거되지 않는다(불완전 머지 방지).
    schedule=[GOLD_STRUCTURED_DATASET, GOLD_EMBEDDINGS_DATASET],
    start_date=pendulum.datetime(2026, 6, 21, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,                     # 머지 동시 실행 방지
    tags=["maintenance", "delta", "faiss"],
) as dag:

    delta_merge = BashOperator(
        task_id="delta_merge",
        bash_command=(
            NSENTER + '"'
            "cd /home/ec2-user/spark_jobs && "
            "spark-submit --master 'local[2]' --driver-memory 6g "
            "gold_to_delta.py --date {{ ds_nodash }} "
            ">> /var/log/opik-delta-merge.log 2>&1 && "
            "echo MERGE_DONE"
            '"'
        ),
        retries=2,
        retry_delay=timedelta(minutes=10),
    )

    faiss_rebuild = BashOperator(
        task_id="faiss_rebuild",
        bash_command=(
            NSENTER + '"'
            "cd /root/opik-server && "
            "python3 build_index.py "
            ">> /var/log/opik-faiss-rebuild.log 2>&1 && "
            "echo FAISS_REBUILD_DONE"
            '"'
        ),
    )

    restart_server = BashOperator(
        task_id="restart_server",
        bash_command=(
            NSENTER + '"'
            "systemctl restart opik-server && "
            "sleep 5 && "
            "systemctl status opik-server --no-pager | head -5"
            '"'
        ),
    )

    delta_merge >> faiss_rebuild >> restart_server
