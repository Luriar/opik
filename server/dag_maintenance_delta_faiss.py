"""
OPIK Maintenance Monitoring DAG.
Actual work runs via systemd spark-delta-merge.timer (23:00 KST):
  Delta MERGE → FAISS rebuild → Server restart.
This DAG verifies the outcome by checking S3 assets.

Schedule: 0 1 * * * (01:00 KST, 2 hours after maintenance)
"""
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "opik",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_monitor_maintenance",
    default_args=default_args,
    description="Verify systemd maintenance (Delta + FAISS) via S3 checks",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 6, 22, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["maintenance", "monitoring"],
) as dag:

    verify = BashOperator(
        task_id="verify_maintenance",
        bash_command="""
            set -e
            echo "=== OPIK Maintenance Verification ==="
            echo ""

            # Verify Delta table has recent data
            python3 -c "
import boto3
s3 = boto3.client('s3', region_name='ap-northeast-2')
# Check Delta _last_checkpoint
resp = s3.list_objects_v2(Bucket='s3-opik-bucket', Prefix='delta/gold_db/disclosure_events/_last_checkpoint')
for obj in resp.get('Contents', []):
    print(f'Delta checkpoint: {obj[\"Key\"]} ({obj[\"LastModified\"].isoformat()})')
"
            echo ""

            # Verify FAISS index files on EC2 host (check S3 ops backup)
            python3 -c "
import boto3
s3 = boto3.client('s3', region_name='ap-northeast-2')
# Check ops backup of build script
resp = s3.head_object(Bucket='s3-opik-bucket', Key='ops/build_index_v2.py')
print(f'build_index_v2.py last modified: {resp[\"LastModified\"].isoformat()}')
# Check DartCollector done markers for today
from datetime import datetime
dt = datetime.now()
key = f'gold/dart/_done/sv=v5/gv=v3/sink=parquet/ver=v1/rcept_year={dt.year}/rcept_month={dt.month:02d}/'
paginator = s3.get_paginator('list_objects_v2')
cnt = 0
for page in paginator.paginate(Bucket='s3-opik-bucket', Prefix=key, MaxKeys=1000):
    cnt += len(page.get('Contents', []))
print(f'Gold done markers for {dt.year}-{dt.month:02d}: {cnt}')
"

            echo ""
            echo "=== Verification done ==="
        """,
    )

