# OPIK AWS Runtime Env Mapping

이 문서는 현재 코드와 Terraform 배포 구조 기준으로 `airflow`, `server(FastAPI + RAG)`, `spark`, `db`, `storage` 설정값을 어디에 둬야 하는지 정리한다.

## 공식 문서 근거

- EC2 애플리케이션 AWS 자격증명: AWS는 EC2 애플리케이션에 장기 access key를 배포하지 않고 IAM Role/Instance Profile을 붙여 임시 자격증명을 쓰는 방식을 설명한다.  
  Source: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html
- 애플리케이션 비밀값 조회: Secrets Manager 값 조회에는 `secretsmanager:GetSecretValue` 권한이 필요하다.  
  Source: https://docs.aws.amazon.com/secretsmanager/latest/userguide/retrieving-secrets_cli.html
- RDS DB 비밀번호: RDS는 master user password를 Secrets Manager에서 관리하도록 생성할 수 있고, RDS가 비밀번호를 생성/저장/관리한다.  
  Source: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-secrets-manager.html
- 비밀 아닌 설정값: Systems Manager Parameter Store는 환경변수, endpoint, resource id, tuning parameter 같은 configuration data 저장 용도를 제공한다. 이 Terraform은 일부 endpoint를 SSM Parameter로 쓰고, 배포 시점에 확정되는 앱 설정은 `terraform.tfvars`에서 user-data env 파일로 렌더한다.  
  Source: https://docs.aws.amazon.com/systems-manager/latest/userguide/systems-manager-parameter-store.html

위 공식 문서에 직접 나오지 않는 OPIK 전용 분류와 변수명은 이 리포지토리 코드(`server/`, `spark_jobs/`, `terraform/modules/compute/user_data/`)를 바탕으로 정리한 것이다.

## 배치 원칙

| 분류 | AWS/Terraform 위치 | 이유 |
|---|---|---|
| AWS access key / secret key | 배포하지 않음. EC2 IAM Role 사용 | EC2 공식 문서의 IAM Role 패턴 |
| DART, Telegram, Airflow key 같은 앱 비밀값 | AWS Secrets Manager: `opik-dev/airflow/runtime`, `opik-dev/api/runtime` | 값 회전/조회 권한 분리 필요 |
| RDS user/password | RDS managed master user secret | RDS가 생성/관리 |
| S3 bucket, prefix, model id, batch size, path 등 비밀 아닌 설정 | `terraform.tfvars` -> EC2 user-data -> `/opt/opik/*.env` | 배포 환경별 변경값 |
| 내부 endpoint 일부 | SSM Parameter Store + user-data env | 비밀 아닌 런타임 endpoint 공유 |

## Airflow Runtime

Airflow EC2는 `terraform/modules/compute/user_data/airflow.sh.tftpl`에서 `/opt/opik/airflow.env`를 만든다.

| Env | 값 출처 | 현재 코드/배포 기본값 | 비고 |
|---|---|---|---|
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | RDS Airflow managed secret + RDS endpoint | Terraform이 조립 | 직접 입력하지 않음 |
| `AIRFLOW__CELERY__RESULT_BACKEND` | RDS Airflow managed secret + RDS endpoint | Terraform이 조립 | 직접 입력하지 않음 |
| `AIRFLOW__CELERY__BROKER_URL` | local Redis compose service | `redis://redis:6379/0` | 현재 dev는 ElastiCache 대신 Airflow EC2 내부 Redis |
| `AIRFLOW__CORE__FERNET_KEY` | Secrets Manager `airflow/runtime` | 없으면 부팅 시 생성 | 운영은 고정값 권장 |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | Secrets Manager `airflow/runtime` | 없으면 부팅 시 생성 | `.env`의 `AIRFLOW_SECRET_KEY`와 같은 의미 |
| `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` | Secrets Manager `airflow/runtime` | `airflow` / 랜덤 생성 | UI 관리자 계정 |
| `DART_API_KEYS` | Secrets Manager `airflow/runtime` | 없음 | 절대 tfvars/Git 금지 |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | Secrets Manager `airflow/runtime` | 빈값 | briefing 전송용 |
| `OPIK_REPORT_PIPELINE_SCHEDULE` | Secrets Manager `airflow/runtime` | `0 0 * * *` | `.env` 예시는 `0 23 * * *` |
| `S3_BUCKET`, `AWS_REGION` | `data_bucket_name`, `aws_region` | `s3-opik-bucket`, `ap-northeast-2` | 비밀 아님 |
| `STORAGE_BACKEND`, `S3_BASE_PREFIX`, `BRONZE_PREFIX`, `SILVER_PREFIX`, `GOLD_PREFIX` | `storage_config` | `.env`와 동일 기본값 | 데이터 레이크 layout |
| `EMBEDDING_*` | `embedding_config` | e5 / multilingual-e5-small / v1 / 384 | Airflow embedding과 API encoder 정합 필요 |

## FastAPI / RAG Server Runtime

API EC2는 `terraform/modules/compute/user_data/api.sh.tftpl`에서 `/opt/opik/bootstrap.env`를 만든 뒤 `opik-server.service`의 `EnvironmentFile`로 읽는다.

| Env | 값 출처 | 현재 Terraform 값 | 코드 기본값 | 비고 |
|---|---|---|---|---|
| `AWS_REGION`, `AWS_DEFAULT_REGION`, `S3_REGION` | `aws_region` | `ap-northeast-2` | `ap-northeast-2` | boto3/S3/Bedrock region |
| `S3_BUCKET` | `data_bucket_name` | `s3-opik-bucket` | `s3-opik-bucket` | 기존 데이터 버킷 참조 |
| `S3_BASE_PREFIX` | `storage_config.s3_base_prefix` | `""` | 일부 Airflow/storage 코드용 | Spark Delta job은 현재 사용하지 않음 |
| `EMBEDDING_MODEL` | `embedding_config.model` | `intfloat/multilingual-e5-small` | 동일 | `opik_server.py` query encoder |
| `EMBEDDING_PROVIDER`, `EMBEDDING_VERSION`, `EMBEDDING_DIMENSION` | `embedding_config` | `e5`, `v1`, `384` | 일부 코드는 직접 미사용 | 문서/정합용 |
| `FAISS_INDEX_PATH`, `FAISS_IDMAP_PATH` | user-data env | `/data/opik/faiss_index.bin`, `/data/opik/report_ids.json` | 동일 | API 로컬 캐시 |
| `CHAT_HTML_PATH` | user-data env | `/opt/opik/repo/server/chat.html` | `/root/opik-server/chat.html` | Terraform 배포 경로에 맞춰 override 필요 |
| `PROMPT_DIR` | user-data env | `/opt/opik/repo/server/prompts` | `/root/opik-server/prompts` | Terraform 배포 경로에 맞춰 override 필요 |
| `OPIK_DB_PATH` | `server_config.opik_db_path` | `/data/opik/opik.db` | 동일 | Telegram subscriber/conversation SQLite |
| `OPIK_AGENT_ENABLED` | `server_config.opik_agent_enabled` | `false` | `true` | dev 비용/기동 안정성 때문에 Terraform은 false |
| `SEARCH_TOP_K` | `server_config.search_top_k` | `10` | `10` | RAG 검색 기본 개수 |
| `BEDROCK_MODEL` | `server_config.bedrock_model` | `apac.anthropic.claude-3-haiku-20240307-v1:0` | 동일 | `/chat` 기본 생성 모델 |
| `SAFETY_MODEL`, `INTENT_MODEL`, `REPORT_MODEL` | `server_config` | 코드 기본값과 동일 | 코드 기본값 | Agent v2 모델 |
| `DART_MODEL`, `DART_SUMMARIZE_MODEL` | `server_config` | 코드 기본값과 동일 | 코드 기본값 | DART agent 모델 |
| `ANALYSIS_MODEL`, `COMPOSER_MODEL` | `server_config` | 코드 기본값과 동일 | 코드 기본값 | 분석/응답 합성 모델 |
| `SENTIMENT_MODEL`, `DART_SENTIMENT_*` | `server_config` | 코드 기본값과 동일 | 코드 기본값 | DART sentiment batch tuning |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | Secrets Manager `api/runtime` | 빈값 가능 | 빈값 | FastAPI Telegram bot 기능 |

`SERVICE_DB_ENDPOINT`와 `API_RUNTIME_SECRET_ARN`도 env에 들어가지만, 현재 FastAPI 코드에서는 DB URL로 직접 사용하지 않는다. 현재 `server/db.py`는 SQLite(`OPIK_DB_PATH`)를 사용한다. PostgreSQL service DB 연동은 향후 코드에서 `SERVICE_DB_URL` 또는 별도 설정을 읽도록 바꿀 때 연결하면 된다.

## Spark Runtime

Spark Delta job(`spark_jobs/gold_to_delta.py`, `spark_jobs/spark_silver_to_delta.py`)은 현재 S3 bucket과 AWS region만 env로 받고, 세부 데이터 경로는 코드 고정값을 사용한다.

| 항목 | 값 |
|---|---|
| `S3_BUCKET` | env 기본값 `s3-opik-bucket` |
| `AWS_REGION` | env 기본값 `ap-northeast-2` |
| structured 입력 | `s3a://<S3_BUCKET>/gold/structured/` |
| embeddings 입력 | `s3a://<S3_BUCKET>/gold/embeddings/` |
| DART disclosure 입력 | `s3a://<S3_BUCKET>/gold/dart/disclosure_events/` |
| Delta 출력 | `s3a://<S3_BUCKET>/delta/gold_db/...` |

따라서 Spark job에는 S3 이외의 영속 데이터 소스가 없다. DB, Redis, Secrets Manager를 직접 읽지 않는다.

## DB / Storage

| 항목 | 위치 | 현재 값/상태 |
|---|---|---|
| Airflow metadata DB | RDS PostgreSQL + RDS managed secret | Terraform 조립 |
| Service DB | RDS PostgreSQL + RDS managed secret | Terraform 조립. FastAPI 코드는 아직 SQLite 중심 |
| FastAPI conversation/subscriber DB | API EC2 local SQLite | `OPIK_DB_PATH=/data/opik/opik.db` |
| S3 data lake | 기존 S3 bucket data source | `s3-opik-bucket`, Terraform destroy 대상 아님 |
| Secrets containers | Secrets Manager data source | `opik-dev/airflow/runtime`, `opik-dev/api/runtime`, Terraform destroy 대상 아님 |

## 루트 `.env` 기준 처리

| `.env` 키 | 배포 위치 |
|---|---|
| `AIRFLOW_SECRET_KEY`, `AIRFLOW__WEBSERVER__SECRET_KEY` | `airflow/runtime`의 `AIRFLOW__WEBSERVER__SECRET_KEY` |
| `AIRFLOW__CORE__FERNET_KEY` | `airflow/runtime`의 `AIRFLOW__CORE__FERNET_KEY`. 현재 placeholder는 유효한 Fernet key가 아니므로 교체 필요 |
| `DART_API_KEYS` | `airflow/runtime`의 `DART_API_KEYS` |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | `airflow/runtime`, `api/runtime` 둘 다 |
| `SERVICE_DB_URL` | 직접 배포하지 않음. RDS managed secret + endpoint로 조립해야 함 |
| `S3_BUCKET`, `AWS_REGION`, storage prefix, DART tuning, embedding config | `terraform.tfvars` |
| `TELEGRAM_CHAT_IDS` | 현재 코드에서 읽지 않음. 배포 불필요 |

## 추론 또는 OPIK 코드 기반 결정

- `PROMPT_DIR`를 `/opt/opik/repo/server/prompts`로 주입하는 것은 AWS 공식 문서가 아니라 현재 Terraform clone 경로와 `server/opik_server.py`의 기본값 불일치를 근거로 한 수정이다.
- `OPIK_AGENT_ENABLED=false`는 공식 문서 근거가 아니라 현재 dev 배포의 비용/안정성 판단이다. v2 agent를 켜려면 `server_config.opik_agent_enabled=true`로 바꾼다.
- Spark 경로를 고정값으로 유지하는 것은 공식 AWS 요구사항이 아니라 현재 OPIK 데이터 레이크 layout이 `gold/...`, `delta/gold_db`로 고정되어 있다는 코드/운영 판단이다.
