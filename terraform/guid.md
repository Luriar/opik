# OPIK AWS Terraform Architecture Guide

작성일: 2026-06-22  
실행 모드: Maintainability First

## 결론

현재 OPIK는 개발 서버 1대에서 Airflow, Redis, Airflow DB, S3 데이터 파이프라인, Spark/Delta 작업, FastAPI/FAISS RAG 서버를 같이 운영하는 구조다.

AWS로 분리할 때의 1차 목표는 서버를 많이 쪼개는 것이 아니라, 다음 경계를 명확히 나누는 것이다.

1. Public layer: 외부에서 접근하는 진입점만 배치한다.
2. Private app layer: Airflow, FastAPI, RAG, embedding, Spark 작업 서버를 배치한다.
3. Private DB layer: RDS PostgreSQL, Redis/ElastiCache 등 상태 저장소를 배치한다.
4. S3 data lake: Bronze/Silver/Gold/Delta/FAISS 재생성 입력의 원천 저장소로 유지한다.

초기 권장안은 다음과 같다.

| 영역 | 권장 구성 | 이유 |
|---|---|---|
| 네트워크 | VPC + public subnet 2개 + private app subnet 2개 + private DB subnet 2개 | ALB/NAT와 앱/DB 계층 분리 |
| 외부 진입 | Public ALB 1개. ALB 권한이 없으면 임시 public front proxy EC2 | 운영은 ALB 권장. 권한 제약이 있으면 nginx reverse proxy로 dev 배포 검증 |
| NAT | NAT Gateway. 운영은 AZ별 1개, 비용 절감 dev는 1개 | private 서버의 패키지 설치, OS update, 외부 API 호출 |
| Airflow/embedding | Private EC2 1대 | 현재 DAG, embedding, Spark/Delta 작업을 가장 적은 변경으로 이전 |
| FastAPI/RAG/LangChain/LangGraph | Private EC2 1대 + Public ALB target 또는 front proxy target | 사용자 트래픽과 배치 워크로드 격리 |
| Airflow DB | RDS PostgreSQL의 별도 DB 또는 별도 RDS | Airflow metadata와 서비스 데이터를 분리 |
| 서비스 DB | RDS PostgreSQL의 별도 DB | DART/report index, conversation, serving metadata용 |
| Redis | ElastiCache Redis 또는 Airflow 서버 내 Redis | CeleryExecutor 유지 시 필요. 운영은 ElastiCache 권장 |
| 파일/데이터 | S3 | Bronze/Silver/Gold/Delta 원천 및 재처리 기준 |
| 비밀값 | Secrets Manager 또는 SSM Parameter Store | `.env`와 Terraform state에 secret 노출 방지 |

## 현재 프로젝트 기준 구성

현재 repo에서 확인되는 주요 실행 단위는 다음과 같다.

| 구성 | 현재 파일/경로 | 역할 |
|---|---|---|
| Airflow | `docker-compose.yml`, `docker-compose.yaml`, `Dockerfile.airflow`, `dags/` | Bronze/Silver/Gold, DART, model, briefing DAG 실행 |
| Airflow metadata DB | compose의 `postgres` 또는 `airflow-db` | Airflow 전용 metadata |
| Redis | compose의 `redis` | Airflow Celery broker |
| Report pipeline | `dags/bronze`, `dags/silver`, `dags/gold`, `pipeline/` | 증권사 리포트 수집/추출/구조화/embedding |
| DART pipeline | `src/dart_agent/`, `dags/gold/dag_dart_*.py` | DART 수집, gold 생성, serving index |
| Spark/Delta | `spark_jobs/` | S3 Delta table merge |
| FastAPI/RAG | `server/opik_server.py`, `server/agents/` | `/chat`, `/search`, FAISS, LangGraph agent |
| RAG index build | `server/build_index.py`, `server/opik_server.py` | S3 `gold/embeddings/`에서 FAISS index 생성 |
| Service DB schema | `sql/migrations/009_gold_serving.sql` | DART serving index |
| Object storage | `S3_BUCKET`, `bronze/`, `silver/`, `gold/`, `delta/` | 데이터 lake 및 재처리 원천 |

## 권장 AWS 논리 아키텍처

```text
Internet
  |
  v
Route53 / ACM
  |
  v
Public ALB or public front proxy EC2 (public subnets)
  |
  +--> Target group: FastAPI/RAG EC2:8000 (private app subnet)

관리자 접근
  |
  +--> SSM Session Manager or VPN
        |
        +--> Airflow UI:8080 (private app subnet)

Private app subnets
  |
  +--> EC2 opik-airflow-batch
  |      - Airflow webserver/scheduler/worker/triggerer
  |      - embedding DAG
  |      - Spark/Delta merge job
  |      - briefing job
  |
  +--> EC2 opik-api-rag
         - FastAPI / uvicorn
         - FAISS index cache
         - LangChain/LangGraph agents
         - Bedrock 호출

Private DB subnets
  |
  +--> RDS PostgreSQL
  |      - DB: airflow
  |      - DB: opik_service
  |
  +--> ElastiCache Redis
         - Airflow Celery broker/result coordination

S3
  |
  +--> bronze/
  +--> silver/
  +--> gold/
  +--> delta/
  +--> optional faiss-index/
```

## VPC / subnet 설계

권장 CIDR 예시는 다음과 같다. 실제 CIDR은 기존 AWS 계정의 VPC와 충돌하지 않게 조정한다.

| Tier | AZ A | AZ C | 용도 |
|---|---|---|---|
| VPC | `10.20.0.0/16` | - | OPIK 전용 VPC |
| Public subnet | `10.20.0.0/24` | `10.20.1.0/24` | ALB, NAT Gateway |
| Private app subnet | `10.20.10.0/24` | `10.20.11.0/24` | EC2 Airflow, EC2 FastAPI |
| Private DB subnet | `10.20.20.0/24` | `10.20.21.0/24` | RDS, ElastiCache |

Route table 기준:

| Route table | 연결 subnet | 기본 route |
|---|---|---|
| public | public subnets | `0.0.0.0/0 -> Internet Gateway` |
| private-app-a | private app subnet A | `0.0.0.0/0 -> NAT Gateway A` |
| private-app-c | private app subnet C | `0.0.0.0/0 -> NAT Gateway C` |
| private-db | private DB subnets | 기본 인터넷 route 없음 |

운영 환경은 NAT Gateway를 AZ별로 둔다. 한 AZ의 NAT 장애가 다른 AZ private 서버의 outbound까지 막지 않기 때문이다. 비용을 줄이는 dev 환경은 NAT Gateway 1개로 시작할 수 있지만, 해당 NAT가 있는 AZ 장애 시 private 서버의 패키지 설치, OS update, 외부 API 호출이 막힌다.

S3는 Gateway VPC Endpoint를 추가하는 것이 좋다. S3 접근 트래픽을 NAT로 보내지 않아 NAT 비용과 장애 의존성을 줄일 수 있다. 단, pip/apt, 외부 증권사 사이트, DART Open API, Bedrock runtime 등은 NAT 또는 AWS service endpoint 구성이 필요하다.

## Public / private 계층 분리

### Public layer

public subnet에는 원칙적으로 서버를 두지 않는다.

배치 대상:

- Internet Gateway
- NAT Gateway
- Public ALB
- ALB 권한이 없는 dev 계정에서는 임시 public front proxy EC2
- 필요 시 Bastion 대신 SSM Session Manager 사용

Public ALB는 443만 연다. HTTP 80은 HTTPS redirect 용도로만 사용한다.
front proxy EC2를 쓸 경우에는 nginx만 두고, 실제 FastAPI/RAG 서버는 계속 private app subnet에 둔다. 이 방식은 ALB 권한 문제가 있을 때 dev 배포 확인용으로만 사용하고, 운영에서는 ALB나 CloudFront/API Gateway 계층으로 되돌리는 것이 낫다.

### Private app layer

private app subnet에 실제 애플리케이션 서버를 둔다.

| 서버 | public IP | inbound | outbound |
|---|---:|---|---|
| `opik-api-rag` | 없음 | ALB 또는 front proxy에서 8000 | NAT, S3, RDS, Bedrock |
| `opik-airflow-batch` | 없음 | SSM 또는 internal/admin ALB에서 8080 | NAT, S3, RDS, Redis, Bedrock, 외부 수집 대상 |

SSH 22번은 열지 않는 것을 기본값으로 한다. 운영 접속은 SSM Session Manager를 사용한다.

### Private DB layer

private DB subnet에는 RDS와 ElastiCache만 둔다.

| 리소스 | inbound 허용 |
|---|---|
| RDS PostgreSQL | Airflow SG, API SG에서 5432 |
| ElastiCache Redis | Airflow SG에서 6379 |

DB subnet에는 인터넷 outbound route를 두지 않는다.

## 서버 분리 기준

### 1. Airflow 및 embedding 서버

초기에는 Airflow webserver, scheduler, worker, triggerer, Spark/Delta timer, batch embedding 작업을 `opik-airflow-batch` EC2 1대에 둔다.

이유:

- 현재 repo의 Airflow compose가 단일 호스트 운영을 전제로 한다.
- DAG와 embedding 작업은 S3를 정본으로 사용하므로 서버 분리보다 IAM/S3/RDS 분리가 먼저다.
- Spark/Delta merge가 하루 1회성 batch라면 별도 클러스터가 아직 필요 없다.

Airflow UI는 product frontend가 아니라 운영자 admin console이다. 따라서 public에 직접 열지 않는다.

권장 접근 방식:

1. 기본: SSM port forwarding으로 `localhost:8080` 접근
2. 팀 접근 필요 시: internal ALB + VPN 또는 IP allowlist + 인증
3. public ALB 직접 노출은 비권장

추후 분리 조건:

- embedding backfill이 API 서버 지연이나 Airflow task 실패를 유발한다.
- Airflow worker concurrency가 늘어 단일 EC2 CPU/RAM 한계를 넘는다.
- DAG 실행과 webserver 안정성을 분리해야 한다.

그 시점에는 다음처럼 나눈다.

| 역할 | 분리 대상 |
|---|---|
| Airflow webserver/scheduler | 작은 EC2 또는 ECS service |
| Airflow worker | 별도 EC2 Auto Scaling Group 또는 ECS service |
| embedding/backfill worker | GPU가 필요 없다면 memory optimized EC2, 필요 시 별도 job runner |

### 2. FastAPI / RAG / LangChain / LangGraph 서버

`server/opik_server.py`는 FastAPI, FAISS, SentenceTransformer, Bedrock 호출, LangGraph agent를 함께 가진다. 초기에는 `opik-api-rag` EC2 1대로 분리한다.

권장 구조:

- public ALB 또는 public front proxy -> private EC2 `opik-api-rag:8000`
- uvicorn 또는 gunicorn/uvicorn worker를 systemd 또는 Docker로 실행
- FAISS index는 local EBS에 cache하고, 원천은 S3 `gold/embeddings/` 또는 별도 `faiss-index/` prefix에 둔다.
- 서버 재생성 시 S3에서 index를 rebuild/download할 수 있어야 한다.

화면단 분리 판단:

현재는 `server/chat.html` 수준의 단순 화면이 있으므로 별도 frontend 서버를 만들 필요는 낮다. 1차 배포는 FastAPI가 정적 HTML을 같이 제공하거나, S3 + CloudFront에 정적 파일만 올리는 방식이 충분하다.

별도 frontend가 필요한 조건:

- React/Next.js 등 별도 빌드/배포 주기가 생긴다.
- 로그인, 사용자별 세션, 관리자 화면 등 product UI가 커진다.
- API와 화면의 릴리즈 주기를 분리해야 한다.

그 전까지는 별도 frontend EC2를 만들지 않는다. 화면만 정적으로 분리하려면 EC2보다 S3 + CloudFront가 단순하고 운영 부담이 낮다.

## DB 분리 기준

### 권장: RDS PostgreSQL 사용

DB는 EC2에 직접 올리는 것보다 RDS를 권장한다.

이유:

- backup, snapshot, minor version patch, storage 확장이 관리된다.
- DB가 app 서버 lifecycle과 분리된다.
- Terraform으로 subnet group, security group, parameter, backup policy를 관리하기 쉽다.

초기 비용을 줄이려면 RDS PostgreSQL 인스턴스 1개에 database를 2개 만든다.

```text
RDS PostgreSQL
  ├── airflow       # Airflow metadata only
  └── opik_service  # DART/report serving index, conversation, app metadata
```

Airflow metadata와 서비스 DB는 같은 RDS 인스턴스 안에 있어도 database는 반드시 분리한다. Airflow migration, task log metadata, scheduler state가 서비스 테이블과 섞이면 장애 영향과 백업/복구 판단이 어려워진다.

별도 RDS 인스턴스로 분리해야 하는 조건:

- Airflow DB 부하가 서비스 API latency에 영향을 준다.
- 서비스 DB에 사용자/결제/권한 등 더 민감한 데이터가 들어간다.
- Airflow DB 복구 시점과 서비스 DB 복구 시점이 달라야 한다.
- 팀/권한/IAM 경계를 명확히 나눠야 한다.

### Airflow DB

용도:

- DAG run/task instance metadata
- Airflow connection/variable metadata
- Celery result backend

주의:

- Airflow DB는 data lake 원천이 아니다.
- Bronze/Silver/Gold 원천은 계속 S3에 있어야 한다.
- Airflow secrets는 DB에 직접 넣기보다 Secrets Manager backend 또는 environment injection을 사용한다.

### Service DB

용도:

- `dart_report_index`
- report/disclosure serving index
- conversation/session metadata
- API 조회 성능을 위한 작은 normalized/index table

주의:

- S3 Parquet/Delta가 정본이고, RDS는 serving index로 본다.
- RDS 테이블은 S3 gold/delta에서 재생성 가능해야 한다.
- `rcept_no`, `corp_code`, `stock_code`, `source_silver_uri`, `gold_version` 같은 source identity를 유지한다.

## Redis / Celery 선택

현재 compose는 CeleryExecutor와 Redis broker를 사용한다.

운영 권장:

- Airflow CeleryExecutor 유지: ElastiCache Redis 사용
- Airflow 서버 1대에서만 실행하고 worker scale-out이 당장 없다면: Redis 컨테이너 유지도 가능
- 단일 서버에서 단순화가 우선이면: LocalExecutor 전환 검토

현재 dev Terraform은 사용자 요청에 따라 ElastiCache 대신 Airflow EC2 내부 Redis 컨테이너를 사용한다. `127.0.0.1:6379`에만 바인딩하고 public에 노출하지 않는다.

다만 Terraform 기준의 maintainability-first 운영 구조에서는 ElastiCache Redis를 private app/DB 계층에 두는 편이 장애 격리와 worker scale-out에 유리하다.

## S3 데이터 구조 유지 원칙

S3는 이 시스템의 source-of-truth다. EC2 local disk, FAISS index, RDS serving table은 모두 재생성 가능한 파생물이어야 한다.

유지해야 할 prefix:

```text
s3://<bucket>/bronze/
s3://<bucket>/silver/
s3://<bucket>/gold/
s3://<bucket>/delta/
```

추가 권장 prefix:

```text
s3://<bucket>/artifacts/faiss/
s3://<bucket>/artifacts/airflow-logs/
s3://<bucket>/artifacts/model/
```

Bronze/Silver/Gold는 source identifier, observed date, content hash, schema version을 잃지 않아야 한다. FAISS/vector index는 downstream artifact이며 원천 DB로 취급하지 않는다.

## Security Group 설계

| Security group | Inbound | Outbound |
|---|---|---|
| `alb_public_sg` | `0.0.0.0/0:443`, optional `0.0.0.0/0:80` redirect | `api_sg:8000` |
| `front_proxy_sg` | `0.0.0.0/0:80`, optional `0.0.0.0/0:443` | SSM/package update outbound, `api_sg:8000` |
| `api_sg` | `alb_public_sg:8000` 또는 `front_proxy_sg:8000` | NAT, S3 endpoint, RDS 5432, Bedrock HTTPS |
| `airflow_sg` | SSM only, optional admin/internal ALB 8080 | NAT, S3 endpoint, RDS 5432, Redis 6379, external APIs HTTPS |
| `rds_sg` | `api_sg:5432`, `airflow_sg:5432` | 제한 |
| `redis_sg` | `airflow_sg:6379` | 제한 |

SSH inbound는 기본적으로 만들지 않는다. 꼭 필요하면 임시 break-glass rule로 특정 관리자 IP만 허용하고, Terraform 변수로 off 가능한 구조로 둔다.

## IAM Role 설계

EC2에는 access key를 넣지 않고 instance profile을 붙인다.

### Airflow instance role

필요 권한:

- S3 read/write: `bronze/`, `silver/`, `gold/`, `delta/`, `artifacts/airflow-logs/`
- Bedrock invoke: embedding/LLM DAG에서 사용하는 model만 제한
- CloudWatch Logs write
- SSM Managed Instance Core
- Secrets Manager 또는 SSM Parameter Store read

### API/RAG instance role

필요 권한:

- S3 read: `gold/`, `delta/`, `artifacts/faiss/`
- S3 write: FAISS index를 서버에서 rebuild해 저장한다면 `artifacts/faiss/`
- Bedrock invoke: chatbot/agent에서 사용하는 model만 제한
- CloudWatch Logs write
- SSM Managed Instance Core
- Secrets Manager 또는 SSM Parameter Store read

## Terraform 폴더 구조 권장안

이번 문서는 먼저 `terraform/guid.md`로 둔다. 실제 Terraform 코드를 추가할 때는 아래 구조를 권장한다.

```text
terraform/
  guid.md
  README.md
  versions.tf
  providers.tf
  backend.tf

  envs/
    dev/
      main.tf
      variables.tf
      terraform.tfvars.example
      outputs.tf
    prod/
      main.tf
      variables.tf
      terraform.tfvars.example
      outputs.tf

  modules/
    network/
      vpc.tf
      subnets.tf
      nat.tf
      endpoints.tf
      outputs.tf
    security/
      security_groups.tf
      iam.tf
      outputs.tf
    storage/
      s3.tf
      kms.tf
      outputs.tf
    database/
      rds.tf
      redis.tf
      outputs.tf
    compute/
      ec2_airflow.tf
      ec2_api.tf
      user_data/
        airflow.sh
        api.sh
      outputs.tf
    load_balancer/
      alb.tf
      target_groups.tf
      listeners.tf
      outputs.tf
```

처음부터 module을 과하게 쪼개기 싫다면 `envs/dev/main.tf` 하나로 시작해도 된다. 다만 network, security, database, compute의 경계는 파일 단위로라도 분리한다.

## Terraform 리소스 구현 순서

1. Remote state
   - S3 backend bucket
   - DynamoDB lock table
   - KMS key

2. Network
   - VPC
   - public/private app/private DB subnet
   - Internet Gateway
   - NAT Gateway
   - route table
   - S3 Gateway Endpoint

3. Security/IAM
   - security groups
   - EC2 instance profiles
   - least-privilege S3/Bedrock/SSM policies

4. Storage
   - S3 data bucket
   - lifecycle policy
   - encryption
   - public access block

5. Database
   - RDS subnet group
   - RDS PostgreSQL
   - initial DB names: `airflow`, `opik_service`
   - optional ElastiCache Redis

6. Compute
   - `opik-airflow-batch` EC2
   - `opik-api-rag` EC2
   - EBS gp3 volumes
   - user-data bootstrap
   - CloudWatch agent

7. Load balancer
   - public ALB
   - ACM certificate
   - HTTPS listener
   - API target group
   - health check `/health`

8. Secrets/config
   - Secrets Manager or SSM Parameter Store
   - app `.env` generation or systemd environment injection

## 현재 환경변수 매핑

| 현재 변수 | AWS 이전 후 권장 공급 방식 |
|---|---|
| `S3_BUCKET` | Terraform output + SSM Parameter |
| `AWS_REGION`, `AWS_DEFAULT_REGION`, `S3_REGION` | instance environment |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Secrets Manager |
| `AIRFLOW_DB_URL` | Secrets Manager |
| `SERVICE_DB_URL` | Secrets Manager |
| `AIRFLOW__CELERY__BROKER_URL` | Secrets Manager 또는 SSM Parameter |
| `AIRFLOW__CORE__FERNET_KEY` | Secrets Manager |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | Secrets Manager |
| `BEDROCK_REGION` | SSM Parameter |
| `BEDROCK_API_KEY` | 가능하면 제거. Bedrock은 IAM role 기반 사용 |
| `BEDROCK_LLM_MODEL_ID`, `BEDROCK_MODEL` | SSM Parameter |
| `OPIK_REPORT_PIPELINE_SCHEDULE` | SSM Parameter |
| `FAISS_INDEX_PATH`, `FAISS_IDMAP_PATH` | API EC2 local EBS path |

AWS access key/secret key는 EC2 `.env`에 넣지 않는다. EC2 instance role을 사용한다.

## EC2 bootstrap 방향

초기에는 AMI를 복잡하게 만들지 않고 user-data로 최소 bootstrap을 수행한다.

Airflow 서버:

```text
1. install docker / docker compose
2. install aws cli / cloudwatch agent
3. clone or deploy repo
4. pull/build opik-airflow image
5. write environment from Secrets Manager/SSM
6. docker compose up -d
7. register Spark/Delta systemd timer if still host-based
```

API/RAG 서버:

```text
1. install python runtime or docker
2. install aws cli / cloudwatch agent
3. deploy server/ package
4. download or rebuild FAISS index from S3
5. configure systemd opik-server
6. health check /health
```

장기적으로는 AMI bake 또는 Docker image build pipeline으로 bootstrap 시간을 줄인다.

## 운영/비용 기준

dev/staging에서 비용을 줄이는 선택:

- NAT Gateway 1개
- RDS Single-AZ
- ElastiCache 대신 Airflow 서버 내 Redis
- EC2 2대 또는 ALB 권한이 없을 때 front proxy 포함 3대
- CloudFront/ALB 없이 front proxy로 dev 검증

prod에서 안전성을 높이는 선택:

- NAT Gateway AZ별 1개
- RDS Multi-AZ
- ElastiCache Redis
- S3 versioning + lifecycle
- CloudWatch alarm
- ALB access log
- SSM Session Manager only, SSH disabled
- RDS deletion protection

NAT Gateway는 작은 프로젝트에서도 고정 비용이 발생한다. 단, private subnet 서버가 외부 패키지와 업데이트를 안정적으로 받아야 한다는 요구가 있으므로 architecture 기준에서는 NAT를 포함한다. 비용 최적화는 S3 Gateway Endpoint, AWS service Interface Endpoint, 배포 이미지 사전 빌드로 줄인다.

## 마이그레이션 단계

### Phase 1: 네트워크와 상태 저장소 분리

- VPC/subnet/NAT/SG 생성
- S3 bucket/IAM role 정리
- RDS PostgreSQL 생성
- Airflow DB와 service DB를 RDS로 이전
- 기존 단일 서버의 local DB 의존 제거

### Phase 2: API/RAG 서버 분리

- `opik-api-rag` EC2 생성
- FastAPI systemd 또는 Docker 배포
- FAISS index를 S3 기반으로 rebuild/download
- Public ALB 연결
- `/health`, `/chat`, `/search` 검증

### Phase 3: Airflow/batch 서버 이전

- `opik-airflow-batch` EC2 생성
- Airflow compose 이전
- RDS/Redis/S3/IAM role 기반으로 실행
- Bronze/Silver/Gold DAG 재실행 검증
- Spark/Delta timer 이전

### Phase 4: 운영 hardening

- CloudWatch Logs/metrics/alarm
- S3 lifecycle/versioning
- RDS backup/restore drill
- Secrets rotation
- Terraform remote state locking
- admin access SSM/VPN 표준화

## 최종 판단

1차 Terraform 목표는 다음 네 가지다.

1. public에는 ALB/NAT만 둔다.
2. Airflow/batch와 FastAPI/RAG를 private EC2 2대로 분리한다.
3. Airflow DB와 service DB는 RDS PostgreSQL에서 database 단위로 분리한다.
4. S3 Bronze/Silver/Gold/Delta를 source-of-truth로 유지하고, RDS/FAISS/local disk는 재생성 가능한 파생물로 둔다.

화면단은 지금 당장 별도 서버로 분리하지 않는 것이 낫다. 사용자용 UI가 커지면 S3 + CloudFront 정적 frontend로 먼저 분리하고, FastAPI는 계속 private backend로 둔다. Airflow UI는 운영자 도구이므로 public frontend가 아니라 SSM/VPN/internal 접근 대상으로 유지한다.
