# Terraform Change Log

이 파일은 OPIK Terraform/인프라 아키텍처 작업의 목표와 변경 이력을 기록한다.

## 2026-06-22

### 목표

- 현재 단일 개발 서버에 함께 올라가 있는 Airflow, embedding, FastAPI/RAG, DB, Redis, S3 연동 구성을 AWS 계층형 아키텍처 관점에서 분리하는 기준을 문서화한다.
- VPC, public/private subnet, NAT Gateway, private app layer, private DB layer, RDS, Redis, S3 source-of-truth 원칙을 Terraform 작업의 기준으로 정리한다.
- 이후 Terraform/인프라 문서 변경 시 목표와 변경 내역을 추적할 수 있도록 changelog를 만든다.

### 변경 내역

- `terraform/` 폴더를 생성했다.
- `terraform/guid.md`를 추가했다.
  - 현재 프로젝트 실행 단위인 Airflow, Redis, Airflow DB, Spark/Delta, FastAPI/FAISS RAG, DART/report pipeline, S3 data lake를 기준으로 AWS 분리안을 작성했다.
  - public layer에는 ALB/NAT만 두고, Airflow 및 FastAPI/RAG 서버는 private app subnet에 두는 구성을 권장했다.
  - RDS PostgreSQL 안에서 `airflow` DB와 `opik_service` DB를 분리하는 초기안을 제시했다.
  - 비용 절감용 dev 구성과 운영용 prod 구성의 NAT/RDS/Redis 선택 기준을 구분했다.
  - Airflow UI는 public frontend가 아니라 SSM/VPN/internal 접근 대상으로 유지하도록 정리했다.
  - 사용자 화면은 당장 별도 EC2로 분리하지 않고, 필요 시 S3 + CloudFront 정적 frontend로 분리하는 방향을 제시했다.
  - Terraform 폴더 구조, 구현 순서, 환경변수 매핑, EC2 bootstrap, 마이그레이션 단계를 정리했다.
- `terraform/change-log.md`를 추가했다.
- `AGENTS.md`를 추가해 향후 Terraform/인프라 작업의 목표와 변경 내역을 `terraform/change-log.md`에 남기도록 명시했다.

### 검증

- 현재 repo에 기존 `AGENTS.md`, `Agent.MD`, `CODEX.md`, `CLAUDE.md` 파일이 없음을 확인했다.
- `terraform/guid.md`가 정상 생성되어 있고, 현재 Terraform 문서 기준 파일로 사용할 수 있음을 확인했다.

### 남은 작업

- 실제 Terraform 코드 생성 시 `terraform/guid.md`의 권장 구조를 기준으로 `envs/`, `modules/`, `versions.tf`, `providers.tf`, `backend.tf`를 추가한다.
- remote state용 S3 backend/DynamoDB lock table 구성 여부를 먼저 결정한다.
- dev/prod NAT Gateway 개수와 RDS Single-AZ/Multi-AZ 기준을 비용과 운영 요구에 맞춰 확정한다.

## 2026-06-22 — Terraform dev environment implementation

### Date

2026-06-22

### Goal

- `terraform/AGENTS.md`의 목표 지향 Terraform 작업 규칙에 따라, `terraform/guid.md`의 AWS 분리 아키텍처를 실제 Terraform 코드로 구현한다.
- 현재 OPIK 구조를 기준으로 public ALB, private Airflow/batch EC2, private FastAPI/RAG EC2, private RDS, Redis, S3 data lake, NAT, S3 VPC Endpoint, IAM/SSM/Secrets Manager 구성을 plan 가능한 상태까지 만든다.

### Current phase

- Implemented and locally validated.
- `terraform apply`와 실제 AWS resource/service health verification은 보류 상태다.

### Changes made

- `terraform/envs/dev/` 환경을 추가했다.
  - `versions.tf`, `providers.tf`, `variables.tf`, `main.tf`, `outputs.tf`
  - `terraform.tfvars.example`
  - `backend-s3.tf.example`
  - `README.md`
  - `.terraform.lock.hcl`
- `terraform/modules/network/`를 추가했다.
  - VPC
  - public subnets
  - private app subnets
  - private DB subnets
  - Internet Gateway
  - NAT Gateway
  - route tables
  - S3 Gateway VPC Endpoint
- `terraform/modules/security/`를 추가했다.
  - ALB, API, Airflow, RDS, Redis security groups
  - ALB 80/443 ingress
  - ALB to private API 8000
  - optional admin CIDR to Airflow 8080
  - Airflow/API to RDS 5432
  - Airflow to Redis 6379
- `terraform/modules/storage/`를 추가했다.
  - S3 data lake bucket
  - public access block
  - bucket owner enforced ownership
  - versioning
  - SSE-S3 encryption
  - lifecycle rules
- `terraform/modules/database/`를 추가했다.
  - Airflow RDS PostgreSQL instance
  - Service RDS PostgreSQL instance
  - RDS managed master user passwords
  - optional ElastiCache Redis
- `terraform/modules/compute/`를 추가했다.
  - private Airflow/batch EC2
  - private API/RAG EC2
  - EC2 IAM roles and instance profiles
  - SSM Managed Instance Core attachment
  - S3/Bedrock/CloudWatch/SSM/Secrets Manager least-scope policies
  - Secrets Manager secret containers for runtime app secrets
  - SSM parameters for non-secret runtime endpoints
  - bootstrap user-data templates
- `terraform/modules/load_balancer/`를 추가했다.
  - public ALB
  - API target group
  - target attachment to private API EC2
  - HTTP listener for dev
  - optional HTTPS listener and HTTP to HTTPS redirect when `certificate_arn` is set
- `.gitignore`에 Terraform local state/plan/generated artifact ignore 규칙을 추가했다.

### Commands run

```bash
terraform fmt -recursive terraform
cd terraform/envs/dev
terraform init -backend=false
terraform validate
terraform plan -out=tfplan
terraform show -no-color tfplan
```

### Validation result

- `terraform fmt -recursive terraform`: passed.
- `terraform init -backend=false`: passed after network access was allowed for provider download.
- `terraform validate`: passed with no warnings after fixing S3 lifecycle filter configuration.
- `terraform plan -out=tfplan`: passed.
- Plan summary: `69 to add, 0 to change, 0 to destroy`.

### AWS verification result

- AWS provider calls for caller identity, availability zones, and Amazon Linux 2023 ARM64 AMI lookup succeeded during `terraform plan`.
- No AWS resources were created because `terraform apply` was not run.
- Terraform state verification, AWS CLI resource verification, network path verification, and service health checks are not yet possible until apply is explicitly approved and runtime deployment inputs are supplied.

### Failures

- First `terraform init -backend=false` failed because sandboxed execution could not resolve `registry.terraform.io`.
- First non-escalated `terraform validate` and `terraform plan` failed because sandboxed execution could not run the downloaded AWS provider plugin.
- First escalated `terraform plan` failed due to:
  - `coalesce(var.repo_url, "")` with null plus empty-string fallback.
  - Redis SSM parameter `count` depending on apply-time endpoint value.
- Initial S3 lifecycle configuration produced a provider warning because one rule did not include an explicit `filter`.

### Fixes

- Re-ran provider download and provider-backed Terraform commands with allowed execution.
- Replaced `coalesce(var.repo_url, "")` with explicit null handling.
- Added `create_redis` as a compute module input so Redis SSM parameter count is known during planning.
- Added explicit lifecycle `filter {}` to the incomplete multipart upload rule.

### Remaining blockers

- `terraform apply` is not run because it will create cost-bearing AWS resources including NAT Gateway, EC2, two RDS instances, ElastiCache Redis, and ALB.
- Public ALB exposure must be reviewed before apply. Dev currently permits `alb_ingress_cidrs = ["0.0.0.0/0"]`.
- Runtime application deployment is not complete from Terraform alone because `repo_url`, app secrets, Airflow Fernet/webserver secret, Telegram token, DART keys, and optional ACM certificate ARN are user-controlled inputs.
- API/ALB health checks will not prove the real OPIK app until the FastAPI service is deployed on the API EC2 instance.

### Next action

- Review `terraform/envs/dev/terraform.tfvars.example` and create `terraform.tfvars`.
- Decide whether to keep two RDS instances or reduce dev cost by changing the DB topology later.
- Provide explicit approval before running `terraform apply tfplan`.
- After apply, verify resources with Terraform state, AWS CLI, SSM managed instance status, RDS availability, Redis availability, ALB target health, S3 endpoint routing, and app `/health`.

## 2026-06-22 — ALB 권한 우회 및 dev 배포 검증

### Date

2026-06-22

### Goal

- 계정에 ALB/ELBv2 권한이 없을 때도 public/private 계층 구조를 유지하며 FastAPI/RAG 애플리케이션 배포를 확인한다.
- 사용자 요청에 따라 Redis는 ElastiCache가 아니라 Airflow EC2 내부 Redis 컨테이너로 구성한다.

### Changes made

- `terraform/envs/dev/main.tf`에서 ALB module 적용 경로를 제거하고, compute module의 public `front_proxy` EC2 경로로 전환했다.
- `terraform/modules/compute/user_data/front_proxy.sh.tftpl`를 통해 nginx reverse proxy를 구성했다.
  - public front proxy EC2:80
  - private API EC2:8000
- `terraform/modules/compute/user_data/airflow.sh.tftpl`에서 Airflow EC2 내부 Redis 컨테이너를 실행하도록 구성했다.
  - image: `redis:7-bookworm`
  - bind: `127.0.0.1:6379`
  - broker: `redis://127.0.0.1:6379/0`
- `terraform/modules/security/`에 front proxy 전용 security group을 추가했다.
  - public HTTP/HTTPS ingress
  - SSM/package update/API proxying을 위한 outbound
  - private API EC2 8000 ingress from front proxy SG
- `terraform/envs/dev/README.md`와 `terraform/guid.md`를 현재 dev 적용안에 맞춰 갱신했다.

### Commands run

```bash
terraform fmt -recursive terraform
cd terraform/envs/dev
terraform validate
terraform plan -var=repo_url=https://github.com/Luriar/opik.git -out=tfplan
terraform apply -auto-approve tfplan
terraform plan -var=repo_url=https://github.com/Luriar/opik.git -detailed-exitcode
```

### AWS resources verified

- VPC: `vpc-0d2ee09e031fa648a`
- API EC2: `i-0ea034d8eda4b0c00`
  - private IP: `10.20.11.64`
  - SSM: Online
  - `opik-server.service`: active
  - local health: `GET http://127.0.0.1:8000/health` returned `status=ok`
- Airflow EC2: `i-0b82193d2d3a3b141`
  - private IP: `10.20.10.246`
  - SSM: Online
  - Redis container: `opik-airflow-redis`
  - Redis health: `redis-cli ping` returned `PONG`
- Front proxy EC2: `i-02d74dbd52dc7b380`
  - private IP: `10.20.0.225`
  - public IP: `43.203.214.114`
  - SSM: Online after front proxy SG outbound fix
  - nginx: active
  - public health: `GET http://43.203.214.114/health` returned `status=ok`
- Airflow RDS: `opik-dev-airflow-db.ct460c8gc44s.ap-northeast-2.rds.amazonaws.com`
  - status: `available`
- Service RDS: `opik-dev-service-db.ct460c8gc44s.ap-northeast-2.rds.amazonaws.com`
  - status: `available`

### Validation result

- `terraform validate`: passed.
- Initial ALB/ElastiCache apply path was blocked by IAM permissions.
- Front proxy + local Redis apply completed.
- Final Terraform plan result: `No changes. Your infrastructure matches the configuration.`
- Real application health through public front proxy returned:

```json
{"status":"ok","model":"intfloat/multilingual-e5-small","index_size":0,"dim":384,"agent_framework":"not_initialised"}
```

### Notes

- `index_size=0` is expected until embeddings exist under the configured S3 prefix.
- The public front proxy is a dev fallback for missing ALB permissions, not the preferred production entry point.
- The legacy ALB module remains in the repository for future use when ELBv2 permissions are available, but it is not wired into the current dev environment.

## 2026-06-22 — Airflow UI 배포 및 접근 절차 정리

### Date

2026-06-22

### Goal

- private Airflow EC2에서 Airflow webserver/scheduler/worker/triggerer가 실제로 실행되도록 구성한다.
- Airflow UI를 public internet에 직접 노출하지 않고 SSM port forwarding으로 접근하는 절차를 문서화한다.
- 현행 Terraform 배포 시 환경설정이 `.env`가 아니라 Terraform variables, RDS managed secrets, optional runtime secret, EC2 bootstrap env file을 통해 조달되는 흐름을 명확히 정리한다.
- 클라우드 비밀 저장소를 사용하지 않는 경우의 local encrypted secret 운영 방식을 문서화한다.

### Changes made

- `terraform/modules/compute/user_data/airflow.sh.tftpl`에서 Airflow EC2 bootstrap을 실제 Airflow compose stack 실행 방식으로 확장했다.
  - Docker/Compose, git, awscli, jq, openssl 설치
  - repo clone/update
  - RDS managed secret에서 Airflow DB credential 조회
  - optional Airflow runtime secret JSON 조회
  - `/opt/opik/bootstrap.env`, `/opt/opik/airflow.env`, `/opt/opik/airflow-admin.env` 생성
  - `opik-airflow.service` systemd unit 생성
  - Airflow webserver, scheduler, worker, triggerer, Redis compose service 실행
- Airflow webserver는 EC2 localhost `127.0.0.1:8080`에만 bind하도록 유지했다.
- Airflow 로그/config/cache 권한을 Airflow 컨테이너 UID 기준으로 맞춰 scheduler DAG processor permission error를 방지했다.
- `terraform/envs/dev/outputs.tf`에 Airflow UI local URL과 SSM port forwarding command output을 추가했다.
- `terraform/envs/dev/README.md`에 다음 내용을 추가/정리했다.
  - Airflow UI 접근 방법
  - Airflow admin credential 조회 방법
  - Airflow operational health check 방법
  - Terraform 배포 시 환경설정 로딩 흐름
  - `.env`를 plaintext로 쓰지 않는 local encrypted secret 운영 방식

### Validation result

- `terraform fmt -recursive terraform`: passed.
- `terraform validate`: passed.
- `terraform apply` 후 Airflow EC2가 교체되어 새 bootstrap이 적용됐다.
- Airflow EC2에서 `opik-airflow.service`가 `active (exited)` 상태로 정상 완료됐다.
- Airflow compose containers가 healthy 상태임을 확인했다.
  - `opik-airflow-webserver`
  - `opik-airflow-scheduler`
  - `opik-airflow-worker`
  - `opik-airflow-triggerer`
  - `opik-airflow-redis`
- Airflow local health endpoint `http://127.0.0.1:8080/health`가 HTTP 200을 반환했다.
- 최종 `terraform plan -var=repo_url=https://github.com/Luriar/opik.git -detailed-exitcode` 결과는 exit code `0`, `No changes. Your infrastructure matches the configuration.` 이었다.

### Notes

- Airflow UI는 운영자 콘솔이므로 public front proxy나 direct ingress로 열지 않는다.
- Airflow UI 비밀번호는 `/opt/opik/airflow-admin.env`에 root-only `0600` 권한으로 저장된다.
- 현재 dev 구성에서는 Airflow Fernet key가 runtime secret에 없으면 EC2 bootstrap 시 생성된다. 운영에서는 `opik-dev/airflow/runtime` 또는 별도 안전한 secret 공급 방식으로 stable key를 유지해야 한다.

## 2026-06-22 — 배포 시 설정값 주입(.env 비밀 분리) + Secrets Manager 적용 + 재배포 검증

### Date

2026-06-22

### Goal

- `.env`의 비-Airflow 설정값(S3/저장계층 prefix, DART 튜닝, embedding)을 배포 시 Terraform 변수로 주입할 수 있게 구성한다.
- 비밀값(`DART_API_KEYS` 등)은 코드/tfvars/Git에 두지 않고 AWS Secrets Manager로 분리한다.
- 보안 처리(민감정보 노출 방지) 및 다른 머신 배포용 문서를 정비한다.
- 변경된 설정을 현재 환경에 실제로 적용·재배포하고 서비스 정상 동작까지 검증한다.

### Changes made

- `terraform/envs/dev/variables.tf`에 비밀이 아닌 앱 설정용 object 변수 3개 추가: `storage_config`, `dart_config`, `embedding_config` (기본값은 `.env`와 동일).
- `terraform/envs/dev/main.tf` → `modules/compute`로 위 변수 3개를 전달.
- `terraform/modules/compute/variables.tf`에 동일 변수 선언 추가.
- `terraform/modules/compute/main.tf`에서 airflow/api `templatefile`에 설정 전달(DART는 airflow만, storage/embedding은 양쪽).
- `terraform/modules/compute/user_data/airflow.sh.tftpl`: `airflow.env`에 S3_BASE_PREFIX/BRONZE/SILVER/GOLD_PREFIX, STORAGE_BACKEND, LOCAL_STORAGE_BASE_PATH, DART_* 튜닝, EMBEDDING_* 렌더링. (`DART_API_KEYS`는 기존대로 Secrets Manager에서 주입)
- `terraform/modules/compute/user_data/api.sh.tftpl`: `bootstrap.env`에 S3 prefix + EMBEDDING_* 추가(RAG 질의 인코더 정합).
- `terraform/envs/dev/terraform.tfvars.example`: 위 설정 블록 + "비밀 금지" 가이드 주석 추가.
- `terraform/.gitignore` 신규 추가(state/plan/tfvars/.env 폴더 단독 보호, defense-in-depth).
- `terraform/DEPLOYMENT.md` 신규 추가(다른 머신 배포 시 민감정보/설정 배치 기준, `.env` 키→위치 매핑, apply 후 Secrets Manager 주입 절차).

### Commands run

- `terraform fmt -recursive` / `terraform validate`: passed.
- `aws secretsmanager put-secret-value`(airflow runtime): `.env`에서 `DART_API_KEYS`(8 keys) + `AIRFLOW__WEBSERVER__SECRET_KEY` 주입. 값은 임시파일+`file://`로 전달해 CLI args/CloudTrail 노출 방지.
- `terraform plan -out=tfplan` → EC2 3대(airflow/api/front_proxy)만 replace. RDS/S3/network/secret 변경 없음.
- `terraform apply tfplan` 2회: (1) 설정/시크릿 반영, (2) `terraform.tfvars` 생성으로 `repo_url` 주입 후 앱 실제 배포.

### Validation result

- `terraform apply`: 각각 `3 added, 0 changed, 3 destroyed` 성공.
- API: front proxy 경유 `http://<fp-ip>/health` → HTTP 200, `{"status":"ok","model":"intfloat/multilingual-e5-small","dim":384}` (배포된 embedding_config 반영 확인).
- Airflow: webserver/scheduler/worker/triggerer/metadatabase 모두 healthy, `127.0.0.1:8080/health` 정상.
- `airflow.env` 검증: S3 prefix/DART/embedding 비밀 아닌 설정 정상 렌더링, `DART_API_KEYS`(8개)·webserver secret이 Secrets Manager에서 주입됨(값 미노출).

### AWS verification result

- 신규 인스턴스: airflow `i-0c5f8dbc60f79c2d9`, api `i-0f53a03ef1a0ec89d`, front_proxy `i-0f6...`(IP `13.124.205.162`).
- 두 private 인스턴스 SSM `Online`.

### Failures & fixes

- 1차 재배포 후 두 앱 미기동(front proxy 502). 원인: `terraform.tfvars` 부재 → `repo_url`이 `null` 기본값 → 부트스트랩의 `repo_url` 가드에서 앱 배포 skip(기존부터 있던 구성 공백).
- 조치: `terraform.tfvars`를 example에서 생성(`repo_url=https://github.com/Luriar/opik.git`, public repo 확인)하고 재적용 → 정상 기동.

### Notes / Remaining

- `front_proxy_public_ip`는 인스턴스 교체 시마다 변경됨(현재 `13.124.205.162`). 고정 필요 시 EIP 부여 고려.
- `terraform.tfvars`는 `.gitignore` 대상이라 커밋되지 않음(머신 로컬 설정). 다른 머신 배포 시 `DEPLOYMENT.md` 절차 참고.
- `VECTOR_DB_URL`(pgvector) 전용 인프라는 아직 분리되어 있지 않음.

## 2026-06-22 — 기존 S3 버킷 참조 + Airflow UI 공개(IP 제한) + 시크릿/버킷 destroy 보호

### Date

2026-06-22

### Goal

- 앱이 신규 생성된 `opik-dev-...-data`가 아니라 기존 실데이터 버킷 `s3-opik-bucket`을 바라보게 한다.
- Airflow UI를 public 경로로 접근 가능하게 하되, 운영자 PC IP(들)만 허용한다(추후 변경 용이).
- 각 key 값의 저장/조회 흐름을 문서화하고, `terraform destroy` 시 시크릿/버킷이 삭제되지 않게 보호한다.

### Changes made

- `terraform/modules/storage/`: `create_bucket` 변수 추가. `false`면 버킷을 만들지 않고 `data "aws_s3_bucket"`로 **기존 버킷 참조만** 한다(설정 리소스도 생성 안 함). outputs는 locals 기반.
- `terraform/modules/storage/main.tf`: 관리형 버킷 리소스에 `lifecycle { prevent_destroy = true }` 추가(+ `force_destroy=false`).
- `terraform/envs/dev/`: `manage_data_bucket` 변수 추가 → storage 모듈 `create_bucket`로 전달. `terraform.tfvars`에 `data_bucket_name="s3-opik-bucket"`, `manage_data_bucket=false` 설정.
- `terraform/modules/compute/main.tf`: `aws_secretsmanager_secret.airflow_runtime`, `api_runtime`에 `lifecycle { prevent_destroy = true }` 추가. front_proxy templatefile에 `airflow_private_ip` 전달.
- `terraform/modules/security/main.tf`: front proxy SG에 `:8080` ingress(=`admin_airflow_cidrs`만), airflow SG에 `:8080` ingress(front proxy SG에서만) 추가.
- `terraform/modules/compute/user_data/front_proxy.sh.tftpl`: nginx `:8080` server block 추가 → airflow private IP:8080 프록시(websocket 헤더 포함).
- `terraform/modules/compute/user_data/airflow.sh.tftpl`: webserver 포트 바인딩 `127.0.0.1:8080:8080` → `8080:8080`, `AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=true` 추가.
- `terraform/envs/dev/outputs.tf`: `airflow_ui_public_url` 출력 추가.
- `terraform/envs/dev/terraform.tfvars`: `admin_airflow_cidrs=["61.98.195.201/32"]`(운영자 IP).
- 문서: `terraform/DEPLOYMENT.md`에 "키 저장·조회 흐름과 destroy 보호" 섹션 추가.

### Commands run

- `terraform fmt -recursive` / `validate`: passed.
- `terraform plan -out=tfplan` → IAM/SSM(S3 ARN을 s3-opik-bucket으로) update, EC2 3대 replace, SG 규칙 3개 add, 빈 `opik-dev` 버킷+설정 리소스 delete. 시크릿은 무변경.
- `terraform apply tfplan`: 성공. 이후 버킷 `prevent_destroy` 추가 후 `plan` → `No changes`(count=0 메타데이터, no-op).
- `terraform plan -destroy`: 시크릿 2개가 `prevent_destroy`로 **에러 중단**됨(보호 동작 확인).

### Validation / AWS verification result

- S3: `data_bucket_name` 출력 = `s3-opik-bucket`. 신규 airflow.env `S3_BUCKET=s3-opik-bucket` 확인. 빈 `opik-dev-...-data` 버킷은 삭제됨(head-bucket 404), `s3-opik-bucket`은 그대로 존재.
- Airflow UI: `http://3.36.56.222:8080/health` → 200, `/` → 302(login). 컨테이너 webserver/scheduler/worker/triggerer 모두 healthy.
- IP 제한: front-proxy SG `:8080` ingress = `61.98.195.201/32` 단일 확인.
- API: `http://3.36.56.222/health` → 200.
- destroy 보호: `terraform destroy` 시 `aws_secretsmanager_secret.airflow_runtime`/`api_runtime`가 `lifecycle.prevent_destroy`로 중단. 데이터 버킷은 data source 참조(+관리 시 prevent_destroy)로 삭제 불가.

### Notes / Remaining

- 신규 인스턴스: airflow `i-098fff0361356c9f7`, api `i-073149877c86fb0be`, front_proxy IP `3.36.56.222`.
- Airflow UI는 평문 HTTP(+IP 허용목록). 운영 노출 시 ACM/HTTPS 적용 권장.
- 운영자 IP 변경 시 `terraform.tfvars`의 `admin_airflow_cidrs`만 수정 후 `apply`(EC2 교체 없이 SG만 갱신).
- front_proxy 공인 IP는 인스턴스 교체 시 변경됨. 고정하려면 EIP 고려.
- Airflow 관리자 계정(`_AIRFLOW_WWW_USER_USERNAME`/`_AIRFLOW_WWW_USER_PASSWORD`)을 `opik-dev/airflow/runtime` 시크릿에 저장하고 실행 인스턴스에 reset-password로 반영. public UI(`:8080`) 로그인 정상 확인. 비밀번호 값은 Secrets Manager에만 보관(changelog/코드에 미기록). 향후 인스턴스 교체 시 부트스트랩이 이 값으로 admin을 생성하므로 자격증명 유지됨.

## 2026-06-22 — destroy 시 S3·Secrets 보존(둘 다 data source 참조로 전환)

### Date

2026-06-22

### Goal

- `terraform destroy`가 S3 데이터 버킷과 Secrets Manager 시크릿을 삭제하지 않게 하고, 같은 계정 재배포 시 이름 충돌이 없게 한다.

### Changes made

- `terraform/modules/compute/main.tf`: `aws_secretsmanager_secret.airflow_runtime`/`api_runtime`(resource + `prevent_destroy`)를 제거하고 `data "aws_secretsmanager_secret"` 참조로 전환. IAM 정책/EC2 user_data/outputs의 ARN 참조도 `data.*`로 변경.
- S3는 이미 `manage_data_bucket=false`로 data source 참조 중(추가 변경 없음). 관리형 대비 `aws_s3_bucket.data`의 `prevent_destroy`/`force_destroy=false`는 유지.
- `terraform/DEPLOYMENT.md`: 시크릿 컨테이너를 apply 전 1회 `create-secret`해야 함(데이터소스 참조), destroy 보호 설명을 data source 기준으로 갱신.

### Commands run

- `terraform state rm module.compute.aws_secretsmanager_secret.airflow_runtime module.compute.aws_secretsmanager_secret.api_runtime` (시크릿을 state에서만 분리, AWS에는 유지). state 백업 선행.
- `terraform fmt -recursive` / `validate`: passed.
- `terraform plan`: `No changes`(ARN 동일 → EC2/IAM 재생성 없음).
- `terraform plan -destroy`: 정상(에러 없음). `Plan: 0 add, 0 change, 63 destroy`.

### Validation / AWS verification result

- 마이그레이션 후 `plan` = No changes(무중단). 시크릿/버킷은 AWS에 그대로 존재(`describe-secret` 확인).
- `plan -destroy`에서 `secretsmanager_secret`·`aws_s3_bucket` 미포함 확인 → S3·Secrets는 destroy 대상 아님.
- destroy 대상 63개는 VPC/EC2/RDS/IAM/SG 등.

### Notes / Remaining

- 새 계정/리전 최초 배포 시: `opik-dev/airflow/runtime`, `opik-dev/api/runtime` 컨테이너를 apply 전에 `create-secret`로 1회 생성해야 함(없으면 data source 조회 실패).
- **RDS 2개는 여전히 destroy 대상**이며 `rds_skip_final_snapshot=true`라 스냅샷 없이 삭제됨. DB 데이터 보존이 필요하면 별도 조치(스냅샷/deletion_protection) 권장.

## 2026-06-22 — 현재 `.env` 기준 배치 매핑 정리 + Telegram 값 부트스트랩 배선

### Date

2026-06-22

### Goal

- 현재 루트 `.env`를 기준으로, DB 접속 정보를 제외한 각 키를 Secrets Manager / `terraform.tfvars` / RDS 관리형 시크릿 중 어디에 어떤 양식으로 넣는지 확정한다.
- 매핑이 실제로 동작하도록(문서와 코드 일치) 누락된 키를 부트스트랩에 배선하고, 검증 후 요약 매핑 문서를 만든다.

### Changes made

- `terraform/ENV-MAPPING.md` 신규 추가: 현재 `.env` 30개 키별 `.env 키 → target 위치/양식` 요약표 + Secrets Manager JSON 예시(airflow/api runtime) + `terraform.tfvars` 예시 + DB 자동 조립 설명 + 주의사항.
- `terraform/modules/compute/user_data/airflow.sh.tftpl`: runtime 시크릿에서 `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`를 읽어 `airflow.env`에 렌더하도록 추가(배선 전에는 시크릿에 넣어도 pipeline `telegram_briefing`에 전달 안 됨).
- `terraform/modules/compute/user_data/api.sh.tftpl`: API용 `api/runtime` 시크릿을 IAM Role로 조회(`jq` 설치 추가)해 `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`를 `bootstrap.env`에 렌더(+`chmod 600`). FastAPI(`server/opik_server.py`)의 텔레그램 봇 기능에 필요.

### Findings

- `.env`의 `AIRFLOW__CORE__FERNET_KEY=opik-fernet-key-2026-secure-placeholder`는 **유효한 Fernet 키가 아님**(32B url-safe base64 아님). 그대로 주입 시 Airflow 기동 실패 → 유효 키 생성 명령을 문서에 명시.
- `.env`의 `TELEGRAM_CHAT_IDS`(복수형)는 코드 어디에서도 사용하지 않음 → 배치 불필요로 표기.
- `AIRFLOW_SECRET_KEY` 와 `AIRFLOW__WEBSERVER__SECRET_KEY`는 동일 값이며 시크릿 JSON 키 하나(`AIRFLOW__WEBSERVER__SECRET_KEY`)로 통합.

### Commands run (local test, terraform 미설치 환경)

- 부트스트랩의 시크릿 추출 로직(`jq -r '.[$key] // empty'`)을 Python으로 동일 의미 재현 → §2-1 JSON에서 DART/webserver/telegram/schedule 정확 추출, 빈 컨테이너 폴백(스케줄 `0 0 * * *`, 텔레그램 빈값) 무크래시 확인.
- Fernet 유효성: `.env` placeholder=무효, `urlsafe_b64encode(os.urandom(32))` 생성키=유효 확인.
- `api.sh.tftpl` 추가 블록(시크릿 read + `bootstrap.env` 렌더) `bash -n` 문법 검증 통과.

### Validation result

- 추출/폴백/Fernet 검증 어설션 통과(생성 fernet 유효, placeholder 무효).
- 셸 문법 검증 통과.
- `terraform validate`/`plan`/`apply`는 이 머신에 Terraform 미설치로 **미실행**(이전 배포는 다른 머신에서 수행됨). 적용 머신에서 `terraform fmt -recursive`/`validate`/`plan` 재확인 권장.

### Remaining

- 적용 머신에서: ① `opik-dev/api/runtime`에 텔레그램 값 `put-secret-value`, ② `terraform plan/apply`로 EC2 user-data 갱신(인스턴스 교체), ③ `/health` 및 텔레그램 동작 확인.
- 운영 시 Fernet 키 고정값 유지(인스턴스 교체 간 암호화 connection 보존).

## 2026-06-22 — FastAPI/Spark env 외부화 및 공식문서 근거 매핑 정리

### Date

2026-06-22

### Goal

- Terraform 배포 시 Airflow, Server(FastAPI/RAG), Spark, DB, S3 설정값이 어디서 주입되는지 현재 코드 기준으로 정리한다.
- FastAPI 배포 경로와 코드 기본값이 어긋나는 env를 Terraform user-data에서 명시 주입한다.
- Spark Delta job의 S3/Delta 경로를 배포 환경별 prefix에 맞게 env로 override 가능하게 한다.
- 공식 AWS 문서에서 확인한 근거와 OPIK 코드 기반 판단을 문서에서 분리한다.

### Changes made

- `terraform/ENV-MAPPING.md`를 재작성했다.
  - 실제 secret 예시는 제거하고 위치/형식 설명으로 변경.
  - EC2 IAM Role, Secrets Manager, RDS managed password, SSM Parameter Store 공식문서 근거 URL 추가.
  - Airflow, FastAPI/RAG, Spark, DB/Storage별 env 주입 위치와 코드 기본값을 분리 정리.
- `terraform/DEPLOYMENT.md`에 `server_config.*`와 FastAPI/Spark 상세 매핑 참조를 추가했다.
- `terraform/envs/dev/variables.tf`, `terraform/envs/dev/terraform.tfvars.example`에 `server_config` object를 추가했다.
  - `OPIK_AGENT_ENABLED`, `SEARCH_TOP_K`, `OPIK_DB_PATH`, Bedrock/agent model id, DART sentiment tuning을 배포 시점에 설정 가능하게 함.
- `terraform/modules/compute/variables.tf`, `terraform/envs/dev/main.tf`, `terraform/modules/compute/main.tf`에 `server_config` 전달을 추가했다.
- `terraform/modules/compute/user_data/api.sh.tftpl`
  - `PROMPT_DIR=/opt/opik/repo/server/prompts` 추가. 기존 코드 기본값 `/root/opik-server/prompts`는 Terraform clone 경로와 불일치.
  - `OPIK_DB_PATH`, `OPIK_AGENT_ENABLED`, `SEARCH_TOP_K`, Bedrock/agent model id, DART sentiment tuning env 렌더링 추가.
  - Spark 경로 env(`GOLD_STRUCTURED_PREFIX`, `GOLD_EMBEDDINGS_PREFIX`, `GOLD_DISCLOSURE_PREFIX`, `DELTA_BASE_PREFIX`) 추가.
- `terraform/modules/compute/user_data/airflow.sh.tftpl`
  - Airflow 컨테이너 env에도 Spark 경로 env를 추가했다.
- `spark_jobs/gold_to_delta.py`, `spark_jobs/spark_silver_to_delta.py`
  - `S3_BASE_PREFIX`, `GOLD_STRUCTURED_PREFIX`, `GOLD_EMBEDDINGS_PREFIX`, `GOLD_DISCLOSURE_PREFIX`, `DELTA_BASE_PREFIX`를 읽도록 변경.
  - 상대 경로는 `s3a://<bucket>/<S3_BASE_PREFIX>/<relative>`로 조립하고, `s3a://`/`s3://` 전체 URI는 그대로 사용.

### Commands run

- Repository inspection: `Get-Content`, `rg`.
- Official docs lookup:
  - AWS EC2 IAM roles for Amazon EC2
  - AWS Secrets Manager get secret value
  - Amazon RDS password management with Secrets Manager
  - AWS Systems Manager Parameter Store

### Validation result

- 코드/문서 변경 완료.
- `python -m py_compile spark_jobs/gold_to_delta.py spark_jobs/spark_silver_to_delta.py`: passed.
- `terraform fmt -recursive terraform`: failed because Terraform CLI is not installed or not on PATH on this machine.
- `terraform validate`/`terraform plan`: not run because Terraform CLI is unavailable.

### AWS verification result

- 실제 AWS apply/verification은 수행하지 않음.

### Remaining blockers / next action

- Terraform CLI가 있는 적용 머신에서 `terraform fmt -recursive`, `terraform validate`, `terraform plan`을 재실행해야 한다.
- 이미 배포된 EC2에 반영하려면 plan/apply로 API/Airflow user-data 변경에 따른 instance replacement를 검토해야 한다.

## 2026-06-22 — spark_jobs 경로 외부화 원복

### Date

2026-06-22

### Goal

- Spark job은 현재와 향후 모두 같은 S3 bucket/layout을 사용할 예정이므로, 불필요한 Spark S3 세부 경로 외부화 변경을 원복한다.

### Changes made

- `spark_jobs/gold_to_delta.py`, `spark_jobs/spark_silver_to_delta.py`
  - `S3_BASE_PREFIX`, `GOLD_*_PREFIX`, `DELTA_BASE_PREFIX` override 로직 제거.
  - 기존처럼 `S3_BUCKET`, `AWS_REGION`만 env로 받고 `gold/...`, `delta/gold_db` 경로는 코드 고정값으로 복구.
- `terraform/modules/compute/user_data/airflow.sh.tftpl`, `terraform/modules/compute/user_data/api.sh.tftpl`
  - Spark 전용 `GOLD_STRUCTURED_PREFIX`, `GOLD_EMBEDDINGS_PREFIX`, `GOLD_DISCLOSURE_PREFIX`, `DELTA_BASE_PREFIX` env 렌더링 제거.
- `terraform/ENV-MAPPING.md`
  - Spark Runtime 섹션을 고정 S3 layout 기준으로 수정.
- `terraform/DEPLOYMENT.md`, Terraform variable descriptions
  - FastAPI/Spark 표현 중 Spark 외부화로 오해될 수 있는 문구 정리.

### Validation result

- `python -m py_compile spark_jobs/gold_to_delta.py spark_jobs/spark_silver_to_delta.py`: passed.
- `git diff -- spark_jobs/gold_to_delta.py spark_jobs/spark_silver_to_delta.py`: no diff. Spark job source changes are fully reverted.

### AWS verification result

- 실제 AWS apply/verification은 수행하지 않음.

## 2026-06-22 — Airflow UI 8080 접근 불가 원인 확인 및 admin CIDR 갱신

### Date

2026-06-22

### Goal

- 현재 PC에서 Terraform 배포 후 public page 접근이 안 되는 원인을 확인하고, 필요한 범위만 조치한다.

### Current phase

- 조치 및 실제 endpoint 검증 완료.

### Findings

- FastAPI/chat public entrypoint는 정상:
  - `http://15.165.237.95/health` → HTTP 200
  - `http://15.165.237.95/` → HTTP 200, `chat.html` 반환
- 접근 불가 대상은 Airflow UI `http://15.165.237.95:8080`.
- 원인: `terraform/envs/dev/terraform.tfvars`의 `admin_airflow_cidrs`가 예시값 `203.0.113.10/32`로 남아 있었고, 현재 PC public IP는 `222.108.125.33`.
- AWS 상태:
  - API EC2, Airflow EC2, front proxy EC2 모두 `running`.
  - SSM ping 모두 `Online`.
  - Airflow 내부 health는 SG 변경 후 public 경로에서 정상 응답.

### Changes made

- `terraform/envs/dev/terraform.tfvars`:
  - `admin_airflow_cidrs = ["203.0.113.10/32"]`
  - → `admin_airflow_cidrs = ["222.108.125.33/32"]`
- AWS Security Group 즉시 조치:
  - front proxy SG `sg-0e48922bb46f2bca0`
    - revoke `203.0.113.10/32:8080`
    - authorize `222.108.125.33/32:8080`
  - Airflow SG `sg-0fef853375dd349d4`
    - revoke `203.0.113.10/32:8080`
    - authorize `222.108.125.33/32:8080`

### Commands run

- `aws sts get-caller-identity`
- `aws ec2 describe-instances`
- `aws ssm describe-instance-information`
- `aws ec2 describe-security-groups`
- `aws ec2 revoke-security-group-ingress`
- `aws ec2 authorize-security-group-ingress`
- `Invoke-WebRequest http://15.165.237.95/health`
- `Invoke-WebRequest http://15.165.237.95/`
- `Invoke-WebRequest http://15.165.237.95:8080/health`

### Validation result

- `http://15.165.237.95:8080/health` → HTTP 200.
- Airflow health response:
  - metadatabase: healthy
  - scheduler: healthy
  - triggerer: healthy
- `http://15.165.237.95:8080/` → HTTP 302 to `/home`, Airflow UI route reachable.

### AWS verification result

- front proxy SG 8080 ingress now includes `222.108.125.33/32`.
- Airflow SG 8080 direct admin ingress now includes `222.108.125.33/32`.
- Airflow SG still allows 8080 from front proxy SG.

### Notes / Remaining

- Terraform CLI is not available on this shell's PATH, so `terraform plan/apply` was not run here.
- `terraform.tfvars` is gitignored local configuration. If the public IP changes again, update `admin_airflow_cidrs` and re-apply, or repeat the SG update.
- The immediate AWS CLI SG update may be reconciled by the next Terraform run. The local `terraform.tfvars` value has been updated to the same CIDR to keep intent aligned.

## 2026-06-22 — Airflow 로그인 redirect 포트 누락 수정 및 FastAPI 채팅 검증

### Date

2026-06-22

### Goal

- Airflow UI 로그인 후 접속 실패 원인을 확인하고 조치한다.
- FastAPI 채팅 페이지가 실제로 접근/호출 가능한지 확인한다.

### Current phase

- 조치 및 public endpoint 재검증 완료.

### Findings

- Airflow UI 자체는 `http://15.165.237.95:8080/login/`에서 HTTP 200으로 열렸다.
- 로그인 전 `/home` 접근 redirect가 다음처럼 생성되어 있었다.
  - 수정 전: `/login/?next=http%3A%2F%2F15.165.237.95%2Fhome`
  - 문제: public Airflow 포트 `:8080`이 빠져 로그인 후 `http://15.165.237.95/home`으로 이동할 수 있음.
- 원인: front proxy nginx가 `proxy_set_header Host $host;`로 upstream에 전달하고 있었다. nginx `$host`는 포트가 제거될 수 있어 Airflow가 외부 URL을 잘못 계산했다.
- Apache Airflow 공식 reverse proxy 문서는 nginx 예시에서 `proxy_set_header Host $http_host;`, `X-Forwarded-For`, `X-Forwarded-Proto` 전달을 제시한다. 현재 수정은 이 방향에 맞춘 것이다.
- FastAPI/chat public endpoint는 정상:
  - `http://15.165.237.95/` → HTTP 200, `chat.html` 반환
  - `http://15.165.237.95/health` → HTTP 200
  - `POST http://15.165.237.95/chat` → HTTP 200, JSON 응답 반환
- `POST http://15.165.237.95/api/chat`는 HTTP 404가 맞다. 현재 `server/chat.html`은 `/chat`을 호출한다.

### Changes made

- `terraform/modules/compute/user_data/front_proxy.sh.tftpl`
  - API proxy와 Airflow proxy 모두 `Host $host`를 `Host $http_host`로 변경.
  - `X-Forwarded-Host $http_host`, `X-Forwarded-Port $server_port` 추가.
- 실행 중인 front proxy EC2 `i-0de3979e28efab599`에 SSM으로 동일 nginx 설정 hotfix 적용.
  - `/etc/nginx/conf.d/opik-airflow.conf` 백업 생성.
  - `/etc/nginx/conf.d/opik-api.conf`도 Terraform 템플릿과 맞게 동일 proxy header 설정으로 갱신.
  - `nginx -t` 성공 후 `systemctl reload nginx`.

### Commands run

- `Invoke-WebRequest http://15.165.237.95/`
- `Invoke-WebRequest http://15.165.237.95/health`
- `Invoke-WebRequest -Method POST http://15.165.237.95/chat`
- `curl.exe -i -L http://15.165.237.95:8080/`
- `curl.exe -i http://15.165.237.95:8080/login/`
- `aws ssm send-command` for front proxy nginx hotfix
- `aws ssm get-command-invocation`
- Airflow login POST 검증: Secrets Manager의 기존 관리자 계정을 프로세스 내부 변수로만 사용, 값 미출력

### Validation result

- SSM hotfix command status: `Success`.
- `nginx -t`: successful.
- Airflow redirect 재검증:
  - 수정 후: `/login/?next=http%3A%2F%2F15.165.237.95%3A8080%2Fhome`
  - `:8080` 유지 확인.
- Airflow 로그인 POST:
  - HTTP 302
  - `Location: http://15.165.237.95:8080/home`
  - 로그인 세션으로 `/home` 접근 시 HTTP 200 및 인증된 Airflow UI content 확인.
- FastAPI 채팅:
  - `/chat` POST HTTP 200 및 JSON 응답 확인.
  - API proxy header 갱신 후에도 `/`, `/health`, `/chat` 모두 HTTP 200 확인.

### AWS verification result

- front proxy EC2 `i-0de3979e28efab599`: SSM Online, nginx reload 성공.
- API EC2와 Airflow EC2는 기존 health 경로 정상.

### Notes / Remaining

- Terraform CLI가 현재 PC PATH에 없어 `terraform fmt/plan/apply`는 실행하지 못했다.
- Terraform 템플릿은 수정했지만, 현재 EC2에는 SSM hotfix로 먼저 반영했다. 추후 Terraform apply 시 front proxy user-data 변경으로 인스턴스 교체가 발생할 수 있다.
- FastAPI 채팅 URL은 `/chat`이다. `/api/chat`는 현재 서버 코드 기준으로 존재하지 않는다.

## 2026-06-22 — FastAPI S3 env 확인 및 FAISS rebuild 저장 누락 수정

### Date

2026-06-22

### Goal

- FastAPI 내부 S3 관련 환경변수를 확인하고, S3 embedding 데이터를 찾지 못하는 원인을 확인/조치한다.

### Current phase

- 원인 수정 및 public endpoint 검증 완료.

### Findings

- FastAPI EC2 `i-044977c222032e407`의 `/opt/opik/bootstrap.env`와 실제 `uvicorn` 프로세스 env는 동일했다.
- 확인된 S3/embedding 관련 env:
  - `AWS_REGION=ap-northeast-2`
  - `AWS_DEFAULT_REGION=ap-northeast-2`
  - `S3_REGION=ap-northeast-2`
  - `S3_BUCKET=s3-opik-bucket`
  - `S3_BASE_PREFIX=`
  - `STORAGE_BACKEND=auto`
  - `BRONZE_PREFIX=bronze/dart`
  - `SILVER_PREFIX=silver/dart`
  - `GOLD_PREFIX=gold/dart`
  - `EMBEDDING_PROVIDER=e5`
  - `EMBEDDING_MODEL=intfloat/multilingual-e5-small`
  - `EMBEDDING_VERSION=v1`
  - `EMBEDDING_DIMENSION=384`
  - `FAISS_INDEX_PATH=/data/opik/faiss_index.bin`
  - `FAISS_IDMAP_PATH=/data/opik/report_ids.json`
- S3에는 FastAPI가 읽는 prefix `gold/embeddings/`에 parquet 66개, 약 126MB가 존재했다.
- 문제 원인은 env가 아니라 `server/opik_server.py`의 `build_index_from_s3()` 구현 누락이었다.
  - S3에서 parquet를 찾고 embedding 배열을 모으는 부분까지는 실행됐다.
  - 이후 FAISS index 생성, `/data/opik/faiss_index.bin` 저장, `report_ids.json` 저장, global `faiss_index` 갱신, `return n` 코드가 비어 있었다.
  - 그 결과 `/index/rebuild`가 HTTP 200으로 끝나도 `/index/status`는 계속 `ready=false`, `size=0`이었다.

### Changes made

- `server/opik_server.py`
  - `build_index_from_s3()` 끝부분에 FAISS `IndexIDMap(IndexFlatIP)` 생성 로직 추가.
  - `FAISS_INDEX_PATH`, `FAISS_IDMAP_PATH`, `/data/opik/report_info.json` 저장 로직 추가.
  - global `faiss_index`, `report_ids` 갱신 및 `return n` 추가.
- 실행 중인 API EC2 `i-044977c222032e407`에도 SSM으로 동일 hotfix 적용.
  - `/opt/opik/repo/server/opik_server.py` 백업 생성.
  - `py_compile` 후 `opik-server` 재시작.
  - startup rebuild로 S3 `gold/embeddings/`에서 FAISS index 생성.

### Commands run

- `aws ssm send-command` / `aws ssm get-command-invocation`
- `aws s3api list-objects-v2 --bucket s3-opik-bucket --prefix gold/embeddings/`
- EC2 내부:
  - filtered `/opt/opik/bootstrap.env` 확인
  - filtered `/proc/<uvicorn-pid>/environ` 확인
  - `systemctl cat opik-server`
  - `curl http://127.0.0.1:8000/index/status`
  - `curl http://127.0.0.1:8000/health`
  - `systemctl restart opik-server`
- Public endpoint:
  - `GET http://15.165.237.95/index/status`
  - `GET http://15.165.237.95/health`
  - `POST http://15.165.237.95/search`

### Validation result

- 수정 전:
  - `/data/opik`에 `faiss_index.bin`, `report_ids.json` 없음.
  - `/health` → `index_size=0`
  - `/index/status` → `ready=false`, `size=0`
- 수정 후:
  - API 로그: `FAISS index built from S3: 51646 vectors, dim=384`
  - `/data/opik/faiss_index.bin` 생성: 77MB
  - `/data/opik/report_ids.json` 생성: 1.2MB
  - `/data/opik/report_info.json` 생성: 27MB
  - Public `/index/status` → `{"ready":true,"size":51646,"dim":384}`
  - Public `/health` → `index_size=51646`
  - Public `/search` → HTTP 200, 검색 결과 반환

### AWS verification result

- API EC2 `i-044977c222032e407`: `opik-server` active.
- S3 `s3-opik-bucket/gold/embeddings/`: parquet 66개 확인.
- front proxy는 현재 API private IP `10.20.11.37:8000`으로 proxy 중임을 확인.

### Notes / Remaining

- 현재 API EC2에는 SSM hotfix로 즉시 반영했다. 인스턴스가 교체되면 Git/배포 코드 기준으로 다시 적용되므로 `server/opik_server.py` 변경을 commit/push해야 유지된다.
- `GOLD_PREFIX=gold/dart`는 DART/storage 계층용 env이고, 현재 FastAPI FAISS rebuild 코드는 `gold/embeddings/`를 하드코딩으로 읽는다. 이 동작은 현재 코드 기준 확인 내용이다.
- 검색 결과의 일부 한글 필드명이 깨져 보이는 현상은 별도 인코딩/컬럼명 정리 이슈로 보인다. 이번 조치는 FAISS index 적재/검색 가능 상태 복구에 한정했다.
