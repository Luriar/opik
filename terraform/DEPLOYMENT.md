# OPIK Terraform 배포 가이드 (민감정보 배치 기준)

다른 컴퓨터에서 이 코드로 Terraform 배포를 진행할 때, **어떤 정보를 / 어디에** 두어야
하는지 정리한 문서다. 핵심 원칙은 하나다.

> **비밀값(secret)은 Terraform 코드·tfvars·Git에 절대 넣지 않는다.**
> AWS 자격증명은 EC2의 IAM Role이, 애플리케이션 비밀값은 Secrets Manager가 제공한다.

리포지토리 루트의 `.env`는 **로컬 docker-compose 개발 전용**이며, AWS 배포 시 Terraform이
읽지 않는다. `.env`의 키들은 아래 분류에 따라 각자 다른 위치로 흩어진다.

---

## 1. 값의 3가지 분류와 배치 위치

| 분류 | 어디에 두나 | Git 커밋 |
|------|-------------|----------|
| ① 인프라/앱 **비밀값** (DART 키, Bedrock 키, Airflow 키, 관리자 비번) | **AWS Secrets Manager** 런타임 컨테이너 (apply 후 수동 입력) | ❌ |
| ② **DB 접속 정보** (RDS user/password) | **RDS 관리형 시크릿** (Terraform이 자동 생성, 값은 AWS가 관리) | ❌ |
| ③ AWS 자격증명 (Access Key / Secret Key) | **불필요.** EC2 IAM Role로 대체 (배포 운영자 본인 자격증명은 `aws configure`) | ❌ |
| ④ **비밀이 아닌 앱 설정** (S3 prefix, 저장계층 prefix, DART 튜닝, 임베딩, FastAPI/RAG 모델·경로·batch tuning) | `terraform.tfvars` (예시: `terraform.tfvars.example`) | ⚠️ `.example`만 |
| ⑤ Airflow 내부 설정 (executor, UID, fernet, timezone, DB URL) | Terraform/부트스트랩이 자동 처리 — **건드릴 필요 없음** | — |

---

## 2. `.env` 키 → 배치 위치 매핑

루트 `.env`의 각 키가 배포 시 어디로 가는지:

### 비밀값 → Secrets Manager `opik-dev/airflow/runtime` (apply 후 입력)
| `.env` 키 | 비고 |
|-----------|------|
| `DART_API_KEYS` | 콤마 구분 다중 키. **절대 tfvars/Git 금지** |
| (선택) `BEDROCK_API_KEY` | Bedrock을 API 키로 쓸 때만 |
| (선택) `BEDROCK_LLM_MODEL_ID` | 미입력 시 `anthropic.claude-3-haiku-20240307-v1:0` |
| (선택) `AIRFLOW__CORE__FERNET_KEY` | 미입력 시 EC2에서 생성. **운영은 고정 권장** |
| (선택) `AIRFLOW__WEBSERVER__SECRET_KEY` | 미입력 시 EC2에서 생성 |
| (선택) `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` | 미입력 시 `airflow` / 랜덤 생성 |
| (선택) `OPIK_REPORT_PIPELINE_SCHEDULE` | 미입력 시 `0 0 * * *` |

> 루트 `.env`의 `AIRFLOW_SECRET_KEY`는 위 `AIRFLOW__WEBSERVER__SECRET_KEY`에 해당한다.

### DB 접속 → RDS 관리형 시크릿 (자동, 입력 불필요)
| `.env` 키 | 비고 |
|-----------|------|
| `AIRFLOW_DB_URL` | RDS `airflow` + 관리형 시크릿에서 부트스트랩이 URL 조립 |
| `SERVICE_DB_URL` | RDS `opik_service` + 관리형 시크릿 |
| `VECTOR_DB_URL` | 현재 인프라는 pgvector 미분리 — 필요 시 service DB 재사용 |

### AWS 자격증명 → 불필요 (IAM Role)
| `.env` 키 | 비고 |
|-----------|------|
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | **사용 안 함.** EC2 IAM Role이 S3/Bedrock/Secrets 권한 제공 |
| `AWS_REGION` | `terraform.tfvars`의 `aws_region` |

### 비밀이 아닌 앱 설정 → `terraform.tfvars`
| `.env` 키 | tfvars 위치 |
|-----------|-------------|
| `S3_BUCKET` | `data_bucket_name` (null이면 계정/리전 기반 자동 생성) |
| `S3_BASE_PREFIX` | `storage_config.s3_base_prefix` |
| `STORAGE_BACKEND` | `storage_config.storage_backend` |
| `LOCAL_STORAGE_BASE_PATH` | `storage_config.local_storage_base_path` |
| `BRONZE_PREFIX` / `SILVER_PREFIX` / `GOLD_PREFIX` | `storage_config.*_prefix` |
| `DART_BACKFILL_*`, `DART_INCREMENTAL_DAYS`, `DART_LISTED_COMPANY_SCHEDULE`, `DART_COLLECT_MODE`, `DART_*_LIMIT`, `DART_QUOTA_REQUEST_LOG_ENABLED` | `dart_config.*` |
| `EMBEDDING_PROVIDER` / `MODEL` / `VERSION` / `DIMENSION` | `embedding_config.*` |
| `OPIK_AGENT_ENABLED`, `SEARCH_TOP_K`, `OPIK_DB_PATH`, `BEDROCK_MODEL`, agent model IDs, `DART_SENTIMENT_*` | `server_config.*` |
| `PROMPT_DIR`, `CHAT_HTML_PATH`, `FAISS_INDEX_PATH`, `FAISS_IDMAP_PATH` | Terraform API bootstrap 고정 렌더링 |

FastAPI 상세 매핑은 [ENV-MAPPING.md](ENV-MAPPING.md)를 기준으로 본다.

### Airflow 내부 설정 → 자동 (손대지 않음)
`AIRFLOW_UID`, `AIRFLOW_EXECUTOR`, `AIRFLOW_TIMEZONE` 등은 부트스트랩이 고정 렌더링한다.

---

## 3. 새 컴퓨터에서 배포하는 절차

### 사전 준비 (운영자 머신)
1. Terraform ≥ 1.5, AWS CLI v2 설치.
2. **운영자 AWS 자격증명 설정** — tfvars가 아니라 표준 위치에:
   ```bash
   aws configure          # 또는 SSO: aws configure sso
   aws sts get-caller-identity   # 계정 확인
   ```
3. (권장) 원격 상태 백엔드 구성: `envs/dev/backend-s3.tf.example`를 참고해
   상태 버킷 + DynamoDB 잠금 테이블을 만든 뒤 `backend.tf`로 활성화.
   상태 파일에는 계정 ID·ARN·엔드포인트가 들어가므로 암호화된 원격 백엔드를 권장한다.

### 변수 파일 작성
```bash
cd terraform/envs/dev
cp terraform.tfvars.example terraform.tfvars
# terraform.tfvars 편집: 비밀이 아닌 값만! (storage_config / dart_config / embedding_config 등)
```

### 배포
```bash
terraform init
terraform fmt -recursive
terraform validate
terraform plan -out=tfplan
terraform apply tfplan        # 비용·공개노출 확인 후 실행
```

### (새 계정/리전 최초 1회) 시크릿 컨테이너 생성 — apply 전 필수
Terraform은 시크릿 컨테이너를 **data source로 참조만** 한다(생성/삭제하지 않음).
따라서 컨테이너가 없으면 `terraform plan/apply`가 실패한다. 최초 배포 전 1회 생성:

```bash
aws secretsmanager create-secret --region ap-northeast-2 \
  --name opik-dev/airflow/runtime \
  --description "OPIK Airflow runtime secrets" || true
aws secretsmanager create-secret --region ap-northeast-2 \
  --name opik-dev/api/runtime \
  --description "OPIK API runtime secrets" || true
```
> 같은 계정에서 destroy 후 재배포할 때는 컨테이너가 그대로 남아 있으므로 이 단계가 필요 없다.
> (`destroy`는 시크릿을 지우지 않는다 — 5번 참고.)

### 비밀값 주입 (apply 전/후 무관)
컨테이너에 실제 값을 넣는다.

```bash
SECRET_ARN="$(terraform output -raw airflow_secret_container_arn)"
# 또는 apply 전이면: SECRET_ARN=opik-dev/airflow/runtime

aws secretsmanager put-secret-value \
  --region ap-northeast-2 \
  --secret-id "$SECRET_ARN" \
  --secret-string '{
    "DART_API_KEYS": "키1,키2,키3",
    "BEDROCK_LLM_MODEL_ID": "anthropic.claude-3-haiku-20240307-v1:0"
  }'
```
> JSON을 셸 히스토리에 남기지 않으려면 `--secret-string file://secret.json` 사용 후 파일 삭제.

비밀값 갱신 후 Airflow EC2가 재읽도록:
```bash
INSTANCE_ID="$(terraform output -raw airflow_instance_id)"
aws ssm send-command --region ap-northeast-2 --instance-ids "$INSTANCE_ID" \
  --document-name AWS-RunShellScript \
  --parameters 'commands=["systemctl restart opik-airflow"]'
```

API 측 비밀값이 필요하면 `api_secret_container_arn`에 동일 방식으로 입력한다.

---

## 4. 절대 커밋하면 안 되는 것

`terraform/.gitignore`와 루트 `.gitignore`가 차단하지만, 직접 확인:

- `*.tfstate`, `*.tfstate.backup` — 상태 (ARN·계정 ID·엔드포인트 포함)
- `tfplan`, `*.tfplan` — 저장된 plan (민감값 포함 가능)
- `terraform.tfvars`, `*.tfvars` (단 `*.tfvars.example`은 허용)
- `.terraform/`, `*.env`, `.env`
- 비밀값을 평문으로 — tfvars / 변수 default / user-data 리터럴 / outputs / 셸 히스토리 어디에도 금지

커밋 전 점검:
```bash
git status
git diff --cached --name-only | grep -iE 'tfstate|tfplan|\.tfvars$|\.env$' && echo "STOP: 민감파일 staged" || echo "OK"
```

---

## 5. 키/시크릿 저장·조회 흐름과 destroy 보호

### 각 key가 어디에 저장되고 어떻게 조회되나

| key | 저장 위치 | 저장 방법 | 런타임 조회 방법 | destroy 시 |
|-----|-----------|-----------|------------------|------------|
| `DART_API_KEYS` | Secrets Manager `opik-dev/airflow/runtime` | 컨테이너는 1회 `create-secret`, 값은 `put-secret-value`로 입력(Terraform은 data source로 참조만) | Airflow EC2 부팅 시 IAM Role로 `get-secret-value` → `/opt/opik/airflow.env`에 렌더 | **보존**(data source) |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | 〃 | 〃 | 〃 | **보존** |
| `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` (admin) | 〃 | 〃 | 부팅 시 admin 계정 생성/리셋에 사용 | **보존** |
| (선택) `AIRFLOW__CORE__FERNET_KEY`, `BEDROCK_*` | 〃 | 〃(미입력 시 EC2에서 생성/기본값) | 〃 | **보존** |
| API용 비밀 | Secrets Manager `opik-dev/api/runtime` | 〃 | API EC2가 IAM Role로 조회 | **보존**(data source) |
| RDS user/password (airflow, service) | RDS 관리형 시크릿(`rds!db-...`) | RDS가 자동 생성·로테이션(`manage_master_user_password`) | EC2 부팅 시 IAM Role로 `get-secret-value` → DB URL 조립 | RDS 인스턴스와 함께 삭제(주의) |
| AWS 자격증명 | 없음 | — | EC2 IAM Role(인스턴스 프로파일)이 임시 자격증명 제공 | — |
| 비-비밀 앱 설정(S3 prefix/DART 튜닝/embedding) | Terraform 변수 → user-data env 파일 | `terraform.tfvars` | 부팅 시 `/opt/opik/*.env`에 렌더 | 코드/변수라 무관 |

핵심 흐름: **Terraform은 시크릿 "값"을 만들지 않는다.** 빈 컨테이너만 만들고, 값은 운영자가
`put-secret-value`로 넣는다. EC2는 부팅 시 자신의 IAM Role로 Secrets Manager에서 값을 읽어
`/opt/opik/airflow.env`(0600, root) 등에 렌더한 뒤 컨테이너에 주입한다.

### destroy 보호 — S3·Secrets는 destroy 대상이 아님

S3 버킷과 시크릿 컨테이너는 **둘 다 Terraform이 data source로 참조만** 한다(생성/소유/삭제 안 함).
따라서 `terraform destroy`는 이 둘을 **건드리지 않는다**. 별도 플래그·예외 처리 불필요.

- `module.storage.data.aws_s3_bucket.existing` ← `s3-opik-bucket` (`manage_data_bucket=false`)
- `module.compute.data.aws_secretsmanager_secret.airflow_runtime` ← `opik-dev/airflow/runtime`
- `module.compute.data.aws_secretsmanager_secret.api_runtime` ← `opik-dev/api/runtime`

확인:
```bash
terraform plan -destroy -no-color | grep -iE "secretsmanager|s3_bucket"   # 비어 있어야 정상
```

> `terraform destroy`는 VPC/EC2/RDS/IAM/SG 등만 제거하고, S3 데이터와 시크릿 값은 그대로 남는다.
> 같은 계정에서 재배포하면 두 data source가 기존 것을 그대로 참조한다(이름 충돌 없음).

> 참고: 관리형으로 버킷을 만들 경우(`manage_data_bucket=true`)를 대비해
> `aws_s3_bucket.data`에는 `prevent_destroy = true` + `force_destroy = false`도 걸려 있다.

> 주의: **RDS 두 개(서비스/Airflow 메타 DB)는 destroy 대상에 포함**되며 현재 설정상
> 최종 스냅샷 없이 삭제된다(`rds_skip_final_snapshot=true`). DB 데이터를 보존하려면
> `rds_skip_final_snapshot=false`(+`rds_deletion_protection=true`)로 두고 `terraform destroy`를 피한다.

## 6. 참고

- 런타임 로딩 흐름 상세: `envs/dev/README.md` → "Runtime Configuration Loading"
- 클라우드 시크릿 저장이 금지된 환경의 대안(sops/age 등): 같은 README "Local Secret Handling"
- 아키텍처/리소스 개요: `guid.md`, `AGENTS.md`
