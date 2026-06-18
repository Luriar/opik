# OPIK — AI 한국 주식 브리핑 시스템

증권사 리포트·DART 공시 데이터를 수집·가공하여 AI 기반 일일 브리핑과 종목 추천을 제공하는 시스템입니다. 뉴스 같은 불확실한 데이터를 배제하고, 확실한 '공시'와 '리포트'에 집중하여 가치를 검증합니다.

## 로드맵

| Phase | 내용 | 상태 |
|-------|------|------|
| **Phase 1** | 증권사 리포트 수집 → 텍스트 추출 → 정규식 구조화 → 카카오톡 브리핑 | 완료 |
| **Phase 2** | EC2/Airflow/Spark/Delta Lake → LLM Gold → 스코어링 → 텔레그램 자동 브리핑 → RAG 양방향 QA 챗봇 | 설계 완료, 브리핑·DAG·Spark·RAG 구현 중 |
| **Phase 3** | 실시간 모니터링, 즉시 스코어링, 선제적 푸시 알림 | 예정 |

## 아키텍처

```
Bronze (S3 PDF 원본)  →  Silver (S3 JSON 텍스트)  →  Gold (Delta Lake 구조화 + LLM)
        ↑                         ↑                            ↓
   네이버 금융               PyMuPDF 추출              Spark 3.5 + Delta Lake
   한국투자증권              99.99% 성공률              투자의견·목표주가·종목코드
   DART 공시                                         → 종합 스코어링 → 텔레그램
```

## 현재 성과 (Phase 1)

- **수집**: 네이버 31개 증권사 ~37,000건, 한국투자증권 자체 사이트 ~30,000건 (2020~2026)
- **텍스트 추출**: 51,294건 PDF → 텍스트 변환 완료 (PyMuPDF, 99.99% 성공)
- **구조화 추출** (정규식 기반, 비용 제로):
  - 투자의견 90.8% 정확도
  - 목표주가 75.0% 추출률 (9-layer false positive 방어, 오탐율 0.044% 이하)
  - 종목코드 87.5% 추출률
- **텔레그램 브리핑**: 일일 브리핑 HTML 포맷 전송 완료

## Phase 2 설계 요약

| 구성요소 | 선택 |
|----------|------|
| 컴퓨팅 | EC2 r6g.large (ARM64, 16GB) |
| 배치 처리 | Apache Spark 3.5 (local mode) + Delta Lake 3.0 |
| 워크플로우 | Airflow 2.10.0, Bronze/Silver/Gold 일배치 DAG |
| LLM 추출 | Claude Haiku (건당 $0.005, reason/risks/keywords) |
| 스코어링 | 3-way 종합 점수 = 0.4a(주가) + 0.3b(공시) + 0.3c(리포트) |
| QA 챗봇 | RAG: 임베딩(384d) 유사도 검색 + Haiku 응답 생성 |
| 전달 | Telegram Bot (@Luriarbot) HTML 브리핑 + 대화형 QA |
| 월 운영비 | 약 8.5만원 (EC2 예약 + S3 + Haiku API) |

## 팀

| 팀원 | 담당 | 데이터 소스 |
|------|------|-----------|
| **박찬호** | 주가 예측 모델 | 주가 데이터 |
| **신상용** | DART 공시 수집·분석 | DART Open API |
| **윤준호 + 강태주** | 증권사 리포트 수집·가공 | 네이버 금융, 증권사 자체 사이트 |

## 현재 Airflow DAG 구성

`taeju-airflow` 브랜치 기준으로 증권사 리포트 파이프라인은 일배치 DAG 형태로 전환되어 있습니다.

| DAG | 파일 | 역할 | 기본 스케줄 |
|-----|------|------|-------------|
| `opik_bronze_naver` | `dags/bronze/upload_naver.py` | 네이버 금융 경유 증권사 리포트 PDF를 S3 `bronze/`에 적재 | 매일 00:00 KST |
| `opik_bronze_koreainvest` | `dags/bronze/upload_koreainvest.py` | 한국투자증권 사이트 리포트 PDF를 S3 `bronze/`에 적재 | 매일 00:00 KST |
| `opik_bronze_shinhaninvest` | `dags/bronze/upload_shinhaninvest.py` | 신한투자증권 API 리포트 PDF를 S3 `bronze/`에 적재 | 매일 00:00 KST |
| `opik_silver_extract` | `dags/silver/silver.py` | 세 Bronze 완료 후 PDF를 S3 `silver/` JSON으로 변환 | 매일 00:00 KST, sensor 대기 |
| `opik_gold_structured` | `dags/gold/structured.py` | Silver 완료 후 투자지표를 월별 `gold/structured` Parquet에 upsert | 매일 00:00 KST, sensor 대기 |
| `opik_gold_embeddings` | `dags/gold/embedding.py` | Silver 완료 후 Haiku/E5 결과를 월별 `gold/embeddings` Parquet에 upsert | 매일 00:00 KST, sensor 대기 |

실행 의존성:

```text
opik_bronze_naver ─────────┐
opik_bronze_koreainvest ───┼─> opik_silver_extract ─┬─> opik_gold_structured
opik_bronze_shinhaninvest ─┘                         └─> opik_gold_embeddings
```

공통 설정:

- 모든 DAG는 `catchup=False`, `max_active_runs=1` 기준입니다.
- 다섯 DAG는 `OPIK_REPORT_PIPELINE_SCHEDULE`을 공유합니다. 기본값은 `0 0 * * *`입니다.
- Silver의 세 `ExternalTaskSensor`가 같은 target date의 Bronze task 성공을 모두 기다립니다.
- 각 Gold DAG의 `ExternalTaskSensor`가 같은 target date의 Silver task 성공을 기다립니다.
- 센서는 worker slot을 점유하지 않는 `reschedule` 모드이며 60초 간격, 최대 6시간 대기합니다.
- target date는 `data_interval_end`를 `Asia/Seoul`로 변환한 뒤 하루를 뺀 날짜입니다. 6월 18일 자정 실행은 발행이 끝난 6월 17일 데이터를 처리합니다.
- Silver는 해당 날짜의 `bronze/{증권사}/{YYYY-MM-DD}/_manifest.json`만 읽습니다.
- Gold는 해당 날짜의 Silver JSON을 기존 월 Parquet와 `report_id` 기준으로 upsert하므로 재실행해도 중복되지 않습니다.
- Gold 출력은 `gold/structured/year=YYYY/month=MM/data.parquet`, `gold/embeddings/year=YYYY/month=MM/data.parquet`입니다.
- 한국투자증권은 사이트 보안/세션 특성이 있어 일배치 순회 제한을 둡니다.
  - `KOREAINVEST_DAILY_PAGE_BATCH`: 기본 `5`
  - `KOREAINVEST_DAILY_MAX_PAGES`: 기본 `50`

기존 `{{ ds }}`는 Airflow logical date의 UTC 날짜였습니다. 자정 이후 예약 실행은 이전 data interval의 시작 시각을 logical date로 사용하므로 6월 18일 새벽 실행이 6월 16일로 렌더링될 수 있었고, 오후 수동 실행은 현재 날짜를 사용해 결과가 달라졌습니다. 현재는 KST `data_interval_end - 1일`을 사용하므로 6월 18일 중 예약·수동 실행 모두 6월 17일을 처리합니다.

## EC2 / Docker Airflow 운영 인수인계

### 1. DAG 파일 배치

EC2 Airflow 컨테이너는 `/opt/airflow/dags` 아래의 파일을 파싱합니다. 로컬 repo는 `dags/bronze`, `dags/silver` 하위 구조를 쓰지만, EC2에서는 필요에 따라 파일을 flat하게 둘 수 있습니다.

예시:

```text
/opt/airflow/dags/
├── upload_naver.py
├── upload_koreainvest.py
├── upload_shinhaninvest.py
├── silver.py
├── structured.py
└── embedding.py
```

파일을 바꾼 뒤에는 scheduler/worker가 새 파일을 읽도록 재시작하는 것이 가장 확실합니다.

```bash
docker compose restart airflow-scheduler airflow-worker
```

### 2. 환경변수

AWS 자격증명은 EC2 IAM Role 또는 컨테이너 환경변수 중 하나로 공급합니다. `.env` 파일에는 비밀값을 커밋하지 않습니다.

필수/권장 환경변수:

```env
S3_BUCKET=s3-opik-bucket
S3_REGION=ap-northeast-2
AWS_REGION=ap-northeast-2
AWS_DEFAULT_REGION=ap-northeast-2

# EC2 IAM Role을 쓰지 않는 경우에만 필요
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...

# Airflow UI에서 worker task log 403/JWT 에러 방지용
AIRFLOW__WEBSERVER__SECRET_KEY=opik-airflow-fixed-secret-key-2026

# Gold/LLM 작업에서 필요
BEDROCK_REGION=ap-northeast-2
BEDROCK_API_KEY=...
BEDROCK_LLM_MODEL_ID=anthropic.claude-3-haiku-20240307-v1:0

# 다섯 DAG가 반드시 같은 값을 공유해야 함
OPIK_REPORT_PIPELINE_SCHEDULE="0 0 * * *"
```

`AIRFLOW__WEBSERVER__SECRET_KEY`는 webserver/scheduler/worker에 동일하게 주입되어야 합니다. 값이 다르면 UI에서 task log를 열 때 다음과 같은 에러가 날 수 있습니다.

```text
Could not read served logs: 403 Client Error: FORBIDDEN
jwt.exceptions.InvalidSignatureError: Signature verification failed
```

이 에러는 증권사 사이트 차단이 아니라 Airflow 내부 log server 인증 문제입니다.

### 3. Airflow 이미지와 Python 패키지

Airflow 컨테이너에서 DAG를 파싱/실행하려면 `requirements.txt`의 패키지가 설치되어 있어야 합니다.

주요 패키지:

- `boto3`: S3 접근
- `requests`: 신한투자증권 API/PDF 다운로드
- `aiohttp`: 네이버/한국투자증권 비동기 HTTP
- `beautifulsoup4`: 네이버/한국투자증권 HTML 파싱
- `pymupdf`: Silver PDF 텍스트 추출 (`fitz`)
- `pyarrow`, `sentence-transformers`: Gold/embedding 작업
- `pendulum`: Airflow DAG timezone-aware `start_date`

운영 서버에서는 실행 중인 컨테이너에 직접 `pip install`하지 않습니다. 해당 변경은 컨테이너 재생성 시 사라지고, 범위 버전 설치는 Airflow의 Celery/Click 의존성을 깨뜨릴 수 있습니다. 저장소의 `Dockerfile.airflow`로 공통 이미지를 빌드합니다.

`docker-compose.yaml`과 같은 경로에 `Dockerfile.airflow`, `requirements.txt`를 둔 뒤 공통 설정을 다음과 같이 지정합니다.

```yaml
x-airflow-common:
  &airflow-common
  image: opik-airflow:2.10.0
  build:
    context: .
    dockerfile: Dockerfile.airflow
```

최초 배포 또는 requirements 변경 시:

```bash
docker compose build --no-cache
docker compose up -d --force-recreate
```

모든 Airflow 서비스가 같은 이미지와 패키지 버전을 사용해야 합니다. worker만 별도 설치하면 scheduler와 실행 환경이 달라져 DAG 파싱 또는 task 실행 결과가 달라질 수 있습니다.

핵심 버전은 다음과 같이 고정되어 있습니다.

```text
apache-airflow                  2.10.0
apache-airflow-providers-celery 3.7.3
celery                         5.4.0
click                          8.1.7
sentence-transformers          3.0.1
transformers                   4.41.2
huggingface-hub                0.23.4
```

최신 `sentence-transformers`/`huggingface-hub`를 제한 없이 설치하면 `click`이 8.4 이상으로 올라갈 수 있습니다. Airflow 2.10.0의 Celery worker는 이 조합에서 hostname을 `None`으로 전달받아 다음 오류로 재시작됩니다.

```text
AttributeError: 'NoneType' object has no attribute 'split'
Error: No nodes replied within time constraint
```

버전과 worker 상태 확인:

```bash
docker compose exec -T airflow-scheduler python - <<'PY'
from importlib.metadata import version

for name in [
    "apache-airflow",
    "apache-airflow-providers-celery",
    "celery",
    "click",
    "kombu",
    "billiard",
]:
    print(name, version(name))
PY

docker compose exec airflow-worker \
  celery --app airflow.providers.celery.executors.celery_executor.app \
  inspect ping --timeout=10
```

정상 worker는 `pong`을 반환합니다.

이미 실행 중인 서버에서 위 Click 오류가 발생한 경우에만 임시 복구합니다.

```bash
docker compose exec --user airflow airflow-scheduler \
  python -m pip install --no-cache-dir "click==8.1.7"

until docker compose exec --user airflow airflow-worker \
  python -m pip install --no-cache-dir "click==8.1.7"
do
  sleep 2
done

docker compose restart airflow-scheduler airflow-worker
```

이 방식은 컨테이너 writable layer만 수정하므로 영구 배포 방법이 아닙니다. 복구 후 반드시 커스텀 이미지를 다시 빌드합니다.

패키지 import 확인:

```bash
docker compose exec airflow-worker python -c "import fitz, bs4, aiohttp, boto3, pendulum; print('ok')"
docker compose exec airflow-scheduler python -c "import fitz, bs4, aiohttp, boto3, pendulum; print('ok')"
docker compose exec airflow-worker python -c "import pyarrow, sentence_transformers; print('gold packages ok')"
```

### 4. DAG 등록 확인

```bash
docker compose exec airflow-scheduler airflow dags list | grep opik
docker compose exec airflow-scheduler airflow dags list-import-errors
```

UI에서 `No results`가 보이면 import error가 아니라 scheduler 갱신 지연일 수 있습니다. 잠시 기다리거나 scheduler를 재시작합니다.

```bash
docker compose restart airflow-scheduler
```

### 5. Task 로그 확인

UI log가 403으로 안 열릴 때는 worker 컨테이너 로그 파일을 직접 확인합니다.

```bash
docker compose exec airflow-worker bash -lc '
find /opt/airflow/logs/dag_id=opik_bronze_koreainvest -type f -name "*.log" -print | sort
'
```

최신 로그 tail:

```bash
docker compose exec airflow-worker bash -lc '
tail -n 200 "$(find /opt/airflow/logs/dag_id=opik_bronze_koreainvest -type f -name "*.log" | sort | tail -1)"
'
```

실시간 worker 로그:

```bash
docker compose logs -f airflow-worker | grep -Ei "koreainvest|한국투자|naver|shinhan|manifest|error|failed|traceback|done|uploaded"
```

## 파일 구조

```
opik/
├── dags/
│   ├── bronze/
│   │   ├── upload_naver.py             # 네이버 금융 → S3 Bronze
│   │   ├── upload_koreainvest.py       # 한국투자증권 → S3 Bronze
│   │   └── upload_shinhaninvest.py     # 신한투자증권 → S3 Bronze
│   ├── silver/
│   │   └── silver.py                   # Bronze PDF → Silver JSON
│   └── gold/
│       ├── embedding.py                # Silver → Haiku/E5 → 월별 Gold embeddings DAG
│       ├── structured.py               # Silver → 정규식 → 월별 Gold structured DAG
│       └── extract_gold_llm.py         # 이전 백필용 스크립트(DAG 미등록)
├── docs/                               # 설계/작업 문서
├── Dockerfile.airflow                  # Airflow 2.10.0 공통 실행 이미지
├── .dockerignore                       # 이미지 빌드 시 비밀값/불필요 파일 제외
├── opik_config.py                      # 로컬 실행용 legacy config
├── opik_s3.py                          # 로컬 실행용 legacy S3 helper
└── requirements.txt                    # Airflow 호환 버전으로 고정된 DAG 의존성
```

## 개발 접근법

Build → Validate → Refine → Document → Design Next

일단 데이터를 만지고, 거기서 배운 걸로 설계합니다. 자세한 내용은 [DEVELOPMENT_LOG.md](DEVELOPMENT_LOG.md) 참고.

## 라이선스

MIT
