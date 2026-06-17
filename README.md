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
| 워크플로우 | Airflow 2.8, 8-task nightly DAG (16:00 KST) |
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
| `opik_bronze_naver` | `dags/bronze/upload_naver.py` | 네이버 금융 경유 증권사 리포트 PDF를 S3 `bronze/`에 적재 | 매일 01:50 KST |
| `opik_bronze_koreainvest` | `dags/bronze/upload_koreainvest.py` | 한국투자증권 사이트 리포트 PDF를 S3 `bronze/`에 적재 | 매일 01:50 KST |
| `opik_bronze_shinhaninvest` | `dags/bronze/upload_shinhaninvest.py` | 신한투자증권 API 리포트 PDF를 S3 `bronze/`에 적재 | 매일 01:50 KST |
| `opik_silver_extract` | `dags/silver/extract_silver.py` | `bronze/` PDF를 PyMuPDF로 텍스트 추출해 S3 `silver/` JSON으로 적재 | 기본 매일 02:30 KST |

공통 설정:

- 모든 DAG는 `catchup=False`, `max_active_runs=1` 기준입니다.
- Bronze DAG는 `target_date="{{ ds }}"` 하루치만 수집합니다.
- Silver DAG는 같은 `{{ ds }}` 날짜의 `bronze/{증권사}/{YYYY-MM-DD}/_manifest.json`만 읽습니다.
- Silver 스케줄은 `SILVER_SCHEDULE` 환경변수로 override할 수 있습니다. 기본값은 `30 2 * * *`입니다.
- 한국투자증권은 사이트 보안/세션 특성이 있어 일배치 순회 제한을 둡니다.
  - `KOREAINVEST_DAILY_PAGE_BATCH`: 기본 `5`
  - `KOREAINVEST_DAILY_MAX_PAGES`: 기본 `50`

## EC2 / Docker Airflow 운영 인수인계

### 1. DAG 파일 배치

EC2 Airflow 컨테이너는 `/opt/airflow/dags` 아래의 파일을 파싱합니다. 로컬 repo는 `dags/bronze`, `dags/silver` 하위 구조를 쓰지만, EC2에서는 필요에 따라 파일을 flat하게 둘 수 있습니다.

예시:

```text
/opt/airflow/dags/
├── upload_naver.py
├── upload_koreainvest.py
├── upload_shinhaninvest.py
└── silver.py
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
```

`AIRFLOW__WEBSERVER__SECRET_KEY`는 webserver/scheduler/worker에 동일하게 주입되어야 합니다. 값이 다르면 UI에서 task log를 열 때 다음과 같은 에러가 날 수 있습니다.

```text
Could not read served logs: 403 Client Error: FORBIDDEN
jwt.exceptions.InvalidSignatureError: Signature verification failed
```

이 에러는 증권사 사이트 차단이 아니라 Airflow 내부 log server 인증 문제입니다.

### 3. Python 패키지 설치

Airflow 컨테이너에서 DAG를 파싱/실행하려면 `requirements.txt`의 패키지가 설치되어 있어야 합니다.

주요 패키지:

- `boto3`: S3 접근
- `requests`: 신한투자증권 API/PDF 다운로드
- `aiohttp`: 네이버/한국투자증권 비동기 HTTP
- `beautifulsoup4`: 네이버/한국투자증권 HTML 파싱
- `pymupdf`: Silver PDF 텍스트 추출 (`fitz`)
- `pyarrow`, `sentence-transformers`: Gold/embedding 작업
- `pendulum`: Airflow DAG timezone-aware `start_date`

EC2 host의 `requirements.txt`가 컨테이너에 마운트되어 있지 않다면 먼저 복사합니다.

```bash
docker compose cp requirements.txt airflow-worker:/opt/airflow/requirements.txt
docker compose cp requirements.txt airflow-scheduler:/opt/airflow/requirements.txt
docker compose cp requirements.txt airflow-webserver:/opt/airflow/requirements.txt
```

설치:

```bash
docker compose exec --user airflow airflow-worker python -m pip install -r /opt/airflow/requirements.txt
docker compose exec --user airflow airflow-scheduler python -m pip install -r /opt/airflow/requirements.txt
docker compose exec --user airflow airflow-webserver python -m pip install -r /opt/airflow/requirements.txt
```

`--user` 옵션은 사용하지 않습니다. 현재 Airflow 컨테이너는 virtualenv 환경이라 `--user` 설치가 막힙니다.

권한 문제가 나면 root로 설치합니다.

```bash
docker compose exec --user root airflow-worker python -m pip install -r /opt/airflow/requirements.txt
docker compose exec --user root airflow-scheduler python -m pip install -r /opt/airflow/requirements.txt
docker compose exec --user root airflow-webserver python -m pip install -r /opt/airflow/requirements.txt
```

설치 확인:

```bash
docker compose exec airflow-worker python -c "import fitz, bs4, aiohttp, boto3, pendulum; print('ok')"
docker compose exec airflow-scheduler python -c "import fitz, bs4, aiohttp, boto3, pendulum; print('ok')"
```

설치 후 재시작:

```bash
docker compose restart airflow-webserver airflow-scheduler airflow-worker
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
│   │   └── extract_silver.py           # Bronze PDF → Silver JSON
│   └── gold/
│       ├── embedding.py                # Silver/embedding_input → Gold embeddings
│       ├── extract_gold_llm.py         # LLM 기반 reason/risks/keywords 추출
│       └── extract_gold_structured.py  # 정규식 기반 구조화 Gold
├── docs/                               # 설계/작업 문서
├── opik_config.py                      # 로컬 실행용 legacy config
├── opik_s3.py                          # 로컬 실행용 legacy S3 helper
└── requirements.txt
```

## 개발 접근법

Build → Validate → Refine → Document → Design Next

일단 데이터를 만지고, 거기서 배운 걸로 설계합니다. 자세한 내용은 [DEVELOPMENT_LOG.md](DEVELOPMENT_LOG.md) 참고.

## 라이선스

MIT
