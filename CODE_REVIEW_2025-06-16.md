# OPIK 통합 코드 리뷰 — 2026-06-16

세 코드베이스(OPIK, DartCollector, finanace_AI_Agent)의 종합 리뷰입니다.

---

## Critical Issues (즉시 수정 필요)

### 1. `telegram_briefing.py` — `eval()` 코드 인젝션 위험

**_safe_list()에서 `eval(val)` 사용. LLM 출력 파싱을 위해 eval()을 쓰고 있지만, S3 데이터 변조 시 RCE 가능.**

```python
# telegram_briefing.py L184
result = eval(val)  # 위험
```

**수정**: `json.loads(val)` 또는 `ast.literal_eval(val)`로 대체.

### 2. `dags/nightly_batch.py` — `spark-submit -c`가 올바른 명령어가 아님

rollback_delta_on_failure()에서 `subprocess.run(["spark-submit", "-c", rollback_code])`를 호출하는데, spark-submit의 `-c` 옵션은 존재하지 않음. Delta rollback이 실제로 작동하지 않는다.

**수정**: 별도 PythonOperator로 Delta rollback 로직을 분리하거나, `.py` 파일로 저장 후 spark-submit.

### 3. `extract_gold_structured.py` — TP 필터링 조건이 너무 광범위

```python
if len(raw) == 6 and raw.startswith('0'):
    continue  # L139
```

모든 6자리 0-start 숫자를 stock code로 간주하고 목표주가(TP) 후보에서 제외함. 실제 TP가 100,000원(6자리)인 경우 잘못 필터링될 수 있다.

**수정**: `raw == "000000"` 정확히 체크하거나, 6자리 종목코드 범위(0XXXXX) + TP 컨텍스트를 함께 검증.

### 4. `dart_agent/workflows.py` — API 키가 DB 로그에 평문 저장

_log_api_request 호출 시 request_params에 `crtfc_key`가 포함된 채로 `api_request_log` 테이블에 JSONB 저장됨. 감사 로그에 API 키 노출.

**수정**: `_log_api_request` 호출 전 params에서 `crtfc_key` 제거.

### 5. `bronze_to_silver_신한투자증권.py` — boto3 client를 ThreadPoolExecutor에서 공유

boto3 client는 thread-safe하지 않음. 20개 worker가 동일한 s3 객체를 공유하면 ConnectionPool 에러 발생 가능.

**수정**: 각 worker 내에서 `boto3.client("s3")` 새로 생성.

### 6. `embedding.py` — `load_rows()`에 JSON 파싱 실패 예외 처리 없음

`load_json_from_s3()` 실패 시 전체 파이프라인이 중단됨. 5만 건 중 1건만 깨져도 전체 중단.

**수정**: 개별 예외를 catch하고 `skipped["json_error"]`로 카운트 후 continue.

### 7. `extract_한국투자증권.py` / `extract_신한투자증권.py` — report_id 충돌 가능성

```python
report_id = hashlib.md5(f"{BROKER_NAME}_{title}_{reg_date}".encode()).hexdigest()
```

종목코드가 seed에 포함되지 않아, 같은 날 같은 제목의 다른 종목 리포트가 충돌.

**수정**: `f"{BROKER_NAME}_{stock_code}_{title}_{reg_date}"`로 seed 확장.

---

## Medium Issues (설계 개선 필요)

### 인프라/공통

| # | 파일 | 문제 | 수정 방향 |
|---|------|------|----------|
| M1 | 다수 | boto3 client를 8개 파일에서 모듈 레벨로 각각 생성 → import 시 S3 호출, 세션 재사용 불가 | `opik/s3_client.py` 단일 모듈로 통합 |
| M2 | 다수 | 환경변수 기본값 하드코딩 중복 (S3_BUCKET 등 8곳) | `opik/config.py` 공통 설정 모듈 생성 |
| M3 | 다수 | 체크포인트 파일 로직 중복 (upload_naver, upload_koreainvest, extract_silver) | `opik/checkpoint.py` 클래스로 추출 |
| M4 | 전체 | 테스트 코드 0건 | 정규식 검증, TP 추출, JSON 파싱 등 핵심 로직에 pytest 추가 |
| M5 | requirements.txt | 의존성 분산 (embedding/requirements.txt, extract/docstring 내 pip install) | `pyproject.toml`로 통합 |

### OPIK

| # | 파일 | 문제 |
|---|------|------|
| M6 | `extract_silver.py` | `_fallback_extract`가 `_fitz_open_safe`를 사용하지 않아 segfault 가능 |
| M7 | `upload_naver.py` | 스레드 예외 발생 시 해당 청크 전체 중단 (try-except 없음) |
| M8 | `spark_silver_to_delta.py` | `UPDATE SET *` + mergeSchema 미설정으로 컬럼 추가 시 MERGE 실패 |
| M9 | `dags/nightly_batch.py` | deliver_telegram에 trigger_rule 미설정 → compute_recommendations skipped 시 실패 |

### DartCollector

| # | 파일 | 문제 |
|---|------|------|
| M10 | `workflows.py` | `run_bronze_completion`에서 `dict(row)` 2회 중복 호출 |
| M11 | `workflows.py` | `run_silver_incremental`에서 N+1 쿼리 가능성 (done_keys 미전달) |
| M12 | `completion.py` | match 키를 f-string으로 SQL에 직접 연결 — 컬럼명 변경 시 깨짐 |
| M13 | `rate_limiter.py` | FOR UPDATE + 별도 UPDATE가 원자적이지 않음 |

### finanace_AI_Agent

| # | 파일 | 문제 |
|---|------|------|
| M14 | `common.py` | `prompt_date()`가 배치 환경에서 input() 영원히 대기 — Airflow DAG에서 호출 시 문제 |
| M15 | `embedding.py` | Parquet 임시 파일명이 고정값 → 동시 실행 시 충돌 |
| M16 | `common.py` | `BEDROCK_API_KEY` fallback 체인에 `CLUADE` 오타 포함 |
| M17 | `extract_한국투자증권.py` | Playwright browser.close()가 예외 시 누락 → 메모리 누수 |

---

## 코드 중복 분석

### 심각한 중복

1. **extract_한국투자증권 vs extract_신한투자증권**: `normalize_date()`, `make_report_id()`, `extract_stock_code()`, `StoreManifest`가 사실상 동일 코드. `extract/common_collector.py`로 추출 필요.

2. **bronze_to_silver_신한투자증권 vs OPIK extract_silver.py**: Bronze→Silver 변환 로직이 양쪽에 중복 존재. 신한 manifest가 통파일 구조라서 OPIK extract_silver가 인식하지 못해 별도 구현됨. OPIK extract_silver를 확장하여 통파일/개별파일 모두 지원해야 함.

3. **OPIK 체크포인트 로직**: `upload_naver.py`, `upload_koreainvest.py`, `extract_silver.py`에 거의 동일한 JSON 체크포인트 읽기/쓰기 코드가 3회 중복.

4. **finanace_AI_Agent common.py vs OPIK 공통 유틸**: S3 read/write, 날짜 파싱, JSON 처리 등이 양쪽에 독립 구현. 통합 시 `opik/utils/`로 이관하고 import.

---

## Strengths (잘한 점)

### OPIK
- **정규식 검증 레이어**(`_validate_tp_context`): 18가지 rejection 패턴으로 목표주가 false positive 0.044%
- **백필 인프라**: 2-Phase 구조(병렬 수집 + 청크 업로드), Graceful shutdown, 체크포인트 resume
- **에러 복구**: 모든 PDF 다운로드에 exponential backoff 재시도, 응답 크기 검증
- **S3 파티셔닝**: Bronze/Silver/Gold Hive 스타일 파티셔닝으로 Spark 호환성 확보

### DartCollector
- **분산 rate limiter**: DB 기반 3계층 quota(분당/일일/글로벌) + FOR UPDATE SKIP LOCKED
- **멱등성 설계**: request_hash, ON CONFLICT, 마커 파일로 모든 단계 재실행 안전
- **운영 도구**: recollect, rebuild, requeue 등 보정용 workflow 체계적 준비

### finanace_AI_Agent
- **관대한 JSON 파싱**(`parse_lenient_llm_json_text`): 3단계 fallback으로 LLM 출력 100% 복구
- **임베딩 텍스트 구성**: narrative_context + table_signal + reason/risks/keywords의 다층 조합
- **배치 인퍼런스 워크플로우**: 6개 단일책임 스크립트로 구성, 비동기 실행으로 로컬 단절에도 안전
- **프롬프트 디자인**: Haiku 특성에 맞게 JSON 예시 포함, 출력 제한 명시, hallucination 방지 지시

---

## 통합 권장사항

### 즉시 (이번 주)
1. `eval()` → `json.loads()` 교체
2. `spark-submit -c` → PythonOperator로 변경
3. boto3 client 단일 모듈화 (`opik/s3_client.py`)
4. report_id seed에 종목코드 포함

### 단기 (Phase 2 전)
5. `extract/common_collector.py`로 증권사 수집기 공통 로직 추출
6. OPIK extract_silver.py에 통파일 manifest 지원 추가
7. 모든 스크립트에 Airflow-friendly `run()` 함수 추가
8. `prompt_date()` 제거하고 args 기반으로만 동작하도록 수정

### 중기 (Phase 2 진행 중)
9. `opik/utils/` 아래 공통 유틸 통합 (common.py + OPIK 유틸)
10. `pyproject.toml`로 의존성 통합
11. 핵심 로직 pytest 추가 (정규식, JSON 파싱, TP 추출)
12. Delta Lake MERGE에 mergeSchema 옵션 추가

### Bedrock 전환 관련
- 태주님 코드의 Bedrock Batch Inference 파이프라인이 잘 갖춰져 있음
- OPIK `extract_gold_llm.py`의 Bedrock 전환 시 `common.py`의 client 생성 로직 재사용 가능
- Bedrock은 리전 내 통신이라 EC2 배포 시 API 직접 호출보다 빠르고 저렴
- Batch Inference(JSONL)는 대량 처리에 적합하나, 실시간 일일 건수(20~50건)에는 실시간 Converse API가 더 적합

---

## 팀원별 액션 아이템

### 윤준호 (OPIK)
- [ ] `eval()` 제거
- [ ] spark-submit -c 수정
- [ ] S3 client 단일 모듈화
- [ ] DAG trigger_rule 설정
- [ ] 체크포인트 로직 공통화

### 상용 (DartCollector)
- [ ] API 키 로깅 제거
- [ ] SQLAlchemy 원자적 UPDATE로 변경
- [ ] Gold 구현 전: Parquet 스키마 설계, VectorDB 스키마 설계, incremental watermark 설계

### 태주 (finanace_AI_Agent)
- [ ] boto3 thread-safety 수정
- [ ] report_id seed 확장
- [ ] load_rows() JSON 예외 처리 추가
- [ ] prompt_date() 제거
- [ ] 중복 추출 로직 common_collector로 이관
