# RAG Spark / Delta / FAISS 구조 정리

최종 갱신: 2026-06-23

## 결론

현재 RAG 요청 처리 경로에서 Spark는 실시간 검색 엔진으로 쓰이지 않는다.

Spark의 역할은 S3 Gold Parquet을 S3 Delta Lake serving table로 멱등 MERGE하는 배치 작업이다. 사용자가 `/chat` 또는 `/search`로 질문할 때는 FastAPI 서버가 FAISS, Delta/Pandas, S3 Parquet fallback, Bedrock을 조합해서 응답한다.

```text
실시간 질문 경로:
user -> FastAPI -> intent parsing -> FAISS or Delta/Parquet lookup -> Bedrock answer

배치 갱신 경로:
Airflow gold 완료 -> Spark Delta MERGE -> FAISS rebuild -> opik-server restart
```

## 현재 데이터 계층

| 계층 | 위치 | 성격 |
|---|---|---|
| Bronze | `s3://<bucket>/bronze/...` | 원본 PDF/API 응답 보존 |
| Silver | `s3://<bucket>/silver/...` | PDF/API를 파싱한 중간 산출물 |
| Gold Parquet | `s3://<bucket>/gold/...` | RAG/브리핑/Delta/FAISS의 주 입력 |
| Delta Lake | `s3://<bucket>/delta/gold_db/...` | Gold Parquet의 serving/upsert table |
| FAISS index | API EC2 local `/data/opik/faiss_index.bin` | semantic search용 파생 캐시 |
| id/info map | API EC2 local `/data/opik/report_ids.json`, `/data/opik/report_info.json` | FAISS row id를 report/chunk metadata로 복원 |

Source of truth는 Bronze/Silver/Gold S3 데이터다. Delta와 FAISS는 조회 성능과 운영 편의를 위한 재생성 가능한 파생물로 본다.

## Spark 사용 위치와 목적

### 1. 유지보수 DAG에서 Spark 호출

현재 코드 기준 Spark는 `dags/maintenance/dag_maintenance_delta_faiss.py`에서 호출된다.

트리거 조건:

- `s3://s3-opik-bucket/gold/structured/`
- `s3://s3-opik-bucket/gold/embeddings/`

두 Airflow Dataset이 모두 갱신되면 maintenance DAG가 실행된다.

실행 순서:

```text
delta_merge -> faiss_rebuild -> restart_server
```

`delta_merge`는 Airflow worker 컨테이너 안에서 직접 Spark를 실행하지 않는다. worker 컨테이너에는 `spark-submit`, Java, `/home/ec2-user`가 없기 때문이다. 대신 `docker run --privileged --pid=host ... nsenter --target 1` 패턴으로 EC2 host namespace에 진입해서 host의 `spark-submit`을 실행한다.

실행 명령 형태:

```bash
cd /home/ec2-user/spark_jobs
spark-submit --master 'local[2]' --driver-memory 6g gold_to_delta.py --date {{ ds_nodash }}
```

### 2. `spark_jobs/gold_to_delta.py`

현재 주 Spark job이다. S3 Gold Parquet을 읽어서 Delta table에 primary key 기준으로 MERGE한다.

| 입력 Gold | 출력 Delta | PK |
|---|---|---|
| `gold/structured/` | `delta/gold_db/structured` | `report_id` |
| `gold/embeddings/` | `delta/gold_db/embeddings` | `report_id` |
| `gold/dart/facts/material_event/` | `delta/gold_db/material_event` | `event_id` |
| `gold/dart/facts/financial_statement/` | `delta/gold_db/dart_financial_statement` | `fact_id` |
| `gold/dart/facts/ownership/` | `delta/gold_db/dart_ownership` | `ownership_fact_id` |
| `gold/dart/facts/regular_structured/` | `delta/gold_db/dart_regular_structured` | `fact_id` |

Spark 설정:

```text
master: local[4]
driver memory: 6g
Delta extension: io.delta.sql.DeltaSparkSessionExtension
S3 access: s3a + EC2 InstanceProfileCredentialsProvider
```

주요 처리:

- 월별 `gold/structured`, `gold/embeddings` Parquet을 읽어 `report_id`로 upsert
- DART `material_event`는 최근 월 파티션을 읽고 `event_id` 중복 제거 후 upsert
- DART facts는 schema drift를 피하기 위해 개별 Parquet 파일을 읽고 전체 컬럼을 string으로 cast한 뒤 union
- source key 중복으로 Delta MERGE가 실패하지 않도록 PK 기준 `dropDuplicates` 적용
- daily mode와 backfill mode를 모두 지원

Spark가 하지 않는 일:

- 사용자 질문 처리
- query embedding 생성
- FAISS 검색
- Bedrock LLM 응답 생성
- RDS/Redis/Secrets Manager 직접 조회

### 3. `spark_jobs/spark_silver_to_delta.py`

이 파일은 구버전 Delta MERGE job에 가깝다.

기존 대상:

- `gold/structured/`
- `gold/embeddings/`
- `gold/dart/disclosure_events/`

현재는 `gold_to_delta.py`가 DART facts v2 경로와 6개 Delta table을 다루므로 운영 기준 문서에서는 `gold_to_delta.py`를 우선 기준으로 본다. `spark_silver_to_delta.py`는 legacy/호환 작업으로 정리하는 것이 맞다.

### 4. Backfill 전용 Spark job

`spark_jobs/backfill_dart_facts.py`는 DART facts 3종을 일괄 Delta로 적재하기 위한 backfill 스크립트다.

대상:

- `financial_statement`
- `ownership`
- `regular_structured`

역할은 초기 적재나 schema drift 복구에 가깝고, 실시간 RAG 요청 경로에는 관여하지 않는다.

## Delta Lake 역할

Delta는 RAG의 원천 저장소가 아니라 Gold Parquet을 조회하기 쉽게 만든 serving table이다.

사용 목적:

- Gold Parquet의 월별/파일별 산출물을 PK 기준으로 upsert
- 같은 `report_id`, `event_id`, `fact_id`의 재처리 결과를 최신 row로 유지
- date browse나 DART event 조회에서 많은 Parquet partition scan을 줄임
- 실패 시 partial write를 피하는 ACID commit 제공
- Delta read 실패 시 S3 Parquet fallback 유지

현재 FastAPI/Agent 조회 경로:

| 코드 | Delta 사용 방식 |
|---|---|
| `server/agents/data_helper.py` | `deltalake.DeltaTable(...).to_pandas()`로 Delta 우선 read, 실패 시 S3 Parquet fallback |
| `server/opik_server.py::_scan_reports_by_date` | 날짜 기반 리포트 browse에서 `structured` Delta 우선, 실패 시 `gold/structured` 월별 Parquet scan |
| `server/agents/report_agent.py::search_by_date` | 날짜 기반 report agent 조회에서 Delta 우선, 실패 시 S3 Parquet scan |
| `server/dart_query.py::query_disclosure_events` | `material_event` Delta 우선, 실패 시 `gold/dart/facts/material_event` Parquet scan |
| `server/dart_query.py::query_financials` | 현재는 `financial_statement` Gold Parquet 직접 scan |
| `server/dart_query.py::query_insider_transactions` / `query_major_shareholders` | 현재는 `ownership` Gold Parquet 직접 scan |

주의할 점:

- Delta read는 현재 pandas DataFrame으로 전체 로드하는 방식이다. 데이터가 커지면 predicate pushdown 또는 SQL query layer가 필요하다.
- `gold_to_delta.py`는 DART facts도 MERGE하지만 maintenance DAG의 Dataset trigger는 현재 `gold/structured`, `gold/embeddings`만 본다. DART facts만 갱신되는 날에는 별도 trigger가 없으면 즉시 Delta MERGE가 돌지 않을 수 있다.

## FAISS 역할

FAISS는 semantic search용 로컬 벡터 인덱스다. 원천 DB가 아니다.

현재 구조:

```text
S3 gold embeddings parquet
  -> server/build_index.py
  -> /data/opik/faiss_index.bin
  -> /data/opik/report_ids.json
  -> /data/opik/report_info.json
  -> FastAPI startup load
  -> /search, /chat semantic retrieval
```

인덱스 형식:

- embedding model: `intfloat/multilingual-e5-small`
- dimension: `384`
- document prefix: `passage:`
- query prefix: `query:`
- vector normalization: enabled
- FAISS index: `IndexIDMap(IndexFlatIP)`
- normalized vector + inner product이므로 cosine similarity와 같은 의미로 사용

### FAISS build 경로

운영 maintenance DAG는 Spark MERGE 이후 `server/build_index.py`를 실행한다.

`server/build_index.py`가 읽는 입력:

- 증권사 리포트: `gold/embeddings/year=YYYY/month=MM/*.parquet`
- DART 공시: `gold/dart/rag/embedding/model=intfloat_multilingual-e5-small/version=v1/.../*.parquet`

저장 결과:

- `/data/opik/faiss_index.bin`
- `/data/opik/report_ids.json`
- `/data/opik/report_info.json`

이후 `opik-server`를 restart해서 새 index를 로드한다.

### FastAPI startup / rebuild 경로

`server/opik_server.py`는 기동 시 로컬 index 파일이 있으면 읽고, 없으면 `build_index_from_s3()`를 호출한다.

중요한 차이:

- `server/build_index.py`: 증권사 리포트 + DART embedding을 모두 읽는다.
- `server/opik_server.py::build_index_from_s3()`: 현재 코드상 `gold/embeddings/` 증권사 리포트만 읽는다.

따라서 운영에서 DART까지 포함한 통합 FAISS를 유지하려면 maintenance DAG의 `server/build_index.py` 경로를 기준으로 봐야 한다. `/index/rebuild` 엔드포인트는 현재 DART embedding까지 포함하지 않을 수 있으므로, 둘을 통합하는 리팩터링이 필요하다.

## RAG 요청 처리 경로

### `/search`

```text
query
  -> "query:" prefix
  -> SentenceTransformer encode
  -> FAISS search
  -> report_ids[idx]로 report_id/chunk_id 복원
  -> report_info metadata 결합
  -> optional date filter
```

Spark와 Delta는 `/search` 직접 경로에 관여하지 않는다.

### `/chat`

```text
message
  -> intent_parser
  -> safety/refusal gate
  -> needs_reports이면 FAISS 또는 date browse
  -> needs_dart이면 dart_query
  -> complex analysis이면 agent pipeline
  -> context build
  -> Bedrock answer
  -> conversation_store에 turn 저장
```

report 검색:

- 일반 의미 검색: FAISS top-k
- 정확한 날짜 browse: `structured` Delta 우선, 없으면 `gold/structured` Parquet scan
- 다중 종목 비교: 종목별 FAISS search 후 dedup/merge

DART 조회:

- `dart_disclosure`: `material_event` Delta 우선, fallback Parquet
- `dart_financial`: Gold financial statement Parquet 직접 조회
- `dart_insider`, `dart_shareholder`: Gold ownership Parquet 직접 조회

Conversation memory:

- `server/conversation_store.py`는 follow-up 질문의 종목/날짜 맥락을 보존한다.
- 이 저장소는 RAG source-of-truth가 아니며 Spark/Delta/FAISS와 별개다.

## 운영 순서

현재 코드 기준 갱신 순서는 다음과 같다.

```text
1. Gold structured DAG 완료
2. Gold embeddings DAG 완료
3. dag_maintenance_delta_faiss 트리거
4. Spark: gold_to_delta.py --date <ds_nodash>
5. Python: server/build_index.py
6. systemctl restart opik-server
7. FastAPI가 새 FAISS index와 metadata load
```

이 구조의 이유:

- Delta MERGE는 table serving consistency를 맞춘다.
- FAISS rebuild는 semantic search의 신규 embedding 반영을 담당한다.
- 서버 restart는 로컬 index 파일을 FastAPI process memory에 다시 올리기 위한 단계다.

## 현재 한계와 정리 필요점

| 항목 | 현재 상태 | 권장 정리 |
|---|---|---|
| Spark job 중복 | `gold_to_delta.py`와 `spark_silver_to_delta.py`가 공존 | `gold_to_delta.py`를 운영 기준으로 명시하고 legacy job은 deprecated 처리 |
| FAISS rebuild 경로 불일치 | `server/build_index.py`는 DART 포함, `/index/rebuild`는 리포트만 포함 | 공통 builder 함수로 통합 |
| DART Delta trigger | maintenance DAG는 structured/embeddings Dataset만 구독 | DART Gold facts/embedding Dataset도 outlet/trigger로 연결 |
| Delta read 방식 | pandas 전체 로드 중심 | 데이터 증가 시 predicate pushdown/query API 도입 |
| hardcoded bucket | 일부 DAG/build script에 `s3-opik-bucket` 고정 | `S3_BUCKET` env 기준으로 통일 |
| FAISS local cache | `/data/opik` 로컬 파일 | 재생성 가능하지만, rebuild 실패 시 이전 index 보존/rollback 전략 필요 |

## 파일별 책임 요약

| 파일 | 책임 |
|---|---|
| `dags/maintenance/dag_maintenance_delta_faiss.py` | Gold Dataset 완료 후 Delta MERGE, FAISS rebuild, server restart orchestration |
| `spark_jobs/gold_to_delta.py` | Gold Parquet -> Delta Lake MERGE의 현재 주 경로 |
| `spark_jobs/spark_silver_to_delta.py` | 구형/legacy Delta MERGE 경로 |
| `spark_jobs/backfill_dart_facts.py` | DART facts Delta 초기/복구 backfill |
| `server/build_index.py` | 증권사 + DART embedding을 로컬 FAISS index로 rebuild |
| `server/opik_server.py` | FastAPI `/search`, `/chat`, startup index load, Bedrock answer generation |
| `server/agents/data_helper.py` | Delta 우선, S3 Parquet fallback read helper |
| `server/dart_query.py` | DART facts 조회 엔진. 일부 Delta 우선, 일부 Parquet 직접 조회 |
| `server/agents/report_agent.py` | FAISS report search와 날짜 기반 report browse |
| `server/conversation_store.py` | follow-up 질문 맥락 관리. RAG 데이터 원천은 아님 |

