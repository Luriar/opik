# RAG / AI Agent 미니프로젝트_Stock

> **태주님 원본**에서 실제 운영 상태와 일치하도록 수정 (2026-06-22 기준)
> 수정 항목: OHLCV 소스, DAG 스케줄, DART 통합 상태, FAISS 규모, 에이전트 파이프라인 구조, 브리핑 상세

---

## 프로젝트 개요

### 프로젝트 목표

**KOSPI 투자 도우미 AI Agent**

전날 게시된 기업 공시 및 증권사별 투자 분석 리포트의 핵심 내용을 요약하고, 주가예측모델을 활용하여 매일의 증권시장 현황을 요약·발송하는 챗봇 서비스.

- **Pain Point**: 공시정보 및 투자 분석 리포트를 매일 학습하여 투자 의사 결정에 활용하기 어렵다.
- **Solution**: RAG 기반 AI Agent

### 프로젝트 기간 & 예산

- 최종 발표: 2026-06-23
- 예산: $200

### 원천 데이터

1. KOSPI 주가 데이터 (KRX OHLCV + 거시지표)
2. 증권사별 투자 분석 리포트 (네이버페이증권·한국투자증권)
3. DART — 기업 공시 (OpenDART API)

---

## 데이터 파이프라인

```
KOSPI OHLCV·Macro → LightGBM 주가예측모델 → Top10 S3 Gold 적재
증권사 리포트 + DART 공시 → Medallion (Bronze→Silver→Gold) → FAISS RAG → LLM (AI Agent)
```

### 구성

| 도메인 | 핵심 Task |
|--------|-----------|
| 증권사 투자 분석 리포트 | 수집 자동화 → PDF 파싱 → RAG (FAISS + Haiku/Sonnet) |
| DART 기업 공시 | OpenDART API 수집 → Medallion 가공 → Hybrid RAG (SQL + Vector) |
| KOSPI 주가예측모델 | 학습 데이터셋 구성 → LightGBM 모델링 → 매일 재학습·예측 |

---

### 1. 증권사별 투자 분석 리포트

**타겟 데이터**: PDF 리포트

- 벡터 검색 기반 구축을 위해 약 5만 1천 건의 증권사 분석 리포트를 초기 백필 대상으로 수집·가공.
- 초기 백필 ETL과 임베딩 생성은 로컬 환경에서 수행.
- 이후 코드를 일배치 구조로 전환하고 EC2의 Docker 기반 Airflow 환경에 배포 (파이프라인 자동화).

#### Source

| 증권사 | 수집 경로 | 상태 |
|--------|----------|------|
| 네이버페이증권 | 국내 주요 증권사 리포트 모아서 제공 (HTML 순회 → PDF 다운로드) | 운영 중 |
| 한국투자증권 | 네이버페이증권 미수록 — 자체 수집기 개발 | 개발 완료 (618건 dry-run 확인), 가동 시 로컬 실행 필요 |
| 신한투자증권 | 네이버페이증권 미수록 — 자체 수집기 개발 | 해결 완료 |

#### Bronze

- Extract: PDF 51,294건
- 웹사이트별 HTML 구조 분석 → 5년 치 리포트 자동 순회, PDF 다운로드
- S3: `bronze/{증권사}/YYYY-MM-DD/{report_id}.pdf`

#### Silver

- PDF → JSON (PyMuPDF 기반, 99.99% 추출 성공)
- S3: `silver/{증권사}/YYYY-MM-DD/{report_id}.json`

```python
# Silver 메타데이터 스키마
{
    "report_id":  "md5({증권사}_{제목}_{발행일})",
    "source":     "네이버" | "한국" | "신한",
    "증권사":     "교보증권" | "대신증권" | ...,
    "종목명":     str,
    "발행일":     "YYYY-MM-DD",
    "title":      str,
    "text":       str,      # PDF 전문 텍스트
    "text_len":   int,
}
```

#### Gold

JSON → Parquet. 수집은 일 단위, Gold Parquet은 연/월 파티션.

**Structured** (`gold/structured/year=YYYY/month=MM/data.parquet`)

종목코드, 투자의견, 목표주가, 현재주가 등을 정규식으로 추출.

| 지표 | 추출률 |
|------|--------|
| 투자의견 | 90.8% |
| 목표주가 | 75.9% |
| 종목코드 | 87.5% |

목표주가 오인율: 0.088% (9단계 false-positive 방어, 애매하면 null 저장)

```python
# Structured 스키마
{
    "report_id":       str,
    "증권사":          str,
    "종목명":          str,
    "종목코드":        str,
    "발행일":          str,
    "title":           str,
    "source":          str,
    "text_len":        int,
    "pages_total":     int,
    "투자의견":        str,
    "목표주가":        int,
    "현재주가":        int,
    "상승여력_pct":    float,
    "종목코드_list":   str,
    "실적추정_raw":    str,
}
```

**Embeddings** (`gold/embeddings/year=YYYY/month=MM/data.parquet`)

AWS Bedrock Haiku 3.0으로 reason / risks / keywords 추출. `intfloat/multilingual-e5-small` (384d, L2 정규화)로 passage 텍스트 벡터화.

```python
# Embeddings 스키마
{
    "report_id":  str,
    "종목코드":   str,
    "reason":     str,
    "risks":      list[str],
    "keywords":   list[str],
    "embedding":  list[float],  # 384차원
}
```

#### RAG · 검색

- **임베딩 모델**: `intfloat/multilingual-e5-small` (384d, L2 정규화)
- **인덱스**: FAISS `IndexIDMap + IndexFlatIP`, 총 299,596 vectors (리포트 51,583 + DART 약 248,000)
- **빌드**: `build_index.py`가 S3 `gold/embeddings/` → 인메모리 인덱스 빌드 → 서버 시작 시 EC2 RAM 상주
- **검색 흐름**:
  1. 질의 → e5 `"query:"` 임베딩
  2. FAISS top-k 검색 → structured/embeddings 메타 결합
  3. Haiku 요약 → 답변 (+출처 report_id)
- **날짜 기반 검색**: `search_by_date()` — S3 `gold/structured/` Parquet을 연/월 파티션으로 직접 스캔 (FAISS 우회). 사용자가 "M월 D일" 처럼 날짜를 지정하거나 "오늘" 키워드를 사용할 때 트리거.
- **최신성 필터**: FAISS 검색 결과에서 180일 초과된 리포트 자동 필터링 (단, 사용자가 명시적 연도를 지정한 경우 제외)

---

### 2. DART 기업 공시

**타겟 데이터**: OpenDART OpenAPI 응답 — KOSPI·KOSDAQ 상장사

DART 공시·재무·지분·주요사항을 계층형으로 적재. SQL(정확한 수치) + Vector(서술형 의미) 하이브리드 RAG의 입력으로 사용. 리포트 파이프라인과 동일한 Medallion 골격을 따르며, S3 버킷 `s3-opik-bucket`의 `gold/dart/` 경로에 적재.

> **현재 상태**: DartCollector Gold 정본 경로 v2로 통합 완료. OPIK 챗봇·브리핑이 DartCollector Gold Parquet을 직접 소비. DART FAISS 297K vectors 통합 완료.

#### Source

**OpenDART OpenAPI** — 금융감독원 전자공시 공식 API

- 상장사 기준: `corpCode` 종목코드 보유 기업, KOSPI·KOSDAQ 한정
- DS001 공시검색·원문·기업개황
- DS002 정기보고서 주요정보 (배당·증자·자기주식·임원)
- DS003 단일회사 전체 재무제표 (연결/별도)
- DS004 지분공시 (대량보유·임원/주요주주)
- DS005 주요사항보고서 (유상증자·합병·감자 등 36개 엔드포인트)
- DS006 증권신고서 (지분/채무증권)

#### Bronze

API 응답 원본 보존. JSON/XML/ZIP을 파싱 없이 원본 그대로 저장. 공시 단위 완결성 marker로 증분 기준 관리.

```
bronze/dart/document/rcept_no=…/original.zip          ← 원문 ZIP
bronze/dart/complete/corp_code=…/rcept_no=….json      ← 완결 marker
```

#### Silver

`rcept_no`(접수번호) 단위 취합 JSON. Bronze의 여러 API 응답을 공시 1건 = `report.json` 하나로 취합. 원문 ZIP은 복제하지 않고 링크만 보존, 정정공시는 새 `rcept_no`로 별도 파일.

```
silver/dart/reports/corp_code=…/report_type=…/rcept_no=…/report.json
silver/dart/_done/sv=<버전>/corp_code=…/rcept_no=….json
```

#### Gold

목적별 분해: facts (정확한 수치·날짜, SQL/Parquet 조회) + rag (서술형 의미, Vector 검색)

- **facts**: `gold/dart/facts/{financial_statement, material_event, ownership, regular_structured, securities}/…/part-*.parquet`
- **rag_chunk**: 검색 텍스트 정본 + 출처 URL + lineage (`source_fact_ids`)
- **embedding**: 384d e5-small 벡터 + 필터 메타만 저장 (chunk_text 미포함, chunk_id로 join). `gold/dart/rag/embedding/model=…/version=…/ingest_mode={backfill|incremental}/…/part-*.parquet`

#### RAG · 검색

리포트와 동일한 e5 규약 사용:

- 문서 `"passage:"` / 질의 `"query:"` 프리픽스 + L2 정규화
- 인덱스: FAISS `IndexIDMap + IndexFlatIP` (384d), DART base(backfill) + delta(incremental)
- 검색 흐름: `질의 → e5 "query:" 임베딩 → FAISS top-k → id_map → rag_chunk(chunk_text+출처) → source_fact_ids → facts 조회로 숫자 확정 → LLM 답변`
- **원칙**: 숫자는 facts(SQL), 서술·요약은 vector

#### 운영 · 자동화

| 항목 | 내용 |
|------|------|
| 실행 환경 | Docker Compose + Airflow(CeleryExecutor), Service DB 분리 |
| 증분 파이프라인 | 평일 07:20~20:10 KST, */3분 간격 |
| 일배치 | raw_to_silver(05:30) · gold_backfill(07:00) · compaction(20:00) |
| 통합 상태 | DartCollector Gold v2 정본 경로 → OPIK RAG/브리핑이 직접 소비 |

**리포트 ↔ DART 평행 파이프라인**

- 정확한 수치는 구조화/SQL: 리포트 `structured` ↔ 공시 `facts`
- 서술형 의미는 벡터 검색: 리포트 `embeddings` ↔ 공시 `rag`
- 임베딩·검색 규약 공유: `multilingual-e5-small`(384d) + FAISS, `"query:"`/`"passage:"` 프리픽스

---

### 3. LightGBM 주가예측모델

**타겟 데이터**: KRX OHLCV + 거시지표 — 약 350개 KOSPI 종목

상승 가능성이 높은 종목 순위를 예측해 브리핑의 ★ 삼중 신호(report ∩ model ∩ DART)에 사용. 매일 아침 배치로 수집 → 피처 생성 → 모델 재학습 → 예측 → Top10 → S3 Gold 적재. 누수(look-ahead) 차단을 최우선으로 설계 — 모든 피처는 T-1 기준.

#### Source

- **FinanceDataReader** (주력): 전일 KRX OHLCV 일괄 수집, 약 350종목. KOSPI + KOSDAQ universe 단일 호출 지원 → `data/raw/kr_stock/ohlcv_full_universe_*.parquet`에 append
- **pykrx** (fallback): FinanceDataReader 실패 시 per-ticker 수집
- **yfinance**: 거시지표 — NASDAQ·S&P500·VIX·WTI·USD/KRW
- 수집 전 Feature Source Completeness Check (모든 피처가 동일 최신일자인지 확인, FAIL 시 파이프라인 종료)

#### Feature

원시 OHLCV·매크로 → 55개 피처 (10개 그룹)

| 그룹 | 내용 |
|------|------|
| price | 가격 변동률 |
| momentum | 모멘텀 지표 |
| volume | 거래량 지표 |
| volatility | 변동성 |
| candlestick | 캔들 패턴 |
| breakout | 돌파 신호 |
| technical | 기술적 지표 |
| cross_sectional | 횡단면 순위 |
| macro | 거시경제 |
| identity | 섹터·시장유형·시총그룹 (범주형) |

누수 차단: 모든 피처 T-1 기준 · shift_before_rolling · T일 OHLCV 금지 · 절대가/절대량 미사용(비율만) · cross-sectional 랭크는 날짜별. 최소 이력 120일.

#### Model · Train

LightGBM 회귀 3-모델 — 매일 1회 재학습, 롤링 350영업일.

| 모델 | 예측 대상 | 용도 |
|------|----------|------|
| `ranking_model` | `ranking_score` (익일 종가수익률, 부호>0=상승) | **메인 신호** |
| `gap_model` | `pred_gap` (전일 종가→당일 시가) | 보조 |
| `intraday_model` | `pred_intraday` (당일 시가→종가) | 보조 |

- `expected_return = pred_gap + pred_intraday`
- `pred_close_price = prev_close × (1+gap) × (1+intraday)`
- 핵심 하이퍼파라미터: learning_rate 0.03 · num_leaves 31 · min_data_in_leaf 100 · feature/bagging_fraction 0.8 · lambda_l2 1.0 · n_estimators 2000 + early_stopping 100 · seed 42
- 학습셋: 롤링 350영업일 × 350종목 × 55피처

#### Prediction · Top10

350종목 예측 → ranking_score 내림차순 정렬 → 상위 10개 Top10.

- AI Score: ranking_score min-max 0~100
- AI Rank: 내림차순 순위
- AI Percentile: rank/universe × 100
- 산출: `gold/model/predictions/dt=YYYY-MM-DD/predictions.parquet`

#### 검증 · 성능

90영업일 walk-forward로 롤링 윈도우 150D/250D/350D 비교 → **350D 채택** (평가 2026-01-30 → 2026-06-17)

| 지표 | 값 |
|------|-----|
| Ranking IC | 0.0106 |
| Top10 일평균 수익률 | +0.98% |
| Win Rate | 61.1% |
| Gap 방향적중 | 54.9% |
| Portfolio Sharpe | 3.64 |
| CAGR | 822% |
| MaxDD | −23% |

*백테스트·거래비용 미반영 — 발표 시 단서 필요*

#### 일일 파이프라인 (run_daily_update)

```
Config Load → Target Update Date → OHLCV(FinanceDataReader, 06:00) → Macro(yfinance)
→ Completeness Check (FAIL→종료) → Feature(55) → Training Dataset → Rolling 350D
→ LightGBM 3-모델 재학습 → 350종목 예측 → Top10 → Daily Archive (06:30 완료)
```

#### 통합 — OPIK 브리핑

- `daily_ohlcv_collection` DAG: 06:00 KST (Mon-Fri)
- `model_daily_prediction` DAG: 06:30 KST (Mon-Fri) → `gold/model/predictions/` 적재
- ★ 삼중 신호: report ∩ model ∩ DART. 모델 몫 = ranking_score 부호(>0), pred_close_price는 참고가
- 전 종목 음수(전체 하락 예측)면 ★ 미생성

---

## RAG · AI Agent 서빙 계층

Gold(S3)를 받아 VectorDB 검색 → AI Agent → 브리핑 → 텔레그램으로 전달하는 다운스트림 계층. 두 산출물: **챗봇**(양방향 질의응답) · **브리핑**(매일 아침 자동 발송).

### 4. VectorDB & RAG 검색

리포트·DART 임베딩을 의미로 검색하고, 정확한 수치는 SQL/facts로 답하는 하이브리드 검색.

#### VectorDB

FAISS 인메모리 (별도 벡터DB 서버 없음).

- 인덱스: `IndexIDMap + IndexFlatIP`, 총 **299,596 vectors** (리포트 51,583 + DART 약 248,000)
- `build_index.py`가 S3 `gold/embeddings/` → 인덱스 빌드 → 서버 시작 시 EC2 RAM 상주
- DART: base(backfill) + delta(incremental) 인덱스 통합 완료

#### e5 규약

- `intfloat/multilingual-e5-small` (384d)
- 문서 `"passage:"` / 질의 `"query:"` 프리픽스 + L2 정규화 (코사인 = 내적)

#### 하이브리드 검색

```
질의 → e5 "query:" 임베딩 → FAISS top-k → 메타 결합 / facts 조회 → LLM 답변 (+출처)
```

---

### 5. AI Agent — 멀티에이전트 챗봇

FastAPI `/v2/chat`이 진입점 — 질의가 순차 에이전트 파이프라인을 통과. (Telegram 봇은 내부적으로 `v2_chat_handler()` 호출)

#### 파이프라인

```
Safety → Intent → Date Pre-check → Route (Supervisor) → [Report / DART / Analysis] → Response
```

- **Safety** (Haiku): 매수추천 등 unsafe 의도 차단. follow-up 메시지는 safety skip
- **Intent** (Haiku): tickers·sectors·기간 + compare/cause/interpret 플래그 추출
- **Date Pre-check**: 순수 날짜 질문("오늘 며칠이냐") short-circuit 감지 → FAISS 검색 전 직접 응답. "M월 D일" → 현재 연도로 복원. "오늘" 키워드 → `search_by_date()` 강제 라우팅
- **Route** (Supervisor, No LLM): 6개 경로 분기 — `safety_refusal` / `general_response` / `report_agent` / `report_with_analysis` / `dart_agent` / `hybrid_parallel`

#### Agent 역할 & LLM

| Agent | LLM | 역할 |
|-------|-----|------|
| Report Agent | Haiku | FAISS top-k 검색 → Haiku 요약. 날짜 지정 시 `search_by_date()`로 S3 Parquet 직접 스캔 |
| DART Agent | Haiku/Sonnet | facts 구조화 쿼리 → 공시 해석 |
| Analysis Agent | Opus | 비교(증권사간/종목간) · 원인추적 (평균 33~52초) |
| Response Composer | Sonnet | 복합 결과(리포트+분석, 리포트+공시)를 자연어로 통합. 단순 리포트 검색은 Haiku가 직접 summarise |

#### 멀티 턴 대화

- Follow-up 감지: 짧은 참조 메시지("그 종목")는 의도 파서 우회 + 대화 컨텍스트 주입
- 상대 날짜 해석("어제"·"지난달") · 세션 영속화(SQLite) · 컨텍스트 full 감지
- 2026-06-22: 날짜 관련 3대 버그 수정 완료 ({today_date} 리터럴 노출·오래된 리포트·날짜 컨텍스트)

#### 실제 코드 경로

```
Telegram long-polling
  → _process_telegram_message() (opik_server.py)
    → v2_chat_handler(FakeReq) (agent_integration.py)
      → _run_agent_pipeline() (agent_integration.py)
        → Safety → Intent → Route → [Report/DART/Analysis]
```

웹 `/chat` 엔드포인트는 opik_server.py 내 자체 intent 라우팅 사용 (레거시). 모든 신규 기능은 `/v2/chat` → `agent_integration.py`에 구현.

---

### 6. 브리핑 ★ / ! 일일 신호

매일 07:00 KST 자동 생성·발송 — 세 파이프라인이 하나의 신호로 수렴하는 산출물.

#### ★ Triple Consensus

- report ∩ model ∩ DART 교집합 → BUY + 상승여력 > 0 확인
- 세 신호가 모두 긍정인 종목 (하루 0~3개)
- "신호 일치"이지 수익 보장 아님 명시

#### ! Major Disclosures

- 주가 직결 주요사항 (유상증자·합병·감자·M&A·부도 등)
- DART Sentiment Agent (Haiku batch)로 긍정/부정/중립 + 한 줄 이유 분류

#### BriefingGraph (9-step)

```
Gold 로드(리포트 3일 lookback + DART + 모델)
  → DART sentiment batch
  → ★/! 산출
  → Sonnet 작성
  → 텔레그램 발송
```

- Airflow DAG: `opik_briefing`, schedule `0 7 * * *` (07:00 KST)
- 날짜 처리: `{{ next_ds_nodash }}` 사용 (Monday bug fix 완료)
- 텔레그램: `.env` 파일 명시적 source 로드, 멀티 수신자 발송

---

### 7. 사용자 접점 & 인프라

#### 사용자 접점 — 멀티유저 텔레그램

- 인바운드: long-polling + `/telegram/webhook` 양방향
- 승인제: 가입(`/start`) → 관리자 승인 → 대화 허용
- 영속화(SQLite): `subscribers` · `briefing_recipients` · `conversations` 멀티턴 대화
- 매일 아침 브리핑 자동 발송 (멀티 수신자)
- typing indicator ("답변중...") 표시

#### 인프라

| 항목 | 사양 |
|------|------|
| EC2 | `r6g.large` (ARM, 2vCPU, 16GB RAM) |
| Airflow | Docker Compose, CeleryExecutor |
| 서버 | systemd `opik-server` (uvicorn, port 8000), `/root/opik-server/` |
| FAISS | 인메모리, 299,596 vectors, 384d |
| Delta Lake | Gold Parquet 일별 MERGE (`spark-delta-merge.timer`, 06:50 KST) |
| LLM | AWS Bedrock — Haiku 3.0 (경량), Sonnet (중간), Opus (분석) |

#### 오케스트레이션 타임라인 (KST, 평일)

```
00:00  수집 브론즈·실버 파이프라인 시작
05:00  DART raw_to_silver
06:00  daily_ohlcv_collection DAG (FinanceDataReader)
06:30  model_daily_prediction DAG (LightGBM 재학습·예측·Top10)
06:50  spark-delta-merge.timer (Delta Lake MERGE)
07:00  opik_briefing DAG (★/! 브리핑 → 텔레그램 발송)
```

> **2026-06-22 현재**: 3개 DAG 모두 unpaused, 정상 작동 중. OHLCV 06:00 / Model 06:30 / Briefing 07:00 KST.

---

## 원본 문서 대비 주요 수정 사항

| 항목 | 태주님 원본 | 실제 운영 (수정) |
|------|-----------|-----------------|
| OHLCV 수집 소스 | pykrx | FinanceDataReader (주력) + pykrx (fallback) |
| OHLCV DAG | 없음 | `daily_ohlcv_collection` 06:00 KST Mon-Fri |
| Model DAG 시간 | 06:00 KST | 06:30 KST (OHLCV 완료 후) |
| 타임라인 | 00:00 수집 → 06:00 모델 → 07:00 브리핑 | 06:00 OHLCV → 06:30 모델 → 06:50 Delta → 07:00 브리핑 |
| FAISS 벡터 수 | 51,583 | 299,596 (리포트 + DART 통합) |
| DART FAISS | "OPIK 과제" | 통합 완료 (297K DART vectors) |
| DART Gold 경로 | "더미 경로 → 이행 예정" | DartCollector v2 정본 경로 통합 완료 |
| 챗봇 API 경로 | `/chat` | Telegram은 `/v2/chat` → `agent_integration._run_agent_pipeline()` |
| 브리핑 날짜 처리 | `{{ ds_nodash }}` | `{{ next_ds_nodash }}` (Monday bug fix) |
| 한국투자증권 | 미기재 | 수집기 개발 완료 (618건 dry-run) |
| 검색 방식 | FAISS only | FAISS + `search_by_date()` (S3 Parquet 직접 스캔) 병행 |
| 최신성 필터 | 없음 | 180일 리컨시 필터 (2026-06-22 추가) |
| 날짜 질문 | 미대응 | 순수 날짜 질문 short-circuit 감지 (2026-06-22 추가) |
