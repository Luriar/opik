# OPIK Agent Architecture — 실제 구현 기준

2026-06-19 | `server/agents/` 11개 .py 파일 + `server/prompts/` 3개 .md 전수 분석 기준

---

## 아키텍처 개요

OPIK은 두 개의 독립적인 실행 경로를 가진다:

```
1. Chat Pipeline (실시간):  사용자 질문 → Safety → Intent → Supervisor → Agent × N → Composer → 응답
2. Briefing Pipeline (배치): Airflow DAG 07:00 → BriefingGraph.run() 9단계 → Telegram DM
```

챗봇 파이프라인은 LangGraph StateGraph 또는 `SupervisorAgent.run()` plain 함수 경로로 동작한다. 브리핑 파이프라인은 LangGraph를 사용하지 않고 `BriefingGraph` 클래스가 9단계를 순차 실행한다.

---

## Chat Pipeline — Agent 실행 흐름

```
사용자 메시지
    │
    ▼
┌─────────────────┐
│  Safety Agent   │  Haiku 3 (apac)  ~0.3s
│  is_safe 여부   │
└────────┬────────┘
         │ unsafe → 즉시 거절 응답 (composer.compose_refusal)
         │ safe
         ▼
┌─────────────────┐
│  Intent Parser  │  Haiku 3 (apac)  ~0.5s
│  intent + params│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Supervisor     │  No LLM — pure routing
│  6개 route 중   │
│  하나로 분기    │
└────────┬────────┘
         │
    ┌────┼────────────────────────────┐
    ▼    ▼           ▼                ▼
 general  report_agent  dart_agent  hybrid_parallel
 (바로응답)    │              │        report + dart
              ▼              ▼        동시 실행
         Report Agent   DART Agent       │
         FAISS search   dart_query       ▼
         + Haiku 요약   engine 호출    Composer
              │              │
              ▼              ▼
         (optional: Analysis Agent)
         report_with_analysis → compare/trace/industry
              │
              ▼
┌─────────────────┐
│ Response        │  Sonnet 4.6 (복합) / Template (단순)
│ Composer        │  2-tier formatting
└─────────────────┘
         │
         ▼
     최종 응답 (한국어)
```

---

## Chat Agents — 실제 역할 및 모델

### 1. Safety Agent (`safety_agent.py`)
| 항목 | 값 |
|------|-----|
| 모델 | Haiku 3 (`apac.anthropic.claude-3-haiku-20240307-v1:0`) |
| 호출 방식 | Bedrock `invoke_model` (temperature 0.0, max_tokens 256) |
| 역할 | 사용자 메시지의 safe/unsafe 분류. 모든 메시지가 첫 단계로 통과한다. |
| 출력 | `{is_safe: bool, violation_type: str|null, redirect_suggestion: str}` |
| 분류 체계 | safe: report_search, dart_query, hybrid, general / unsafe: buy_recommend, sell_recommend, portfolio, timing, guarantee, out_of_domain |
| JSON 파싱 실패 | safe default (통과시킴) |
| 거절 응답 | 6개 violation_type별 한국어 거절 템플릿을 `build_refusal_message()`에서 직접 문자열로 생성 (Composer를 거치지 않음) |
| 주의 | 챗봇 전용. Briefing pipeline에서는 호출되지 않는다. |

### 2. Intent Parser Agent (`intent_agent.py`)
| 항목 | 값 |
|------|-----|
| 모델 | Haiku 3 (`apac.anthropic.claude-3-haiku-20240307-v1:0`) |
| 호출 방식 | Bedrock `invoke_model` (temperature 0.0, max_tokens 512) |
| 역할 | 사용자 질문을 4개 intent로 분류하고 검색 파라미터 추출 |
| Intent | report_search, dart_query, hybrid, general (설계의 stock_price, refuse는 Safety가 처리함) |
| 특수 플래그 | compare, cause_tracking, interpret (boolean) — Analysis Agent 트리거 |
| 파라미터 | tickers[], ticker_names[], brokerages[], sectors[], time_range{from,to}, keywords[], is_greeting, response_style |
| JSON 파싱 실패 | general fallback (빈 params로 계속 진행) |
| 주의 | `{TODAY_DATE}` 치환 후 전송. 챗봇 전용. |

### 3. Supervisor (`supervisor.py`)
| 항목 | 값 |
|------|-----|
| 모델 | 없음 (No LLM — pure Python routing) |
| 역할 | Safety + Intent 결과를 받아 6개 route 중 하나로 분기. 전체 파이프라인 조정. |
| Routes | `safety_refusal`, `general_response`, `report_agent`, `dart_agent`, `dart_with_analysis`, `report_with_analysis`, `hybrid_parallel` |
| `run()` 메서드 | Safety → Intent → Route → Agents 실행 → Result 취합. LangGraph 없이도 동작하는 plain function path. |
| Sub-routing | intent=report_search + compare=true → report_with_analysis / intent=dart_query + interpret=true → dart_with_analysis |
| LangGraph | `build_supervisor_graph()`가 옵셔널. langgraph 미설치 환경에서는 `run()`만 사용. |
| 주의 | Briefing pipeline에는 Supervisor를 통하지 않는다. 챗봇 전용. |

### 4. Report Agent (`report_agent.py`)
| 항목 | 값 |
|------|-----|
| 모델 | Haiku 3 (`apac.anthropic.claude-3-haiku-20240307-v1:0`) |
| 호출 방식 | Bedrock `invoke_model` (temperature 0.3, max_tokens 1500) |
| 역할 | FAISS 의미 검색 + Haiku 한국어 요약 |
| 검색 | `search()`: multilingual-e5-small 임베딩 → FAISS IndexIDMap 51,583 vectors → top_k 결과 |
| 요약 | `summarise()`: FAISS 결과 10건을 context로 Haiku가 자연어 요약 |
| 출력 포맷 | 신뢰도 표시 + 종목명(증권사, 날짜) + 투자의견/TP/상승여력 + 핵심 논리/리스크/키워드 + 출처 |
| 검증 | 모든 수치는 FAISS 결과 그대로 사용. LLM 해석 부분은 "분석 결과"임을 명시. |
| 주의 | Report Agent 자체는 scoring을 하지 않는다. 별도의 composite score는 없다. |

### 5. DART Agent (`dart_agent.py`)
| 항목 | 값 |
|------|-----|
| 해석 모델 | Haiku 4.5 (`global.anthropic.claude-haiku-4-5-20251001-v1:0`) |
| 심층 모델 | Sonnet 4.6 (`global.anthropic.claude-sonnet-4-6`) |
| 호출 방식 | Bedrock `converse` API (interpret: temp 0.2/600tk, summarize: temp 0.3/1500tk) |
| 쿼리 | `disclosure_events`, `financials`, `insider_trades`, `major_shareholders` — 모두 `dart_query.py` 엔진에 위임 |
| interpret | Haiku 4.5: 이벤트 제목 + 한 줄 요약(30단어) + 영향도(긍정/중립/부정) + 주요 내용 3문장 |
| summarize | Sonnet 4.6: what/why/watch 3-section 상세 분석 |
| batch | `interpret_batch()`: 여러 공시를 순차 해석, 각각 impact 필드 추가 |
| 주의 | `dart_query.py`는 별도 모듈. DART Gold Parquet를 S3에서 pandas로 직접 읽는다. |

### 6. Analysis Agent (`analysis_agent.py`)
| 항목 | 값 |
|------|-----|
| 모델 | **Opus 4.8** (`global.anthropic.claude-opus-4-8`) |
| 호출 방식 | Bedrock `converse` API (temperature 0.3, max_tokens 2000~2500) |
| 역할 | 다중 소스 교차 분석 — 가장 높은 추론 능력이 필요한 작업 전담 |
| compare_reports | 종목별 여러 증권사 리포트 비교 (표 형식: 의견/TP/상승여력/논리/리스크 + 차이점 + 종합) |
| trace_cause | 주가 변동 요인 타임라인 분석 (리포트 이벤트 + DART 이벤트 → 가능한 요인 2-4개) |
| industry_analysis | 섹터 애널리스트 종합 (종목별 포지션 테이블 + 공통 키워드 + 섹터 전망 + 차별화 포인트) |
| 주의 | 모든 응답에 확률적 표현("~로 보입니다")과 disclaimer 포함. 챗봇 전용. |

### 7. Response Composer (`response_composer.py`)
| 항목 | 값 |
|------|-----|
| 복합 모델 | Sonnet 4.6 (`global.anthropic.claude-sonnet-4-6`) |
| 호출 방식 | Bedrock `converse` API (temperature 0.4, max_tokens 2000) |
| 2-tier 전략 | `_is_complex()`: compare/industry/cause/analysis intent → Sonnet 자연어 / 나머지 → 템플릿 |
| compose_chat_response | report_summary + dart_summary + analysis 통합. 복합이면 Sonnet, 단순이면 템플릿. |
| compose_briefing | ★/! 브리핑 전용. `_clean_disclosure_text()`로 DART 보일러플레이트 정제. 3900자 초과 시 `split_for_telegram()` 청킹. |
| _clean_disclosure_text | 회사명/보고서명 prefix 제거, 정정 템플릿 블록 strip, whitespace 정규화, 자연어 문장 경계에서 110자 truncation |
| compose_refusal | SafetyAgent의 거절 템플릿 위임 |
| 주의 | 챗봇과 브리핑 양쪽에서 사용. 유일하게 두 파이프라인을 모두 담당하는 모듈. |

---

## Briefing Pipeline — 독립 실행 경로

### Briefing Graph (`briefing_graph.py`)
| 항목 | 값 |
|------|-----|
| 타입 | LangGraph가 아닌 순수 Python 클래스 (`BriefingGraph.run()`) |
| LLM 사용 | Step 4(DART Sentiment Agent), Step 8(Compose Briefing → Sonnet)에서만 |
| 실행 위치 | Airflow DAG `opik_briefing` (07:00 KST) → 단일 PythonOperator |
| 호출 | Chat Supervisor를 통하지 않는다. DAG에서 직접 `run_briefing_pipeline(date)` 호출. |

**9단계 파이프라인:**

| Step | 작업 | LLM | 데이터 소스 |
|------|------|-----|-----------|
| 1 | Gold Structured 로드 | 없음 | `s3://s3-opik-bucket/gold/structured/` 월별 Parquet |
| 2 | Gold LLM 로드 | 없음 | `gold/embeddings/` 월별 Parquet + LEFT JOIN |
| 3 | DART Events 로드 | 없음 | `gold/dart/disclosure_events/` 92일 윈도우 월별 Parquet |
| 4 | DART Sentiment 분류 | Haiku 3 batch | DartSentimentAgent: 25건/배치, 2회 retry, temperature jitter |
| 5 | Model Predictions 로드 | 없음 | `gold/model/predictions/dt=YYYY-MM-DD/predictions.parquet` |
| 6 | ★ Triple Consensus | 없음 | set intersection + boolean filter: report ∩ model ∩ DART 1분기(pos\|정기보고서) |
| 7 | ! Major Disclosures | 없음 | B-type DART events, sentiment ≠ neutral |
| 8 | Compose Briefing | **Sonnet 4.6** | ResponseComposer.compose_briefing() |
| 9 | Telegram 전송 | 없음 | Bot API `sendMessage` |

### ★ Triple Consensus 상세

```
report ∩ model (INNER JOIN by 종목코드)
    → DART 1분기(92일) 필터: positive event OR 정기보고서 제출
    → negative-only 종목 제외
    → 빈 집합이면 fallback: DART 필터 없이 report ∩ model 전체 사용
    → BUY + upside > 0% 최종 확인
```

모델의 `ranking_score > 0`은 LightGBM이 계산한 값이다. Briefing Graph 자체는 어떤 scoring도 수행하지 않는다. `ranking_score`를 `> 0` 임계값으로 필터만 한다.

### ! Major Disclosures 상세

```
오늘 날짜 DART events 중 B-type (주요사항보고서 등)
    → sentiment ≠ neutral (positive 또는 negative)
    → _clean_disclosure_text() 정제 후 표시
```

### DART Sentiment Agent (`dart_sentiment_agent.py`)
| 항목 | 값 |
|------|-----|
| 모델 | Haiku 3 (`apac.anthropic.claude-3-haiku-20240307-v1:0`) |
| 호출 방식 | Bedrock `invoke_model` |
| 사용처 | Briefing pipeline Step 4 **전용**. Chat에서는 호출되지 않는다. |
| Batch 크기 | 25건/호출 (환경변수 `DART_SENTIMENT_BATCH_SIZE`) |
| 동시성 | 최대 20 concurrent (환경변수 `DART_SENTIMENT_CONCURRENT`) |
| Retry | 최대 2회, temperature jitter, `_neutral_fallback()` |
| v2 안정화 | `_extract_json_text()`: markdown fence strip + newline 정규화 / `_parse_sentiment_response()`: structured field 파싱 |
| 검증 결과 | 43배치 0실패 (이전 v1: 8/26 batch 실패, 31% → 완전 해결) |

---

## Data Helper (`data_helper.py`)

| 항목 | 값 |
|------|-----|
| 역할 | 모든 Agent가 데이터를 읽는 통합 경로 |
| 우선순위 | 1. Local Delta Lake → 2. S3 Parquet |
| 함수 | `read_structured(month_key)`, `read_embeddings(month_key)`, `read_dart(bucket, keys)`, `read_model_predictions(date)` |
| LLM | 사용하지 않음. 순수 I/O 모듈. |

---

## Prompt 파일

| 파일 | 줄 | 사용처 | 역할 |
|------|-----|------|------|
| `server/prompts/system.md` | 236줄 | 챗봇 시스템 프롬프트 | Zone A/B/C/D 경계, anti-hallucination hard limit 4개, 거절 규칙, 한국어 스타일, 응답 구조, 대화 메모리 |
| `server/prompts/intent_parser.md` | 146줄 | Intent Parser Agent | 5-intent taxonomy, parameter extraction (refers_to_previous 포함), JSON 출력 포맷 |
| `server/prompts/answer_generator.md` | 208줄 | Response Composer | 응답 생성 템플릿 |

---

## LLM 모델 배정 — 최종

| Agent | 모델 | Inference Profile | Bedrock API |
|-------|------|-------------------|-------------|
| Safety | Haiku 3 | `apac.anthropic.claude-3-haiku-20240307-v1:0` | `invoke_model` |
| Intent | Haiku 3 | `apac.anthropic.claude-3-haiku-20240307-v1:0` | `invoke_model` |
| Report | Haiku 3 | `apac.anthropic.claude-3-haiku-20240307-v1:0` | `invoke_model` |
| DART interpret | Haiku 4.5 | `global.anthropic.claude-haiku-4-5-20251001-v1:0` | `converse` |
| DART summarize | Sonnet 4.6 | `global.anthropic.claude-sonnet-4-6` | `converse` |
| DART Sentiment | Haiku 3 | `apac.anthropic.claude-3-haiku-20240307-v1:0` | `invoke_model` |
| Analysis | Opus 4.8 | `global.anthropic.claude-opus-4-8` | `converse` |
| Response Composer | Sonnet 4.6 | `global.anthropic.claude-sonnet-4-6` | `converse` |

**설계 대비 변경**: Analysis Sonnet → Opus 4.8, DART interpret Haiku 3 → Haiku 4.5.

---

## 설계 대비 구현 차이 요약

| 설계 | 실제 |
|------|------|
| LangGraph StateGraph compiled graph | `SupervisorAgent.run()` plain function path 사용. LangGraph는 `build_supervisor_graph()`로 옵셔널. |
| Briefing을 LangGraph Briefing Agent로 | `BriefingGraph.run()` 순차 9단계 Python 클래스로. LangGraph 미사용. |
| composite score (a+b+c) | **폐지**. ★/! 이진 consensus로 대체 (set intersection + boolean filter). |
| stock_price intent | SafetyAgent에서 처리. 별도 intent 분류 없음. |
| Delta Lake 우선 읽기 | Data helper에 구현되어 있으나, 대부분 S3 Parquet 직접 읽기로 동작 중. Delta는 briefing_graph에서 사용하지 않음. |
| Haiku 3 for DART interpret | Haiku 4.5로 상향. DART summarize는 설계에 없던 Sonnet 4.6 추가. |
| Sonnet for Analysis | Opus 4.8로 상향. |
| spark_compute_scores.py | **파일 삭제됨**. 모든 scoring 로직 제거. |
| Partner 데이터 소스 | 미구현. 설계만 존재. |

---

## 경로 참조

- EC2: `/home/ec2-user/airflow/opik/server/agents/` (11개 .py)
- EC2: `/home/ec2-user/airflow/opik/server/prompts/` (3개 .md)
- 로컬: `C:\Users\HP\Documents\opik\server\agents\`
- 로컬: `C:\Users\HP\Documents\opik\server\prompts\`
