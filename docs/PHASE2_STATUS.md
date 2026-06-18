# OPIK Phase 2 — 설계 대비 구현 현황

2026-06-19 기준

---

## 1. 전체 진행률 요약

| 단계 | 설계 범위 | 구현 상태 |
|------|----------|----------|
| Phase 2a Week 1 | LangGraph 코어 + Delta Lake + 7개 Agent | 완료 |
| Phase 2a Week 2 | Analysis Agent 고급 기능 + Response Composer + E2E | 부분 완료 (stub) |
| Phase 2b | Briefing Redesign (★/! 파이프라인) | 완료 |
| Phase 2c | Production Hardening | 미진행 |

**전체 완성도: 설계 대비 약 65%**

---

## 2. Agent 구현 현황 — 설계 대비

### 2.1 Safety Agent
- **설계**: Haiku로 6종 unsafe 패턴(buy_recommend, sell_recommend, portfolio, timing, guarantee, out_of_domain) 필터, 거절 응답 + 대안 제시
- **구현**: 완료. 설계와 동일하게 구현. `agent_integration.py`의 pipeline 첫 단계로 호출됨.
- **동작**: Bedrock Haiku 호출로 사용자 메시지 분류 → is_safe 판정 → unsafe면 즉시 거절 (Zone C)

### 2.2 Intent Parser Agent
- **설계**: Haiku로 intent + params 추출. compare / cause_tracking / interpret 플래그 포함.
- **구현**: 완료. 설계와 동일하게 구현. `agent_integration.py`의 두 번째 단계.
- **동작**: Bedrock Haiku로 intent 분류 + tickers/brokerages/time_range/keywords 추출

### 2.3 Report Agent
- **설계**: FAISS 시맨틱 검색 + Haiku 요약. `search_faiss()`, `browse_by_date()`, `get_report_detail()`.
- **구현**: 완료. 51,583개 벡터 FAISS 인덱스 사용, multilingual-e5-small 임베딩.
- **동작**: 사용자 질문 → SentenceTransformer 임베딩 → FAISS cosine 유사도 검색 → Bedrock Haiku로 top-k 리포트 요약

### 2.4 DART Agent
- **설계**: 6개 Gold 테이블 쿼리 (`disclosure_events`, `financials`, `insider_trades`, `major_shareholders` 등) + 공시 해석(`summarize_disclosure`).
- **구현**: 기본 쿼리 완료. `query_dart_engine` 통해 4종 쿼리 지원. 공시 해석은 `interpret_disclosure`로 Haiku 사용.
- **미구현**: `summarize_disclosure` (설계상 별도 메서드로 정의되었으나 `interpret_disclosure`가 유사 역할 수행). Delta 테이블 직접 쿼리는 아직 — 기존 Parquet read 경로 사용 중.
- **동작**: `dart_query.py` 기반으로 종목코드/날짜범위로 S3 Parquet 쿼리 → 결과를 Haiku로 해석

### 2.5 DART Sentiment Agent
- **설계**: 1개월 DART 공시를 25건씩 Haiku 배치로 분류 (positive/negative/neutral + reason). asyncio 20 병렬로 1~2초.
- **구현**: Agent 클래스 완료 (`dart_sentiment_agent.py`). `classify_batch()` + `classify_sync()` 메서드 구현.
- **현재 상태**: 일일 브리핑에서는 **의도적으로 skip** (neutral default). 이유: 12K+ 이벤트를 Haiku 배치로 처리하면 60분+ 소요, 07:00 브리핑 파이프라인 타임라인(수 분)에 맞지 않음. 설계 문서(7.4절)는 200~300건으로 추정했으나 실제 OPIK disclosure_events는 월 12K+건.
- **해결 필요**: DART Sentiment는 Phase 2a 설계의 핵심인데 실운영에서 skip 중. 해결책: (a) 오프라인 pre-compute → Delta에 sentiment 컬럼 추가 (b) 필터로 1차 거르기 (B-type only로 범위 축소)

### 2.6 Analysis Agent
- **설계**: 4개 하위 기능 — `compare_reports`(리포트 비교), `industry_analysis`(업계 분석), `trace_cause`(주가 원인 추적), `check_triple_consensus`(삼중 신호). Sonnet 사용.
- **구현**: **3개 stub 완료, 1개 미구현**.
  - `compare_reports`: 구현됨 (구조화된 비교 텍스트 생성)
  - `trace_cause`: 구현됨 (타임라인 + 가능 요인 제시)
  - `industry_analysis`: 구현됨 (섹터 검색 + 분석)
  - **실제 LLM 호출은 하지 않음** — rule-based 또는 template-based 동작. Sonnet 추론은 미연동.
- **동작**: `agent_integration.py`에서 `compare`나 `cause_tracking` 플래그가 있을 때 호출되나, 실제 추론보다는 템플릿 기반 응답 생성 수준.

### 2.7 Response Composer
- **설계**: 각 에이전트 출력 → 사용자 최적화 형식 조합. 팩트에 출처 첨부, LLM 해석은 "분석 결과"임을 명시.
- **구현**: 완료. `compose_chat_response()` + `compose_briefing()`.
- **동작**: 템플릿 기반으로 ★/! 브리핑 포맷 조합, 챗봇 응답에 신뢰도 + 출처 표시

### 2.8 Supervisor Agent
- **설계**: Routing decision — intent 기반으로 어떤 에이전트를 활성화할지 결정 (6개 route).
- **구현**: 완료. `supervisor.py` 342라인. 7개 route: general_response, report_agent, report_with_analysis, dart_agent, dart_with_analysis, hybrid_parallel, fallback.
- **동작**: `agent_integration.py`에서 intent + params → route 결정. 설계보다 route 수가 더 많고 세분화됨.

### 2.9 Briefing Graph
- **설계**: 9-step 파이프라인 (Step 1~9), LangGraph StateGraph.
- **구현**: 완료. `briefing_graph.py` 615라인. EC2에서 검증됨 (2026-03-19: 5★ / 181! / 3초).
- **동작**: Step 1~5 S3 로드 → Step 4 DART Sentiment skip → Step 6 Triple Consensus (Pandas) → Step 7 ! 필터 → Step 8 Compose → Step 9 Telegram

---

## 3. 인프라 구현 현황

| 항목 | 설계 상태 | 구현 상태 |
|------|----------|----------|
| EC2 r6g.large | 단일 서버 | 배포 완료, systemd 동작 중 |
| FAISS Index | 51,583 vectors | 완료, 챗봇 검색 정상 |
| Delta Lake MERGE (cron) | 매일 06:50 KST | `/etc/cron.d/opik-delta-merge` 등록 완료 |
| Delta 테이블 백필 | 84개월 structured + embeddings | structured 161건, embeddings 132건, disclosure_events 12K+건 변환 완료 |
| spark_silver_to_delta.py | EC2 cron | 배포 완료, 3개 테이블 MERGE |
| Telegram 연동 | Hermes Agent 봇 | env 설정 완료, message_id=97 전송 확인 |
| Airflow DAG (model) | 0 6 * * * | `dags/model/daily_prediction.py` 완료 |
| Airflow DAG (briefing) | 0 7 * * * | `dags/briefing/daily_briefing.py` 완료 |
| DartCollector EC2 공동 배포 | Phase 2a 항목 | 미진행 (보류) |
| Elastic IP | Phase 2c 항목 | 미진행 |
| CloudWatch 메트릭 | Phase 2c 항목 | 미진행 |

---

## 4. 남은 과제 (Phase 2a 잔여 + 2b + 2c)

### 긴급 (핵심 기능 누락)
1. **DART Sentiment Agent 실운영화** — 현재 skip 중인 LLM sentiment 분류를 실제 동작하도록 수정
   - 옵션 A: 오프라인 pre-compute → Delta에 sentiment 컬럼 영구 저장
   - 옵션 B: B-type only로 범위 축소 (전체 12K+ 대신 200~300건으로)
   - 옵션 C: 1일 단위로 좁혀서 batch (DART 1일 30~50건 수준)

2. **Analysis Agent Sonnet 연동** — `compare_reports`, `trace_cause`, `industry_analysis`에 실제 LLM 호출 연결

3. **Partner 데이터 연동** — 설계 문서에 정의되지 않은 신규 과제

### 중요 (품질/완성도)
4. **DART Gold 날짜 범위 확장** — 현재 2025-06 ~ 2026-03 (10개월). 매월 DartCollector ETL 필요.
5. **챗봇 Delta 테이블 읽기** — 현재 Parquet direct read. Delta 우선 읽기 + fallback 구현.
6. **Analysis Agent `industry_analysis`** 실구현 — 섹터 분석 기능
7. **Response Composer에 Sonnet 도입** — 복잡한 응답은 Sonnet, 단순 응답은 템플릿 (설계 4.6)
8. **DART Agent에 `summarize_disclosure` 별도 구현** — 공시 의미 해석 전용 프롬프트

### Production Hardening (Phase 2c)
9. EC2 Elastic IP 할당
10. CloudWatch 메트릭 + 알람
11. LangSmith tracing (또는 custom)
12. 에러 핸들링 (agent timeout, LLM failover)
13. deploy.sh SSM 기반 최종 정리
14. CHATBOT_RESPONSE_POLICY.md 재검토

### 보류 (사용자 결정)
15. DartCollector EC2 공동 배포
16. Python 3.9 → 3.10+ 업그레이드
17. DartCollector 주기적 실행 스케줄링

---

## 5. Agent 동작 원리 (전체 파이프라인)

### 챗봇 (/v2/chat)

```
사용자 메시지
    │
    ▼
[Step 1: Safety Agent] ─── Haiku ───┐
  - 6종 unsafe 패턴 검출              │ unsafe → 즉시 거절 응답
  - is_safe 판정                       │ (Zone C: 투자 조언 거부)
    │ safe                              ┘
    ▼
[Step 2: Intent Parser] ─── Haiku ───
  - intent 분류 (report_search | dart_query | hybrid | general)
  - params 추출 (tickers, brokerages, time_range, compare, cause_tracking)
    │
    ▼
[Step 3: Supervisor Router]
  - intent + params → 7개 route 중 선택
    │
    ├── general_response → 템플릿 응답
    │
    ├── report_agent → FAISS 검색 (51,583 vectors) → Haiku 요약
    │
    ├── report_with_analysis → FAISS + Analysis.compare_reports
    │
    ├── dart_agent → S3 Parquet 쿼리 → 결과 포맷
    │
    ├── dart_with_analysis → S3 Parquet + Haiku interpret_disclosure
    │
    └── hybrid_parallel → Report + DART 병렬 → ResponseComposer 통합
    │
    ▼
[Step 4: Response Composer]
  - 출처 citations + 신뢰도 표시 + 디스클레이머
  - conversation_store에 세션 저장 (턴 기반 context 관리)
```

### 브리핑 (BriefingGraph.run)

```
07:00 KST 트리거
    │
    ▼
Step 1: Gold Structured 로드 → 당일 발행일 필터 (23건)
Step 2: Gold LLM 로드 → reason/risks/keywords (527건)
Step 3: DART disclosure_events → 1개월 범위 (16,900건)
Step 4: DART Sentiment → 현재 neutral default (skip)
Step 5: Chanho 모델 prediction → 348종목 ranking_score
    │
    ▼
Step 6: ★ Triple Consensus (Pandas in-process)
  - report ∩ model ∩ dart_positive → 8개 교집합
  - BUY + 상승여력 > 0% → 5개 ★ 확정
    │
    ▼
Step 7: ! Major Disclosures (B-type only)
  - ★에 없는 종목 중 B-type 주요사항보고 → 181건
    │
    ▼
Step 8: Briefing Composer → ★/! 포맷 조합
Step 9: Telegram 전송 → Hermes Agent 봇
```

### 핵심: ★ Triple Consensus 이진 판단

설계에서 composite score(a+b+c)는 전면 폐지. 대신 3개 소스의 신호 방향만 확인:

```
★ 조건:
  1. 오늘 리포트 발행 종목 AND
  2. 모델 ranking_score > 0 (양수 = 상방 전망) AND
  3. DART positive 이벤트 있음 AND negative 없음
  4. 리포트: BUY + 상승여력 > 0%

! 조건:
  - B-type 주요사항보고 + ★에 없는 종목
  - sentiment 분류 있으면 positive only
```

이 방식의 장점: 점수 합산의 임의성(weight tuning)을 제거하고, 데이터가 말하는 신호의 방향만 확인한다. 3개 소스가 모두 긍정인 종목만 ★로 선정되므로 false positive가 composite score 방식보다 낮다.
