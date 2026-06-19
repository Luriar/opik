# OPIK — AI 주식 브리핑 시스템 아키텍처

최종 갱신: 2026-06-19

## 프로젝트 개요

한국 주식시장의 공시·리포트 데이터를 수집·가공하여 AI 기반 브리핑과 챗봇을 제공하는 시스템.
뉴스 같은 불확실한 데이터를 배제하고, 확실한 '공시'와 '리포트'에 집중하여 가치를 검증한다.

## 3단계 로드맵

### Phase 1 — 단방향 시황 브리핑 (MVP) ✅ 완료
당일 DART 공시 + 증권사 신규 리포트가 나온 종목만 추출 → LLM이 평어로 요약 → 브리핑 전달.

### Phase 2 — ★/! 브리핑 + 양방향 QA 챗봇 ✅ 운영 중
매일 07:00 KST ★/! 이진 consensus 브리핑 → Telegram DM. FAISS 임베딩 기반 RAG 검색으로 증권사 리포트 + DART 공시에 대한 양방향 QA 챗봇. EC2 r6g.large에서 Airflow 8 DAG + systemd timer로 완전 자동화.

### Phase 3 — 실시간 AI 주식 비서 (미착수)
실시간 모니터링, 즉시 시그널, 선제적 푸시 알림.

## 팀 역할 분담

| 팀원 | 담당 | 데이터 소스 | 기술 스택 |
|------|------|-----------|-----------|
| **찬호** | 주가 예측 모델 | 주가 데이터 | LightGBM, S3 |
| **상용** | DART 공시 수집·분석 | DART Open API | DartCollector, LLM |
| **윤준호 + 태주** | 증권사 리포트 수집·가공 + Airflow 인프라 | 네이버 금융, 증권사 자체 사이트 | Python aiohttp, S3, LLM, Airflow |

## 윤준호 + 태주 파트 — 증권사 리포트 파이프라인

### 데이터 수집 전략

```
[네이버 금융] ──▶ 31개 증권사 리포트 (~37,000건, 2020~2026)
[한국투자증권] ──▶ 자체 사이트 수집기 (~30,000건, 2006~2026)
[신한투자증권] ──▶ 네이버 + 자체 사이트 병행
[LS증권]      ──▶ 보류 (로그인 + Eversafe 난독화)
```

### 메달리온 아키텍처

```
Bronze (S3)          Silver (S3)           Gold (S3 Parquet)          Delta (S3)
PDF 원본 + 메타  ──▶  텍스트 추출      ──▶  구조화 + 임베딩      ──▶  ACID MERGE
                                  (PyMuPDF)       (정규식 + Haiku)       (spark-submit)
```

#### Bronze — 완료
- `bronze/{증권사}/YYYY-MM-DD/{report_id}.pdf`
- 네이버: ~37,000건, 한국투자증권: ~30,000건

#### Silver — 완료
- `silver/{증권사}/YYYY-MM-DD/{report_id}.json` — 추출 텍스트 + 메타
- 구현: `extract_silver.py` (PyMuPDF, asyncio 병렬, 체크포인트)
- 2020~2026 총 51,294건 완료 (OCR 필요 3건)

#### Gold Structured — 완료
- `gold/structured/year={Y}/month={MM}/data.parquet` — 정규식 기반 구조화
- 추출 필드: 투자의견(90.8%), 목표주가(75.9%), 종목코드(87.5%), 현재주가, 상승여력, 실적추정
- 2020~2026 84개월 완료, 51,294건

#### Gold LLM — 완료
- `gold/embeddings/` — Haiku: reason/risks/keywords + multilingual-e5-small 임베딩 (384d)
- 2020~2026 84개월 완료

#### Delta Lake — 운영 중
- S3 `delta/gold_db/` 아래 3개 테이블: structured (75커밋), embeddings (61커밋), disclosure_events (1커밋)
- `spark_silver_to_delta.py` → systemd timer 매일 06:50 KST 자동 MERGE
- pyspark 4.0.3 + delta-spark 4.0.0, JDK 17

### ★/! 브리핑 (Phase 2 핵심)

2026-06-19 현재 composite score(a+b+c)는 폐지되었다. 대신:

| 신호 | 로직 |
|------|------|
| ★ | report ∩ model ∩ DART 1분기(pos\|정기보고서) → set intersection + boolean filter |
| ! | B-type DART events, sentiment ≠ neutral |

모든 판정은 set intersection + boolean consensus. Briefing Graph 자체는 scoring을 수행하지 않는다. 모델의 `ranking_score > 0`만 임계값 필터로 사용한다.

## 기술 스택

| 구성요소 | 선택 | 비고 |
|----------|------|------|
| 데이터 수집 | Python aiohttp | 비동기 병렬 페칭 |
| 스토리지 | AWS S3 + Delta Lake (Parquet 기반) | 메달리온 + ACID 트랜잭션 |
| 배치 처리 | Apache Spark (pyspark 4.0.3, local mode) | Delta MERGE용, EC2 호스트에서만 |
| 워크플로우 | Airflow 2.10.0 (CeleryExecutor, Docker) | 8개 DAG, 00:00 / 06:00 / 06:50 / 07:00 KST |
| LLM | Bedrock Claude (Haiku 3/4.5, Sonnet 4.6, Opus 4.8) | 챗봇 7 agent + Briefing 2 agent |
| 챗봇 서빙 | EC2 r6g.large (FastAPI + systemd) | FAISS 51,583 vectors, LangGraph 옵셔널 |
| 벡터 검색 | FAISS IndexIDMap + multilingual-e5-small | 384-dim, 코사인 유사도 |
| 메시징 | Telegram Bot API | 개인 DM (chat_id=6409771651), 매일 07:00 |

## 현재 상태 (2026-06-19)

| 레이어 | 상태 |
|--------|------|
| Bronze - 네이버 | 완료 (~37,000건) |
| Bronze - 한국투자증권 | 완료 (~30,000건) |
| Silver | 완료 (51,294건, 2020~2026) |
| Gold - Structured | 완료 (84개월, 2020~2026) |
| Gold - LLM | 완료 (84개월, Haiku 97% 커버리지) |
| Gold - DART disclosure_events | 10개월 (2025-06~2026-03), 2026-04~06 확장 필요 |
| Delta Lake | 운영 중 (systemd timer 06:50 KST) |
| Airflow DAG ×8 | 전부 unpaused, 자동 실행 |
| ★/! 브리핑 | E2E 검증 완료, Telegram DM 수신 확인 |
| 챗봇 (FAISS + 7 agent) | 운영 중, Opus 4.8 Analysis |
| Phase 2c (Prod Hardening) | Sentiment/DAG/Delta 안정화 완료, CloudWatch/Elastic IP 미진행 |

## 파일 구조 (2026-06-19 기준)

```
opik/
├── server/
│   ├── agents/                  # 11개 .py — 7 agent + Supervisor + BriefingGraph + DataHelper + Sentiment
│   │   ├── safety_agent.py
│   │   ├── intent_agent.py
│   │   ├── supervisor.py
│   │   ├── report_agent.py
│   │   ├── dart_agent.py
│   │   ├── analysis_agent.py
│   │   ├── response_composer.py
│   │   ├── briefing_graph.py
│   │   ├── dart_sentiment_agent.py
│   │   ├── data_helper.py
│   │   └── __init__.py
│   ├── prompts/                 # 3개 .md — 시스템/인텐트/응답 프롬프트
│   │   ├── system.md
│   │   ├── intent_parser.md
│   │   └── answer_generator.md
│   └── opik_server.py           # FastAPI /chat + /search 엔드포인트
├── dags/                        # Airflow DAG
│   ├── bronze/                  # Bronze×3 수집
│   ├── silver/                  # Silver 추출
│   ├── gold/                    # Gold×2 (structured + embeddings)
│   ├── model/                   # model_daily_prediction
│   └── briefing/                # opik_briefing
├── spark_jobs/
│   └── spark_silver_to_delta.py # Delta Lake MERGE (systemd timer)
├── src/model/                   # 찬호 LightGBM 모델 파이프라인 (57개 .py)
└── docs/                        # 설계 + 운영 문서
    ├── ARCHITECTURE.md           # 이 문서
    ├── AGENT_ARCHITECTURE.md     # Agent 상세 구현 명세
    ├── OPIK_PHASE2_OPERATIONS_STATUS.md  # 설계 대비 운영 현황
    ├── PHASE1_DESIGN.md          # Phase 1 설계
    ├── PHASE2_MULTIAGENT_DESIGN.md       # Phase 2 멀티에이전트 설계
    ├── DART_PIPELINE_DESIGN.md   # DART 파이프라인 설계 기록 (미구현)
    ├── CHATBOT_RESPONSE_POLICY.md        # 챗봇 응답 정책
    ├── DEVELOPMENT_LOG.md        # 개발 패턴 회고
    ├── CONTRIBUTING.md           # 기여 가이드
    ├── HOW_BRONZE_TO_SILVER_WORKS.md     # Bronze→Silver 상세
    └── HOW_SILVER_TO_GOLD_WORKS.md       # Silver→Gold 상세
```

## 관련 문서

- Agent 아키텍처: [AGENT_ARCHITECTURE.md](AGENT_ARCHITECTURE.md)
- 운영 현황: [OPIK_PHASE2_OPERATIONS_STATUS.md](OPIK_PHASE2_OPERATIONS_STATUS.md)
- Phase 2 설계: [PHASE2_MULTIAGENT_DESIGN.m