# OPIK Phase 2 — 설계 대비 운영 현황

2026-06-19 기준 | SSM 직접 검증 완료

---

## 1. 개요

Phase 2 설계문서(`PHASE2_MULTIAGENT_DESIGN.md`)는 LangGraph 멀티에이전트 아키텍처, ★/! 브리핑 파이프라인, Delta Lake MERGE, Production Hardening의 4단계 로드맵을 정의한다.

```
Phase 2a (Agent Framework)    ████████████████████  완료
Phase 2b (Briefing Redesign)  ████████████████████  완료
Phase 2c (Prod Hardening)     ██░░░░░░░░░░░░░░░░░░  Sentiment/DAG 안정화만 완료, 모니터링/Elastic IP/LLM failover 미진행
```

Phase 2a/2b는 설계 핵심 목표를 모두 달성했다. Phase 2c는 운영 안정화에 직결된 3개 항목(Sentiment v2, Telegram non-fatal, S3 date format fix)만 완료되었고, 인프라 하드닝(CloudWatch, Elastic IP, LLM failover)은 미진행 상태다.

---

## 2. 인프라 — 설계 대비 현황

| 항목 | 설계 | 실제 | 상태 |
|------|------|------|------|
| EC2 인스턴스 | r6g.large (2vCPU, 16GB) | r6g.large, 15GB RAM (10GB avail), 30GB disk (44% used) | ✅ 일치 |
| Docker | Airflow 2.10.0 + PG 13 + Redis 7.2 | Airflow 2.10.0 (CeleryExecutor), 6개 컨테이너 | ✅ 일치 |
| Spark | 설계 없음 | EC2 호스트에 pyspark 4.0.3 + delta-spark 4.0.0 + JDK 17 Corretto 설치됨. Docker 컨테이너 내부에는 없음. | ✅ (호스트 전용) |
| Python | 서버 3.9, 컨테이너 3.12 | 서버 3.9, 컨테이너 3.12.5 | ✅ 일치 |
| FAISS | 51,583 vectors, IndexIDMap | 51,583 vectors, intfloat/multilingual-e5-small | ✅ 일치 |
| uvicorn | FastAPI on 0.0.0.0:8000 | systemd opik-server.service | ✅ 일치 |
| Elastic IP | 설계됨 | 동적 IP | ❌ 미구현 |
| CloudWatch | 설계됨 | 없음 | ❌ 미구현 |

---

## 3. 에이전트 — 설계 대비 구현

설계문서의 7개 에이전트 + Supervisor + BriefingGraph 전부 구현 완료. 실제 EC2 `/home/ec2-user/airflow/opik/server/agents/`에 11개 .py 파일 존재 확인. 상세 아키텍처는 `AGENT_ARCHITECTURE.md` 참조.

| 에이전트 | 설계 모델 | 실제 모델 | 파일 | 상태 |
|----------|----------|----------|------|------|
| Safety Agent | Haiku (apac) | Haiku 3 (apac) | `safety_agent.py` | ✅ |
| Intent Parser | Haiku (apac) | Haiku 3 (apac) | `intent_agent.py` | ✅ |
| Report Agent | Haiku (apac) | Haiku 3 (apac) | `report_agent.py` | ✅ |
| DART Agent | Haiku (apac) | Haiku 4.5 (global) | `dart_agent.py` | ✅ 상향 |
| DART Sentiment | Haiku batch | Haiku 3 (apac) | `dart_sentiment_agent.py` | ✅ v2 안정화 |
| Analysis Agent | Sonnet | Opus 4.8 (global) | `analysis_agent.py` | ✅ 상향 |
| Response Composer | Sonnet | Sonnet 4.6 (global) | `response_composer.py` | ✅ v3 |
| Supervisor | — | routing logic | `supervisor.py` | ✅ |
| Briefing Graph | — | 9-step pipeline | `briefing_graph.py` | ✅ |

**설계 대비 변경점**: Analysis Agent Sonnet → Opus 4.8, DART interpret Haiku 3 → Haiku 4.5.

### 챗봇 4대 기능

| 기능 | 설계 | 실제 파일/메서드 | 검증 |
|------|------|------|------|
| 공시 의미 해석 | `dart_query + interpret` | `dart_agent.py` → `interpret_disclosure()` + `summarize_disclosure()` | ✅ |
| 리포트 비교 분석 | `report_search + compare` | `analysis_agent.py` → `compare_reports()` | ✅ |
| 업계/경쟁사 분석 | `report_search + sector` | `analysis_agent.py` → `industry_analysis()` | ✅ |
| 주가 움직임 원인 추적 | `report_search + cause_tracking` | `analysis_agent.py` → `trace_cause()` | ✅ |

### Prompts (EC2 실존 확인)

| 파일 | 줄 수 | 내용 | 상태 |
|------|------|------|------|
| `server/prompts/system.md` | 236줄 | Zone A/B/C/D + anti-hallucination 4개 + 대화맥락 + 컨텍스트 윈도우 관리 | ✅ |
| `server/prompts/intent_parser.md` | 146줄 | 5-intent taxonomy + `refers_to_previous` 플래그 + parameter extraction | ✅ |
| `server/prompts/answer_generator.md` | 208줄 | 응답 생성 템플릿 | ✅ (문서 미기재) |

---

## 4. 스케줄링 — 설계 대비 운영

### 4.1 Airflow DAG (8개)

EC2 `docker exec scheduler airflow dags list` 직접 확인.

| DAG ID | Schedule (KST) | 상태 | 비고 |
|--------|---------------|------|------|
| `opik_bronze_naver` | 0 0 * * * | ✅ unpaused | Naver 증권사 리포트 수집 |
| `opik_bronze_shinhaninvest` | 0 0 * * * | ✅ unpaused | 신한투자증권 수집 |
| `opik_bronze_koreainvest` | 0 0 * * * | ✅ unpaused | 한국투자증권 수집 |
| `opik_silver_extract` | 0 0 * * * | ✅ unpaused | 텍스트/메타데이터 추출 |
| `opik_gold_structured` | 0 0 * * * | ✅ unpaused | 정규식 기반 Structured 추출 |
| `opik_gold_embeddings` | 0 0 * * * | ✅ unpaused | FAISS 임베딩 생성 |
| `model_daily_prediction` | 0 6 * * * | ✅ unpaused | LightGBM 348종목 예측 |
| `opik_briefing` | 0 7 * * * | ✅ unpaused | ★/! 브리핑 → Telegram |

첫 자동 실행: 2026-06-20 06:00 KST (model) + 07:00 KST (briefing).

### 4.2 Systemd Timer — Delta MERGE (신규)

Spark는 Docker 컨테이너 내부에 없고 EC2 호스트에만 설치되어 있어, DAG 대신 systemd timer로 운영한다. **2026-06-19에 등록 완료.**

| 항목 | 값 |
|------|-----|
| 타이머 | `spark-delta-merge.timer` |
| 실행 시각 | 매일 06:50 KST (21:50 UTC) — model DAG 완료 후, briefing 직전 |
| 실행 스크립트 | `/home/ec2-user/spark_jobs/run_delta_merge.sh` → `spark_silver_to_delta.py --date` |
| Spark 설정 | local[4], driver-memory 6g, pyspark 4.0.3 + delta-spark 4.0.0 |
| 대상 테이블 | structured (PK: report_id), embeddings (PK: report_id), disclosure_events (PK: rcept_no) |
| 수동 테스트 | `run_delta_merge.sh 20260618` — exit code 0, 정상 동작 확인 |

---

## 5. Briefing Pipeline — 설계 대비 구현

### 5.1 9단계 파이프라인 (E2E 검증 완료)

| Step | 실제 동작 | 6/17 검증 결과 |
|------|----------|---------------|
| 1. Gold Structured | `gold/structured/` S3 → structured 리포트 | 19 rows |
| 2. Gold LLM | `gold/embeddings/` S3 + LEFT JOIN | 298 rows |
| 3. DART Events | `gold/dart/disclosure_events/` 월별 | 2,048 rows (2026-03) |
| 4. DART Sentiment | Haiku batch 25건/호출 | 43 batches, 0 failures |
| 5. Model Predictions | `gold/model/predictions/dt=YYYY-MM-DD/` | 348 stocks |
| 6. ★ Triple Consensus | report ∩ model ∩ DART 1분기 | ★ 0 (해당일 리포트 부재, 로직 정상) |
| 7. ! Major Disclosures | B-type, sentiment ≠ neutral | ! 0 (DART Gold 최근월 부재) |
| 8. Compose Briefing | template/Sonnet 2-tier | 245 chars |
| 9. Telegram | Bot API | DM 수신 확인 (chat_id=6409771651) |

### 5.2 Briefing DAG 실행 방식

설계 결정대로 **단일 PythonOperator로 `briefing_graph.py` 9단계 전체를 in-process 실행**한다. Spark composite score(a+b+c)는 폐지되었고, ★/!는 set intersection + boolean consensus로 판정한다. briefing_graph 자체에는 scoring 로직이 없고, 모델의 `ranking_score`를 `> 0` 임계값 필터로만 사용한다.

### 5.3 ★/! 필터 기준 (v3, 현재)

| 신호 | 기준 | 결과 (3/19) |
|------|------|------------|
| ★ | report ∩ model ∩ DART 1분기 (positive or 정기보고서) | 3종목 |
| ! | B-type DART events, sentiment ≠ neutral | 3건 |

---

## 6. 모델 파이프라인 (찬호)

### 6.1 배포 구성

| 항목 | 실제 |
|------|------|
| 코드 | `src/model/` 57개 .py 파일 (pipeline, features, data, execution, backtest) |
| 데이터 | 7개 parquet/csv, features max date = 2026-06-17 |
| 모델 | LightGBM 4.6.0, 55 features, 250-day rolling window |
| DAG | `model_daily_prediction` (06:00 KST) |
| 출력 | `s3://s3-opik-bucket/gold/model/predictions/dt={YYYY-MM-DD}/predictions.parquet` |
| Gold 컬럼 | prediction_date, ticker, ticker_name, ranking_score, pred_close_price |

### 6.2 검증

- 수동 실행: 20260617 → 86,928 training rows, 348 predictions, S3 업로드 완료 ✅
- DAG unpaused, 첫 자동 트리거: 2026-06-20 06:00 KST 대기 중
- S3 date format fix: briefing_graph.py에서 YYYYMMDD → YYYY-MM-DD 변환 적용 완료

---

## 7. 데이터 — 설계 대비 현황

### 7.1 S3 Gold Parquet

| Prefix | 실제 파티션 | 비고 |
|--------|-----------|------|
| `gold/structured/` | **year=2020 ~ year=2026** (84개월) | ✅ 설계 수준 충족 |
| `gold/embeddings/` | **year=2020 ~ year=2026** (84개월) | ✅ 설계 수준 충족 |
| `gold/dart/disclosure_events/` | **dt=2025-06 ~ dt=2026-03** (10개월) | ⚠️ 2026-04~06 부재 |
| `gold/model/predictions/` | dt=2026-06-17 (348종목) | ✅ |
| `gold/dart/_done/` | 55,654 objects | ✅ |

### 7.2 Delta Lake

EC2에서 2026-06-18 backfill 실행으로 이미 구축된 상태.

| Delta 테이블 | S3 경로 | PK | 커밋 수 | 상태 |
|-------------|---------|-----|---------|------|
| structured | `delta/gold_db/structured/` | report_id | 75 | ✅ |
| embeddings | `delta/gold_db/embeddings/` | report_id | 61 | ✅ |
| disclosure_events | `delta/gold_db/disclosure_events/` | rcept_no | 1 | ⚠️ 거의 비어있음 |

2026-06-19부터 systemd timer로 매일 06:50 KST에 MERGE 자동화됨.

### 7.3 DART Gold 공백

S3에 존재하는 disclosure_events 파티션: `dt=2025-06`, `07`, `08`, `09`, `10`, `11`, `12`, `2026-01`, `02`, `03` — 총 10개월.

briefing_graph.py의 `load_dart_events()`는 오늘 기준 92일 전부터 월별 Parquet를 스캔하므로, 2026-06-19 기준으로는 2026-03만 92일 윈도우에 들어간다. `dt=2026-04`, `2026-05`, `2026-06` 파티션이 없어서 오늘 날짜 DART 이벤트가 0건이다.

| 구간 | 상태 | 영향 |
|------|------|------|
| 2025-06 ~ 2025-12 | S3에 존재, 92일 윈도우 밖 | ★ 필터에서 미사용 |
| 2026-01 ~ 2026-03 | S3에 존재, 2026-03만 윈도우 내 | 부분 사용 |
| 2026-04 ~ 2026-06 | **S3에 없음** | ! today=0건, ★ 1분기 윈도우 불완전 |

---

## 8. Phase 2c — Production Hardening

| 항목 | 설계 | 상태 |
|------|------|------|
| Sentiment JSON 안정화 | error handling 개선 | ✅ v2 완료 (43배치 0실패) |
| DAG Telegram non-fatal | 오류 graceful 처리 | ✅ 완료 |
| S3 date format fix | YYYYMMDD → YYYY-MM-DD | ✅ 완료 |
| Systemd timer for Delta | 설계 없음 | ✅ 2026-06-19 신규 등록 |
| CloudWatch 메트릭 + 알람 | DAG 실패, Bedrock throttle, disk | ❌ |
| EC2 Elastic IP | 고정 IP | ❌ |
| LLM failover | Haiku throttle → Sonnet | ❌ |
| deploy.sh SSM 통합 | 단일 배포 스크립트 | ❌ (S3 경유 수동 배포) |
| CHATBOT_RESPONSE_POLICY.md | v2 프롬프트 검토 | ❌ |

---

## 9. 데이터 파이프라인 타임라인 (KST)

```
00:00  Airflow DAG ×6    Bronze×3 → Silver → Gold×2            ✅
02:00  완료               Gold structured + embeddings 생성 완료
06:00  model_daily_pred.  LightGBM 348종목 예측 → S3            ✅ (6/20 첫 자동)
06:50  systemd timer      Spark Delta MERGE (structured/emb./disc.) ✅ (신규)
07:00  opik_briefing      9-step ★/! → Telegram DM              ✅
09:00  장 시작
상시   opik-server        /chat 챗봇 (FAISS + 7 agent)          ✅
```

---

## 10. LLM 모델 배정 최종

| Agent | 모델 | Inference Profile |
|-------|------|-------------------|
| Safety | Haiku 3 | `apac.anthropic.claude-3-haiku` |
| Intent | Haiku 3 | `apac.anthropic.claude-3-haiku` |
| Report | Haiku 3 | `apac.anthropic.claude-3-haiku` |
| DART interpret | Haiku 4.5 | `global.anthropic.claude-haiku-4-5` |
| DART summarize | Sonnet 4.6 | `global.anthropic.claude-sonnet-4-6` |
| DART Sentiment | Haiku 3 (apac) | `apac.anthropic.claude-3-haiku` |
| Analysis | Opus 4.8 | `global.anthropic.claude-opus-4-8` |
| Response | Sonnet 4.6 | `global.anthropic.claude-sonnet-4-6` |

---

## 11. 남은 과제

### 중요 (1주 이내)
1. **DART Gold 확장** — 2026-04, 05, 06월 3개월치 DartCollector ETL + Gold builder 실행. ! today 이벤트와 ★ 1분기 DART 윈도우가 정상화됨. S3에는 이미 2025-06~2026-03 10개월치 존재.
2. **6/20 첫 자동 실행 모니터링** — 06:00 model DAG + 06:50 Delta MERGE + 07:00 briefing DAG 전 구간 최초 자동 실행 확인.

### Phase 2c
3. CloudWatch 메트릭 + 알람
4. EC2 Elastic IP
5. LLM failover (Haiku throttle → Sonnet fallback)
6. CHATBOT_RESPONSE_POLICY.md 업데이트

### 개선
7. deploy.sh SSM 통합 (현재 S3 경유 수동 배포)
8. disclosure_events Delta 테이블 확장 (현재 1개 커밋만 존재, backfill 필요)

### 보류
9. Partner 데이터 연동 (설계만 존재)
10. DartCollector EC2 공동 배포
11. Python 3.9 → 3.10+ 업그레이드

---

## 12. 파일 구현 현황 — 설계 대비

| 설계 파일 | 설계 변경 | 실제 | 상태 |
|----------|----------|------|------|
| `opik_server.py` | LangGraph 연동, Delta 우선 읽기 | FAISS + agent 연동 (Delta 폴백) | 부분 구현 |
| `spark_compute_scores.py` | **삭제** | 삭제 완료 | ✅ |
| `telegram_briefing.py` | Briefing Graph 기반 재작성 | `briefing_graph.py` + `daily_briefing.py` | ✅ |
| `spark_silver_to_delta.py` | 신규 (Delta MERGE) | EC2 `/home/ec2-user/spark_jobs/` 264줄, systemd timer 등록 완료 | ✅ 운영화 |
| `prompts/system.md` | Multi-agent 역할 체계 반영 | 236줄, EC2 실존 | ✅ |
| `prompts/intent_parser.md` | `refers_to_previous` 플래그 | 146줄, EC2 실존 | ✅ |
| `prompts/answer_generator.md` | 설계서 미기재 | 208줄, EC2 실존 | ✅ |
| `requirements.txt` | langgraph, langchain 추가 | EC2 수동 설치 | ✅ |
| `deploy.sh` | SSH → SSM 기반 | 미구현 (S3 경유 수동 배포) | ❌ |

설계 신규 파일 10개 중 9개 구현 완료, 1개(Spark Delta MERGE)는 2026-06-19부로 운영화 완료. `deploy.sh`만 미구현.

---

## 13. 요약

OPIK Phase 2 운영 현황:

- **멀티에이전트**: 7개 에이전트 + Supervisor routing, LangGraph StateGraph 위에서 FAISS 51,583건과 연동하여 작동 중
- **★/! 브리핑**: 9단계 파이프라인 자동화, 매일 07:00 KST Telegram DM 발송. 6/17 E2E 검증 완료.
- **찬호 모델**: LightGBM 일일 예측이 S3 Gold로 출력되어 triple consensus에 반영. 6/20 첫 자동 실행 대기.
- **Delta Lake**: S3에 3개 테이블 구축 완료 (structured 75커밋, embeddings 61커밋). systemd timer로 매일 06:50 MERGE 자동화 등록.
- **Sentiment v2**: Haiku batch 분류 0실패 (이전 31% 실패율 → 완전 해결)

DART Gold 2026-04~06월 3개월치만 확장되면 ★/! 브리핑이 설계 의도대로 완전 작동한다. Phase 2c 하드닝(CloudWatch, Elastic IP, LLM failover)은 아직 미진행 상태다.
