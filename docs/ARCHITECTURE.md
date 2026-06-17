# OPIK — AI 주식 브리핑 시스템 아키텍처

## 프로젝트 개요

한국 주식시장의 공시·리포트 데이터를 수집·가공하여 AI 기반 브리핑과 챗봇을 제공하는 시스템.
뉴스 같은 불확실한 데이터를 배제하고, 확실한 '공시'와 '리포트'에 집중하여 가치를 검증한다.

## 3단계 로드맵

### Phase 1 — 단방향 시황 브리핑 (MVP)
당일 DART 공시 + 증권사 신규 리포트가 나온 종목만 추출 → LLM이 평어로 요약 → 주가 예측 모델 → 브리핑 전달.

### Phase 2 — 일일 배치 스코어링, 추천, 양방향 QA 챗봇
장 마감 후 야간 배치로 종목별 종합 점수(a+b+c) 산출 → 장 시작 전 추천 종목 제공. 임베딩 기반 RAG 검색으로 증권사 리포트에 대한 양방향 QA 챗봇.

### Phase 3 — 실시간 AI 주식 비서
실시간 모니터링, 즉시 스코어링, 선제적 푸시 알림.

## 팀 역할 분담

| 팀원 | 담당 | 데이터 소스 | 기술 스택 |
|------|------|-----------|-----------|
| **찬호** | 주가 예측 모델 | 주가 데이터 | ML/DL, Spark |
| **상용** | DART 공시 수집·분석 | DART Open API | Spark, LLM |
| **윤준호 + 태주** | 증권사 리포트 수집·가공 | 네이버 금융, 증권사 자체 사이트 | Python aiohttp, S3, Spark, LLM |

## 윤준호 + 태주 파트 — 증권사 리포트 파이프라인

### 데이터 수집 전략

```
[네이버 금융] ──▶ 31개 증권사 리포트 (~37,000건, 2020~2026)
[한국투자증권] ──▶ 자체 사이트 수집기 (~30,000건, 2006~2026)
[LS증권]      ──▶ 보류 (로그인 + Eversafe 난독화)
```

네이버에서 신한투자증권·한국투자증권은 PDF 첨부가 안 되어 있어 자체 사이트에서 별도 수집.  
첫 백필은 5년치, 이후 일 단위로 자동화.

### 메달리온 아키텍처

```
Bronze (S3)          Silver (S3)           Gold (S3 Parquet)
PDF 원본 + 메타  ──▶  텍스트 추출      ──▶  구조화 + 임베딩
                                  (PyMuPDF)       (Spark + Haiku)
```

#### Bronze — 완료
- `bronze/{증권사}/YYYY-MM-DD/{report_id}.pdf`
- `bronze/{증권사}/YYYY-MM-DD/_manifest.json`
- 네이버: ~37,000건, 한국투자증권: ~30,000건

#### Silver — 완료
- `silver/{증권사}/YYYY-MM-DD/{report_id}.json` — 추출 텍스트 + 메타
- 구현: `extract_silver.py` (PyMuPDF, asyncio 병렬, 체크포인트)
- 2020~2026 총 51,294건 완료 (OCR 필요 3건)

#### Gold — Structured 완료, LLM 예정
- `gold/structured/year={Y}/month={MM}/data.parquet` — 정규식 기반 구조화 완료 (비용 제로)
- `gold/embeddings/` — Parquet (LLM 추출: 핵심 논리 요약, 리스크, 키워드, 임베딩) — 미구현
- 구현: `extract_gold_structured.py` (boto3 + PyArrow, asyncio 병렬 20 workers)
- Structured 추출 필드: 투자의견(90.8%), 목표주가(75.0%), 종목코드(87.5%), 현재주가, 상승여력, 실적추정(raw)
- 종목별 분리 추출 (섹터 리포트에서도 삼성전자 매수·하이닉스 중립 구분 가능)
- 상세: [HOW_SILVER_TO_GOLD_WORKS.md](HOW_SILVER_TO_GOLD_WORKS.md)

#### LLM 추출 필드 (Haiku, 건당 약 ₩1.5, 예정)
| 필드 | 내용 |
|------|------|
| reason | 종목별 핵심 논리 1~2문장 |
| risks | 리스크 요인 목록 |
| keywords | 핵심 키워드 5~10개 |

나머지(투자의견, 목표주가, 종목코드, 현재주가)는 정규식으로 공짜 추출 — 이미 Gold Structured에 적재 완료.

### 스코어링 (Phase 2 연동)

3개 스코어 중 **리포트 센티멘트 점수(c)** 에 Gold 데이터가 투입됨:
- 투자의견(매수/중립/매도) → 매핑 점수
- 목표주가 vs 현재주가 → 상승여력 점수
- LLM 추출 reason/risks → 텍스트 센티멘트 점수

a(주가 예측) + b(공시 임팩트) + c(리포트 센티멘트) = 종합 스코어 → Threshold 넘으면 추천.

## 기술 스택

| 구성요소 | 선택 | 비고 |
|----------|------|------|
| 데이터 수집 | Python aiohttp | 비동기 병렬 페칭 |
| 스토리지 | AWS S3 + Delta Lake (Parquet 기반) | 메달리온 + ACID 트랜잭션 |
| 배치 처리 | Apache Spark 3.5 (local mode) | Phase 2, Scala UDF 미사용 (PySpark) |
| 워크플로우 | Airflow 2.8 + SparkSubmitOperator | 장 마감 후 16:00 트리거 |
| LLM 추출/요약 | Claude Haiku (Python asyncio) | 비용 최적화, 건당 $0.005 |
| 챗봇 서빙 | EC2 (Spark + FastAPI) | Phase 3 |
| Vector DB | Delta Lake float array + Spark SQL | 코사인 유사도 = Spark SQL dot product |

## 예산

| 항목 | 예상 비용 |
|------|-----------|
| Haiku Gold 백필 (~67,000건) | ~12만원 (1회) |
| EC2 r6g.large 1년 예약 | ~50만원 (ARM64, 16GB RAM) |
| S3 스토리지 + Delta Lake | ~6만원/년 |
| Claude Haiku API | ~12만원/년 |
| **월 운영비** | 약 8.5만원 (Phase 2 full spec, Spark + LLM) |

## 파일 구조

```
opik/
├── collectors/
│   ├── naver.py              # 네이버 금융 수집기
│   └── koreainvest.py        # 한국투자증권 수집기
├── upload_naver.py           # 네이버 → S3 Bronze
├── upload_koreainvest.py     # 한국투자증권 → S3 Bronze
├── extract_silver.py               # Bronze → Silver
├── extract_gold_structured.py      # Silver → Gold Structured (정규식 추출)
├── extract_gold_llm.py             # Silver → Gold LLM (Haiku: reason/risks/keywords)
├── telegram_briefing.py            # Gold → 텔레그램 HTML 브리핑 (Structured + LLM 통합)
├── spark_jobs/
│   ├── spark_silver_to_delta.py    # Parquet → Delta Lake MERGE (Phase 2b)
│   └── spark_compute_scores.py     # 3-way JOIN + 종합 점수 계산 (Phase 2b)
├── dags/
│   └── nightly_batch.py            # Airflow DAG (16:00 daily, 8-task)
├── README.md                       # 프로젝트 개요, 로드맵, 설치
├── PHASE1_DESIGN.md                # Phase 1 전체 설계
├── PHASE2_DESIGN.md                # Phase 2 전체 설계 (EC2/Airflow/Spark/Delta)
├── DART_PIPELINE_DESIGN.md         # DART 공시 파이프라인 재설계
├── HOW_BRONZE_TO_SILVER_WORKS.md   # Bronze→Silver 상세
├── HOW_SILVER_TO_GOLD_WORKS.md     # Silver→Gold 상세
├── DEVELOPMENT_LOG.md              # 개발 패턴 회고
├── debug_pdf.py                    # PDF URL 도메인 진단
├── check_silver.py                 # Silver 적재 확인
├── check_silver_quality.py         # Silver 품질 검증
├── _batch_run.py                   # 배치 실행 스크립트
├── requirements.txt                # Python 의존성
└── ARCHITECTURE.md                 # 이 문서
```

## 현재 상태 (2026-06-15)

| 레이어 | 상태 |
|--------|------|
| Bronze - 네이버 | 완료 (~37,000건) |
| Bronze - 한국투자증권 | 완료 (~30,000건) |
| Bronze - LS증권 | 보류 (로그인 필요) |
| Silver | 완료 (51,294건, 2020~2026) |
| Gold - Structured | 완료 (51,294건, 정규식, Opinion 90.8% / TP 75.0% / Code 87.5%) |
| Gold - LLM | 완료 (72개월, Haiku: reason/risks/keywords/embedding 384d, 커버리지 97%) |
| 텔레그램 브리핑 | 완료 (Structured + LLM 통합 HTML 브리핑) |
| Phase 1 - 전체 파이프라인 | 완료 ([PHASE1_DESIGN.md](PHASE1_DESIGN.md)) |
| Phase 2 - 설계 | 완료 ([PHASE2_DESIGN.md](PHASE2_DESIGN.md)) |
| Phase 2 - Airflow DAG | 완료 ([dags/nightly_batch.py](dags/nightly_batch.py)) |
| Phase 2 - Spark jobs | 스켈레톤 완료 (Phase 2b EC2 배포 시 활성화) |
| DART - 설계 | 완료 ([DART_PIPELINE_DESIGN.md](DART_PIPELINE_DESIGN.md)) — 상용님 구현 중 |
