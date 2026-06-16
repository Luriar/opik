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

## 파일 구조

```
opik/
├── collectors/
│   ├── naver.py                    # 네이버 금융 수집기
│   └── koreainvest.py              # 한국투자증권 수집기
├── upload_naver.py                 # 네이버 → S3 Bronze
├── upload_koreainvest.py           # 한국투자증권 → S3 Bronze
├── extract_silver.py               # Bronze → Silver (PyMuPDF)
├── extract_gold_structured.py      # Silver → Gold Structured (정규식)
├── telegram_briefing.py            # Gold → 텔레그램 브리핑
├── spark_jobs/                     # Phase 2 Spark job (예정)
│   ├── spark_silver_to_delta.py
│   └── spark_compute_scores.py
├── dags/                           # Airflow DAG (예정)
│   └── nightly_batch.py
├── ARCHITECTURE.md                 # 전체 아키텍처
├── PHASE1_DESIGN.md                # Phase 1 설계
├── PHASE2_DESIGN.md                # Phase 2 설계 (EC2/Airflow/Spark/Delta)
├── DART_PIPELINE_DESIGN.md         # DART 공시 파이프라인 재설계
├── HOW_BRONZE_TO_SILVER_WORKS.md   # Bronze→Silver 상세
├── HOW_SILVER_TO_GOLD_WORKS.md     # Silver→Gold 상세
├── DEVELOPMENT_LOG.md              # 개발 패턴 회고
└── requirements.txt
```

## 개발 접근법

Build → Validate → Refine → Document → Design Next

일단 데이터를 만지고, 거기서 배운 걸로 설계합니다. 자세한 내용은 [DEVELOPMENT_LOG.md](DEVELOPMENT_LOG.md) 참고.

## 라이선스

MIT
