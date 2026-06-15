# OPIK — DART 공시 Bronze→Silver→Gold 파이프라인 재설계

## 문제 정의

상용님 파이프라인의 근본 문제는 하나로 수렴된다:

> **`request_hash` 기반 DONE 마킹은 DART API의 특성과 맞지 않는다.**

구체적 증상:
- `collect_job.status='DONE'`이 API 정상/NoData/빈리스트 세 가지를 구분 못 함
- `request_hash` 유니크 제약 → NoData였던 날짜는 영원히 재수집 불가
- `written_rows`, `file_size` 등 완전성 메타데이터가 job 테이블에 없음
- 백필과 실시간이 같은 코드를 공유 → DISCLOSURE_DOCUMENT 271K건 사고

## 설계 원칙

1. **rcept_no가 진짜 PK다.** DART가 발급하는 접수번호(14자리)는 API 호출과 무관하게 공시 그 자체의 식별자다. 이걸 축으로 삼으면 request_hash 기반의 모든 문제가 사라진다.
2. **상태는 공시 단위로 관리한다.** job이 아니라 disclosure가 상태를 갖는다.
3. **백필과 실시간은 경로가 다르다.** 목적이 다르면 코드도 다르다.
4. **레이어마다 검증 게이트를 둔다.** 각 레이어 진입 전에 하위 레이어의 완전성을 검사하고 통과한 것만 승격시킨다.

---

## 1. 전체 아키텍처

```
DART API (40,000 calls/day)
        │
        ▼
┌─ Bronze (S3: JSON) ───────────────────────────────────────────┐
│  Raw API response, rcept_no 단위                                │
│                                                                 │
│  bronze/dart/                                                   │
│  ├── disclosures/{rcept_dt}/{rcept_no}.json   ← 공시검색 결과    │
│  ├── financials/{corp_code}/{bsns_year}/{reprt_code}.json       │
│  ├── companies/{corp_code}.json               ← 기업개황         │
│  └── _states/{rcept_dt}/state.parquet         ← 상태 머신       │
│                                                                 │
│  상태: DISCOVERED → FETCHED → VERIFIED                          │
└────────────────────────────────────────────────────────────────┘
        │ VERIFIED only
        ▼
┌─ Silver (S3: Parquet) ────────────────────────────────────────┐
│  정제된 구조화 데이터, corp_code 기준 파티셔닝                      │
│                                                                 │
│  silver/dart/                                                   │
│  ├── corp_code={corp_code}/disclosures.parquet                  │
│  ├── corp_code={corp_code}/financials.parquet                   │
│  └── corp_code={corp_code}/structured.parquet                   │
│                                                                 │
│  스키마: 정규화된 필드명, null 처리, 날짜 정규화                     │
└────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─ Gold (S3: Parquet) ──────────────────────────────────────────┐
│  종목×일자 단위 b_score, OPIK Phase 2 입력                         │
│                                                                 │
│  gold/dart/                                                     │
│  ├── disclosure_scores/{기준일자}/data.parquet                   │
│  └── disclosure_scores/{기준일자}/detail.parquet  (디버깅용)      │
│                                                                 │
│  b_score: 규칙기반 우선 → LLM 보강                                │
└────────────────────────────────────────────────────────────────┘
        │
        ▼
   OPIK Phase 2: 0.4a + 0.3b + 0.3c → 텔레그램
```

## 2. Bronze — 상태 머신 기반 수집

### 2.1 상태 정의

기존의 이진(DONE/FAIL) 모델을 4단계 상태 머신으로 교체한다.

```
DISCOVERED  ──→  FETCHED  ──→  VERIFIED
     │               │              │
     └──→ (skip)     └──→ FAILED    └──→ (Silver 승격)
                         │
                    └──→ RETRY (재시도 큐)
```

| 상태 | 의미 | 진입 조건 |
|------|------|----------|
| `DISCOVERED` | 공시검색 API에서 rcept_no 발견. 아직 fetch 안 함. | discovery DAG |
| `FETCHED` | API 호출 완료, 응답 JSON을 S3에 저장. | detail collector |
| `VERIFIED` | JSON 파싱 성공 + 필수 필드 존재 + 빈 응답 아님. | verification step |
| `FAILED` | 최대 재시도 초과. 수동 개입 필요. | detail collector |
| `SKIPPED` | 의도적 스킵 (e.g. 펀드공시, 자산유동화 — 주식 영향 0). | discovery DAG |

### 2.2 상태 저장소

기존 PostgreSQL `collect_job` 테이블 대신 S3 Parquet + 인메모리 상태 관리.

```python
# bronze/dart/_states/{rcept_dt}/state.parquet
STATE_SCHEMA = pa.schema([
    ("rcept_no",      pa.string()),     # PK: DART 접수번호 14자리
    ("corp_code",     pa.string()),     # 고유번호 8자리
    ("corp_name",     pa.string()),     # 회사명
    ("stock_code",    pa.string()),     # 종목코드 6자리 (비상장은 null)
    ("report_nm",     pa.string()),     # 보고서명
    ("pblntf_ty",     pa.string()),     # 공시유형 (A/B/C/D/...)
    ("pblntf_detail_ty", pa.string()),  # 공시상세유형
    ("rcept_dt",      pa.string()),     # 접수일자 YYYYMMDD
    ("state",         pa.string()),     # DISCOVERED | FETCHED | VERIFIED | FAILED | SKIPPED
    ("fetch_status",  pa.string()),     # API status code ("000", "013", ...)
    ("fetch_at",      pa.string()),     # FETCHED 시각
    ("verify_at",     pa.string()),     # VERIFIED 시각
    ("retry_count",   pa.int64()),      # 재시도 횟수
    ("error_msg",     pa.string()),     # 마지막 에러 메시지
    ("s3_key",        pa.string()),     # Bronze JSON S3 경로
    ("file_size",     pa.int64()),      # Bronze JSON 파일 크기 (bytes)
    ("row_count",     pa.int64()),      # JSON 내 레코드 수 (검증용)
])
```

### 2.3 IDEMPOTENCY — request_hash 폐기

request_hash 대신 `(rcept_no, pblntf_ty)`를 멱등키로 사용한다.

- rcept_no는 DART가 발급하므로 API 호출 방식과 무관하게 일정하다.
- 같은 rcept_no가 DISCOVERED 상태면 FETCH를 시도한다.
- NoData(013)였던 rcept_no도 state=DISCOVERED 유지 → 다음 사이클에 재시도.
- VERIFIED 상태의 rcept_no는 다시 fetch하지 않는다.

### 2.4 검증 게이트 (FETCHED → VERIFIED)

```python
def verify_bronze(rcept_no: str) -> bool:
    """Bronze JSON이 완전한지 검증"""
    data = s3.get_object(Bucket=BRONZE_BUCKET, Key=f"bronze/dart/disclosures/{rcept_dt}/{rcept_no}.json")
    obj = json.loads(data["Body"].read())

    # Gate 1: HTTP 200 + DART status "000"
    if obj.get("status") != "000":
        if obj.get("status") == "013":
            state.update(rcept_no, state="SKIPPED", reason="API NoData")  # 의도적 빈 응답
        else:
            state.update(rcept_no, state="FAILED", error=f"DART status {obj.get('status')}")
        return False

    # Gate 2: 필수 필드 존재
    required = ["rcept_no", "corp_code", "corp_name", "report_nm", "rcept_dt"]
    if not all(k in obj for k in required):
        state.update(rcept_no, state="FAILED", error="Missing required fields")
        return False

    # Gate 3: 빈 list 아님 (NoData 013과는 다름 — status=000인데 list=[])
    if isinstance(obj.get("list"), list) and len(obj["list"]) == 0:
        state.update(rcept_no, state="FAILED", error="Empty list with status 000")
        return False

    # Gate 4: file_size > threshold (빈 파일 방지)
    actual_size = data["ContentLength"]
    if actual_size < 50:  # 50 bytes 미만이면 빈 응답
        state.update(rcept_no, state="FAILED", error=f"File too small: {actual_size} bytes")
        return False

    state.update(rcept_no, state="VERIFIED", file_size=actual_size, row_count=len(obj.get("list", [])))
    return True
```

## 3. Discovery — 백필과 실시간 분리

### 3.1 실시간 (incremental)

```
목적: 오늘 접수된 신규 공시를 빠짐없이 가져온다.
실행: 5분 간격 (07:30~20:00), max_active_runs=1
API: 공시검색 (무제한)
파라미터: bgn_de=today, end_de=today, corp_cls=Y+K (유가+코스닥만)
         pblntf_ty=all (전체 유형, 필터링은 state machine에서)
         page_count=100 (최대 페이지 크기)

처리:
  1. 공시검색 API로 오늘자 전체 공시 목록을 가져온다 (페이지네이션).
  2. rcept_no가 state.parquet에 없으면 DISCOVERED로 추가.
  3. 이미 VERIFIED/FAILED/SKIPPED 상태면 건너뛴다.
  4. 새로 DISCOVERED된 건들을 detail_collector 큐에 넣는다.
```

### 3.2 백필 (backfill)

```
목적: 과거 전체 공시 데이터를 빠르게 채운다.
실행: 야간 20:00~23:50 + 주말 전체, max_active_runs=1
API: 공시검색 (무제한)
파라미미터: bgn_de=2015-01-01 (DART 제공범위 시작), end_de=yesterday
         pblntf_ty=A+B+D (정기+주요사항+지분 — 발행/기타/펀드/유동화 제외)
         corp_cls=Y+K

특징:
  - 실시간 DAG와 완전히 분리된 코드 경로.
  - 남은 quota를 계산하면서 진행 (공시검색은 무제한이지만, 뒤따라오는
    detail_collector가 quota를 소비하므로 조절 필요).
  - frontier를 DB에 저장 → 중단/재개 가능.
  - 과거→최신 순으로 진행 (역순).
```

### 3.3 수집 대상 필터링

주식 투자에 영향 없는 공시유형은 discovery 단계에서 SKIPPED 처리:

| 공시유형 | 수집 | 근거 |
|----------|------|------|
| A (정기공시) | O | 사업/분기/반기보고서 — 재무제표 분석 |
| B (주요사항보고) | O | 유상증자, 감자, 계약체결 등 — 최대 impact |
| C (발행공시) | X | 증권신고서 — IPO/유상증자 실행 단계. B에서 이미 감지됨 |
| D (지분공시) | O | 임원·주주 지분 변동 — insider signal |
| E (기타공시) | X | 정정공시 등 — 원본 공시의 부속물. rcept_no로 원본 추적 가능 |
| F (외부감사) | X | 감사보고서 — A에 포함된 감사의견으로 충분 |
| G (펀드공시) | X | 주식 종목과 무관 |
| H (자산유동화) | X | 주식 종목과 무관 |
| I (거래소공시) | X | KRX 공시 — DART와 중복, 조회 전용 |
| J (공정위공시) | X | 기업결합 심사 — 예측 불가능한 정치적 이벤트 |

## 4. Silver — 회사 중심 구조화

### 4.1 Bronze → Silver 변환

상용님이 이미 설계한 `corp_code` 기준 파티셔닝을 유지한다. 이 구조는 RAG builder가 "기업 X의 전체 공시"를 한 번에 읽기에 최적화되어 있다.

```python
# silver/dart/corp_code={corp_code}/disclosures.parquet
SILVER_DISCLOSURES_SCHEMA = pa.schema([
    ("rcept_no",      pa.string()),
    ("corp_code",     pa.string()),
    ("corp_name",     pa.string()),
    ("stock_code",    pa.string()),
    ("report_nm",     pa.string()),
    ("rcept_dt",       pa.string()),   # YYYY-MM-DD 정규화
    ("pblntf_ty",     pa.string()),
    ("pblntf_detail_ty", pa.string()),
    ("is_correction", pa.bool_()),     # 정정공시 여부 (report_nm에서 파싱)
    ("is_closed",     pa.bool_()),     # 유동화/변경등록 최종본 여부
    ("summary",       pa.string()),    # 공시 요약 (LLM 추출, Gold에서 사용)
    ("raw_json",      pa.string()),    # 원본 JSON (필요시 디버깅)
])

# silver/dart/corp_code={corp_code}/financials.parquet  
SILVER_FINANCIALS_SCHEMA = pa.schema([
    ("corp_code",     pa.string()),
    ("bsns_year",     pa.string()),    # 사업연도
    ("reprt_code",    pa.string()),    # 보고서코드 (11011=사업, 11012=반기, 11013=분기, 11014=1분기)
    ("account_nm",    pa.string()),    # 계정명
    ("account_id",    pa.string()),    # 계정ID
    ("account_detail", pa.string()),   # 계정상세
    ("amount",        pa.int64()),     # 금액
    ("ord",           pa.int32()),     # 표시순서
])
```

### 4.2 Silver 승격 조건

```
1. Bronze state = VERIFIED
2. raw_json 파싱 성공 → disclosures.parquet 필드 추출
3. stock_code가 null이 아니면서 6자리인 것만 (비상장 제외 — b_score 산출 불가)
4. corp_code가 유효한 것 (고유번호 zip 대조)
```

## 5. Gold — b_score 산출

### 5.1 개요

OPIK Phase 2의 스코어링 공식에서 b_score의 역할:

```
종합점수 = 0.4a(찬호·주가예측) + 0.3b(상용·DART) + 0.3c(윤준호·리포트)
```

b_score는 **오늘 하루 동안 해당 종목에 발생한 모든 공시의 순 임팩트**를 -1.0(매우 부정) ~ +1.0(매우 긍정) 범위로 표현한다.

### 5.2 산출 방식: 규칙기반 → LLM 보강 (2-tier)

우리 OPIK 리포트 파이프라인과 동일한 철학: 규칙으로 확실한 것 처리, LLM은 맥락 이해가 필요한 것만.

```
Tier 1 — 규칙기반 (비용 0, 적용률 ~70%):
  공시유형 + detail_ty 조합으로 즉시 점수 매핑.
  e.g. "감자 결정" → -0.7, "자기주식 취득" → +0.3

Tier 2 — LLM 분석 (Haiku, 건당 ~$0.005, 적용률 ~30%):
  규칙만으로 점수를 매기기 어려운 복합 공시.
  e.g. "단일판매·공급계약 체결" — 계약금액이 매출의 몇 %인가?
       "최대주주 변경" — 우호세력인가 적대세력인가?
       "유상증자 결정" — 운영자금인가 시설투자인가 타개목적인가?
```

### 5.3 규칙기반 스코어링 테이블 (Tier 1)

```python
DISCLOSURE_IMPACT_RULES = {
    # === 주요사항보고 (B) ===
    ("B", "유상증자결정"): Score(-0.5, "지분 희석"),
    ("B", "무상증자결정"): Score(+0.3, "주주환원 시그널, 유동성 개선"),
    ("B", "유무상증자결정"): Score(-0.2, "유상+무상 혼합 — 유상분 할인율 따져야 함 → Tier 2"),
    ("B", "감자결정"): Score(-0.7, "자본잠식/재무악화 신호"),
    ("B", "부도발생"): Score(-1.0, "최대 리스크 이벤트"),
    ("B", "회생절차개시신청"): Score(-1.0, "사실상 투자원금 손실"),
    ("B", "해산사유발생"): Score(-1.0, "청산 리스크"),
    ("B", "단일판매공급계약체결"): Score(None, "계약금액·매출비중 분석 필요 → Tier 2"),
    ("B", "최대주주변경"): Score(None, "변경 사유·신규 주주 성격 분석 필요 → Tier 2"),
    ("B", "주권관련사채권양도결정"): Score(-0.2, "CB/BW 물량 부담"),

    # === 지분공시 (D) ===
    ("D", "임원주요주주소유보고"): Score(None, "매수/매도 구분 필요 → Tier 2"),
    ("D", "대량보유상황보고"): Score(None, "보유목적(경영참여/단순투자) 구분 필요 → Tier 2"),

    # === 정기공시 (A) ===
    ("A", None): Score(None, "실적 서프라이즈 판단 필요 → Tier 2"),
}
```

### 5.4 LLM 분석 프롬프트 (Tier 2)

```python
SYSTEM_PROMPT = """당신은 한국 주식시장의 DART 공시를 분석하는 금융 AI입니다.
오늘 접수된 공시 정보를 바탕으로 해당 종목의 일일 공시 임팩트 점수(b_score)를 산출하세요.

출력 JSON:
{
  "b_score": 0.0,      // -1.0(최악) ~ +1.0(최고)
  "reason": "한줄 요약",
  "impact_type": "원인 태그"  // e.g. "지분희석", "실적서프라이즈", "계약호재", "내부자매도"
}

스코어링 가이드:
- 단일판매·공급계약: 계약금액 vs 회사 연매출 비교. 10%↑ +0.5, 5~10% +0.3, 5%↓ +0.1
- 최대주주 변경: 경영참여 목적 우호세력이면 +0.2, 적대적M&A면 -0.3
- 임원 지분 매수: +0.2~0.4 (금액·지위 비례), 매도: -0.1~-0.3
- 실적(영업이익): 컨센서스 대비 ±10% → ±0.3, ±20% → ±0.5
- 여러 공시가 섞인 경우: 가장 큰 임팩트의 공시 기준, 나머지는 가중치 0.2로 합산
- 공시가 없는 종목은 처리하지 않음 (null 반환)"""
```

### 5.5 Gold Parquet 스키마

```python
# gold/dart/disclosure_scores/{기준일자}/data.parquet — OPIK Phase 2 입력
GOLD_SCORES_SCHEMA = pa.schema([
    ("stock_code",    pa.string()),     # 종목코드 6자리 (PK with 기준일자)
    ("corp_name",     pa.string()),     # 회사명
    ("b_score",       pa.float64()),    # -1.0 ~ +1.0
    ("disclosure_count", pa.int64()),   # 금일 공시 건수
    ("top_disclosure", pa.string()),    # 가장 큰 임팩트의 공시 report_nm
    ("score_source",  pa.string()),     # "rule" | "llm" | "mixed"
    ("기준일자",      pa.string()),     # YYYY-MM-DD (파티션 키)
])

# gold/dart/disclosure_scores/{기준일자}/detail.parquet — 디버깅/이력용
GOLD_DETAIL_SCHEMA = pa.schema([
    ("stock_code",    pa.string()),
    ("rcept_no",      pa.string()),
    ("report_nm",     pa.string()),
    ("pblntf_ty",     pa.string()),
    ("individual_score", pa.float64()),  # 해당 공시 개별 점수
    ("score_source",  pa.string()),
    ("reason",        pa.string()),
    ("기준일자",       pa.string()),
])
```

### 5.6 집계 로직

한 종목에 여러 공시가 있는 경우:

```python
def aggregate_b_score(disclosures: list[dict]) -> float:
    """여러 공시의 개별 점수를 종목 단위 b_score로 집계"""
    if not disclosures:
        return 0.0  # 공시 없음 = 중립

    scores = [d["individual_score"] for d in disclosures]

    # Rule: 가장 절대값이 큰 부정 이벤트가 dominant
    negative = [s for s in scores if s < 0]
    positive = [s for s in scores if s >= 0]

    if negative:
        # 한 개라도 -0.5 이하 부정 이벤트가 있으면 그게 주도
        dominant_neg = min(negative)
        if dominant_neg <= -0.5:
            return dominant_neg + 0.1 * sum(positive)  # 긍정 이벤트는 약하게 반영

        # 약한 부정 + 긍정 혼합 → 평균
        return sum(scores) / len(scores)

    # 긍정 only → 가중 평균 (큰 임팩트에 더 높은 가중치)
    weights = [abs(s) + 0.5 for s in positive]  # 최소 가중치 0.5
    return sum(s * w for s, w in zip(positive, weights)) / sum(weights)
```

## 6. OPIK Phase 2와의 연결

### 6.1 인터페이스

```
상용 DART 파이프라인 출력:
  S3: gold/dart/disclosure_scores/{YYYY-MM-DD}/data.parquet
  Delta Lake: dart.disclosure_scores (partition: 기준일자)

OPIK Phase 2 spark_compute_scores.py 입력:
  spark.read.format("delta")
    .load("s3a://s3-opik-bucket/delta/dart/disclosure_scores/")
    .filter(F.col("기준일자") == date)
```

### 6.2 DAG 조율

```
상용 Airflow:
  dag_dart_incremental_discovery  (07:30~20:00, 5분)
  dag_dart_detail_collector       (연속)
  dag_dart_raw_to_silver          (Silver 변환)
  dag_dart_silver_to_gold         (b_score 산출 — 15:30 데드라인)

OPIK Airflow:
  nightly_batch                   (16:00)
    wait_for_partners → gold/dart/disclosure_scores/{ds}/data.parquet 존재 확인
```

상용님 DAG가 15:30까지 b_score 생성을 마치면, OPIK DAG는 16:00에 아무 지연 없이 3-way 스코어링을 돌릴 수 있다.

### 6.3 비상용 CPU 비용 추정

| 항목 | 값 |
|------|-----|
| DART API (40,000건/일) | 0 (무료) |
| Silver 변환 (Pandas/PyArrow) | 0 (EC2 내) |
| LLM (Tier 2, 전체 공시의 ~30%) | 일 ~30건 × $0.005 = $0.15/일 |
| S3 Parquet 저장 (증분) | $0.5/월 |
| **추가 월 비용** | **~$5** |

## 7. 구현 우선순위

```
Phase 1: Bronze 재설계 (상용님 현재 작업)
  ├─ state.parquet 기반 상태 머신 구현
  ├─ rcept_no PK로 request_hash 제거
  ├─ verification gate 구현 (4단계)
  ├─ discovery: 실시간/백필 경로 분리
  └─ detail_collector: VERIFIED까지 자동 승격

Phase 2: Silver → Gold (우리가 도와줄 부분)
  ├─ Bronze VERIFIED → Silver 변환 자동화
  ├─ Tier 1 규칙기반 b_score 구현
  ├─ Tier 2 LLM b_score 구현 (Haiku)
  ├─ gold/dart/disclosure_scores Parquet 출력
  └─ OPIK Phase 2 wait_for_partners 연동 테스트

Phase 3: 정기공시(A) 심화 (추후 — 재무제표 분석)
  ├─ Silver financials 기반 실적 서프라이즈 감지
  ├─ 영업이익/매출 컨센서스 대비 변동률 → b_score 반영
  └─ LLM: 실적 코멘트 요약
```
