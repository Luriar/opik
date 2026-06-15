# OPIK Silver → Gold 파이프라인 설명

## 개요

Silver는 PDF에서 추출한 텍스트 JSON이다. Gold는 그 텍스트에서 의미 있는 구조화 정보를 뽑은 Parquet 파일이다.

Silver는 단순 텍스트 덩어리지만, Gold는 **투자의견, 목표주가, 종목코드, 현재주가, 상승여력**이 정형화된 분석 가능한 데이터다.

## S3 저장 구조

```
Silver (입력)                                  Gold (출력)
──────────────────────────────────     ─────────────────────────────────
silver/{증권사}/{날짜}/{id}.json  →    gold/structured/year={Y}/month={MM}/data.parquet
```

Silver 디렉터리 트리를 월별 Parquet 하나로 압축한다. 월별 파티셔닝으로 Spark 쿼리 효율을 확보한다.

> **왜 Parquet인가?** 51,294건의 JSON을 개별 파일로 두면 Spark/Hive에서 수만 번의 작은 읽기가 발생한다. 월별 Parquet는 컬럼 기반 압축(snappy)으로 쿼리 속도와 저장 효율을 동시에 잡는다. Parquet는 Phase 2 Spark 배치 처리의 기본 포맷이기도 하다.

## Gold Structured Parquet 스키마

```python
GOLD_SCHEMA = pa.schema([
    ("report_id",  pa.string()),     # Silver의 report_id 그대로
    ("증권사",     pa.string()),     # e.g. "미래에셋증권"
    ("종목명",     pa.string()),     # e.g. "삼성전자"
    ("종목코드",   pa.string()),     # 정규식 추출, 6자리 (e.g. "005930")
    ("발행일",     pa.string()),     # "2026-01-15"
    ("title",      pa.string()),     # 리포트 원제목
    ("source",     pa.string()),     # "naver" 또는 "koreainvest"
    ("text_len",   pa.int64()),      # 텍스트 길이 (디버깅/품질 지표)
    ("pages_total",pa.int64()),      # PDF 페이지 수
    ("투자의견",   pa.string()),     # 정규식 추출: BUY/HOLD/SELL/NOT_RATED/null
    ("목표주가",   pa.int64()),      # 정규식 추출, 원 단위 정수
    ("현재주가",   pa.int64()),      # 정규식 추출, 리포트 작성 시점 주가
    ("상승여력_pct",pa.float64()),   # 계산: (목표주가 - 현재주가) / 현재주가 * 100
    ("종목코드_list",pa.string()),   # JSON array, 멀티종목 리포트 대비
    ("실적추정_raw",pa.string()),    # JSON dict, 매출/영업이익 언급 (비정제)
])
```

## 추출 로직 — extract_from_silver()

Silver JSON 한 건을 입력받아 Gold 행 하나를 출력한다. `silver.text`를 정규식으로 분석하며, 추가 컨텍스트가 필요한 경우 title도 함께 사용한다.

```
extract_from_silver(silver_json)
  │
  ├─ extract_opinion(title + text[:2500])      → "BUY" / "HOLD" / "SELL" / "NOT_RATED" / null
  ├─ extract_target_price(title + text)        → int (원) / null
  ├─ extract_current_price(title + text)       → int (원) / null
  ├─ extract_stock_codes(title + text)         → list[str]
  ├─ extract_estimates(text[:5000])            → dict / null
  └─ if TP & CP valid → 상승여력_pct 계산
```

### 1. 투자의견 (Opinion)

4단계 분류 체계로 매핑한다. 리포트 헤드라인 성격을 띠는 title + text 앞 2500자에서 검색한다.

| 정규식 패턴 | 매핑 |
|------------|------|
| `매수 (유지/신규/강력/적극)?` | BUY |
| `BUY (유지/신규/Maintain)?` (Not/Under 선행 제외) | BUY |
| `Trading Buy`, `Strong Buy`, `Outperform` | BUY |
| `중립`, `보유`, `시장수익률` | HOLD |
| `NEUTRAL`, `HOLD`, `MARKET PERFORM`, `MARKET WEIGHT` | HOLD |
| `매도`, `비중축소` | SELL |
| `SELL`, `UNDERPERFORM`, `UNDERWEIGHT`, `REDUCE` | SELL |
| `Not Rated`, `NR`, `N/R`, `N.R`, `미제시` | NOT_RATED |

예외 처리:
- `코넥스기업 분석보고서` / `KONEX Research Project` → NOT_RATED
- `기술분석보고서` → NOT_RATED (투자의견 없음)

### 2. 목표주가 (Target Price) — 3단계 방어

TP 추출은 가장 복잡한 로직이다. 단순 숫자 매칭만 하면 PER, 목표 시가총액, 연도, 괴리율 등 false positive가 대량 발생하기 때문이다.

**Step 1 — 6가지 TP 패턴 매칭**

```
목표주가(12M): 280,000원  →  TP=280,000
적정주가: 150,000원       →  TP=150,000
목표가격: 85,000만원      →  TP=85,000 (* 10000 = 850,000)
TP(12M): 200,000원        →  TP=200,000
Target Price: 150,000원   →  TP=150,000
적정가치: 300만원         →  TP=300 (* 10000 = 3,000,000)
```

패턴은 OR로 연결되어 먼저 매칭된 것을 취한다. `만원` 접미사가 있으면 ×10000 한다.

**Step 2 — 컨텍스트 기각 (extract_target_price 내)**

매칭 전후 문맥을 읽어서:
- `목표주가 괴리율` / `변동추이` / `변동내역` / `평균` / `X 숫자` → 이전 TP 변경내역 테이블이므로 **skip**
- `달러`, `USD`, `달러기준`, `52주최저`, `52주최고` → 해외주식/차트 데이터이므로 **skip**
- 6자리 숫자가 0으로 시작하면 종목코드(e.g. 005930)로 판단 → **skip**
- 100 미만이면 **skip** (실제 TP가 100원인 종목은 존재하지 않음)

**Step 3 — 세부 컨텍스트 검증 (_validate_tp_context, 9개 레이어)**

추출된 숫자가 진짜 TP인지 주변 25자 문맥에서 검증:

| # | 조건 | 예시 (false positive) |
|---|------|----------------------|
| 1 | 뒤에 `년/개월/배/분기/일/월/주` | "12개월 목표주가"의 12 |
| 2 | 뒤에 `%` | "CB 100% 전환"의 100 |
| 3 | 뒤에 `M` (12M = 12개월) | "TP(12M)"의 12 |
| 4 | 앞에 `'` + `년/E` | "'24년 목표"의 24 |
| 5 | 뒤에 날짜 패턴 | "25.01.15" |
| 6 | 정확히 0 | 그래프 축 레이블 |
| 7 | 앞에 `X` 또는 `×` | "목표주가 X 100" |
| 8 | 뒤에 `\n숫자` (테이블 split) | "105\\n2,000" → 실제 TP=105,000 |
| 9 | 뒤에 `,;\n숫자` (변형 split) | "170,\\n700" → 실제 TP=170,000 |

9개 게이트를 모두 통과해야 TP로 인정된다.

### 3. 현재주가 (Current Price)

```
현재주가: 275,000원  →  CP=275,000
현재가: 85,000만원   →  CP=850,000 (fallback 패턴)
```

매칭 실패 시 null, 빈 문자열 캡처 시 null 반환.

### 4. 종목코드 (Stock Code)

8개 패턴을 순차 적용, 중복 제거하며 수집:

| 패턴 | 예시 |
|------|------|
| `(005930)` 괄호 내 6자리 | `삼성전자(005930)` |
| `005930 기업분석` / `종목분석` | 리포트 헤더 |
| `005930 / KOSPI` | 시장 구분 표기 |
| `KONEX: 123456` | 코넥스 역순 표기 |
| `005930.KS` / `005930 KQ` | 종목코드+시장접미사 |
| 단독 줄 `005930` | 헤드 영역 |
| `삼성전자 005930` | 종목명 바로 뒤 |
| `[005930]` 대괄호 래핑 | 일부 리포트 형식 |

년도 오인 방지: 1900~2099 범위의 숫자는 종목코드에서 제외.

### 5. 실적추정 (Estimates) — 실험적

매출액, 영업이익 언급을 정규식으로 캡처하지만, 단위(조/십억/억/백만)와 연도 매핑이 안 되어**raw로만 저장**한다. 향후 LLM Gold에서 정제 예정.

### 6. 상승여력

TP와 CP가 둘 다 있고 CP > 0일 때만 계산:
```
상승여력_pct = (목표주가 - 현재주가) / 현재주가 × 100
```

## 처리 성능

51,294건 전체 백필 기준:
- **2020-2022:** ~14,258건, 3분 내외
- **2023-2026:** ~37,036건, 평균 월당 2~4초
- **병렬처리:** asyncio + Semaphore(20), 200건 배치 단위
- **네트워크:** S3에서 JSON 개별 다운로드 → 메모리에서 정규식 처리 → 로컬 Parquet 생성 후 S3 업로드
- **캐싱:** `.silver_keys_cache.json`으로 S3 list 반복 방지 (~12초 절약)

## 실행 명령어

```bash
# 전체 백필 (처음 실행 시 오래 걸림)
python extract_gold_structured.py --workers 20

# 특정 연도
python extract_gold_structured.py --year 2026 --workers 20

# 특정 기간
python extract_gold_structured.py --start 2025-10-01 --end 2025-12-31 --workers 20

# 증권사별 샘플 테스트 (31개사 각 1건 → 추출률 사전 점검)
python extract_gold_structured.py --sample-firms

# 재실행 (기존 Parquet 덮어쓰기 + Silver 키 목록도 갱신)
python extract_gold_structured.py --start 2026-01-01 --end 2026-06-30 --force-refresh

# 건수만 확인
python extract_gold_structured.py --dry-run
```

## 최종 현황 (2026-06-13)

| 연도 | 건수 | Opinion | TP | Code | 특징 |
|------|------|---------|-----|------|------|
| 2020 | 4,903 | 97.4% | 77.7% | 91.0% | 네이버 위주, 한국투자증권 없음 |
| 2021 | 4,148 | 89.5% | 68.3% | 80.8% | 한국투자증권 유입 시작 |
| 2022 | 5,207 | 88.6% | 70.5% | 83.2% | |
| 2023 | 9,649 | 92.9% | 78.9% | 90.8% | |
| 2024 | 10,547 | 90.7% | 77.0% | 88.6% | |
| 2025 | 11,389 | 88.6% | 73.2% | 85.8% | 건수 최대 |
| 2026 | 5,451 | 88.6% | 74.9% | 89.2% | 6월까지 |
| **합계** | **51,294** | **90.8%** | **75.0%** | **87.5%** | |

TP false positive (500원 미만으로 잘못 추출): **17건 (0.044%)**

## TP 결측의 주요 원인

51,294건 중 12,823건의 TP가 null이다. 원인 분석:

| 원인 | 추정 건수 | 설명 |
|------|----------|------|
| 빈 파일 | ~2,400 | PDF 파싱 실패, text_len=0 |
| 해외주식 (USD) | ~1,500 | 한국투자증권 글로벌 리포트 |
| IR협의회 | ~1,140 | 기업설명회 요약, TP 미기재 |
| 숏노트 | ~990 | 1~2페이지 요약본 |
| Not Rated | ~400 | 코넥스/기술분석/신규 커버리지 중단 |
| 실제 TP 있으나 정규식 한계 | ~1,800 | 텍스트 품질 저하, 비표현 포맷 |
| 한국투자증권 기타 | ~4,600 | 섹터/전략/해외/채권 등 TP 비해당 |

2020년과 2023-2026년의 추출률 차이는 주로 한국투자증권 비중 차이에서 발생한다. 한국투자증권은 해외주식·섹터·전략·채권 리포트 비중이 높아 TP 추출률이 구조적으로 낮다(55%대).

## 결측치 처리 원칙

모든 추출 함수는 실패 시 **null을 그대로 저장**한다. LLM 추론이나 기본값 대체(default imputation)는 하지 않는다. 근거는 두 가지다:

**1. false positive가 false negative보다 위험하다.** 잘못된 TP 하나가 상승여력 계산과 스코어링을 오염시킨다. 회의론적 접근(skeptical extraction) — 애매하면 null.

**2. null 자체가 신호다.** TP=null + Opinion=BUY면 "매수 의견은 있는데 목표주가를 명시하지 않은 리포트"로 해석할 수 있다. 기본값을 채워넣으면 이런 구분이 사라진다.

필드별 null 의미:
| 필드 | null 의미 |
|------|----------|
| 투자의견=null | 텍스트에서 opinion 패턴을 전혀 찾지 못함. NOT_RATED와 다름 (NOT_RATED는 명시적으로 "의견 없음" 표기) |
| 목표주가=null | TP가 없거나, 정규식으로 추출 실패. 해외주식·섹터·IR 등 원천적으로 TP 미기재인 경우가 대부분 |
| 종목코드=null | 6자리 코드를 찾지 못함. 코넥스/기술분석/섹터 리포트에서 자주 발생 |
| 현재주가=null | "현재주가" 표기 없음 |
| 상승여력_pct=null | TP 또는 CP가 null이면 자동으로 null (계산 불가) |

각 필드는 독립적으로 추출된다. 종목코드가 null이어도 opinion과 TP는 추출을 시도하며, 역도 마찬가지다.

**미래에 LLM Gold가 도입돼도 이 원칙은 유지한다.** LLM은 완전히 새로운 필드(reason, risks, keywords)를 추가할 뿐, 정규식이 실패한 케이스를 소급해서 채워넣지 않는다. 정규식 파이프라인은 "확실한 것만 담는" 층으로 고정하고, LLM은 "텍스트 이해가 필요한" 층을 담당하는 **역할 분리** 구조다.

## LLM Gold — 다음 단계

현재 `extract_gold_structured.py`는 정규식만으로 작동하는 비용 제로 파이프라인이다. 75%의 TP 추출률을 넘어서려면 LLM이 필요하다.

계획된 LLM 추출 필드 (Claude Haiku, 건당 약 ₩1.5):
- **reason:** 종목별 핵심 논리 1~2문장
- **risks:** 리스크 요인 목록
- **keywords:** 핵심 키워드 5~10개
- **implied TP:** 밸류에이션 산식에서 역산 (정규식으로 못 잡은 케이스 보완)
- **multi-stock 분해:** 섹터 리포트에서 종목별 opinion/TP 분리

## 의존성

- Python: `boto3`, `pyarrow` (Parquet), `asyncio`
- S3: `s3-opik-bucket`, region `ap-northeast-2`
- AWS 자격증명: shared credentials file
