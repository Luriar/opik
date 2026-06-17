# OPIK Phase 2 설계 — 일일 배치 스코어링 및 추천

## 1. Phase 2 정의

Phase 1(현재)은 데이터 수집과 정규식 구조화까지 완료된 상태다. Phase 2의 목표는:

> **장 마감 후 야간 배치로 종목별 종합 점수를 산출하고, 다음날 장 시작 전 추천 종목을 텔레그램으로 제공 + 임베딩 기반 RAG 검색으로 증권사 리포트에 대한 양방향 QA 챗봇 구축**

```
Phase 1 (완료):     수집 → Bronze → Silver → Gold Structured (정규식)
Phase 2 (진행 중):  Gold LLM → 스코어링 → 추천 → 텔레그램 전송 → RAG QA 챗봇
Phase 3 (미래):     실시간 모니터링, 즉시 스코어링, 선제적 푸시 알림
```

## 2. Spark 선정 근거

Phase 1에서는 Python 단일 머신(Pandas/PyArrow)으로 충분했지만, Phase 2에서는 Apache Spark를 도입한다.

### 왜 Spark인가

| 이유 | 설명 |
|------|------|
| **Delta Lake** | ACID 트랜잭션, Time Travel(과거 스냅샷 복원), Schema Evolution. 매일 쌓이는 Gold 데이터의 버전 관리와 롤백이 필수적이다. |
| **Spark SQL** | 코사인 유사도, 윈도우 집계, 멀티테이블 JOIN을 SQL로 표현 가능. 추후 Vector Search를 별도 DB 없이 Spark SQL float array 연산으로 처리한다. |
| **스케일 아웃** | 현재 51,294건이지만 찬호의 ML 피처 생성, 상용의 DART 전수 분석, 추후 실시간 스트리밍(Phase 3)으로 데이터 볼륨이 수십 배 증가할 때 Spark는 클러스터만 늘리면 선형 확장된다. |
| **통합 프레임워크** | 수집부터 스코어링, LLM 배치 추론, 텔레그램 전송까지 단일 런타임에서 E2E 파이프라인을 구성한다. 각 단계가 Pandas/Boto3/asyncio로 분산된 Phase 1과 달리 디버깅과 모니터링이 일원화된다. |
| **팀 협업** | 찬호(ML/Spark), 상용(Spark/LLM)과 동일 기술 스택으로 데이터 교환이 매끄럽다. Pandas DataFrame을 S3 Parquet로 주고받는 대신 Delta Lake 테이블을 공유한다. |

### Spark ↔ Pandas 역할 분리

Spark를 도입하지만 Pandas를 완전히 대체하지는 않는다. 역할을 나눈다:

```
Spark (분산 배치):
  - S3 → Delta Lake ETL
  - 월별/연도별 집계, 윈도우 함수
  - 멀티테이블 JOIN (Gold + Predictions + DART)
  - 코사인 유사도 (float array 연산)
  - Delta Lake 타임트래블, VACUUM, OPTIMIZE

Pandas (단일 머신):
  - PDF → 텍스트 변환 (PyMuPDF) — Phase 1과 동일
  - 정규식 추출 (extract_gold_structured.py) — Phase 1과 동일
  - LLM API 호출 및 응답 파싱
  - 텔레그램 메시지 포맷팅 및 전송
```

## 3. 일일 처리량 기준

| 지표 | 값 |
|------|-----|
| 일평균 리포트 | 22건 (분기말 12건 ~ 성수기 34건) |
| 피크일 | ~48건 (2025-07, 2026-04) |
| 설계 기준 | **50건/일** (피크의 2배 여유) |
| 증권사 수 | 31개사 |
| 커버 종목 | ~2,000종목 |
| 장 마감 | 15:30 KST |
| 배치 시작 | 16:00 KST |
| 추천 완료 목표 | 20:00 KST (장 시작 13시간 전) |

## 4. 전체 아키텍처

```
┌──────────────────────────────────────────────────────────────────────┐
│                     EC2 r6g.large (2vCPU, 16GB RAM, ARM64)           │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │              Airflow (Docker) + Spark 3.5 (Local Mode)          │ │
│  │                                                                 │ │
│  │  16:00  [DAG: nightly_batch]                                    │ │
│  │    │                                                            │ │
│  │    ├─ Task 1: collect_reports (Python)                          │ │
│  │    │   ├─ upload_naver.py --date {{ds}}                         │ │
│  │    │   └─ upload_koreainvest.py --date {{ds}}                   │ │
│  │    │   → S3: bronze/{증권사}/{날짜}/*.pdf + _manifest.json      │ │
│  │    │                                                            │ │
│  │    ├─ Task 2: extract_silver (Python)                           │ │
│  │    │   └─ extract_silver.py --start {{ds}} --end {{ds}}         │ │
│  │    │   → S3: silver/{증권사}/{날짜}/*.json                      │ │
│  │    │                                                            │ │
│  │    ├─ Task 3: extract_gold_structured (Python)                  │ │
│  │    │   └─ extract_gold_structured.py --start {{ds}}             │ │
│  │    │   → S3: gold/structured/year={Y}/month={M}/data.parquet   │ │
│  │    │                                                            │ │
│  │    ├─ Task 4: extract_gold_llm (Python)                         │ │
│  │    │   └─ extract_gold_llm.py --date {{ds}}                     │ │
│  │    │   LLM: Claude Haiku (Anthropic API)                        │ │
│  │    │   → S3: gold/llm/year={Y}/month={M}/data.parquet          │ │
│  │    │                                                            │ │
│  │    ├─ Task 5: silver_to_delta (Spark)          ★ Spark 진입점   │ │
│  │    │   └─ spark_silver_to_delta.py --date {{ds}}                │ │
│  │    │   → Delta Lake: gold_db.structured, gold_db.llm            │ │
│  │    │   Spark가 Parquet를 읽어 Delta Lake 테이블에 MERGE          │ │
│  │    │                                                            │ │
│  │    ├─ Task 6: wait_for_partners (Sensor)                        │ │
│  │    │   ├─ Delta: predictions.stock_scores (찬호)                 │ │
│  │    │   └─ Delta: dart.disclosure_scores (상용)                   │ │
│  │    │                                                            │ │
│  │    ├─ Task 7: compute_recommendations (Spark)                   │ │
│  │    │   └─ spark_compute_scores.py --date {{ds}}                 │ │
│  │    │   → Delta Lake: recommendations.daily_picks                │ │
│  │    │   Spark SQL로 3-way JOIN + 윈도우 집계 + 점수 계산          │ │
│  │    │                                                            │ │
│  │    └─ Task 8: deliver_telegram (Python)                         │ │
│  │         └─ telegram_briefing.py --date {{ds}}                   │ │
│  │         → Telegram: OPIK 브리핑 채널                             │ │
│  │                                                                 │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
│  의존성: Docker, Python 3.10, PySpark 3.5, Delta Lake 3.0,           │
│          PyMuPDF, boto3, anthropic, requests, apache-airflow          │
└──────────────────────────────────────────────────────────────────────┘

                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                           AWS Services                                │
│                                                                      │
│  S3 (s3-opik-bucket)            Delta Lake (S3-backed)               │
│  ├─ bronze/                     ├─ gold_db.structured/               │
│  ├─ silver/                     ├─ gold_db.llm/                      │
│  ├─ gold/structured/            ├─ gold_db.embeddings/               │
│  ├─ gold/llm/                   ├─ predictions.stock_scores/  ←찬호  │
│  ├─ gold/embeddings/            ├─ dart.disclosure_scores/    ←상용  │
│  ├─ predictions/    ← 찬호      └─ recommendations.daily_picks/      │
│  ├─ dart/           ← 상용                                          │
│  └─ recommendations/           Secrets Manager                      │
│                                 ├─ opik/anthropic (API key)          │
│  CloudWatch                     ├─ opik/telegram (bot token)        │
│  ├─ Log groups                  └─ opik/aws (access key)            │
│  └─ Metrics + Alarms                                                │
│                                                                      │
│  EventBridge (Phase 3: 실시간 트리거)                                 │
└──────────────────────────────────────────────────────────────────────┘
```

## 5. Delta Lake 테이블 설계

### 5.1 왜 Delta Lake인가

Parquet만으로는 부족한 이유:

| Delta Lake 기능 | 필요한 이유 |
|----------------|-----------|
| **ACID 트랜잭션** | 일일 50건 INSERT + UPDATE가 동시에 발생. Parquet는 원자적 쓰기를 보장하지 않아 Spark Job 실패 시 파일이 깨질 수 있다. |
| **Time Travel** | `VERSION AS OF`로 과거 스냅샷 쿼리. "어제 추천과 오늘 추천이 어떻게 달라졌나"를 한 줄 SQL로 비교 가능. |
| **MERGE (Upsert)** | 같은 report_id가 재처리될 때 INSERT OR UPDATE를 단일 오퍼레이션으로 처리. |
| **Schema Evolution** | LLM 필드 추가, Partner 스키마 변경 시 ALTER TABLE로 대응. Parquet는 스키마 불일치 시 전체 재작성 필요. |
| **OPTIMIZE + ZORDER** | 쿼리 패턴에 맞춰 파일 레이아웃 최적화. `ZORDER BY (종목코드)`로 종목별 필터링 시 수백 배 빠른 프루닝. |
| **VACUUM** | 오래된 Parquet 스냅샷 파일 정리로 스토리지 비용 관리. |

### 5.2 Delta 테이블 스키마

```python
# === gold_db.structured (Gold Structured, Phase 1 정규식 결과) ===
CREATE TABLE gold_db.structured (
    report_id       STRING NOT NULL,
    증권사          STRING,
    종목명          STRING,
    종목코드        STRING,
    발행일          STRING,
    title           STRING,
    source          STRING,
    text_len        BIGINT,
    pages_total     BIGINT,
    투자의견        STRING,
    목표주가        BIGINT,
    현재주가        BIGINT,
    상승여력_pct    DOUBLE,
    종목코드_list   STRING,
    실적추정_raw    STRING
) USING DELTA
PARTITIONED BY (발행일)  -- 일별 파티셔닝 (월별이었던 Parquet에서 변경)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- === gold_db.llm (Phase 2 LLM Gold) ===
CREATE TABLE gold_db.llm (
    report_id        STRING NOT NULL,
    발행일            STRING,          -- partition column (must be declared explicitly)
    reason           STRING,
    risks            STRING,          -- JSON array
    keywords         STRING,          -- JSON array
    sentiment_score  DOUBLE,          -- -1.0 ~ +1.0
    implied_tp       BIGINT,          -- LLM 역산 목표주가 (추후 Phase 2c scoring 사용, 현재 단계에서는 미사용)
    target_quarter   STRING,          -- 실적추정 기준분기 (추후 Phase 2c scoring 사용, 현재 단계에서는 미사용)
    llm_model        STRING,
    llm_tokens_in    BIGINT,
    llm_tokens_out   BIGINT
) USING DELTA
PARTITIONED BY (발행일)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- === gold_db.embeddings (추후 Vector Search 용) ===
CREATE TABLE gold_db.embeddings (
    report_id    STRING NOT NULL,
    embedding    ARRAY<FLOAT>,     -- 1536-dim (embedding-small)
    chunk_idx    INT               -- 멀티청크 인덱스
) USING DELTA;

-- === predictions.stock_scores (찬호 — 주가 예측) ===
CREATE TABLE predictions.stock_scores (
    종목코드      STRING NOT NULL,
    종목명        STRING,
    현재주가      BIGINT,
    a_score       DOUBLE,          -- -1.0 ~ +1.0
    confidence    DOUBLE,          -- 0.0 ~ 1.0
    pred_return   DOUBLE,          -- 예측 수익률 (%)
    기준일자      STRING
) USING DELTA
PARTITIONED BY (기준일자);

-- === dart.disclosure_scores (상용 — 공시 임팩트) ===
CREATE TABLE dart.disclosure_scores (
    종목코드      STRING NOT NULL,
    종목명        STRING,
    b_score       DOUBLE,          -- -1.0 ~ +1.0
    disclosures   STRING,          -- JSON array
    keywords      STRING,          -- JSON array
    기준일자      STRING
) USING DELTA
PARTITIONED BY (기준일자);

-- === recommendations.daily_picks (종합 추천 결과) ===
CREATE TABLE recommendations.daily_picks (
    종목코드      STRING NOT NULL,
    종목명        STRING,
    종합점수      DOUBLE,
    a_score       DOUBLE,
    b_score       DOUBLE,
    c_score       DOUBLE,
    추천등급      STRING,
    report_count  BIGINT,
    avg_upside    DOUBLE,
    opinions      STRING,          -- JSON array
    firms         STRING,          -- JSON array
    기준일자      STRING
) USING DELTA
PARTITIONED BY (기준일자);
```

## 6. Spark ETL — spark_silver_to_delta.py

Phase 1 스크립트들이 생성한 Parquet 파일을 Delta Lake로 적재하는 Spark Job이다.

```python
# spark_silver_to_delta.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, coalesce
from delta.tables import DeltaTable
import sys

spark = SparkSession.builder \
    .appName("OPIK Silver to Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

def silver_to_delta(date: str):
    """S3 Parquet → Delta Lake MERGE"""

    # 1. Structured Gold → Delta
    df_s = spark.read.parquet(
        f"s3a://s3-opik-bucket/gold/structured/"
    ).filter(col("발행일") == date)

    if df_s.count() > 0:
        delta_table = DeltaTable.forPath(spark, "s3a://s3-opik-bucket/delta/gold_db/structured/")
        delta_table.alias("t").merge(
            df_s.alias("s"),
            "t.report_id = s.report_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"[Delta] MERGE {df_s.count()} rows → gold_db.structured")

    # 2. LLM Gold → Delta
    llm_path = f"s3a://s3-opik-bucket/gold/llm/year={date[:4]}/month={date[5:7]}/data.parquet"
    try:
        df_l = spark.read.parquet(llm_path)
        if df_l.count() > 0:
            delta_table = DeltaTable.forPath(spark, "s3a://s3-opik-bucket/delta/gold_db/llm/")
            delta_table.alias("t").merge(
                df_l.alias("s"),
                "t.report_id = s.report_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            print(f"[Delta] MERGE {df_l.count()} rows → gold_db.llm")
    except:
        print("[Delta] LLM Gold not available, skipping")


if __name__ == "__main__":
    silver_to_delta(sys.argv[1])
```

## 7. Spark 스코어링 — spark_compute_scores.py

```python
# spark_compute_scores.py
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import sys

spark = SparkSession.builder \
    .appName("OPIK Compute Scores") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "6g") \
    .getOrCreate()

def compute_scores(date: str):
    # 1. Gold Structured + LLM JOIN
    df_s = spark.read.format("delta") \
        .load("s3a://s3-opik-bucket/delta/gold_db/structured/") \
        .filter(F.col("발행일") == date)

    df_l = spark.read.format("delta") \
        .load("s3a://s3-opik-bucket/delta/gold_db/llm/") \
        .filter(F.col("발행일") == date)

    df = df_s.join(df_l, "report_id", "left")

    # 2. 리포트 센티멘트 점수(c) 계산
    # opinion_score
    df = df.withColumn("opinion_score",
        F.when(F.col("투자의견") == "BUY", 1.0)
         .when(F.col("투자의견") == "HOLD", 0.0)
         .when(F.col("투자의견") == "SELL", -1.0)
         .otherwise(0.0)
    )

    # upside_score: min-max scaling to [-1, +1]
    df = df.withColumn("upside_raw",
        F.when(
            F.col("목표주가").isNotNull() & F.col("현재주가").isNotNull()
            & (F.col("현재주가") > 0),
            (F.col("목표주가") - F.col("현재주가")) / F.col("현재주가") * 100
        ).otherwise(0.0)
    ).withColumn("upside_score",
        F.least(F.lit(1.0), F.greatest(F.lit(-1.0), F.col("upside_raw") / 100))
    )

    # llm_sentiment (없으면 0)
    llm_s = F.coalesce(F.col("sentiment_score"), F.lit(0.0))

    # c_score = 0.4 * opinion + 0.35 * upside + 0.25 * llm_sentiment
    df = df.withColumn("c_score",
        0.4 * F.col("opinion_score") + 0.35 * F.col("upside_score") + 0.25 * llm_s
    )

    # 3. 종목별 집계 (여러 증권사 리포트 평균)
    stock_c = df.groupBy("종목코드", "종목명").agg(
        F.count("report_id").alias("report_count"),
        F.avg("c_score").alias("c_score"),
        F.avg("상승여력_pct").alias("avg_upside"),
        F.collect_list("투자의견").alias("opinions"),
        F.collect_list("증권사").alias("firms"),
    )

    # 4. Partner 데이터 JOIN
    df_preds = spark.read.format("delta") \
        .load("s3a://s3-opik-bucket/delta/predictions/stock_scores/") \
        .filter(F.col("기준일자") == date)
    df_dart = spark.read.format("delta") \
        .load("s3a://s3-opik-bucket/delta/dart/disclosure_scores/") \
        .filter(F.col("기준일자") == date)

    df_all = stock_c \
        .join(df_preds.select("종목코드", "a_score"),  # confidence, pred_return는 추후 스코어링에 활용 예정
              "종목코드", "full_outer") \
        .join(df_dart.select("종목코드", "b_score"),   # disclosures, keywords는 브리핑에만 사용
              "종목코드", "full_outer")

    # 5. 종합 점수 = 0.4a + 0.3b + 0.3c
    df_all = df_all.withColumn("종합점수",
        0.4 * F.coalesce(F.col("a_score"), F.lit(0.0)) +
        0.3 * F.coalesce(F.col("b_score"), F.lit(0.0)) +
        0.3 * F.coalesce(F.col("c_score"), F.lit(0.0))
    )

    # 6. 추천등급 (Spark SQL CASE WHEN)
    df_all = df_all.withColumn("추천등급",
        F.when(F.col("종합점수") >= 0.7, "최우선")
         .when(F.col("종합점수") >= 0.5, "강력추천")
         .when(F.col("종합점수") >= 0.3, "추천")
         .otherwise("관심")
    ).withColumn("기준일자", F.lit(date))

    # 7. 상위 20종목만 Delta에 저장
    top_picks = df_all \
        .filter(F.col("종합점수") >= 0.0) \
        .orderBy(F.desc("종합점수")) \
        .limit(20)

    top_picks.write.format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"기준일자 = '{date}'") \
        .save("s3a://s3-opik-bucket/delta/recommendations/daily_picks/")

    print(f"[Spark] {top_picks.count()} recommendations saved for {date}")
    top_picks.select("종목명", "종합점수", "추천등급", "report_count") \
        .show(20, truncate=False)


if __name__ == "__main__":
    compute_scores(sys.argv[1])
```

## 8. LLM Gold — extract_gold_llm.py

### 8.1 개요

Gold Structured가 정규식으로 추출한 투자의견·목표주가·종목코드에 더해, LLM(Gold)은 **텍스트 이해가 필요한 필드**를 Haiku로 추출한다. LLM 호출은 Python(asyncio)에서 수행하고, 결과는 Parquet로 저장한 뒤 Spark가 Delta로 MERGE한다.

```
입력: Silver JSON (text + title)
출력: gold/llm/year={Y}/month={M}/data.parquet
  → Spark silver_to_delta가 Delta Lake gold_db.llm으로 MERGE
```

### 8.2 Haiku 프롬프트

```python
SYSTEM_PROMPT = """당신은 한국 증권사 리포트를 분석하는 금융 AI입니다.
주어진 리포트 텍스트에서 다음 정보를 추출하세요. 없는 항목은 null로, 추측하지 마세요.

출력은 반드시 아래 JSON 스키마를 따르세요:
{
  "reason": "종목별 핵심 논리 1~3문장. 실적 전망, 업황, 밸류에이션 근거를 포함",
  "risks": ["리스크 요인 1", "리스크 요인 2", ...],
  "keywords": ["키워드1", "키워드2", ...],  // 5~10개, 우선순위순
  "sentiment_score": 0.0,  // -1.0(매우 부정) ~ +1.0(매우 긍정)
  "implied_tp": null,  // 밸류에이션 산식에서 역산한 목표주가 (원). 없으면 null
  "target_quarter": null  // 실적추정 기준분기 (e.g. "3Q26"). 없으면 null
}

규칙:
- reason은 '~입니다' 종결형, 객관적 톤 유지
- risks는 구체적이고 측정 가능한 요인만. "시장 리스크" 같은 포괄적 표현 금지
- keywords는 종목명, 섹터명 제외. 재무/밸류에이션/산업 용어 중심
- sentiment_score는 reason과 risks의 전체적 톤을 반영
- implied_tp는 "목표 PER 12배 적용 → 적정주가 85,000원" 같은 명시적 산식에서만 추출
- 해외주식(USD), IR협의회, 숏노트, 기술분석은 reason만 간략히, 나머지는 null"""
```

### 8.3 처리 아키텍처

```python
# 건당 ~2초 (API latency), 50건 병렬처리 시 2~3초 내 완료
# Haiku Context Window: 200K tokens — 30페이지 리포트도 여유 있게 처리

async def process_batch(silver_keys: list[str], date: str):
    sem = asyncio.Semaphore(20)  # 동시 20건 (Rate limit: Haiku 50 req/s)

    async def process_one(key):
        async with sem:
            data = await s3_download_json(key)
            if not data or not data.get("text"):
                return None

            # LLM 호출 재시도: 5회, 지수 백오프 (1s → 2s → 4s → 8s → 16s)
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    response = await anthropic_client.messages.create(
                        model="claude-haiku-3-5",
                        max_tokens=512,
                        system=SYSTEM_PROMPT,
                        messages=[{
                            "role": "user",
                            "content": f"종목명: {data.get('종목명', '')}\n제목: {data.get('title', '')}\n\n{data['text'][:15000]}"
                        }]
                    )

                    result = json.loads(response.content[0].text)
                    result["report_id"] = data["report_id"]
                    result["llm_model"] = response.model
                    result["llm_tokens_in"] = response.usage.input_tokens
                    result["llm_tokens_out"] = response.usage.output_tokens
                    return result
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait = 2 ** attempt
                        print(f"[LLM] Retry {attempt+1}/{max_retries} for {key} after {wait}s: {e}")
                        await asyncio.sleep(wait)
                    else:
                        print(f"[LLM] Failed {max_retries} retries for {key}: {e}")
                        return None

    tasks = [process_one(k) for k in silver_keys]
    results = await asyncio.gather(*tasks)
    return [r for r in results if r is not None]
```

### 8.4 LLM 비용 추정

| 항목 | 값 |
|------|-----|
| Haiku input (15K chars avg) | ~4,000 tokens |
| Haiku output (JSON) | ~200 tokens |
| 건당 비용 | $0.00525 |
| 일 50건 | $0.26/일 |
| 월 | ~$7.90 |
| **연간** | **~$95** |

## 9. 텔레그램 전송 — telegram_briefing.py

텔레그램은 4,096자, HTML 파싱, 멀티라인을 지원한다. Spark가 생성한 Delta 테이블을 읽어 포맷팅 후 전송한다.

```python
def send_daily_briefing(date: str):
    # Spark로 Delta 테이블 쿼리
    spark = get_spark_session()
    recs = spark.sql(f"""
        SELECT * FROM delta.`s3a://s3-opik-bucket/delta/recommendations/daily_picks`
        WHERE 기준일자 = '{date}'
        ORDER BY 종합점수 DESC
    """).toPandas()  # 20건 → Pandas 변환 (Spark→Python bridge)

    # 오늘 리포트 요약
    summary = spark.sql(f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN 투자의견 = 'BUY' THEN 1 ELSE 0 END) as buy,
            SUM(CASE WHEN 투자의견 = 'HOLD' THEN 1 ELSE 0 END) as hold,
            SUM(CASE WHEN 투자의견 = 'SELL' THEN 1 ELSE 0 END) as sell
        FROM delta.`s3a://s3-opik-bucket/delta/gold_db/structured`
        WHERE 발행일 = '{date}'
    """).collect()[0]

    # 포맷팅 후 전송
    briefing = format_briefing(date, recs, summary)
    token = get_secret("opik/telegram")["TELEGRAM_BOT_TOKEN"]
    chat_id = get_secret("opik/telegram")["TELEGRAM_CHAT_ID"]

    for chunk in split_message(briefing, max_len=4096):
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"},
            timeout=10
        )
```

## 10. Airflow DAG

```python
# dags/nightly_batch.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
import subprocess

# ===== Spark Job 실패 시 Delta Rollback =====
def rollback_delta_on_failure(context):
    """Spark Job 실패 시 Delta Lake Time Travel로 이전 버전 복원"""
    date = context["ds"]
    table = "recommendations.daily_picks"
    spark_rollback_cmd = f"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
history = spark.sql(f"DESCRIBE HISTORY delta.`s3a://s3-opik-bucket/delta/{table}`")
last_good = history.filter("operation == 'WRITE'").orderBy("version", ascending=False).limit(2).collect()
if len(last_good) == 2:
    prev_version = last_good[1]["version"]
    spark.sql(f"RESTORE TABLE delta.`s3a://s3-opik-bucket/delta/{table}` TO VERSION AS OF {prev_version}")
    print(f"[Rollback] {table} restored to version {prev_version}")
spark.stop()
"""
    subprocess.run(["spark-submit", "-c", spark_rollback_cmd], check=False)

default_args = {
    "owner": "opik",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 14),
    "retries": 0,  # per-task retries 아래 개별 설정
    "execution_timeout": timedelta(hours=4),
}

with DAG(
    dag_id="nightly_batch",
    default_args=default_args,
    schedule="0 16 * * *",   # 매일 16:00 KST (장 마감 30분 후)
    catchup=False,
    tags=["opik", "phase2"],
) as dag:

    # ===== Phase 1: 수집 (독립 실행으로 fallback 보장) =====
    # Naver 수집 실패해도 한국투자증권은 진행
    collect_naver = BashOperator(
        task_id="collect_naver",
        bash_command="cd /opt/airflow/scripts && python upload_naver.py --date {{ ds }}",
        retries=3,
        retry_delay=timedelta(minutes=5),
        trigger_rule="all_done",  # 실패해도 다음 task 진행
    )

    collect_koreainvest = BashOperator(
        task_id="collect_koreainvest",
        bash_command="cd /opt/airflow/scripts && python upload_koreainvest.py --date {{ ds }}",
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    extract_silver = BashOperator(
        task_id="extract_silver",
        bash_command="python /opt/airflow/scripts/extract_silver.py --start {{ ds }} --end {{ ds }} --workers 20",
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    extract_gold_structured = BashOperator(
        task_id="extract_gold_structured",
        bash_command="python /opt/airflow/scripts/extract_gold_structured.py --start {{ ds }} --end {{ ds }} --workers 20 --force-refresh",
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    extract_gold_llm = BashOperator(
        task_id="extract_gold_llm",
        bash_command="python /opt/airflow/scripts/extract_gold_llm.py --date {{ ds }}",
        retries=1,
        retry_delay=timedelta(minutes=2),
    )

    # ===== Spark jobs =====
    silver_to_delta = SparkSubmitOperator(
        task_id="silver_to_delta",
        application="/opt/airflow/spark_jobs/spark_silver_to_delta.py",
        application_args=["{{ ds }}"],
        conn_id="spark_local",
        conf={
            "spark.driver.memory": "6g",
            "spark.executor.memory": "4g",
        },
        retries=2,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=rollback_delta_on_failure,
    )

    # Partner 데이터 대기: S3에 Parquet 파일이 생길 때까지 30분 polling
    wait_for_partners = FileSensor(
        task_id="wait_for_partners",
        filepath="predictions/{{ ds }}/stock_scores.parquet",
        fs_conn_id="s3_opik",
        poke_interval=timedelta(minutes=10),
        timeout=timedelta(hours=6),  # 22:00 KST까지 대기
        mode="reschedule",
        soft_fail=True,  # 타임아웃되도 다음 task 진행 (c_score만으로 추천)
    )

    compute_recommendations = SparkSubmitOperator(
        task_id="compute_recommendations",
        application="/opt/airflow/spark_jobs/spark_compute_scores.py",
        application_args=["{{ ds }}"],
        conn_id="spark_local",
        conf={
            "spark.driver.memory": "6g",
            "spark.executor.memory": "4g",
        },
        retries=2,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=rollback_delta_on_failure,
    )

    deliver_telegram = BashOperator(
        task_id="deliver_telegram",
        bash_command="python /opt/airflow/scripts/telegram_briefing.py --date {{ ds }}",
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    # DAG 의존성
    [collect_naver, collect_koreainvest] >> extract_silver
    extract_silver >> extract_gold_structured >> extract_gold_llm
    extract_gold_llm >> silver_to_delta >> wait_for_partners >> compute_recommendations >> deliver_telegram
```
> **의존성 설명:** `silver_to_delta` 이후 `wait_for_partners`가 Partner의 Delta 데이터를 기다린다. Partner 데이터가 22:00까지 도착하지 않으면 `soft_fail=True`로 타임아웃되고, `compute_recommendations`가 리포트 점수(c_score)만으로 추천을 생성한다. `deliver_telegram`은 `compute_recommendations`→`silver_to_delta`에 모두 transitively 의존하므로 structured, llm, daily_picks Delta 테이블이 모두 준비된 상태에서 실행된다.

## 11. 소요 시간 추정

| Task | 처리량 | 예상 소요시간 | 엔진 |
|------|--------|-------------|------|
| 수집 (네이버 + 한국투자, 독립 병렬) | ~50건 | 5초 | Python |
| Silver (PDF→텍스트) | ~50건 × 20 workers | 10초 | Python |
| Gold Structured (정규식) | ~50건 | 3초 | Python |
| Gold LLM (Haiku) | ~50건 × 20 workers | 5~10초 | Python |
| Spark JVM 시작 | — | ~15초 | Spark (cold start) |
| Silver→Delta MERGE | ~50건 + 51K 기존 | 5초 | Spark |
| **wait_for_partners** | **S3 polling** | **0~6시간** | **FileSensor** |
| 스코어링 (3-way JOIN) | ~2,000종목 | 3초 | Spark SQL |
| 텔레그램 전송 | 1건 | 1초 | Python |
| **총 (Partner 제외)** | | **~50초** | |
| **총 (Partner 포함)** | | **~50초 ~ 최대 6시간** | |

> Partner 데이터가 오후에 미리 준비되면 16:00:50에 완료. 늦어도 22:00 데드라인까지 기다린 후 c_score만으로 추천 생성. Spark JVM cold start 15초는 Phase 2a에서 Spark Thrift Server 상시 구동으로 제거 가능.

## 12. EC2 인스턴스 상세 스펙

### 12.1 선택: r6g.large (ARM Graviton2, Memory Optimized)

| 항목 | 사양 |
|------|------|
| vCPU | 2 (ARM64) |
| RAM | **16 GB** |
| 네트워크 | 최대 10 Gbps |
| EBS | gp3 50 GB |
| OS | Amazon Linux 2023 (ARM) |
| 비용 | $0.1008/hr → 약 $73/월 (on-demand) |
| 예약 (1년) | ~$42/월 |

### 12.2 선정 근거

Spark 3.5 local mode 기준 메모리 할당:

```
전체: 16 GB
  ├─ OS + Airflow + Docker: 3 GB
  ├─ Spark Driver: 6 GB
  ├─ Spark Executor (x1, local mode): 4 GB
  └─ 여유: 3 GB (Python 프로세스 + PyMuPDF + boto3)
```

- **r6g (Memory Optimized)**: Spark는 연산보다 메모리 병목이 빈번하다. 50MB Parquet도 Spark Catalyst 옵티마이저의 실행 계획 생성과 중간 shuffle에 상당한 메모리를 사용한다.
- **t4g (General Purpose)** 는 burst credit 시스템 때문에 Spark 같은 지속 부하 작업에 부적합. 저녁 16시 한 번 Spark Job이 도는 패턴이지만 그 한 번에 burst credit을 초과할 위험이 있다.
- **Graviton2 ARM**: x86 대비 20% 저렴, Spark 3.3+부터 ARM 네이티브 지원 완전.

### 12.3 Spark 설정

```bash
# spark-defaults.conf
spark.master                    local[*]
spark.driver.memory             6g
spark.driver.memoryOverhead     1g
spark.executor.memory           4g
spark.executor.memoryOverhead   512m
spark.sql.shuffle.partitions    8        # 2vCPU × 4 = 8
spark.sql.adaptive.enabled      true
spark.sql.adaptive.coalescePartitions.enabled  true
spark.sql.extensions            io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.databricks.delta.retentionDurationCheck.enabled  false
spark.serializer                org.apache.spark.serializer.KryoSerializer
```

## 13. 오류 처리 및 모니터링

### 13.1 Airflow 재시도 정책

| 실패 유형 | 처리 |
|-----------|------|
| 네이버 수집 실패 | 3회 재시도 (5분 간격), `trigger_rule="all_done"`으로 실패해도 한국투자증권 수집 진행 |
| 한국투자증권 수집 실패 | 3회 재시도 (5분 간격), 없으면 그날은 Bronze 데이터 없는 채로 진행 (다음날 재수집) |
| Silver/Gold Structured 실패 | 2회 재시도 (5분 간격), 실패 시 Slack 알림 |
| LLM API 오류 | 스크립트 내장 5회 지수 백오프 (1s→2s→4s→8s→16s), 전건 실패 시 LLM 필드는 null, structured만으로 진행 |
| Spark Job 실패 | 2회 재시도 (5분 간격), 실패 시 `on_failure_callback`으로 Delta Time Travel Rollback |
| Partner 데이터 미도착 | `FileSensor`로 S3 polling (10분 간격, 최대 6시간 → 22:00). `soft_fail=True`로 타임아웃 시 리포트 점수(c)만으로 추천 |
| 텔레그램 전송 실패 | 3회 재시도 (2분 간격), 실패 시 S3에 HTML로 저장 + CloudWatch 알람 |

### 13.2 Delta Lake 운영

```sql
-- Time Travel: 전날 추천과 오늘 추천 비교
SELECT * FROM recommendations.daily_picks
  VERSION AS OF (SELECT MAX(version)-1 FROM (DESCRIBE HISTORY recommendations.daily_picks))
  WHERE 기준일자 = '2026-06-14';

-- Rollback: Spark Job 실패 시
RESTORE TABLE recommendations.daily_picks TO VERSION AS OF <version>;

-- 유지보수
OPTIMIZE gold_db.structured ZORDER BY (종목코드);
VACUUM gold_db.structured RETAIN 168 HOURS;  -- 7일 보존
```

### 13.3 CloudWatch 알람

- DAG 실행 시간 > 1시간 → 경고
- Spark Job OOM → 경고 + 자동 Rollback
- LLM API 오류율 > 10% → 경고
- Partner 데이터 22:00까지 미도착 → 경고

## 14. 월간 운영 비용

| 항목 | 비용 | 비고 |
|------|------|------|
| EC2 r6g.large (1년 예약) | $42/월 | ARM64, 16GB |
| EBS gp3 50GB | $4/월 | Spark 로컬 디스크 포함 |
| S3 스토리지 (67K PDF + Parquet + Delta) | $5/월 | Delta는 Parquet + transaction log 추가 |
| Claude Haiku API | $8/월 | 일 50건 기준 |
| Secrets Manager | $0.5/월 | 토큰 3개 |
| Data Transfer | $1/월 | |
| **합계** | **~$61/월 (약 85,000원)** | |

예산(1.5만원)을 크게 초과하지만, 이는 Spark + Delta Lake 도입의 구조적 비용이다. 비용 절감 방안:

- EC2 예약 인스턴스 3년 선결제 → $28/월 (r6g.large)
- LLM 호출 제한 (opinion+TP 있는 리포트만) → $3/월

## 15. Partner 인터페이스

### 15.1 찬호 — 주가 예측 (a_score)

```
Delta Lake: predictions.stock_scores
  Partition: 기준일자

Schema:
  종목코드: string     # 6자리
  종목명: string
  현재주가: bigint      # 전일 종가
  a_score: double       # -1.0 ~ +1.0, 높을수록 상승 예측
  confidence: double    # 0.0 ~ 1.0
  pred_return: double   # 예측 수익률 (%)
  기준일자: string      # YYYY-MM-DD

쓰기 권한: 찬호가 Airflow DAG 또는 별도 파이프라인으로 Delta Lake에 직접 write
읽기: spark_compute_scores.py가 Delta 테이블에서 읽어 JOIN
```

### 15.2 상용 — DART 공시 임팩트 (b_score)

```
Delta Lake: dart.disclosure_scores
  Partition: 기준일자

Schema:
  종목코드: string     # 6자리
  종목명: string
  b_score: double       # -1.0 ~ +1.0
  disclosures: string   # JSON array
  keywords: string      # JSON array
  기준일자: string      # YYYY-MM-DD

쓰기 권한: 상용이 Airflow DAG 또는 별도 파이프라인으로 Delta Lake에 직접 write
읽기: spark_compute_scores.py가 Delta 테이블에서 읽어 JOIN
```

## 16. 구현 우선순위

```
Phase 2a (지금):  파이프라인 검증
  ├─ EC2 r6g.large 프로비저닝 (ARM64, 16GB)
  ├─ Docker + Airflow + Spark 3.5 설치
  ├─ Delta Lake 테이블 생성 (DDL 실행)
  ├─ 텔레그램 봇 생성 + Secrets Manager 연동
  ├─ spark_silver_to_delta.py 구현 → Parquet to Delta MERGE
  ├─ spark_compute_scores.py 구현 → 3-way JOIN + 점수 계산
  ├─ nightly_batch DAG 배포 (LLM 제외, Structured + Spark만)
  └─ 매일 16:00 자동 실행 → 텔레그램 전송 확인

Phase 2b (다음):  LLM Gold 추가
  ├─ Anthropic API 키 발급
  ├─ extract_gold_llm.py 구현 (Python asyncio)
  └─ DAG에 LLM Task + Delta MERGE 추가

Phase 2c (협업):  Partner 스코어링 통합
  ├─ 찬호/상용과 Delta 테이블 스키마 협의
  ├─ predictions.stock_scores, dart.disclosure_scores Delta 테이블 생성
  └─ 3-way 종합 스코어링 전환
```

## 17. Phase 2a 시작 체크리스트

- [ ] AWS EC2 r6g.large 생성 (Amazon Linux 2023 ARM, 16GB RAM)
- [ ] Java 17 + Spark 3.5 + Delta Lake 3.0 패키지 설치
- [ ] Docker + docker-compose + Airflow 2.8 설치
- [ ] opik/ 레포지토리 클론
- [ ] Spark Job 코드를 spark_jobs/ 디렉토리에 배치
- [ ] Delta Lake 초기 테이블 DDL 실행 (CREATE TABLE ... USING DELTA)
- [ ] Phase 1 백필 데이터(51,294건 Parquet)를 Delta로 초기 적재
- [ ] .env 파일에 AWS credential + Anthropic API key 설정
- [ ] 텔레그램 @BotFather로 봇 생성 → 토큰 발급
- [ ] Secrets Manager에 `opik/telegram` 시크릿 저장
- [ ] S3 접근용 IAM Role 생성 (EC2에 attach)
- [ ] Airflow DAG 배포 및 수동 트리거 테스트
- [ ] 텔레그램 메시지 수신 확인
- [ ] CloudWatch 로그 + Spark History Server 확인
