# OPIK Phase 1 설계 — 데이터 수집·가공 파이프라인

## 1. Phase 1 정의

Phase 1은 **증권사 리포트를 수집해서 텍스트로 변환하고, 정규식으로 구조화 정보를 뽑아내는** 파이프라인이다. 모든 것이 Python 단일 머신에서 돌아가고, LLM 비용은 0원이다.

> Phase 1 목표: 31개 증권사 리포트를 매일 수집 → PDF를 텍스트로 → 정규식으로 투자의견·목표주가·종목코드 추출 → 카카오톡 브리핑

```
Phase 1 (완료):     수집 → Bronze → Silver → Gold Structured → 카카오톡
Phase 2 (진행 중):  Gold LLM → Spark 스코어링 → 텔레그램 → RAG 양방향 QA 챗봇
Phase 3 (미래):     실시간 모니터링, 즉시 스코어링, 선제적 푸시 알림
```

## 2. 운영 환경

Phase 1은 AWS 클라우드가 아니라 **로컬 VM(Cowork Linux 샌드박스)**에서 수동 실행된다. 이게 Phase 1 설계의 핵심 전제다.

| 항목 | 구성 |
|------|------|
| 실행 환경 | Cowork Linux VM (Ubuntu 22) |
| Python | 3.10 |
| 외부 접근 | S3 (boto3), 네이버 금융 (aiohttp), 한국투자증권 (aiohttp), Anthropic API (KakaoTalk용) |
| 저장소 | 로컬 디스크 (체크포인트, 캐시) + S3 (데이터) |
| 자동화 | 없음. 모든 스크립트 수동 실행. `mcp__scheduled-tasks__create_scheduled_task`는 미사용 상태 |
| 비용 | **S3 스토리지 외 $0/월** (LLM 호출 없음) |

## 3. Medallion 아키텍처

```
Bronze (S3 PDF)           Silver (S3 JSON)          Gold (S3 Parquet)
─────────────────         ─────────────────         ─────────────────────
원본 PDF + 메타데이터      텍스트 추출 완료           정규식 구조화 완료

bronze/{증권사}/날짜/     silver/{증권사}/날짜/      gold/structured/
  {id}.pdf                  {id}.json                 year={Y}/month={M}/
  _manifest.json                                       data.parquet

예:
bronze/미래에셋증권/       silver/미래에셋증권/        gold/structured/
  2026-06-12/               2026-06-12/                year=2026/month=06/
    abc123.pdf                abc123.json                data.parquet
    _manifest.json
```

### Bronze (완료)

**파일 구조:**
```
bronze/{증권사}/YYYY-MM-DD/
  ├── {report_id}.pdf          # 원본 PDF
  └── _manifest.json           # 당일 업로드 목록 + 메타데이터
```

**매니페스트 형식:**
```json
{
  "date": "2026-06-12",
  "firm": "미래에셋증권",
  "report_count": 3,
  "reports": [
    {
      "report_id": "abc123",
      "title": "삼성전자 4Q25 Review",
      "종목명": "삼성전자",
      "종목코드": "005930",
      "download_url": "https://stock.pstatic.net/stock/research/.../abc123.pdf",
      "pages": 12
    }
  ]
}
```

**현황:**
| 소스 | 건수 | 기간 |
|------|------|------|
| 네이버 금융 | ~37,000건 | 2020~2026 |
| 한국투자증권 직접 | ~30,000건 | 2006~2026 |
| LS증권 | 보류 | 로그인 + Eversafe 난독화 |

### Silver (완료)

**파일 구조:**
```
silver/{증권사}/YYYY-MM-DD/{report_id}.json
```

**JSON 형식:**
```json
{
  "report_id": "abc123",
  "source": "naver",
  "증권사": "미래에셋증권",
  "종목명": "삼성전자",
  "발행일": "2026-06-12",
  "title": "삼성전자 4Q25 Review",
  "text": "4분기 실적은...\n\n투자의견 매수 유지...\n\n...",
  "text_len": 4521,
  "pages_total": 8,
  "pages_with_text": 8
}
```

**추출 엔진:** PyMuPDF (fitz) — C++ 네이티브 라이브러리, `page.get_text()` 사용
**병렬처리:** asyncio + ThreadPoolExecutor (PyMuPDF는 C++ blocking 호출이므로 스레드 풀에서 실행)
**체크포인트:** `.silver_checkpoint.json` — 날짜 단위로 완료 기록, `--resume`으로 중단 지점부터 재개
**매니페스트 캐시:** `.silver_manifest_cache.json` — S3 list 반복 방지 (~12초 절약)

### Gold Structured (완료)

**파일 구조:**
```
gold/structured/year={Y}/month={MM}/data.parquet
```

**Parquet 스키마:**
```python
GOLD_SCHEMA = pa.schema([
    ("report_id",       pa.string()),
    ("증권사",           pa.string()),
    ("종목명",           pa.string()),
    ("종목코드",         pa.string()),    # 정규식 추출
    ("발행일",           pa.string()),
    ("title",            pa.string()),
    ("source",           pa.string()),    # "naver" | "koreainvest"
    ("text_len",         pa.int64()),
    ("pages_total",      pa.int64()),
    ("투자의견",         pa.string()),    # BUY/HOLD/SELL/NOT_RATED/null
    ("목표주가",         pa.int64()),
    ("현재주가",         pa.int64()),
    ("상승여력_pct",     pa.float64()),
    ("종목코드_list",    pa.string()),    # JSON array (멀티종목 대비)
    ("실적추정_raw",     pa.string()),    # JSON dict (비정제)
])
```

**추출 로직:** `extract_gold_structured.py` — 5개 함수가 Silver JSON 텍스트를 정규식으로 분석
- `extract_opinion()`: title + text[:2500] → BUY/HOLD/SELL/NOT_RATED
- `extract_target_price()`: 6개 패턴 + 9-layer `_validate_tp_context` 방어
- `extract_current_price()`: 2개 패턴
- `extract_stock_codes()`: 8개 패턴, 중복 제거, 연도 오인 방지
- `extract_estimates()`: 매출액·영업이익 raw 캡처 (정제 안 함)

**병렬처리:** asyncio + Semaphore(20), 200건 배치 단위
**캐싱:** `.silver_keys_cache.json` — Silver 키 목록 캐싱

## 4. 데이터 수집기

### 4.1 네이버 금융 (naver.py)

**대상:** 31개 증권사 리포트 (네이버 금융 증권사 리포트 페이지)
**URL:** `https://finance.naver.com/research/company_list.naver`
**인코딩:** EUC-KR (meta 태그는 UTF-8이라 표기하지만 실제는 EUC-KR)
**PDF CDN:** `stock.pstatic.net`
**날짜 파싱:** `YY.MM.DD` 형식

**HTML 파싱 구조:**
```html
<tr>
    <td> <a class="stock_item" href="/item/main.naver?code=287840">종목명</a> </td>
    <td> <a href="company_read.naver?nid=93508">리포트 제목</a> </td>
    <td> 증권사명 </td>
    <td class="file"> <a href="https://stock.pstatic.net/...pdf"> </td>
    <td class="date"> 26.06.11 </td>
    <td class="date"> 페이지수 </td>
</tr>
```

**증권사명 정규화:**
```python
FIRM_MAP = {
    "미래에셋증권": "miraeasset",
    "삼성증권": "samsung",
    "NH투자증권": "nh",
    "KB증권": "kb",
    # ... 31개사
}
```

**핵심 로직:** `fetch_all_since_async()` — 지정일 이후 모든 페이지를 비동기 페칭, 날짜 중복 제거, PDF URL 추출

### 4.2 한국투자증권 (koreainvest.py)

**대상:** 한국투자증권 자체 사이트 (네이버에 PDF 첨부 안 되는 증권사 직접 수집)
**이유:** 신한투자증권·한국투자증권은 네이버에서 PDF 첨부가 안 돼 있음
**수집 범위:** 2006~2026 (~30,000건)
**형식:** 금융투자협회 표준 PDF + 메타데이터

### 4.3 LS증권 (보류)

**이유:** 로그인 필요 + Eversafe 난독화로 자동화 난이도 높음. 우선순위 하향.

## 5. 업로더

### 5.1 upload_naver.py

```
네이버 수집기(collectors/naver.py) → PDF 다운로드 → S3 Bronze 적재
```

**실행:**
```bash
python upload_naver.py                           # 당일
python upload_naver.py --date 2026-06-12         # 특정일
python upload_naver.py --backfill --start 2021-06-01 --end 2026-06-10  # 백필
```

**처리 흐름:**
1. `NaverCollector.fetch_all_since_async(date)` → 당일 리포트 목록
2. 각 리포트마다 PDF 다운로드 (aiohttp, Semaphore로 동시성 제어)
3. S3에 `bronze/{증권사}/{날짜}/{report_id}.pdf` 업로드
4. S3에 `_manifest.json` 업로드 (당일 전체 목록)
5. `.backfill_checkpoint.json`에 진행상황 기록

**증권사 필터링:** 31개 증권사 중 네이버에 PDF가 있는 업체만 (신한투자증권·한국투자증권 제외)

### 5.2 upload_koreainvest.py

```
한국투자증권 수집기(collectors/koreainvest.py) → PDF 다운로드 → S3 Bronze 적재
```

네이버와 동일한 Bronze 적재 구조를 사용. `source: "koreainvest"`로 태깅.

## 6. 추출 파이프라인

### 6.1 extract_silver.py — Bronze → Silver

**핵심 기능:** S3 Bronze PDF → PyMuPDF 텍스트 추출 → S3 Silver JSON

**실행:**
```bash
python extract_silver.py --dry-run                      # 건수만 확인
python extract_silver.py --days 5 --workers 40          # 최근 5일치
python extract_silver.py --start 2026-01-01 --end 2026-06-30 --workers 20  # 기간 지정
python extract_silver.py --resume --workers 30          # 중단 지점부터 재개
```

**처리 단계:**

1. **Discovery:** S3의 `bronze/` 아래 모든 `_manifest.json`을 찾는다. 최초 1회만 S3 list 후 `.silver_manifest_cache.json`에 캐싱
2. **날짜별 처리:** 매니페스트에 기록된 모든 리포트를 `발행일` 기준으로 그룹화. `--days`로 동시 처리 날짜 수, `--workers`로 날짜 내 PDF 동시 처리 수 제어
3. **PDF→텍스트:** 각 PDF마다:
   - 스킵 체크: Silver JSON 이미 존재하면 건너뜀
   - S3에서 Bronze PDF 다운로드
   - PyMuPDF(`fitz`)로 모든 페이지 `page.get_text()`
   - 타임아웃 30초 (ThreadPoolExecutor 별도 스레드)
   - Fallback: PyMuPDF 전체 추출 실패 시 페이지별 개별 추출
4. **OCR 필요 판정:** text_len < 200자 또는 텍스트 있는 페이지 < 30% → `needs_ocr: true`
5. **체크포인트:** 날짜 하나 끝날 때마다 `.silver_checkpoint.json` 기록

### 6.2 extract_gold_structured.py — Silver → Gold

**핵심 기능:** Silver JSON 텍스트 → 정규식 구조화 추출 → Gold Parquet

**실행:**
```bash
python extract_gold_structured.py --workers 20                        # 전체 백필
python extract_gold_structured.py --year 2026 --workers 20            # 특정 연도
python extract_gold_structured.py --start 2026-01-01 --end 2026-06-30 --workers 20  # 기간
python extract_gold_structured.py --sample-firms                      # 증권사별 샘플 테스트
python extract_gold_structured.py --force-refresh --start 2026-06-01  # 덮어쓰기
python extract_gold_structured.py --dry-run                           # 건수만
```

**추출 파이프라인 (extract_from_silver):**
```
extract_from_silver(silver_json)
  │
  ├─ extract_opinion(title + text[:2500])      → BUY/HOLD/SELL/NOT_RATED/null
  ├─ extract_target_price(title + text)        → int (원) / null
  ├─ extract_current_price(title + text)       → int (원) / null
  ├─ extract_stock_codes(title + text)         → list[str]
  ├─ extract_estimates(text[:5000])            → dict / null
  └─ if TP & CP valid → 상승여력_pct 계산
```

**결측치 처리 원칙:**
- null을 그대로 저장 (기본값 대체, LLM 추론 없음)
- "회의론적 접근(skeptical extraction) — 애매하면 null"
- null 자체가 신호: TP=null + Opinion=BUY → "매수의견 있으나 TP 미기재 리포트"

## 7. 일일 운영 흐름 (현재 수동)

Phase 1에서 매일 장 마감 후 운영자가 수동으로 실행하는 흐름:

```
16:00  수동 실행

  1. python upload_naver.py --date 2026-06-12          (~3초, 15건)
  2. python upload_koreainvest.py --date 2026-06-12     (~2초, 5건)
  3. python extract_silver.py --start 2026-06-12 --end 2026-06-12 --workers 20  (~10초)
  4. python extract_gold_structured.py --start 2026-06-12 --end 2026-06-12 --workers 20 --force-refresh  (~5초)
  5. KakaoTalk 전송 (수동)                               (~1초)

  총 소요: ~21초
```

**현재 자동화 수준:** 0%. 모든 단계를 VM에서 직접 명령어 입력. `mcp__scheduled-tasks` 미사용.

**일일 처리량:** 평균 22건, 피크 48건 → 50건 기준으로 설계

## 8. KakaoTalk 전송

Phase 1의 유일한 출력 채널. `mcp__playmcp-gateway__KakaotalkChat-MemoChat` MCP를 통해 전송한다.

**제약사항:**
- 최대 200자 (심각한 제약)
- 평문만 (HTML/마크다운 파싱 불가)
- 개인 메모 채팅으로 전송됨

**전략:** 200자 제한 때문에 풀브리핑이 불가능하다. 오늘의 헤드라인(상위 종목 + 상승여력)을 극도로 압축해서 보낸다.

```
📊 6/12 리포트 15건
🔥 아이씨티케이 +165% / KB금융 +45%
📈 매수 14 / 중립 1
```

## 9. 캐싱 전략

Phase 1은 반복되는 S3 list 호출을 줄이기 위해 로컬 파일 기반 캐시를 사용한다.

| 캐시 파일 | 용도 | 크기 |
|-----------|------|------|
| `.silver_manifest_cache.json` | Bronze manifest 목록 (S3 list 1회 대체) | ~500KB |
| `.silver_keys_cache.json` | Silver JSON 키 목록 (S3 list 1회 대체) | ~1MB |
| `.silver_checkpoint.json` | Silver 진행상황 (resume 지점) | ~100B |
| `.backfill_checkpoint.json` | Naver 백필 진행상황 | ~1KB |
| `.backfill_checkpoint_koreainvest.json` | 한국투자증권 백필 진행상황 | ~1KB |

모든 캐시는 `.gitignore`에 등록. 삭제해도 안전하며 다음 실행 시 재생성된다.

## 10. 현황

### 10.1 데이터 현황

| 레이어 | 상태 | 건수 |
|--------|------|------|
| Bronze (네이버) | 완료 | ~37,000건 |
| Bronze (한국투자증권) | 완료 | ~30,000건 |
| Bronze (LS증권) | 보류 | — |
| Silver | 완료 | 51,294건 |
| Gold Structured | 완료 | 51,294건 |

### 10.2 추출률 (Gold Structured)

| 필드 | 추출률 | 비고 |
|------|--------|------|
| 투자의견 | 90.8% | NAVER 97.4%, 전체 평균 |
| 목표주가 | 75.0% | TP<500 false positive 0.044% (17건) |
| 종목코드 | 87.5% | |
| 현재주가 | 추출 가능 시만 | — |
| 상승여력 | TP+CP 모두 있을 때만 | — |

### 10.3 파일 목록

```
opik/
├── collectors/
│   ├── naver.py                    # 네이버 금융 수집기
│   └── koreainvest.py              # 한국투자증권 수집기
├── upload_naver.py                 # 네이버 → S3 Bronze
├── upload_koreainvest.py           # 한국투자증권 → S3 Bronze
├── extract_silver.py               # Bronze → Silver (PyMuPDF)
├── extract_gold_structured.py      # Silver → Gold Structured (정규식)
├── check_silver.py                 # Silver 적재 확인
├── check_silver_quality.py         # Silver 품질 검증
├── debug_pdf.py                    # PDF URL 도메인 진단
├── _batch_run.py                   # 수동 배치 실행 (미사용)
├── requirements.txt                # Python 의존성
├── .env                            # AWS credential + S3_BUCKET
├── HOW_BRONZE_TO_SILVER_WORKS.md   # Bronze→Silver 상세
├── HOW_SILVER_TO_GOLD_WORKS.md     # Silver→Gold 상세
├── PHASE1_DESIGN.md                # 이 문서
├── PHASE2_DESIGN.md                # Phase 2 설계
├── ARCHITECTURE.md                 # 전체 아키텍처
└── (cache files)
    ├── .silver_checkpoint.json
    ├── .silver_manifest_cache.json
    ├── .silver_keys_cache.json
    └── .backfill_checkpoint*.json
```

## 11. Phase 1 → Phase 2 전환 포인트

Phase 1의 어떤 부분이 Phase 2에서 바뀌고, 어떤 부분이 그대로 가는지:

| 컴포넌트 | Phase 1 (현재) | Phase 2 (설계) | 변경 이유 |
|----------|---------------|---------------|----------|
| 실행 환경 | Cowork VM (수동) | EC2 r6g.large (Airflow 자동) | 매일 수동 실행 불가 |
| 스토리지 | S3 Parquet | S3 Delta Lake | 일일 UPSERT + 트랜잭션 |
| 스코어링 | 없음 | Spark SQL 3-way JOIN | 종목별 종합 점수 산출 |
| LLM | 없음 (정규식만) | Claude Haiku (reason 등) | 텍스트 이해 필요 |
| 출력 채널 | KakaoTalk (200자) | Telegram (4,096자 HTML) | 풀브리핑 전송 |
| 모니터링 | 없음 | CloudWatch + Delta History | 운영 안정성 |
| 수집기 | 그대로 유지 | 그대로 유지 | — |
| Silver 추출 | 그대로 유지 | 그대로 유지 | — |
| Gold Structured | 그대로 유지 | 그대로 유지 | — |

Phase 1의 수집기·Silver·Gold Structured는 Phase 2에서도 그대로 사용된다. Phase 2는 이 위에 LLM 레이어·스코어링·자동화·텔레그램을 추가하는 것이다.

## 12. 의존성

```
Python 3.10
├── aiohttp          # 비동기 HTTP (수집)
├── beautifulsoup4   # HTML 파싱
├── boto3            # S3 I/O
├── PyMuPDF (fitz)   # PDF → 텍스트 (C++ 네이티브)
├── pyarrow          # Parquet I/O
└── requests         # 동기 HTTP (fallback)
```

C++ 네이티브 의존성(PyMuPDF) 때문에 Lambda 배포가 까다롭다 — 이게 Phase 2에서 Lambda 대신 EC2를 선택한 이유 중 하나다.
