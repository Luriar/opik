# OPIK Bronze → Silver 파이프라인 설명

## 개요

Bronze는 S3에 저장된 PDF 원본이고, Silver는 그 PDF에서 텍스트를 뽑아낸 JSON 파일이다.
즉 PDF → 텍스트 변환 계층이다.

## S3 저장 구조

```
Bronze (입력)                            Silver (출력)
─────────────────────────────           ──────────────────────────────
bronze/{증권사}/{날짜}/{id}.pdf    →    silver/{증권사}/{날짜}/{id}.json
bronze/{증권사}/{날짜}/_manifest.json

예: bronze/미래에셋증권/2026-01-15/abc123.pdf → silver/미래에셋증권/2026-01-15/abc123.json
```

## 동작 흐름 (extract_silver.py)

### 1. 매니페스트 열람 (Discovery)
S3의 `bronze/` 아래 모든 `_manifest.json` 파일을 찾는다. 매니페스트에는 해당 날짜에 업로드된 리포트들의 메타데이터(증권사, 종목명, 제목, report_id)가 담겨 있다.
매니페스트 개수만 2,000개 이상이라 한 번 스캔한 뒤 `.silver_manifest_cache.json` 파일로 캐싱해둔다.

### 2. 날짜별 병렬 처리
매니페스트에 기록된 모든 리포트를 `발행일` 기준으로 그룹화한 후, 날짜 단위로 돌린다.
`--days` 옵션으로 동시에 몇 개 날짜를 처리할지, `--workers`로 날짜 내에서 몇 개 PDF를 동시에 처리할지 정한다.

예: `--days 5 --workers 40` → 하루치 약 100건의 PDF를 40개씩 병렬로, 5일치를 동시에.

### 3. PDF → 텍스트 변환 (PyMuPDF)
각 PDF마다:
1. **스킵 체크**: Silver JSON이 이미 존재하면 건너뛴다.
2. **다운로드**: S3에서 Bronze PDF를 다운로드한다.
3. **텍스트 추출**: PyMuPDF(`fitz`)로 PDF를 열어 모든 페이지의 텍스트를 추출한다.
   - `page.get_text()` 사용
   - 타임아웃 30초 (ThreadPoolExecutor로 별도 스레드에서 실행)
   - PyMuPDF가 C++ 라이브러리라 가끔 죽는 PDF가 있는데, 이런 경우 fallback으로 한 페이지씩 개별 추출 시도
4. **OCR 필요 판정**: 추출된 텍스트 길이가 200자 미만이거나 텍스트 있는 페이지가 전체의 30% 미만이면 `needs_ocr: true` 플래그를 붙이고 `silver/_ocr_needed/{날짜}.json`에 따로 기록
5. **업로드**: 결과 JSON을 S3 Silver에 저장

### 4. Silver JSON 구조
```json
{
  "report_id": "abc123",
  "source": "naver",
  "증권사": "미래에셋증권",
  "종목명": "삼성전자",
  "발행일": "2026-01-15",
  "title": "삼성전자 4Q25 Review",
  "text": "4분기 실적은...\n\n투자의견 매수 유지...\n\n...",
  "text_len": 4521,
  "pages_total": 8,
  "pages_with_text": 8
}
```

### 5. 체크포인트 & 재개
날짜 하나 끝날 때마다 `.silver_checkpoint.json`에 마지막 완료 날짜를 기록한다.
`--resume` 옵션으로 중단된 지점부터 이어서 돌릴 수 있다.

## 현재 상태 (2026-06-19)

| 항목 | 수치 |
|------|------|
| Bronze 총 PDF | ~67,000건 (네이버 37,000 + 한국투자증권 30,000) |
| Silver 변환 완료 | 51,294건 (2020~2026 전량 완료) |
| OCR 필요 판정 | 3건 (99.99% 이상 텍스트 추출 성공) |
| 실행 방식 | EC2 Airflow DAG `opik_silver_extract` (00:00 KST, 자동) |

## 의존성

- Python: `boto3`, `pymupdf` (fitz), `asyncio`
- S3: `s3-opik-bucket`, region `ap-northeast-2`
- AWS 자격증명: `.env` 파일에서 로드

## 실행 명령어

```bash
# 최근 5일치만 빠르게
python extract_silver.py --days 5 --workers 40

# 특정 기간
python extract_silver.py --start 2026-01-01 --end 2026-06-30 --workers 20

# 중단된 지점부터 이어서
python extract_silver.py --resume --workers 30

# 건수만 확인 (dry-run)
python extract_s