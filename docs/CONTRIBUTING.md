# OPIK 기여 가이드

## 브랜치 전략

- **`main`**: 프로덕션 준비 완료. 항상 배포 가능해야 함.
- **`develop`**: 통합 브랜치. Phase 2 작업의 기본 브랜치.
- **`feature/<이름>`**: 기능 개발. `develop`에서 분기하여 `develop`으로 PR.
- **`fix/<이름>`**: 버그 수정. `main` 또는 `develop`에서 분기.
- **`docs/<이름>`**: 문서 전용 변경.

### 브랜치 네이밍 예시

```
feature/add-rag-chatbot
fix/tp-filter-false-positive
docs/update-architecture
```

## PR 규칙

1. **모든 코드는 PR을 통해 머지** — `main`에 직접 푸시 금지.
2. **PR 제목은 한글로**: `[수집] 신한투자증권 manifest 통파일 지원 추가`
3. **설명에 체크리스트 포함**:
   ```
   ## 변경 내용
   - 신한투자증권 manifest 구조가 통파일이어서 extract_silver.py 인식 못 하는 문제 해결

   ## 테스트
   - [x] 로컬에서 2026-06-15 데이터로 dry-run 확인
   - [x] 기존 네이버/한국투자증권 처리 경로 회귀 없음
   ```
4. **최소 1명의 리뷰 승인 필요**. CODEOWNERS에 지정된 담당자가 자동 태그됨.
5. **머지 전에 `develop` rebase** 해서 충돌 미리 해결.

## 커밋 컨벤션

```
<타입>: <한글 설명>

예:
feat: RAG 챗봇 기본 구조 추가
fix: report_id 생성 시 종목코드 누락 수정
refactor: S3 client 공통 모듈로 추출
docs: Phase 2 RAG 설계 문서 추가
chore: .gitignore 배치 출력 디렉토리 추가
```

**타입**: `feat`, `fix`, `refactor`, `docs`, `chore`, `test`, `perf`

## 개발 환경

### 필수 의존성

```bash
pip install -r requirements.txt --break-system-packages
```

### 환경 변수

`.env` 파일을 프로젝트 루트에 생성 (`.gitignore`에 등록됨):

```
S3_BUCKET=s3-opik-bucket
S3_REGION=ap-northeast-2
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
ANTHROPIC_API_KEY=...
```

### 코드 스타일

- Python 3.10+ 타입 힌트: `str | None` 스타일 사용 (`Optional[str]` 대신)
- 한글 변수명 사용 (DataFrame 컬럼명): `종목명`, `증권사`, `발행일`
- 파일명: 영문 + 한글 허용 (예: `extract_한국투자증권.py`)
- 모든 public 함수에 docstring 작성
- 로깅은 `logging` 모듈 사용 (print 금지 — one-off 스크립트 제외)

### 실행 (Phase 1)

```bash
# 당일 데이터 수집 → 텔레그램 브리핑
python upload_naver.py --date 2026-06-15
python upload_koreainvest.py --date 2026-06-15
python extract_silver.py --start 2026-06-15 --end 2026-06-15 --workers 20
python extract_gold_structured.py --start 2026-06-15 --end 2026-06-15 --workers 20 --force-refresh
python telegram_briefing.py --date 2026-06-15
```

### 실행 (Phase 2 — Airflow)

DAG `nightly_batch`가 매일 16:00 KST에 자동 실행됨.
EC2 배포 전까지는 로컬에서 수동 테스트.

## 디렉토리 구조

```
opik/
├── collectors/       # 네이버/한국투자증권 등 수집기
├── extract/          # 개별 증권사 직접 수집 (Playwright 기반)
├── embedding/        # 임베딩 파이프라인 (LLM 추출 + 벡터화)
├── dags/             # Airflow DAG 정의
├── spark_jobs/       # Spark 배치 처리
├── *.py              # 핵심 스크립트 (upload, extract, telegram_briefing)
└── docs/*.md         # 설계 문서
```

## DART 통합 (상용님 담당)

DART 데이터는 별도 레포지토리 (DartCollector)에서 관리.
Gold 레이어 완성 후 OPIK scoring pipeline과 통합.

## 연락처

- 윤준호: 리포트 수집 + 전체 아키텍처
- 태주: 임베딩 + LLM 파이프라인
- 상용: DART 공시 데이터
- 찬호: 주가 예측 모델
