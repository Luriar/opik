"""OPIK — 신한투자증권 리포트 Bronze 적재 DAG.

핵심 로직은 기존 로컬 수집 스크립트와 동일하다.
    - 신한 프론트 API: curPage/startPage/startId 페이지네이션
    - f3 PDF URL 직접 다운로드
    - report_id = md5(증권사 + 종목코드 + 제목 + 발행일)
    - bronze/신한투자증권/YYYY-MM-DD/{report_id}.pdf 저장
    - bronze/신한투자증권/YYYY-MM-DD/_manifest.json 저장

Usage:
    python upload_shinhaninvest.py --date 2026-06-12
    python upload_shinhaninvest.py --start 2026-06-01 --end 2026-06-12
    python upload_shinhaninvest.py --date 2026-06-12 --dry-run
"""

from __future__ import annotations

from pathlib import Path
import sys

OPIK_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(OPIK_ROOT))

import argparse
import hashlib
import json
import logging
import os
import re
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from typing import Any

import pendulum
import requests
import boto3
from botocore.exceptions import ClientError

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # 로컬 CLI 실행 환경에는 Airflow가 없을 수 있다.
    DAG = None
    PythonOperator = None


def load_local_env() -> None:
    """Load .env without depending on opik_config."""
    candidates = []
    if root := os.getenv("OPIK_ROOT"):
        candidates.append(Path(root) / ".env")
    candidates.extend([
        OPIK_ROOT / ".env",
        Path(__file__).resolve().parents[1] / ".env",
        Path(__file__).parent / ".env",
        Path.cwd() / ".env",
    ])

    for env_path in candidates:
        if not env_path.exists():
            continue
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip("'\""))
        break


load_local_env()

S3_BUCKET = (
    os.getenv("S3_BUCKET")
    or os.getenv("AWS_S3_BUCKET_NAME")
    or "s3-opik-bucket"
).strip("'\"")
S3_REGION = (
    os.getenv("S3_REGION")
    or os.getenv("AWS_REGION")
    or os.getenv("AWS_DEFAULT_REGION")
    or "ap-northeast-2"
).strip("'\"")
s3_client = boto3.client("s3", region_name=S3_REGION)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("opik.bronze.shinhaninvest")

LIST_URL = "https://bbs2.shinhansec.com/bbs/list/gicompanyanalyst"
FIRM_NAME = "신한투자증권"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.shinhansec.com/siw/insights/industry/gicompanyanalyst/view.do",
}

PUT_EXTRA: dict[str, Any] = {}


def normalize_date(value: str | date, fmt: str = "iso") -> str:
    if isinstance(value, date):
        parsed = value
    else:
        parsed = None
        for pattern in ("%Y-%m-%d", "%Y.%m.%d"):
            try:
                parsed = datetime.strptime(str(value), pattern).date()
                break
            except ValueError:
                pass
        if parsed is None:
            raise ValueError(f"날짜 형식 오류: {value} (YYYY-MM-DD 또는 YYYY.MM.DD)")
    return parsed.strftime("%Y.%m.%d") if fmt == "api" else parsed.isoformat()


def bronze_key(pub_date: str, report_id: str) -> str:
    return f"bronze/{FIRM_NAME}/{pub_date}/{report_id}.pdf"


def manifest_key(pub_date: str) -> str:
    return f"bronze/{FIRM_NAME}/{pub_date}/_manifest.json"


def s3_exists(key: str) -> bool:
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def s3_upload_json(key: str, data: Any) -> None:
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )


def get_json(session: requests.Session, params: dict[str, Any]) -> dict[str, Any]:
    response = session.get(LIST_URL, params=params, timeout=15)
    response.raise_for_status()
    return response.json()


def extract_stock_code(title: str) -> str:
    match = re.search(r"\((\d{6})\)", title or "")
    return match.group(1) if match else "000000"


def make_report_id(item: dict[str, Any], reg_date: str) -> str:
    stock_code = extract_stock_code(item.get("f1", ""))
    raw = f"{FIRM_NAME}_{stock_code}_{item.get('f1', '')}_{reg_date}"
    return hashlib.md5(raw.encode()).hexdigest()


def download_and_upload_pdf(
    session: requests.Session,
    pdf_url: str,
    s3_key: str,
    dry_run: bool,
) -> tuple[str, str]:
    if not pdf_url:
        return "", "pdf_missing"
    if s3_exists(s3_key):
        return s3_key, "pending"
    if dry_run:
        return "", "dry_run"

    try:
        response = session.get(pdf_url, timeout=20)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "").lower()
        is_pdf = "pdf" in content_type or response.content[:4] == b"%PDF"
        if not is_pdf:
            logger.warning("PDF 아님: %s | content-type=%s", pdf_url[:100], content_type)
            return "", "pdf_missing"
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=response.content,
            ContentType="application/pdf",
            **PUT_EXTRA,
        )
        return s3_key, "pending"
    except Exception as exc:
        logger.warning("PDF 실패: %s | %s", pdf_url[:100], exc)
        return "", "pdf_missing"


def run_bronze_shinhaninvest(
    target_date: str | date | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    page_sleep: float = 1.5,
    pdf_sleep: float = 1.0,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Airflow PythonOperator 엔트리포인트."""
    if target_date:
        start_api = end_api = normalize_date(target_date, "api")
    else:
        default_day = date.today() - timedelta(days=1)
        start_api = normalize_date(start_date or default_day, "api")
        end_api = normalize_date(end_date or start_date or default_day, "api")
    if start_api > end_api:
        raise ValueError(f"start_date가 end_date보다 늦습니다: {start_api} > {end_api}")

    session = requests.Session()
    session.headers.update(HEADERS)

    cur_page = 1
    start_page = 1
    start_id = None
    stop = False
    seen_ids: set[str] = set()
    seen_report_ids: dict[str, str] = {}
    manifests_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    stats = Counter()

    while not stop:
        params = {
            "v": int(time.time() * 1000),
            "curPage": cur_page,
            "startPage": start_page,
        }
        if start_id:
            params["startId"] = start_id

        try:
            data = get_json(session, params)
        except Exception as exc:
            logger.error("목록 요청 실패: page=%s startPage=%s startId=%s | %s",
                         cur_page, start_page, start_id, exc)
            stats["list_failed"] += 1
            break

        items = data.get("list", [])
        if not items or items[0].get("fn") in seen_ids:
            logger.info("페이지 %s: 더 이상 새 항목 없음", cur_page)
            break

        for item in items:
            fn = item.get("fn")
            if fn in seen_ids:
                continue
            seen_ids.add(fn)

            reg_date = item.get("f0", "")
            if reg_date > end_api:
                continue
            if reg_date < start_api:
                logger.info("[%s] 시작일(%s) 이전 도달 → 종료", reg_date, start_api)
                stop = True
                break

            report_id = make_report_id(item, reg_date)
            if report_id in seen_report_ids:
                logger.warning("중복 report_id: %s 이전 fn=%s 현재 fn=%s",
                               report_id, seen_report_ids[report_id], fn)
            seen_report_ids[report_id] = fn

            pub_date = reg_date.replace(".", "-")
            pdf_key = bronze_key(pub_date, report_id)
            s3_key, parse_status = download_and_upload_pdf(
                session,
                item.get("f3", ""),
                pdf_key,
                dry_run=dry_run,
            )
            if pdf_sleep and not dry_run:
                time.sleep(pdf_sleep)

            entry = {
                "report_id": report_id,
                "source": FIRM_NAME,
                "title": item.get("f1", ""),
                "종목명": item.get("f2", ""),
                "종목코드": extract_stock_code(item.get("f1", "")),
                "증권사": FIRM_NAME,
                "발행일": pub_date,
                "s3_key": s3_key,
                "파싱상태": parse_status,
            }
            manifests_by_date[pub_date].append(entry)
            stats[parse_status] += 1
            stats["total"] += 1

        logger.info("페이지 %s 완료 — 누적 %d건", cur_page, stats["total"])

        page_ids = data.get("pageInfo", {}).get("pages", [])
        if len(page_ids) > 1:
            start_id = page_ids[1]
        cur_page += 1
        start_page += 1
        if page_sleep:
            time.sleep(page_sleep)

    if not dry_run:
        for pub_date, entries in manifests_by_date.items():
            s3_upload_json(manifest_key(pub_date), entries)
            logger.info("manifest 저장: %s (%d건)", manifest_key(pub_date), len(entries))

    result = {
        "start_date": start_api.replace(".", "-"),
        "end_date": end_api.replace(".", "-"),
        "total": stats["total"],
        "pending": stats["pending"],
        "pdf_missing": stats["pdf_missing"],
        "dry_run": stats["dry_run"],
        "list_failed": stats["list_failed"],
        "manifest_dates": sorted(manifests_by_date.keys()),
    }
    logger.info("신한 bronze 완료: %s", result)
    return result


def build_dag():
    if DAG is None:
        return None

    default_args = {
        "owner": "opik",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    }

    with DAG(
        dag_id="opik_bronze_shinhaninvest",
        description="신한투자증권 기업분석 리포트 PDF를 bronze S3에 일배치 적재",
        default_args=default_args,
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
        schedule="50 1 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["opik", "bronze", "shinhaninvest", "reports"],
    ) as dag_obj:
        PythonOperator(
            task_id="upload_shinhaninvest_reports_to_bronze",
            python_callable=run_bronze_shinhaninvest,
            op_kwargs={
                "target_date": "{{ ds }}",
                "page_sleep": 1.5,
                "pdf_sleep": 1.0,
            },
        )

    return dag_obj


dag = build_dag()


def parse_args():
    parser = argparse.ArgumentParser(description="OPIK Bronze 적재 (신한투자증권)")
    parser.add_argument("--date", type=str, help="수집일 YYYY-MM-DD")
    parser.add_argument("--start", type=str, help="시작일 YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="종료일 YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--page-sleep", type=float, default=1.5)
    parser.add_argument("--pdf-sleep", type=float, default=1.0)
    return parser.parse_args()


def main():
    args = parse_args()
    result = run_bronze_shinhaninvest(
        target_date=args.date,
        start_date=args.start,
        end_date=args.end,
        page_sleep=args.page_sleep,
        pdf_sleep=args.pdf_sleep,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
