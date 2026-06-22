"""OPIK — 네이버 경유 증권사 리포트 Bronze 일배치 DAG.

핵심 로직:
    - NaverCollector로 실행일 하루치 리포트 메타데이터 수집
    - PDF URL이 있는 항목만 다운로드
    - bronze/{증권사}/{YYYY-MM-DD}/{report_id}.pdf 저장
    - bronze/{증권사}/{YYYY-MM-DD}/_manifest.json 저장

일배치 전제:
    - 백필/checkpoint/resume 로직은 제거
    - 이미 PDF가 존재해도 manifest에는 pending 상태로 다시 기록
    - silver DAG는 이 manifest를 읽어 같은 날짜의 PDF만 처리

Usage:
    python upload_naver.py --date 2026-06-12
    python upload_naver.py --date 2026-06-12 --dry-run
"""

from __future__ import annotations

from pathlib import Path
import sys

OPIK_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(OPIK_ROOT))
sys.path.insert(0, str(Path(__file__).parent))

import argparse
import asyncio
import hashlib
import json
import logging
import os
import random
import re
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from typing import Any

import pendulum
import aiohttp
import boto3
from botocore.exceptions import ClientError

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:
    DAG = None
    PythonOperator = None


REPORT_PIPELINE_SCHEDULE = os.getenv("OPIK_REPORT_PIPELINE_SCHEDULE", "0 0 * * *")
KST_TARGET_DATE_TEMPLATE = (
    "{{ data_interval_end.in_timezone('Asia/Seoul').subtract(days=1).to_date_string() }}"
)


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
logger = logging.getLogger("opik.bronze.naver")

BASE_URL = "https://finance.naver.com/research/company_list.naver"
DETAIL_URL = "https://finance.naver.com/research/company_read.naver"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

FIRM_MAP: dict[str, str] = {
    "미래에셋증권": "miraeasset",
    "삼성증권": "samsung",
    "NH투자증권": "nh",
    "KB증권": "kb",
    "한국투자증권": "koreainv",
    "신한투자증권": "shinhan",
    "하나증권": "hana",
    "키움증권": "kiwoom",
    "대신증권": "daishin",
    "유안타증권": "yuanta",
    "메리츠증권": "meritz",
    "신영증권": "shinyoung",
    "한화투자증권": "hanwha",
    "교보증권": "kyobo",
    "현대차증권": "hyundai",
    "DB금융투자": "db",
    "SK증권": "sk",
    "IBK투자증권": "ibk",
    "LS증권": "ls",
    "BNK투자증권": "bnk",
    "이베스트투자증권": "ebest",
    "케이프투자증권": "cape",
    "다올투자증권": "daol",
    "부국증권": "bookook",
    "유진투자증권": "eugene",
    "상상인증권": "sangsangin",
    "한양증권": "hanyang",
    "흥국증권": "heungkuk",
}


def parse_date(value: str | date | None) -> date:
    if value is None:
        return date.today()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def bronze_key(firm_kr: str, pub_date: str, report_id: str) -> str:
    return f"bronze/{firm_kr}/{pub_date}/{report_id}.pdf"


def manifest_key(firm_kr: str, pub_date: str) -> str:
    return f"bronze/{firm_kr}/{pub_date}/_manifest.json"


def parse_naver_date(raw: str) -> str | None:
    match = re.match(r"(\d{2})\.(\d{2})\.(\d{2})", raw.strip())
    if not match:
        return None
    year = 2000 + int(match.group(1))
    return f"{year}-{match.group(2)}-{match.group(3)}"


def parse_naver_rows(soup: Any) -> list[Any]:
    for selector in [
        "table.type_1 tr",
        "div.box_type_m table tr",
        "table.Nnavi tr",
    ]:
        rows = soup.select(selector)
        if rows:
            return [
                r for r in rows
                if r.select_one("td.file") or len(r.select("td")) >= 5
            ]
    return [row for row in soup.select("tr") if len(row.select("td")) >= 5]


def parse_naver_row(row: Any) -> dict[str, Any] | None:
    cells = row.select("td")
    if len(cells) < 5:
        return None

    stock_name = ""
    stock_code = ""
    stock_link = cells[0].select_one("a.stock_item") or cells[0].select_one("a")
    if stock_link:
        stock_name = stock_link.get("title") or stock_link.get_text(strip=True)
        if code_match := re.search(r"code=(\d{6})", stock_link.get("href", "")):
            stock_code = code_match.group(1)

    title_link = cells[1].select_one("a")
    if not title_link:
        return None
    title = title_link.get_text(strip=True)
    if not title:
        return None

    nid = ""
    if nid_match := re.search(r"nid=(\d+)", title_link.get("href", "")):
        nid = nid_match.group(1)

    firm_kr = cells[2].get_text(strip=True)
    if firm_kr == "신한투자증권":
        return None
    firm_code = FIRM_MAP.get(firm_kr, firm_kr.lower().replace(" ", ""))

    pdf_url = None
    pdf_link = cells[3].select_one("a")
    if pdf_link:
        href = pdf_link.get("href", "").strip()
        if href and not href.startswith("#") and not href.startswith("javascript:"):
            pdf_url = href

    pub_date = parse_naver_date(cells[4].get_text(strip=True))
    if not pub_date:
        return None

    report_id = hashlib.sha256(f"naver|{nid or title}|{pub_date}".encode()).hexdigest()[:16]
    return {
        "report_id": report_id,
        "source": firm_kr,
        "securities_firm": firm_code,
        "증권사": firm_kr,
        "title": title,
        "stock_code": stock_code,
        "stock_name": stock_name,
        "종목명": stock_name,
        "publish_date": pub_date,
        "발행일": pub_date,
        "pdf_url": pdf_url,
        "detail_url": f"{DETAIL_URL}?nid={nid}" if nid else None,
        "nid": nid,
        "category": "company",
        "collected_at": datetime.now().isoformat(),
    }


def naver_has_next(soup: BeautifulSoup, current_page: int) -> bool:
    pagination = soup.select_one("div.paging, table.Nnavi")
    if not pagination:
        return False
    for link in pagination.select("a"):
        if match := re.search(r"page=(\d+)", link.get("href", "")):
            if int(match.group(1)) > current_page:
                return True
    return False


async def collect_naver_date(target: date) -> list[dict[str, Any]]:
    from bs4 import BeautifulSoup

    results: list[dict[str, Any]] = []
    page = 1
    connector = aiohttp.TCPConnector(limit=5, limit_per_host=3)
    max_pages = 15  # safety cap
    cookie_jar = aiohttp.CookieJar()  # persist JSESSIONID
    async with aiohttp.ClientSession(
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        },
        connector=connector,
        cookie_jar=cookie_jar,
    ) as session:
        while page <= max_pages:
            try:
                async with session.get(
                    f"{BASE_URL}?page={page}",
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    if response.status != 200:
                        logger.warning("Naver list HTTP %d: page=%d", response.status, page)
                        break
                    raw = await response.read()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.warning("Naver list failed: page=%d | %s", page, exc)
                break

            try:
                html = raw.decode("euc-kr")
            except UnicodeDecodeError:
                html = raw.decode("utf-8", errors="replace")

            soup = BeautifulSoup(html, "html.parser")
            rows = parse_naver_rows(soup)
            if not rows:
                break

            reached_past = False
            for row in rows:
                meta = parse_naver_row(row)
                if not meta:
                    continue
                pub_date = datetime.strptime(meta["발행일"], "%Y-%m-%d").date()
                if pub_date == target:
                    results.append(meta)
                elif pub_date < target:
                    reached_past = True
                    break

            if reached_past or not naver_has_next(soup, page):
                break
            page += 1
            await asyncio.sleep(0.3)

    if len(results) == 0 and target.weekday() >= 5:
        logger.info("Naver %s -> 0 reports (weekend)", target.isoformat())
    else:
        logger.info("Naver %s -> %d reports (pages: %d)", target.isoformat(), len(results), page)
    return results


def s3_exists(key: str) -> bool:
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def s3_upload_pdf(key: str, body: bytes) -> bool:
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=body,
            ContentType="application/pdf",
        )
        return True
    except Exception as exc:
        logger.error("S3 PDF upload failed: %s -> %s", key, exc)
        return False


def s3_upload_json(key: str, data: Any) -> None:
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )


async def download_pdf(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    meta: dict[str, Any],
    retries: int = 2,
) -> bytes | None:
    url = meta.get("pdf_url")
    if not url:
        return None

    for attempt in range(retries + 1):
        try:
            async with semaphore:
                await asyncio.sleep(random.uniform(0.02, 0.15))
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    if resp.status != 200:
                        if attempt < retries:
                            await asyncio.sleep(1.5 * (2 ** attempt))
                            continue
                        logger.warning("PDF HTTP %d: %s", resp.status, url[:100])
                        return None
                    data = await resp.read()
                    if len(data) < 1024:
                        if attempt < retries:
                            continue
                        return None
                    return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt < retries:
                await asyncio.sleep(1.5 * (2 ** attempt))
                continue
            logger.warning("PDF download failed: %s -> %s", url[:100], exc)
            return None
    return None


async def download_all_pdfs(metas: list[dict[str, Any]], workers: int) -> dict[str, bytes | None]:
    semaphore = asyncio.Semaphore(workers)
    connector = aiohttp.TCPConnector(limit=workers + 10, limit_per_host=workers + 5)
    async with aiohttp.ClientSession(
        headers={"User-Agent": UA, "Accept": "*/*", "Accept-Encoding": "gzip, deflate, br"},
        connector=connector,
        cookie_jar=cookie_jar,
    ) as session:
        tasks = {
            meta["report_id"]: download_pdf(session, semaphore, meta)
            for meta in metas
            if meta.get("pdf_url")
        }
        if not tasks:
            return {}
        return dict(zip(tasks.keys(), await asyncio.gather(*tasks.values())))


def manifest_entry(meta: dict[str, Any], s3_key: str | None, status: str) -> dict[str, Any]:
    firm = meta["증권사"]
    return {
        "report_id": meta["report_id"],
        "source": firm,
        "title": meta.get("title", ""),
        "종목명": meta.get("종목명", ""),
        "종목코드": meta.get("stock_code") or meta.get("종목코드"),
        "증권사": firm,
        "발행일": meta["발행일"],
        "s3_key": s3_key,
        "파싱상태": status,
    }


async def process_date_metas(
    pub_date: str,
    metas: list[dict[str, Any]],
    pdf_workers: int = 20,
) -> dict[str, Any]:
    """하루치 Naver 메타데이터 -> PDF 업로드 -> firm별 manifest 업로드."""
    started = time.perf_counter()
    stats = Counter(total=len(metas))
    firm_entries: dict[str, list[dict[str, Any]]] = defaultdict(list)
    download_targets: list[dict[str, Any]] = []

    for meta in metas:
        firm = meta["증권사"]
        report_id = meta["report_id"]
        key = bronze_key(firm, meta["발행일"], report_id)

        if not meta.get("pdf_url"):
            firm_entries[firm].append(manifest_entry(meta, None, "pdf_missing"))
            stats["no_pdf"] += 1
            continue

        if s3_exists(key):
            firm_entries[firm].append(manifest_entry(meta, key, "pending"))
            stats["skipped"] += 1
            continue

        download_targets.append(meta)

    pdf_results = await download_all_pdfs(download_targets, workers=pdf_workers)

    for meta in download_targets:
        firm = meta["증권사"]
        report_id = meta["report_id"]
        key = bronze_key(firm, meta["발행일"], report_id)
        pdf_data = pdf_results.get(report_id)

        if pdf_data and s3_upload_pdf(key, pdf_data):
            firm_entries[firm].append(manifest_entry(meta, key, "pending"))
            stats["uploaded"] += 1
        else:
            firm_entries[firm].append(manifest_entry(meta, None, "pdf_missing"))
            stats["failed"] += 1

    for firm, entries in firm_entries.items():
        s3_upload_json(manifest_key(firm, pub_date), entries)
        logger.info("manifest uploaded: %s (%d rows)", manifest_key(firm, pub_date), len(entries))

    elapsed = round(time.perf_counter() - started, 1)
    result = {
        "date": pub_date,
        "total": stats["total"],
        "uploaded": stats["uploaded"],
        "skipped": stats["skipped"],
        "failed": stats["failed"],
        "no_pdf": stats["no_pdf"],
        "manifest_firms": len(firm_entries),
        "elapsed": elapsed,
    }
    logger.info("Naver bronze done: %s", result)
    return result


async def collect_date(target_date: date) -> list[dict[str, Any]]:
    return await collect_naver_date(target_date)


async def run_bronze_naver_async(
    target_date: str | date | None = None,
    pdf_workers: int = 20,
    dry_run: bool = False,
) -> dict[str, Any]:
    target = parse_date(target_date)
    pub_date = target.isoformat()
    metas = await collect_date(target)

    logger.info("Naver target: %s -> %d reports", pub_date, len(metas))
    if dry_run:
        by_firm = dict(sorted(Counter(m.get("증권사", "") for m in metas).items()))
        return {"date": pub_date, "total": len(metas), "by_firm": by_firm, "dry_run": True}

    return await process_date_metas(pub_date, metas, pdf_workers=pdf_workers)


def run_bronze_naver(
    target_date: str | date | None = None,
    pdf_workers: int = 20,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Airflow PythonOperator 엔트리포인트."""
    return asyncio.run(run_bronze_naver_async(target_date, pdf_workers, dry_run))


def build_dag():
    if DAG is None:
        return None

    default_args = {
        "owner": "opik",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    }

    with DAG(
        dag_id="opik_bronze_naver",
        description="네이버 경유 증권사 리포트 PDF를 bronze S3에 일배치 적재",
        default_args=default_args,
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
        schedule=REPORT_PIPELINE_SCHEDULE,
        catchup=False,
        max_active_runs=1,
        tags=["opik", "bronze", "naver", "reports"],
    ) as dag_obj:
        PythonOperator(
            task_id="upload_naver_reports_to_bronze",
            python_callable=run_bronze_naver,
            op_kwargs={
                "target_date": KST_TARGET_DATE_TEMPLATE,
                "pdf_workers": 20,
            },
        )

    return dag_obj


dag = build_dag()


def parse_args():
    parser = argparse.ArgumentParser(description="OPIK Bronze daily upload via Naver")
    parser.add_argument("--date", type=str, help="수집일 YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=20, help="PDF 동시 다운로드 수")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    result = run_bronze_naver(args.date, args.workers, args.dry_run)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
