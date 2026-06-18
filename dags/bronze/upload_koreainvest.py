"""OPIK — 한국투자증권 리서치 리포트 Bronze 일배치 DAG.

핵심 로직:
    - 한국투자증권 collector로 실행일 하루치 리포트 메타데이터 수집
    - PDF URL 다운로드
    - bronze/한국투자증권/{YYYY-MM-DD}/{report_id}.pdf 저장
    - bronze/한국투자증권/{YYYY-MM-DD}/_manifest.json 저장

일배치 전제:
    - 백필/checkpoint/resume 로직은 제거
    - 이미 PDF가 존재해도 manifest에는 pending 상태로 다시 기록
    - silver DAG는 이 manifest를 읽어 같은 날짜의 PDF만 처리

Usage:
    python upload_koreainvest.py --date 2026-06-12
    python upload_koreainvest.py --date 2026-06-12 --dry-run
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
from collections import Counter
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
    "{{ data_interval_end.in_timezone('Asia/Seoul').to_date_string() }}"
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
logger = logging.getLogger("opik.bronze.koreainvest")

FIRM_NAME = "한국투자증권"
BASE_URL = "https://securities.koreainvestment.com"
MAIN_URL = f"{BASE_URL}/main.jsp"
LIST_URL = f"{BASE_URL}/main/research/research/Strategy.jsp"
PDF_SERVLET = "https://file.truefriend.com/servlet/Download"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)
KOREAINVEST_DAILY_PAGE_BATCH = int(os.getenv("KOREAINVEST_DAILY_PAGE_BATCH", "5"))
KOREAINVEST_DAILY_MAX_PAGES = int(os.getenv("KOREAINVEST_DAILY_MAX_PAGES", "50"))

CATEGORY_FILEPATH_MAP: dict[str, str] = {
    "01_01": "research/research01",
    "01_02": "research/research01",
    "01_03": "research/research01",
    "01_04": "research/research01",
    "01_05": "research/research01",
    "01_06": "research/research12",
    "02_01": "research/research02",
    "02_02": "research/research02",
    "02_03": "research/research02",
    "02_04": "research/research02",
    "02_06": "research/research02",
    "02_08": "research/research02",
    "02_09": "research/research02",
    "02_10": "research/research02",
    "02_11": "research/research02",
    "02_12": "research/research02",
    "02_13": "research/research02",
    "02_14": "research/research02",
    "03_01": "research/research03",
    "03_02": "research/research03",
    "03_03": "research/research03",
    "04_00": "research/research04",
    "04_01": "research/research01",
    "04_02": "research/research01",
    "04_03": "research/research01",
    "05_00": "research/research05",
    "05_01": "research/research05",
    "06_01": "research/research06",
    "06_02": "research/research06",
    "07_01": "research/research07",
    "08_03": "research/research08",
    "08_04": "research/research08",
    "08_05": "research/research08",
    "09_00": "research/research11",
    "10_01": "research/research10",
    "10_04": "research/research10",
    "10_06": "research/research_emailcomment",
    "13_01": "research/research11",
    "14_01": "research/research14",
    "15_01": "research/research01",
    "16_01": "research/research15",
    "17_00": "research/research17",
}


def parse_date(value: str | date | None) -> date:
    if value is None:
        return date.today()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def bronze_key(pub_date: str, report_id: str) -> str:
    return f"bronze/{FIRM_NAME}/{pub_date}/{report_id}.pdf"


def manifest_key(pub_date: str) -> str:
    return f"bronze/{FIRM_NAME}/{pub_date}/_manifest.json"


def get_koreainvest_filepath(category1: str, category2: str = "01") -> str:
    key = f"{category1}_{category2}"
    if key in CATEGORY_FILEPATH_MAP:
        return CATEGORY_FILEPATH_MAP[key]
    for mapped_key, filepath in CATEGORY_FILEPATH_MAP.items():
        if mapped_key.startswith(f"{category1}_"):
            return filepath
    return f"research/research{category1.zfill(2)}"


def build_koreainvest_pdf_url(filepath: str, filename: str) -> str:
    if not filename:
        return ""
    if filepath.startswith("http"):
        return f"{filepath.rstrip('/')}/{filename}"
    if filepath.startswith("?") or filepath.startswith("&"):
        params: dict[str, str] = {}
        for part in filepath.lstrip("?&").split("&"):
            if "=" in part:
                key, value = part.split("=", 1)
                params[key] = value
        mapped_path = get_koreainvest_filepath(
            params.get("category1", "05"),
            params.get("category2", "01"),
        )
    else:
        mapped_path = filepath
    return f"{PDF_SERVLET}?file_path={mapped_path}/&file_name={filename}"


def parse_koreainvest_items(html: str) -> list[dict[str, Any]]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, Any]] = []
    for row in soup.select("ul.view_area.line li"):
        item = parse_koreainvest_item(row)
        if item:
            items.append(item)
    return items


def parse_koreainvest_item(row: Any) -> dict[str, Any] | None:
    view = row.select_one("a.view_con")
    if not view:
        return None

    detail_id = ""
    if match := re.search(r"doDetail\('(\d+)'\)", view.get("onclick", "")):
        detail_id = match.group(1)

    head = view.select_one("div.head")
    title = view.select_one("span.body_tit")
    if not title:
        return None
    title_text = title.get_text(strip=True)
    if not title_text:
        return None

    info = view.select("span.tit_info em")
    analyst = info[0].get_text(strip=True) if len(info) >= 1 else ""
    pub_raw = info[-1].get_text(strip=True) if len(info) >= 2 else ""
    date_match = re.match(r"(\d{4})\.(\d{2})\.(\d{2})", pub_raw)
    if not date_match:
        return None
    pub_date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"

    pdf_url = ""
    pdf_path = ""
    pdf_filename = ""
    pdf_button = row.select_one("a.pdf_btn")
    if pdf_button:
        pdf_match = re.search(
            r"(?:prePdfFileView|pdfFileView)\s*\(\s*'([^']*)'\s*,\s*'([^']*)'",
            pdf_button.get("onclick", ""),
        )
        if pdf_match:
            pdf_path = pdf_match.group(1)
            pdf_filename = pdf_match.group(2)
            pdf_url = build_koreainvest_pdf_url(pdf_path, pdf_filename)

    report_id = hashlib.sha256(f"koreainvest|{detail_id or title_text}|{pub_date}".encode()).hexdigest()[:16]
    return {
        "report_id": report_id,
        "source": FIRM_NAME,
        "securities_firm": "koreainv",
        "증권사": FIRM_NAME,
        "title": title_text,
        "analyst": analyst,
        "publish_date": pub_date,
        "발행일": pub_date,
        "pdf_url": pdf_url,
        "detail_id": detail_id,
        "category": "company",
        "category_head": head.get_text(strip=True) if head else "",
        "pdf_filepath": pdf_path,
        "pdf_filename": pdf_filename,
        "collected_at": datetime.now().isoformat(),
    }


def get_koreainvest_last_page(html: str) -> int:
    pages = [int(value) for value in re.findall(r"goPage\('?(\d+)'?\)", html)]
    return max(pages) if pages else 1


async def fetch_koreainvest_page(
    session: aiohttp.ClientSession,
    page: int,
    from_date: str,
    to_date: str,
) -> str | None:
    params = {
        "jkGubun": "10",
        "category1": "05",
        "category2": "01",
        "searchDate": "all",
        "fromDate": from_date,
        "toDate": to_date,
        "currentPage": str(page),
    }
    try:
        async with session.get(
            LIST_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:
            if response.status != 200:
                logger.warning("KoreaInvest list HTTP %d: page=%d", response.status, page)
                return None
            raw = await response.read()
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        logger.warning("KoreaInvest list failed: page=%d | %s", page, exc)
        return None

    try:
        return raw.decode("euc-kr")
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace")


async def collect_koreainvest_date(
    target: date,
    page_batch: int = KOREAINVEST_DAILY_PAGE_BATCH,
    max_pages: int = KOREAINVEST_DAILY_MAX_PAGES,
) -> list[dict[str, Any]]:
    from_date = target.strftime("%Y.%m.%d")
    to_date = target.strftime("%Y.%m.%d")
    target_iso = target.isoformat()
    results: list[dict[str, Any]] = []
    seen: set[str] = set()
    connector = aiohttp.TCPConnector(limit=15, limit_per_host=10)

    async with aiohttp.ClientSession(
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        },
        connector=connector,
    ) as session:
        try:
            async with session.get(MAIN_URL, timeout=aiohttp.ClientTimeout(total=15)) as response:
                await response.read()
        except Exception as exc:
            logger.warning("KoreaInvest session init failed: %s", exc)

        page = 1
        last_page: int | None = None
        reached_past = False
        while True:
            end_page = min(page + page_batch, max_pages + 1)
            pages = list(range(page, end_page))
            if not pages:
                logger.warning("KoreaInvest max_pages reached: target=%s max_pages=%d", target_iso, max_pages)
                break

            logger.info("KoreaInvest list fetch: target=%s pages=%d-%d", target_iso, pages[0], pages[-1])
            html_results = await asyncio.gather(*[
                fetch_koreainvest_page(session, page_no, from_date, to_date)
                for page_no in pages
            ])

            for page_no, html in zip(pages, html_results):
                if not html:
                    continue
                if "BoxError" in html or "조회가 불가능" in html:
                    logger.warning("KoreaInvest error page: page=%d", page_no)
                    continue
                if last_page is None:
                    last_page = get_koreainvest_last_page(html)

                page_items = parse_koreainvest_items(html)
                for item in page_items:
                    pub_date = item.get("발행일", "")
                    if pub_date < target_iso:
                        reached_past = True
                        break
                    if pub_date > target_iso:
                        continue
                    report_id = item["report_id"]
                    if report_id in seen:
                        continue
                    seen.add(report_id)
                    results.append(item)

                if reached_past:
                    logger.info(
                        "KoreaInvest reached past target: target=%s page=%d collected=%d",
                        target_iso,
                        page_no,
                        len(results),
                    )
                    break

            if reached_past:
                break
            if last_page is not None and pages[-1] >= last_page:
                break
            if last_page is None and not any(html_results):
                break
            if pages[-1] >= max_pages:
                logger.warning("KoreaInvest max_pages reached: target=%s max_pages=%d", target_iso, max_pages)
                break
            page += page_batch
            await asyncio.sleep(0.1)

    logger.info("KoreaInvest %s -> %d reports", target_iso, len(results))
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
                await asyncio.sleep(random.uniform(0.05, 0.2))
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
    return {
        "report_id": meta["report_id"],
        "source": FIRM_NAME,
        "title": meta.get("title", ""),
        "analyst": meta.get("analyst", ""),
        "종목명": meta.get("종목명", ""),
        "종목코드": meta.get("종목코드") or meta.get("stock_code"),
        "증권사": FIRM_NAME,
        "발행일": meta["발행일"],
        "s3_key": s3_key,
        "파싱상태": status,
    }


async def process_date_metas(
    pub_date: str,
    metas: list[dict[str, Any]],
    pdf_workers: int = 25,
) -> dict[str, Any]:
    started = time.perf_counter()
    stats = Counter(total=len(metas))
    manifest_entries: list[dict[str, Any]] = []
    download_targets: list[dict[str, Any]] = []

    for meta in metas:
        report_id = meta["report_id"]
        key = bronze_key(meta["발행일"], report_id)

        if not meta.get("pdf_url"):
            manifest_entries.append(manifest_entry(meta, None, "pdf_missing"))
            stats["no_pdf"] += 1
            continue

        if s3_exists(key):
            manifest_entries.append(manifest_entry(meta, key, "pending"))
            stats["skipped"] += 1
            continue

        download_targets.append(meta)

    pdf_results = await download_all_pdfs(download_targets, workers=pdf_workers)

    for meta in download_targets:
        report_id = meta["report_id"]
        key = bronze_key(meta["발행일"], report_id)
        pdf_data = pdf_results.get(report_id)

        if pdf_data and s3_upload_pdf(key, pdf_data):
            manifest_entries.append(manifest_entry(meta, key, "pending"))
            stats["uploaded"] += 1
        else:
            manifest_entries.append(manifest_entry(meta, None, "pdf_missing"))
            stats["failed"] += 1

    s3_upload_json(manifest_key(pub_date), manifest_entries)
    logger.info("manifest uploaded: %s (%d rows)", manifest_key(pub_date), len(manifest_entries))

    elapsed = round(time.perf_counter() - started, 1)
    result = {
        "date": pub_date,
        "total": stats["total"],
        "uploaded": stats["uploaded"],
        "skipped": stats["skipped"],
        "failed": stats["failed"],
        "no_pdf": stats["no_pdf"],
        "elapsed": elapsed,
    }
    logger.info("KoreaInvest bronze done: %s", result)
    return result


def collect_today() -> list[dict[str, Any]]:
    return asyncio.run(collect_koreainvest_date(date.today()))


async def collect_date(target: date) -> list[dict[str, Any]]:
    return await collect_koreainvest_date(target)


async def run_bronze_koreainvest_async(
    target_date: str | date | None = None,
    pdf_workers: int = 25,
    dry_run: bool = False,
) -> dict[str, Any]:
    target = parse_date(target_date)
    pub_date = target.isoformat()
    metas = await collect_date(target)

    logger.info("KoreaInvest target: %s -> %d reports", pub_date, len(metas))
    if dry_run:
        categories = dict(sorted(Counter(m.get("category_head", "") for m in metas).items()))
        return {"date": pub_date, "total": len(metas), "categories": categories, "dry_run": True}

    return await process_date_metas(pub_date, metas, pdf_workers=pdf_workers)


def run_bronze_koreainvest(
    target_date: str | date | None = None,
    pdf_workers: int = 25,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Airflow PythonOperator 엔트리포인트."""
    return asyncio.run(run_bronze_koreainvest_async(target_date, pdf_workers, dry_run))


def build_dag():
    if DAG is None:
        return None

    default_args = {
        "owner": "opik",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    }

    with DAG(
        dag_id="opik_bronze_koreainvest",
        description="한국투자증권 리서치 리포트 PDF를 bronze S3에 일배치 적재",
        default_args=default_args,
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
        schedule=REPORT_PIPELINE_SCHEDULE,
        catchup=False,
        max_active_runs=1,
        tags=["opik", "bronze", "koreainvest", "reports"],
    ) as dag_obj:
        PythonOperator(
            task_id="upload_koreainvest_reports_to_bronze",
            python_callable=run_bronze_koreainvest,
            op_kwargs={
                "target_date": KST_TARGET_DATE_TEMPLATE,
                "pdf_workers": 25,
            },
        )

    return dag_obj


dag = build_dag()


def parse_args():
    parser = argparse.ArgumentParser(description="OPIK Bronze daily upload for KoreaInvest")
    parser.add_argument("--date", type=str, help="수집일 YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=25, help="PDF 동시 다운로드 수")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    result = run_bronze_koreainvest(args.date, args.workers, args.dry_run)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
