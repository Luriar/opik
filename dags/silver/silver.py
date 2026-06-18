"""OPIK — Silver 텍스트 추출 DAG.

Bronze PDF -> PyMuPDF text extraction -> Silver JSON

일배치 전제:
    - 기존 5만 건 백필용 전체 manifest scan/cache/checkpoint 로직은 제거
    - 실행일 하루치 bronze/{증권사}/{YYYY-MM-DD}/_manifest.json만 조회
    - manifest의 s3_key가 있는 PDF만 다운로드/파싱
    - silver/{증권사}/{YYYY-MM-DD}/{report_id}.json 저장
    - 텍스트가 부족한 PDF는 silver/_ocr_needed/{YYYY-MM-DD}.json에 기록

Usage:
    python extract_silver.py --date 2026-06-12
    python extract_silver.py --start 2026-06-01 --end 2026-06-12
    python extract_silver.py --date 2026-06-12 --dry-run
"""

from __future__ import annotations

from pathlib import Path
import sys

OPIK_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(OPIK_ROOT))

import argparse
import asyncio
import json
import logging
import os
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import date, datetime, timedelta
from typing import Any

import pendulum
import boto3
from botocore.exceptions import ClientError

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.sensors.external_task import ExternalTaskSensor
except ImportError:  # 로컬 CLI 실행 환경에는 Airflow가 없을 수 있다.
    DAG = None
    PythonOperator = None
    ExternalTaskSensor = None


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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("opik.silver")

s3 = boto3.client("s3", region_name=S3_REGION)

BRONZE_PREFIX = "bronze/"
SILVER_PREFIX = "silver/"
EXTRACT_TIMEOUT = 30
REPORT_PIPELINE_SCHEDULE = os.getenv("OPIK_REPORT_PIPELINE_SCHEDULE", "0 0 * * *")
KST_TARGET_DATE_TEMPLATE = (
    "{{ data_interval_end.in_timezone('Asia/Seoul').subtract(days=1).to_date_string() }}"
)


def upstream_logical_date_for_target(logical_date, data_interval_end=None, **_):
    """Map this run to the scheduled upstream run that owns the same KST target date."""
    interval_end = pendulum.instance(data_interval_end or logical_date).in_timezone("Asia/Seoul")
    return interval_end.start_of("day").subtract(days=1)


def parse_date(value: str | date, label: str = "date") -> date:
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{label}는 YYYY-MM-DD 형식이어야 합니다: {value}") from exc


def iter_dates(start_date: str | date, end_date: str | date):
    current = parse_date(start_date, "start_date")
    end = parse_date(end_date, "end_date")
    if current > end:
        raise ValueError(f"start_date가 end_date보다 늦습니다: {current} > {end}")
    while current <= end:
        yield current.isoformat()
        current += timedelta(days=1)


def default_target_date(lag_days: int = 1) -> str:
    return (date.today() - timedelta(days=lag_days)).isoformat()


def silver_key(meta: dict[str, Any]) -> str:
    return f"{SILVER_PREFIX}{meta['증권사']}/{meta['발행일']}/{meta['report_id']}.json"


def bronze_pdf_key(meta: dict[str, Any]) -> str:
    return normalize_s3_key(meta.get("s3_key")) or (
        f"{BRONZE_PREFIX}{meta['증권사']}/{meta['발행일']}/{meta['report_id']}.pdf"
    )


def bronze_manifest_key(broker: str, date_str: str) -> str:
    return f"{BRONZE_PREFIX}{broker}/{date_str}/_manifest.json"


def ocr_needed_key(date_str: str) -> str:
    return f"{SILVER_PREFIX}_ocr_needed/{date_str}.json"


def s3_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def s3_download(key: str) -> bytes | None:
    try:
        return s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


def s3_load_json(key: str) -> Any | None:
    body = s3_download(key)
    if body is None:
        return None
    return json.loads(body.decode("utf-8"))


def s3_upload_json(key: str, data: Any) -> None:
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )


def normalize_s3_key(value: str | None) -> str:
    if not value:
        return ""
    value = value.strip()
    if value.startswith("s3://"):
        without_scheme = value[5:]
        parts = without_scheme.split("/", 1)
        return parts[1] if len(parts) == 2 else ""
    return value


def normalize_manifest_entry(
    item: Any,
    broker: str,
    date_str: str,
    manifest_key_for_log: str,
) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        logger.warning("manifest 항목이 dict가 아님: %s", manifest_key_for_log)
        return None

    status = item.get("파싱상태")
    if status in {"pdf_missing", "dry_run"}:
        return None

    s3_key = normalize_s3_key(item.get("s3_key"))
    if not s3_key:
        return None

    report_id = item.get("report_id")
    if not report_id:
        logger.warning("report_id 없는 manifest 항목 skip: %s", manifest_key_for_log)
        return None

    normalized = dict(item)
    normalized["s3_key"] = s3_key
    normalized.setdefault("증권사", broker)
    normalized.setdefault("발행일", date_str)
    normalized.setdefault("source", normalized.get("증권사", broker))

    if normalized["발행일"] != date_str:
        logger.warning(
            "manifest 날짜 불일치 skip: %s item_date=%s target=%s",
            manifest_key_for_log,
            normalized["발행일"],
            date_str,
        )
        return None

    return normalized


def list_bronze_brokers() -> list[str]:
    brokers = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=BRONZE_PREFIX, Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            broker = prefix["Prefix"].rstrip("/").split("/")[-1]
            if broker and not broker.startswith("_"):
                brokers.append(broker)
    return sorted(set(brokers))


def load_daily_manifest_entries(date_str: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for broker in list_bronze_brokers():
        key = bronze_manifest_key(broker, date_str)
        manifest = s3_load_json(key)
        if not manifest:
            continue
        if not isinstance(manifest, list):
            logger.warning("manifest 형식이 list가 아님: %s", key)
            continue

        for item in manifest:
            normalized = normalize_manifest_entry(item, broker, date_str, key)
            if normalized:
                entries.append(normalized)
    return entries


def load_manifest_entries_for_range(start_date: str, end_date: str) -> dict[str, list[dict[str, Any]]]:
    by_date: dict[str, list[dict[str, Any]]] = {}
    for date_str in iter_dates(start_date, end_date):
        by_date[date_str] = load_daily_manifest_entries(date_str)
    return by_date


def _fitz_open_safe(pdf_bytes: bytes):
    try:
        import fitz
    except ImportError as exc:
        raise RuntimeError(
            "PyMuPDF가 설치되어 있지 않아 PDF 텍스트 추출을 실행할 수 없습니다. "
            "Airflow 이미지/컨테이너에 `pymupdf`를 설치하세요."
        ) from exc

    try:
        return fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception:
        return None


def extract_text(pdf_bytes: bytes) -> tuple[str, int, int]:
    doc = _fitz_open_safe(pdf_bytes)
    if doc is None:
        return "", 0, 0
    try:
        pages = []
        pages_with_text = 0
        total_pages = len(doc)
        for page in doc:
            try:
                text = page.get_text()
            except (SystemError, RuntimeError):
                continue
            if text.strip():
                pages.append(text.strip())
                pages_with_text += 1
        return "\n\n".join(pages), total_pages, pages_with_text
    finally:
        doc.close()


def extract_text_safe(pdf_bytes: bytes) -> tuple[str, int, int]:
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(extract_text, pdf_bytes)
        try:
            return future.result(timeout=EXTRACT_TIMEOUT)
        except (FutureTimeoutError, SystemError, RuntimeError) as exc:
            logger.debug("extract timeout/error: %s", exc)
            return fallback_extract(pdf_bytes)


def fallback_extract(pdf_bytes: bytes) -> tuple[str, int, int]:
    doc = _fitz_open_safe(pdf_bytes)
    if doc is None:
        return "", 0, 0
    try:
        pages = []
        pages_with_text = 0
        total_pages = len(doc)
        for page in doc:
            try:
                text = page.get_text()
            except Exception:
                continue
            if text.strip():
                pages.append(text.strip())
                pages_with_text += 1
        return "\n\n".join(pages), total_pages, pages_with_text
    finally:
        doc.close()


def build_silver_payload(meta: dict[str, Any], pdf_bytes: bytes) -> dict[str, Any]:
    text, total_pages, pages_with_text = extract_text_safe(pdf_bytes)
    result = {
        "report_id": meta["report_id"],
        "source": meta.get("source") or meta.get("증권사", ""),
        "증권사": meta["증권사"],
        "종목명": meta.get("종목명", ""),
        "종목코드": meta.get("종목코드"),
        "발행일": meta["발행일"],
        "title": meta.get("title") or meta.get("제목", ""),
        "text": text,
        "text_len": len(text),
        "pages_total": total_pages,
        "pages_with_text": pages_with_text,
        "bronze_s3_key": bronze_pdf_key(meta),
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    if total_pages == 0 or pages_with_text == 0 or (
        pages_with_text < total_pages * 0.3 and len(text) < 200
    ):
        result["needs_ocr"] = True
    return result


def process_one(meta: dict[str, Any], overwrite: bool = False) -> tuple[str, dict[str, Any] | None]:
    out_key = silver_key(meta)
    if not overwrite and s3_exists(out_key):
        return "skipped", None

    pdf_key = bronze_pdf_key(meta)
    pdf_bytes = s3_download(pdf_key)
    if not pdf_bytes:
        return "no_pdf", None

    try:
        payload = build_silver_payload(meta, pdf_bytes)
    except Exception as exc:
        logger.error("extract fail %s: %s", meta.get("report_id", "?")[:12], exc)
        return "extract_failed", None

    s3_upload_json(out_key, payload)
    return ("ocr_needed" if payload.get("needs_ocr") else "extracted"), payload


async def process_date(
    date_str: str,
    entries: list[dict[str, Any]],
    workers: int = 10,
    overwrite: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    stats = Counter({"total": len(entries)})

    if dry_run:
        stats["dry_run"] = len(entries)
        return {
            "date": date_str,
            "total": len(entries),
            "dry_run": len(entries),
            "elapsed": round(time.perf_counter() - t0, 1),
        }

    sem = asyncio.Semaphore(workers)

    async def bounded(meta):
        async with sem:
            return await asyncio.to_thread(process_one, meta, overwrite)

    results = await asyncio.gather(*[bounded(entry) for entry in entries], return_exceptions=True)
    ocr_entries = []

    for result in results:
        if isinstance(result, Exception):
            stats["failed"] += 1
            continue

        status, payload = result
        stats[status] += 1
        if status == "ocr_needed" and payload:
            ocr_entries.append({
                "report_id": payload["report_id"],
                "증권사": payload["증권사"],
                "종목명": payload.get("종목명", ""),
                "발행일": payload["발행일"],
                "title": payload.get("title", ""),
                "pages_total": payload.get("pages_total", 0),
                "pages_with_text": payload.get("pages_with_text", 0),
                "text_len": payload.get("text_len", 0),
                "bronze_s3_key": payload.get("bronze_s3_key", ""),
            })

    if ocr_entries:
        s3_upload_json(ocr_needed_key(date_str), ocr_entries)

    elapsed = round(time.perf_counter() - t0, 1)
    return {
        "date": date_str,
        "total": stats["total"],
        "extracted": stats["extracted"],
        "skipped": stats["skipped"],
        "no_pdf": stats["no_pdf"],
        "extract_failed": stats["extract_failed"],
        "upload_failed": stats["upload_failed"],
        "failed": stats["failed"],
        "ocr_needed": stats["ocr_needed"],
        "dry_run": stats["dry_run"],
        "elapsed": elapsed,
    }


def summarize_by_broker(entries: list[dict[str, Any]]) -> dict[str, int]:
    return dict(sorted(Counter(e.get("증권사", "") for e in entries).items()))


async def run_silver_async(
    target_date: str | date | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    workers: int = 10,
    days_parallel: int = 1,
    overwrite: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    if target_date:
        start = end = parse_date(target_date, "date").isoformat()
    else:
        start = parse_date(start_date or default_target_date(), "start").isoformat()
        end = parse_date(end_date or start, "end").isoformat()

    by_date = load_manifest_entries_for_range(start, end)
    total_entries = sum(len(entries) for entries in by_date.values())
    broker_counts = Counter()
    for entries in by_date.values():
        broker_counts.update(e.get("증권사", "") for e in entries)

    logger.info(
        "Silver target: %s ~ %s | entries=%d | workers=%d | days_parallel=%d",
        start,
        end,
        total_entries,
        workers,
        days_parallel,
    )
    if dry_run:
        logger.info("broker counts: %s", dict(sorted(broker_counts.items())))

    dates = sorted(by_date.keys())
    date_results = []
    for idx in range(0, len(dates), days_parallel):
        chunk = dates[idx:idx + days_parallel]
        results = await asyncio.gather(*[
            process_date(d, by_date[d], workers=workers, overwrite=overwrite, dry_run=dry_run)
            for d in chunk
        ])
        date_results.extend(results)
        for result in results:
            logger.info(
                "[%s] extracted=%d skipped=%d ocr=%d total=%d (%.1fs)",
                result["date"],
                result.get("extracted", 0),
                result.get("skipped", 0),
                result.get("ocr_needed", 0),
                result["total"],
                result["elapsed"],
            )

    totals = Counter()
    for result in date_results:
        totals.update({k: v for k, v in result.items() if isinstance(v, int)})

    return {
        "start_date": start,
        "end_date": end,
        "total_entries": total_entries,
        "broker_counts": dict(sorted(broker_counts.items())),
        "date_results": date_results,
        "totals": dict(totals),
        "dry_run": dry_run,
    }


def run_silver(
    target_date: str | date | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    workers: int = 10,
    days_parallel: int = 1,
    overwrite: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Airflow PythonOperator 엔트리포인트."""
    return asyncio.run(run_silver_async(
        target_date=target_date,
        start_date=start_date,
        end_date=end_date,
        workers=workers,
        days_parallel=days_parallel,
        overwrite=overwrite,
        dry_run=dry_run,
    ))


def build_dag():
    if DAG is None:
        return None

    default_args = {
        "owner": "opik",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    }

    with DAG(
        dag_id="opik_silver_extract",
        description="bronze PDF를 silver JSON 텍스트로 일배치 변환",
        default_args=default_args,
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
        schedule=REPORT_PIPELINE_SCHEDULE,
        catchup=False,
        max_active_runs=1,
        tags=["opik", "silver", "pdf", "reports"],
    ) as dag_obj:
        wait_for_naver = ExternalTaskSensor(
            task_id="wait_for_bronze_naver",
            external_dag_id="opik_bronze_naver",
            external_task_id="upload_naver_reports_to_bronze",
            execution_date_fn=upstream_logical_date_for_target,
            allowed_states=["success"],
            failed_states=["failed", "skipped"],
            check_existence=True,
            mode="reschedule",
            poll_interval=60,
            timeout=6 * 60 * 60,
        )
        wait_for_koreainvest = ExternalTaskSensor(
            task_id="wait_for_bronze_koreainvest",
            external_dag_id="opik_bronze_koreainvest",
            external_task_id="upload_koreainvest_reports_to_bronze",
            execution_date_fn=upstream_logical_date_for_target,
            allowed_states=["success"],
            failed_states=["failed", "skipped"],
            check_existence=True,
            mode="reschedule",
            poll_interval=60,
            timeout=6 * 60 * 60,
        )
        wait_for_shinhaninvest = ExternalTaskSensor(
            task_id="wait_for_bronze_shinhaninvest",
            external_dag_id="opik_bronze_shinhaninvest",
            external_task_id="upload_shinhaninvest_reports_to_bronze",
            execution_date_fn=upstream_logical_date_for_target,
            allowed_states=["success"],
            failed_states=["failed", "skipped"],
            check_existence=True,
            mode="reschedule",
            poll_interval=60,
            timeout=6 * 60 * 60,
        )
        extract_task = PythonOperator(
            task_id="extract_bronze_pdf_to_silver_json",
            python_callable=run_silver,
            op_kwargs={
                "target_date": KST_TARGET_DATE_TEMPLATE,
                "workers": 10,
                "days_parallel": 1,
                "overwrite": False,
            },
        )

        [wait_for_naver, wait_for_koreainvest, wait_for_shinhaninvest] >> extract_task

    return dag_obj


dag = build_dag()


def parse_args():
    parser = argparse.ArgumentParser(description="OPIK Silver extract")
    parser.add_argument("--date", type=str, help="처리일 YYYY-MM-DD")
    parser.add_argument("--start", type=str, help="시작일 YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="종료일 YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--days", type=int, default=1, help="동시에 처리할 날짜 수")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    result = run_silver(
        target_date=args.date,
        start_date=args.start,
        end_date=args.end,
        workers=args.workers,
        days_parallel=args.days,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
