"""OPIK — Silver 텍스트 추출 파이프라인

S3 bronze PDF → PyMuPDF 텍스트 추출 → S3 silver JSON 적재

사용법:
    python extract_silver.py --dry-run
    python extract_silver.py --start 2026-01-01 --end 2026-12-31 --workers 10
    python extract_silver.py --days 5 --workers 40
    python extract_silver.py --resume
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import datetime
from pathlib import Path
from typing import Optional

import boto3
import fitz
from botocore.config import Config
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("opik.silver")

S3_BUCKET = os.getenv("S3_BUCKET", "s3-opik-bucket")
S3_REGION = os.getenv("S3_REGION", "ap-northeast-2")
CHECKPOINT_FILE = Path(__file__).parent / ".silver_checkpoint.json"
MANIFEST_CACHE_FILE = Path(__file__).parent / ".silver_manifest_cache.json"

s3 = boto3.client(
    "s3", region_name=S3_REGION,
    config=Config(max_pool_connections=50),
)


def silver_key(meta: dict) -> str:
    return f"silver/{meta['증권사']}/{meta['발행일']}/{meta['report_id']}.json"


def bronze_pdf_key(meta: dict) -> str:
    return f"bronze/{meta['증권사']}/{meta['발행일']}/{meta['report_id']}.pdf"


async def s3_download(key: str) -> Optional[bytes]:
    def _dl():
        try:
            return s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        except ClientError as e:
            if "NoSuchKey" in str(e):
                return None
            raise
    return await asyncio.to_thread(_dl)


async def s3_exists(key: str) -> bool:
    try:
        await asyncio.to_thread(s3.head_object, Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError:
        return False


async def s3_upload_json(key: str, data) -> bool:
    try:
        body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        await asyncio.to_thread(
            s3.put_object, Bucket=S3_BUCKET, Key=key,
            Body=body, ContentType="application/json",
        )
        return True
    except Exception as e:
        logger.error("S3 upload fail %s: %s", key, e)
        return False


async def discover_manifests() -> list[str]:
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    page_n = 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="bronze/"):
        page_n += 1
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith("_manifest.json"):
                keys.append(k)
        if page_n % 10 == 0:
            logger.info("  listing page %d... (manifests %d)", page_n, len(keys))
    return keys


async def load_manifest_entries(manifest_key: str) -> list[dict]:
    try:
        body = await s3_download(manifest_key)
        if not body:
            return []
        entries = json.loads(body.decode("utf-8"))
        return [e for e in entries if e.get("s3_key") and e.get("파싱상태") != "pdf_missing"]
    except Exception as e:
        logger.warning("manifest load fail %s: %s", manifest_key, e)
        return []


_EXTRACT_TIMEOUT = 30


def _fitz_open_safe(pdf_bytes: bytes):
    """fitz.open() 자체가 C++ 콜백 충돌로 죽는 PDF 방어"""
    try:
        return fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception:
        return None


def extract_text(pdf_bytes: bytes) -> tuple:
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


def extract_text_safe(pdf_bytes: bytes) -> tuple:
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(extract_text, pdf_bytes)
        try:
            return future.result(timeout=_EXTRACT_TIMEOUT)
        except (FutureTimeoutError, SystemError, RuntimeError) as e:
            logger.debug("extract timeout/error: %s", e)
            return _fallback_extract(pdf_bytes)


def _fallback_extract(pdf_bytes: bytes) -> tuple:
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
                if text.strip():
                    pages.append(text.strip())
                    pages_with_text += 1
            except Exception:
                continue
        return "\n\n".join(pages), total_pages, pages_with_text
    finally:
        doc.close()


def _do_extract(meta: dict, pdf_bytes: bytes) -> dict:
    text, total_pages, pages_with_text = extract_text_safe(pdf_bytes)
    result = {
        "report_id": meta["report_id"],
        "source": meta.get("source", ""),
        "증권사": meta["증권사"],
        "종목명": meta.get("종목명", ""),
        "발행일": meta["발행일"],
        "title": meta.get("title", meta.get("제목", "")),
        "text": text,
        "text_len": len(text),
        "pages_total": total_pages,
        "pages_with_text": pages_with_text,
    }
    if pages_with_text == 0 or (pages_with_text < total_pages * 0.3 and len(text) < 200):
        result["needs_ocr"] = True
    return result


async def process_one(meta: dict) -> tuple:
    pdf_key = bronze_pdf_key(meta)
    slv_key = silver_key(meta)

    if await s3_exists(slv_key):
        return "skipped", None

    pdf_data = await s3_download(pdf_key)
    if not pdf_data:
        return "no_pdf", None

    try:
        result = await asyncio.to_thread(_do_extract, meta, pdf_data)
    except Exception as e:
        logger.error("extract fail %s: %s", meta.get("report_id", "?")[:12], e)
        return "extract_failed", None

    if await s3_upload_json(slv_key, result):
        return ("ocr_needed" if result.get("needs_ocr") else "extracted"), result
    return "upload_failed", None


async def process_date(date_str: str, entries: list[dict], workers: int) -> dict:
    t0 = time.perf_counter()
    sem = asyncio.Semaphore(workers)

    async def bounded(meta):
        async with sem:
            return await process_one(meta)

    results = await asyncio.gather(*[bounded(e) for e in entries], return_exceptions=True)

    stats = {
        "date": date_str, "total": len(entries),
        "extracted": 0, "skipped": 0, "no_pdf": 0, "failed": 0, "ocr_needed": 0,
    }
    ocr_entries = []

    for r in results:
        if isinstance(r, Exception):
            stats["failed"] += 1
            continue
        status, result = r
        stats[status] = stats.get(status, 0) + 1
        if status == "ocr_needed" and result:
            ocr_entries.append({
                "report_id": result["report_id"],
                "증권사": result["증권사"],
                "종목명": result.get("종목명", ""),
                "발행일": result["발행일"],
                "title": result.get("title", ""),
                "pages_total": result.get("pages_total", 0),
                "pages_with_text": result.get("pages_with_text", 0),
                "text_len": result.get("text_len", 0),
            })

    if ocr_entries:
        ocr_key = f"silver/_ocr_needed/{date_str}.json"
        await s3_upload_json(ocr_key, ocr_entries)

    stats["elapsed"] = round(time.perf_counter() - t0, 1)
    return stats


def load_checkpoint() -> Optional[str]:
    if not CHECKPOINT_FILE.exists():
        return None
    try:
        return json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8")).get("last_date")
    except Exception:
        return None


def save_checkpoint(date_str: str):
    CHECKPOINT_FILE.write_text(json.dumps({
        "last_date": date_str,
        "updated_at": datetime.now().isoformat(),
    }, ensure_ascii=False), encoding="utf-8")


async def run_silver(days_parallel: int = 3, workers: int = 10, resume: bool = False,
                    start_date: str = None, end_date: str = None):
    t0 = time.perf_counter()
    all_entries: dict[str, list[dict]] = defaultdict(list)

    if MANIFEST_CACHE_FILE.exists():
        logger.info("loading manifest cache...")
        cached = json.loads(MANIFEST_CACHE_FILE.read_text(encoding="utf-8"))
        for date_str, entries in cached.items():
            all_entries[date_str] = entries
        logger.info("cache: %d entries (%d days)",
                     sum(len(v) for v in all_entries.values()), len(all_entries))
    else:
        logger.info("scanning S3 manifests...")
        manifest_keys = await discover_manifests()
        logger.info("%d manifests found, parallel loading (20)...", len(manifest_keys))

        sem = asyncio.Semaphore(20)
        async def _load_one(mk):
            async with sem:
                return await load_manifest_entries(mk)

        all_lists = await asyncio.gather(*[_load_one(mk) for mk in manifest_keys])
        for entries in all_lists:
            for e in entries:
                all_entries[e["발행일"]].append(e)

        total = sum(len(v) for v in all_entries.values())
        logger.info("total %d entries (%d days)", total, len(all_entries))

        MANIFEST_CACHE_FILE.write_text(
            json.dumps({k: v for k, v in all_entries.items()}, ensure_ascii=False),
            encoding="utf-8",
        )

    dates = sorted(all_entries.keys())
    if start_date:
        dates = [d for d in dates if d >= start_date]
    if end_date:
        dates = [d for d in dates if d <= end_date]
    if start_date or end_date:
        logger.info("date filter: %s~%s -> %d days", start_date or "first", end_date or "last", len(dates))
    if resume:
        cp = load_checkpoint()
        if cp:
            dates = [d for d in dates if d > cp]
            logger.info("resume from %s: %d days", cp, len(dates))

    total_extracted = 0
    days_done = 0

    for i in range(0, len(dates), days_parallel):
        chunk = dates[i:i + days_parallel]
        tasks = [process_date(d, all_entries[d], workers) for d in chunk]
        day_results = await asyncio.gather(*tasks)

        for dr in day_results:
            days_done += 1
            total_extracted += dr.get("extracted", 0)
            save_checkpoint(dr["date"])
            logger.info("[%s] extracted=%d / total=%d (%.1fs)",
                        dr["date"], dr["extracted"], dr["total"], dr["elapsed"])

        elapsed = time.perf_counter() - t0
        remaining = elapsed / days_done * (len(dates) - days_done) if days_done else 0
        logger.info("progress: %d/%d days | extracted: %d | ~%.1fh remaining",
                     days_done, len(dates), total_extracted, remaining / 3600)

    logger.info("=== Silver done: %d / %d days (%.1fh) ===",
                 total_extracted, days_done, (time.perf_counter() - t0) / 3600)


def main():
    parser = argparse.ArgumentParser(description="OPIK Silver extract")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--date", type=str)
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    args = parser.parse_args()

    if args.dry_run:
        async def _dry():
            mks = await discover_manifests()
            by_firm = defaultdict(int)
            for mk in mks:
                parts = mk.split("/")
                if len(parts) >= 2:
                    by_firm[parts[1]] += 1
            logger.info("%d manifests", len(mks))
            logger.info("by firm: %s", dict(sorted(by_firm.items())))
            if mks:
                sample = await load_manifest_entries(mks[0])
                logger.info("sample: %d entries", len(sample))
                if sample:
                    e = sample[0]
                    logger.info("  id=%s firm=%s date=%s",
                                e.get("report_id", "?")[:12],
                                e.get("증권사", "?"),
                                e.get("발행일", "?"))
            logger.info("est total: ~%d", len(mks) * 22)
        asyncio.run(_dry())
        return

    asyncio.run(run_silver(args.days, args.workers, args.resume, args.start, args.end))


if __name__ == "__main__":
    main()
