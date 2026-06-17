"""OPIK — 네이버 경유 증권사 리포트 Bronze 적재 (Async 병렬 + 단일패스 백필)

메달리온 구조:
    Bronze: s3://s3-opik-bucket/bronze/{증권사}/YYYY-MM-DD/{report_id}.pdf
            s3://s3-opik-bucket/bronze/{증권사}/YYYY-MM-DD/_manifest.json

사용법:
    python upload_naver.py                                  # 당일
    python upload_naver.py --date 2026-06-10                # 특정일
    python upload_naver.py --backfill --start 2021-06-01 --end 2026-06-10
    python upload_naver.py --backfill --start 2021-06-01 --end 2026-06-10 --days 5 --resume
    python upload_naver.py --dry-run [--date ...]           # 수집만, 업로드 X

환경변수: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / S3_BUCKET=s3-opik-bucket
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import signal
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import aiohttp
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).parent))
from collectors.naver import NaverCollector, fetch_all_since_async
from opik_config import S3_BUCKET, S3_REGION, load_dotenv
from opik_s3 import get_s3_client

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("opik.bronze")

# ── 설정 ──────────────────────────────────────────────────────────────
CHECKPOINT_FILE = Path(__file__).parent / ".backfill_checkpoint.json"

s3_client = get_s3_client(max_pool_connections=50)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"

_shutdown = False


def bronze_key(firm_kr: str, pub_date: str, report_id: str) -> str:
    return f"bronze/{firm_kr}/{pub_date}/{report_id}.pdf"


def manifest_key(firm_kr: str, pub_date: str) -> str:
    return f"bronze/{firm_kr}/{pub_date}/_manifest.json"


# ── S3 유틸 ──────────────────────────────────────────────────────────

def _s3_exists(key: str) -> bool:
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def _s3_upload(key: str, body: bytes, content_type: str = "application/pdf") -> bool:
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType=content_type)
        return True
    except Exception as e:
        logger.error("S3 업로드 실패: %s → %s", key, e)
        return False


async def s3_exists(key: str) -> bool:
    return await asyncio.to_thread(_s3_exists, key)


async def s3_upload(key: str, body: bytes, content_type: str = "application/pdf") -> bool:
    return await asyncio.to_thread(_s3_upload, key, body, content_type)


async def s3_upload_json(key: str, data) -> bool:
    return await s3_upload(key, json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"), "application/json")


# ── Async PDF 다운로드 ─────────────────────────────────────────────────

async def download_pdf(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    meta: dict,
    retries: int = 2,
) -> Optional[bytes]:
    url = meta.get("pdf_url")
    if not url:
        return None

    for attempt in range(retries + 1):
        if _shutdown:
            return None
        try:
            async with semaphore:
                await asyncio.sleep(random.uniform(0.02, 0.15))
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    if resp.status != 200:
                        if attempt < retries:
                            await asyncio.sleep(1.5 * (2 ** attempt))
                            continue
                        logger.error("PDF HTTP %d: %s", resp.status, url[:80])
                        return None
                    data = await resp.read()
                    if len(data) < 1024:
                        if attempt < retries:
                            continue
                        return None
                    return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < retries:
                await asyncio.sleep(1.5 * (2 ** attempt))
                continue
            logger.error("PDF 다운로드 실패: %s → %s", url[:80], e)
            return None
    return None


async def download_all_pdfs(metas: list[dict], workers: int = 20) -> dict[str, Optional[bytes]]:
    semaphore = asyncio.Semaphore(workers)
    connector = aiohttp.TCPConnector(limit=workers + 10, limit_per_host=workers + 5)
    async with aiohttp.ClientSession(
        headers={"User-Agent": UA, "Accept": "*/*", "Accept-Encoding": "gzip, deflate, br"},
        connector=connector,
    ) as session:
        tasks = {meta["report_id"]: download_pdf(session, semaphore, meta) for meta in metas if meta.get("pdf_url")}
        if not tasks:
            return {}
        results = dict(zip(tasks.keys(), await asyncio.gather(*tasks.values())))
    return results


# ── 단일일 처리 (수집 제외, 메타데이터는 이미 있음) ──────────────────

async def process_date_metas(
    pub_str: str,
    metas: list[dict],
    pdf_workers: int,
    s3_executor: ThreadPoolExecutor,
) -> dict:
    """이미 수집된 메타데이터로 PDF 다운로드 → S3 업로드 → manifest"""
    t0 = time.perf_counter()

    if not metas:
        return {"date": pub_str, "total": 0, "uploaded": 0, "skipped": 0, "failed": 0, "no_pdf": 0, "elapsed": 0}

    # 중복 필터링 (S3 exists 병렬)
    async def _check_dup(meta: dict) -> tuple[dict, bool]:
        if not meta.get("pdf_url"):
            return meta, False
        key = bronze_key(meta["증권사"], meta["발행일"], meta["report_id"])
        exists = await s3_exists(key)
        return meta, exists

    check_results = await asyncio.gather(*[_check_dup(m) for m in metas])
    new_metas = [m for m, is_dup in check_results if not is_dup]
    skipped = sum(1 for _, is_dup in check_results if is_dup)

    if not new_metas:
        return {"date": pub_str, "total": len(metas), "uploaded": 0, "skipped": skipped, "failed": 0, "no_pdf": 0, "elapsed": round(time.perf_counter() - t0, 1)}

    # PDF 병렬 다운로드
    pdf_results = await download_all_pdfs(new_metas, workers=pdf_workers)

    # S3 병렬 업로드
    uploaded = 0
    failed = 0
    no_pdf_cnt = 0
    firm_entries: dict[str, list[dict]] = defaultdict(list)

    def _do_upload(meta: dict) -> tuple[str, dict]:
        pdf_data = pdf_results.get(meta["report_id"])
        firm = meta["증권사"]
        rid = meta["report_id"]
        pub = meta["발행일"]
        key = bronze_key(firm, pub, rid) if pdf_data else None

        entry = {
            "report_id": rid,
            "source": firm,
            "종목명": meta.get("종목명", ""),
            "증권사": firm,
            "발행일": pub,
            "s3_key": key,
            "파싱상태": "pending" if key else "pdf_missing",
        }

        if pdf_data is None:
            return ("no_pdf" if not meta.get("pdf_url") else "failed", entry)

        if _s3_upload(key, pdf_data):
            return ("uploaded", entry)
        entry["s3_key"] = None
        entry["파싱상태"] = "pdf_missing"
        return ("failed", entry)

    loop = asyncio.get_running_loop()
    futures = [loop.run_in_executor(s3_executor, _do_upload, m) for m in new_metas]
    for fut in asyncio.as_completed(futures):
        status, entry = await fut
        firm = entry["증권사"]
        firm_entries[firm].append(entry)
        if status == "uploaded":
            uploaded += 1
        elif status == "failed":
            failed += 1
        else:
            no_pdf_cnt += 1

    # Manifest 업로드
    await asyncio.gather(*[s3_upload_json(manifest_key(f, pub_str), entries) for f, entries in firm_entries.items()])

    elapsed = time.perf_counter() - t0
    return {
        "date": pub_str,
        "total": len(metas),
        "new": len(new_metas),
        "uploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
        "no_pdf": no_pdf_cnt,
        "elapsed": round(elapsed, 1),
    }


# ── 체크포인트 ────────────────────────────────────────────────────────

def load_checkpoint() -> Optional[str]:
    if not CHECKPOINT_FILE.exists():
        return None
    try:
        data = json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
        return data.get("last_completed_date")
    except Exception:
        return None


def save_checkpoint(date_str: str, stats: dict):
    CHECKPOINT_FILE.write_text(
        json.dumps({
            "last_completed_date": date_str,
            "total_uploaded": stats.get("total_uploaded", 0),
            "days_done": stats.get("days_done", 0),
            "updated_at": datetime.now().isoformat(),
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ── 백필 (단일 패스 수집 + 크로스데이 병렬 업로드) ────────────────────

def _shutdown_handler(signum, frame):
    global _shutdown
    if not _shutdown:
        logger.warning("종료 신호 수신. 현재 청크 완료 후 정지... (한 번 더 누르면 강제종료)")
        _shutdown = True
    else:
        logger.error("강제 종료!")
        os._exit(1)


async def backfill_async(
    start_date: date,
    end_date: date,
    pdf_workers: int = 25,
    s3_workers: int = 15,
    days_parallel: int = 5,
    resume_from: Optional[str] = None,
) -> dict:
    """단일 패스 수집 → 날짜별 그룹핑 → 크로스데이 병렬 업로드"""
    batch_start = time.perf_counter()

    # ── Phase 1: 병렬 페이지 페칭 (aiohttp, 20페이지씩 동시) ──
    logger.info("━━━ Phase 1: 병렬 페이지 수집 (%s → %s) ━━━", start_date.isoformat(), date.today().isoformat())
    t_collect = time.perf_counter()

    all_metas = await fetch_all_since_async(start_date, page_batch=20, max_conn=30)

    collect_elapsed = time.perf_counter() - t_collect
    logger.info("수집 완료: %d건 (%.1f초)", len(all_metas), collect_elapsed)

    if not all_metas:
        logger.info("수집된 리포트 없음")
        return {"total_uploaded": 0, "days_done": 0, "elapsed_hours": 0}

    # ── 날짜별 그룹핑 ──
    by_date: dict[str, list[dict]] = defaultdict(list)
    for meta in all_metas:
        by_date[meta["발행일"]].append(meta)

    # end_date 까지만 필터 + resume 적용
    target_dates = sorted(d for d in by_date.keys() if start_date.isoformat() <= d <= end_date.isoformat())
    if resume_from:
        target_dates = [d for d in target_dates if d > resume_from]
        logger.info("체크포인트 복구: %s 이후부터 (%d일)", resume_from, len(target_dates))

    total_dates = len(target_dates)
    logger.info("━━━ Phase 2: 병렬 업로드 (%d일, days=%d, pdf=%d, s3=%d) ━━━",
                 total_dates, days_parallel, pdf_workers, s3_workers)

    # ── Phase 2: 청크 단위 병렬 업로드 ──
    total_uploaded = 0
    days_done = 0
    s3_executor = ThreadPoolExecutor(max_workers=s3_workers)

    try:
        for chunk_start_idx in range(0, total_dates, days_parallel):
            if _shutdown:
                logger.warning("백필 중단됨")
                break

            chunk_dates = target_dates[chunk_start_idx:chunk_start_idx + days_parallel]
            chunk_num = chunk_start_idx // days_parallel + 1
            total_chunks = (total_dates + days_parallel - 1) // days_parallel

            tasks = [process_date_metas(d, by_date[d], pdf_workers, s3_executor) for d in chunk_dates]
            day_results = await asyncio.gather(*tasks, return_exceptions=True)

            for d, result in zip(chunk_dates, day_results):
                if isinstance(result, Exception):
                    logger.error("%s 예외: %s", d, result)
                    days_done += 1
                    continue

                days_done += 1
                total_uploaded += result.get("uploaded", 0)

                # 체크포인트
                save_checkpoint(d, {"total_uploaded": total_uploaded, "days_done": days_done})

                if result.get("total", 0) > 0:
                    logger.info("[%s] uploaded=%d / total=%d (%.1fs)",
                                d, result["uploaded"], result["total"], result.get("elapsed", 0))

            # 진행률
            elapsed = time.perf_counter() - batch_start
            rate = elapsed / days_done if days_done > 0 else 0
            remaining = rate * (total_dates - days_done)
            logger.info("CHUNK %d/%d 완료 | 진행: %d/%d일 (%d%%) | %.1f초/일 | 남은 약 %.1f시간",
                         chunk_num, total_chunks, days_done, total_dates,
                         int(days_done / total_dates * 100), rate, remaining / 3600)

    finally:
        s3_executor.shutdown(wait=False)

    elapsed = time.perf_counter() - batch_start
    prefix = "중단:" if _shutdown else ""
    logger.info("=== %s 백필 %s: %d건 / %d일 (%.1f시간) ===",
                prefix, "완료" if not _shutdown else "", total_uploaded, days_done, elapsed / 3600)

    return {"total_uploaded": total_uploaded, "days_done": days_done, "elapsed_hours": elapsed / 3600}


# ── 단일일 모드 ───────────────────────────────────────────────────────

async def process_single_day_full(
    target_date: date,
    pdf_workers: int = 20,
    s3_workers: int = 15,
) -> dict:
    """당일/특정일: 수집 + PDF + S3"""
    collector = NaverCollector()
    metas = await asyncio.to_thread(collector.fetch_list, target_date)
    collector.session.close()

    pub_str = target_date.isoformat()
    if not metas:
        logger.info("%s → 0건", pub_str)
        return {"date": pub_str, "total": 0, "uploaded": 0, "skipped": 0, "failed": 0, "no_pdf": 0, "elapsed": 0}

    s3_executor = ThreadPoolExecutor(max_workers=s3_workers)
    try:
        result = await process_date_metas(pub_str, metas, pdf_workers, s3_executor)
    finally:
        s3_executor.shutdown(wait=False)
    return result


def run_single_day(target_date: date, pdf_workers: int = 20, s3_workers: int = 15):
    async def _run():
        result = await process_single_day_full(target_date, pdf_workers, s3_workers)
        logger.info("=== 완료: %s → total=%d uploaded=%d (%.1fs) ===",
                     result["date"], result["total"], result["uploaded"], result["elapsed"])
        return result
    asyncio.run(_run())


# ── CLI ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="OPIK Bronze 적재 (네이버 경유, Async + 단일패스 백필)")
    parser.add_argument("--date", type=str, help="수집일 (YYYY-MM-DD)")
    parser.add_argument("--backfill", action="store_true", help="백필 모드")
    parser.add_argument("--start", type=str, help="백필 시작일")
    parser.add_argument("--end", type=str, help="백필 종료일")
    parser.add_argument("--workers", type=int, default=25, help="PDF 동시 다운로드 수 (기본 25)")
    parser.add_argument("--s3-workers", type=int, default=15, help="S3 동시 업로드 수 (기본 15)")
    parser.add_argument("--days", type=int, default=5, help="크로스데이 동시 처리 일수 (기본 5)")
    parser.add_argument("--resume", action="store_true", help="체크포인트에서 이어서")
    parser.add_argument("--dry-run", action="store_true", help="수집만, 업로드 X")
    args = parser.parse_args()

    if args.dry_run:
        collector = NaverCollector()
        target = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()
        metas = collector.fetch_list(target)
        print(f"\n=== Dry Run: {target.isoformat()} → {len(metas)}건 ===\n")
        for m in metas:
            print(f"  [{m['증권사']}] {m.get('종목명','-')} ({m.get('stock_code','-')})")
            print(f"    {m['title'][:70]}")
            print(f"    PDF: {m.get('pdf_url','없음')}")
            print()
        return

    if args.backfill:
        start = datetime.strptime(args.start, "%Y-%m-%d").date()
        end = datetime.strptime(args.end, "%Y-%m-%d").date()

        resume_from = None
        if args.resume:
            cp = load_checkpoint()
            if cp:
                resume_from = cp
                logger.info("체크포인트 복구: %s 이후부터 재개", cp)
            else:
                logger.info("체크포인트 없음, 처음부터")

        logger.info("백필: %s → %s (pdf=%d, s3=%d, days=%d, resume=%s)",
                     start.isoformat(), end.isoformat(),
                     args.workers, args.s3_workers, args.days, resume_from or "처음부터")

        signal.signal(signal.SIGINT, _shutdown_handler)
        try:
            asyncio.run(backfill_async(start, end, args.workers, args.s3_workers, args.days, resume_from))
        except KeyboardInterrupt:
            logger.info("Ctrl+C 감지, 체크포인트 저장됨. --resume 으로 이어서 실행 가능")
    else:
        target = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()
        run_single_day(target, args.workers, args.s3_workers)


if __name__ == "__main__":
    main()
