"""OPIK — 한국투자증권 리서치 리포트 Bronze 적재

메달리온 구조:
    Bronze: s3://s3-opik-bucket/bronze/한국투자증권/YYYY-MM-DD/{report_id}.pdf
            s3://s3-opik-bucket/bronze/한국투자증권/YYYY-MM-DD/_manifest.json

사용법:
    python upload_koreainvest.py                           # 당일
    python upload_koreainvest.py --date 2026-06-12         # 특정일 (자동 3개월 범위 수집 후 필터링)
    python upload_koreainvest.py --backfill --start 2020-01-01 --end 2026-06-11 --days 5 --workers 25
    python upload_koreainvest.py --backfill --start 2020-01-01 --end 2026-06-11 --days 5 --resume
    python upload_koreainvest.py --dry-run [--date ...]    # 수집만

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

OPIK_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(OPIK_ROOT))
sys.path.insert(0, str(Path(__file__).parent))

from opik_config import S3_BUCKET, S3_REGION, load_dotenv
from opik_s3 import get_s3_client

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # 로컬 CLI 실행 환경에는 Airflow가 없을 수 있다.
    DAG = None
    PythonOperator = None

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("opik.bronze.koreainvest")

# ── 설정 ──────────────────────────────────────────────────────────────
CHECKPOINT_FILE = Path(__file__).parent / ".backfill_checkpoint_koreainvest.json"
FIRM_NAME = "한국투자증권"

s3_client = get_s3_client(max_pool_connections=50)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"

_shutdown = False


def bronze_key(pub_date: str, report_id: str) -> str:
    return f"bronze/{FIRM_NAME}/{pub_date}/{report_id}.pdf"


def manifest_key(pub_date: str) -> str:
    return f"bronze/{FIRM_NAME}/{pub_date}/_manifest.json"


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


# ── Async PDF 다운로드 (file.truefriend.com) ──────────────────────────

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
                await asyncio.sleep(random.uniform(0.05, 0.2))
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    if resp.status != 200:
                        if attempt < retries:
                            await asyncio.sleep(1.5 * (2 ** attempt))
                            continue
                        logger.error("PDF HTTP %d: %s", resp.status, url[:100])
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


async def download_all_pdfs(metas: list[dict], workers: int = 15) -> dict[str, Optional[bytes]]:
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


# ── 날짜별 처리 ────────────────────────────────────────────────────────

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
        key = bronze_key(meta["발행일"], meta["report_id"])
        exists = await s3_exists(key)
        return meta, exists

    check_results = await asyncio.gather(*[_check_dup(m) for m in metas])
    new_metas = [m for m, is_dup in check_results if not is_dup]
    skipped = sum(1 for _, is_dup in check_results if is_dup)

    if not new_metas:
        elapsed = round(time.perf_counter() - t0, 1)
        return {"date": pub_str, "total": len(metas), "uploaded": 0, "skipped": skipped, "failed": 0, "no_pdf": 0, "elapsed": elapsed}

    # PDF 병렬 다운로드
    pdf_results = await download_all_pdfs(new_metas, workers=pdf_workers)

    # S3 병렬 업로드
    uploaded = 0
    failed = 0
    no_pdf_cnt = 0
    firm_entries: list[dict] = []

    def _do_upload(meta: dict) -> tuple[str, dict]:
        pdf_data = pdf_results.get(meta["report_id"])
        firm = FIRM_NAME
        rid = meta["report_id"]
        pub = meta["발행일"]
        key = bronze_key(pub, rid) if pdf_data else None

        entry = {
            "report_id": rid,
            "source": firm,
            "title": meta.get("title", ""),
            "analyst": meta.get("analyst", ""),
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
        firm_entries.append(entry)
        if status == "uploaded":
            uploaded += 1
        elif status == "failed":
            failed += 1
        else:
            no_pdf_cnt += 1

    # Manifest 업로드
    await s3_upload_json(manifest_key(pub_str), firm_entries)

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


# ── 단일일 모드 ───────────────────────────────────────────────────────

def _collect_today() -> list[dict]:
    """당일 수집: 순차 페이지 순회 (오늘 날짜까지만)"""
    from collectors.koreainvest import KoreaInvestCollector

    collector = KoreaInvestCollector()
    results: list[dict] = []
    page = 1
    seen: set[str] = set()
    today_str = date.today().strftime("%Y-%m-%d")

    while True:
        items, total, last_page = collector.fetch_page(page=page)
        if not items:
            break

        for item in items:
            pub = item.get("발행일", "")
            if pub == today_str and item["report_id"] not in seen:
                seen.add(item["report_id"])
                results.append(item)
            elif pub and pub < today_str:
                # 어제 이전 데이터는 스킵
                break

        if page >= last_page:
            break

        page += 1
        time.sleep(0.3)

    collector.close()
    logger.info("당일 수집: %d건", len(results))
    return results


async def process_single_day_full(
    target_date: date,
    pdf_workers: int = 25,
    s3_workers: int = 15,
) -> dict:
    """당일/특정일: 수집 + PDF + S3"""
    from collectors.koreainvest import fetch_all_async

    pub_str = target_date.isoformat()

    if target_date == date.today():
        metas = await asyncio.to_thread(_collect_today)
    else:
        # 특정일: fromDate/toDate로 해당일 범위만 수집 후 필터링
        from_str = target_date.strftime("%Y.%m.%d")
        to_str = target_date.strftime("%Y.%m.%d")
        all_items = await fetch_all_async(page_batch=15, max_conn=20, search_date="all", from_date=from_str, to_date=to_str)
        metas = [m for m in all_items if m.get("발행일") == pub_str]

    if not metas:
        logger.info("%s → 0건", pub_str)
        return {"date": pub_str, "total": 0, "uploaded": 0, "skipped": 0, "failed": 0, "no_pdf": 0, "elapsed": 0}

    s3_executor = ThreadPoolExecutor(max_workers=s3_workers)
    try:
        result = await process_date_metas(pub_str, metas, pdf_workers, s3_executor)
    finally:
        s3_executor.shutdown(wait=False)
    return result


def run_single_day(target_date: date, pdf_workers: int = 25, s3_workers: int = 15):
    async def _run():
        result = await process_single_day_full(target_date, pdf_workers, s3_workers)
        logger.info("=== %s → total=%d uploaded=%d (%.1fs) ===",
                     result["date"], result["total"], result["uploaded"], result["elapsed"])
        return result
    return asyncio.run(_run())


# ── 백필 모드 ─────────────────────────────────────────────────────────

def _shutdown_handler(signum, frame):
    global _shutdown
    if not _shutdown:
        logger.warning("종료 신호 수신. 현재 청크 완료 후 정지...")
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
    """비동기 병렬 백필: 전체 수집 → 날짜별 그룹핑 → 청크 병렬 업로드"""
    from collectors.koreainvest import fetch_all_async

    batch_start = time.perf_counter()

    # ── Phase 1: 비동기 병렬 수집 ──
    logger.info("━━━ Phase 1: 병렬 페이지 수집 (%s → %s) ━━━", start_date.isoformat(), end_date.isoformat())
    t_collect = time.perf_counter()

    from_str = start_date.strftime("%Y.%m.%d")
    to_str = end_date.strftime("%Y.%m.%d")
    all_items = await fetch_all_async(page_batch=15, max_conn=20, search_date="all", from_date=from_str, to_date=to_str)

    collect_elapsed = time.perf_counter() - t_collect
    logger.info("수집 완료: %d건 (%.1f초)", len(all_items), collect_elapsed)

    if not all_items:
        logger.info("수집된 리포트 없음")
        return {"total_uploaded": 0, "days_done": 0, "elapsed_hours": 0}

    # ── 날짜별 그룹핑 ──
    by_date: dict[str, list[dict]] = defaultdict(list)
    for item in all_items:
        pub = item.get("발행일")
        if pub:
            by_date[pub].append(item)

    target_dates = sorted(d for d in by_date.keys() if start_date.isoformat() <= d <= end_date.isoformat())
    if resume_from:
        target_dates = [d for d in target_dates if d > resume_from]
        logger.info("체크포인트 복구: %s 이후부터 (%d일)", resume_from, len(target_dates))

    total_dates = len(target_dates)
    logger.info("━━━ Phase 2: 병렬 업로드 (%d일, pdf=%d, s3=%d, days=%d) ━━━",
                 total_dates, pdf_workers, s3_workers, days_parallel)

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
                save_checkpoint(d, {"total_uploaded": total_uploaded, "days_done": days_done})

                if result.get("total", 0) > 0:
                    logger.info("[%s] uploaded=%d / total=%d (%.1fs)",
                                d, result["uploaded"], result["total"], result.get("elapsed", 0))

            elapsed = time.perf_counter() - batch_start
            rate = elapsed / days_done if days_done > 0 else 0
            remaining = rate * (total_dates - days_done)
            logger.info("CHUNK %d/%d 완료 | 진행: %d/%d일 (%d%%) | %.1f초/일 | 남은 약 %.1f시간",
                         chunk_num, total_chunks, days_done, total_dates,
                         int(days_done / total_dates * 100), rate, remaining / 3600)

    finally:
        s3_executor.shutdown(wait=False)

    elapsed = time.perf_counter() - batch_start
    prefix = "중단: " if _shutdown else ""
    logger.info("=== %s 백필 %s: %d건 / %d일 (%.1f시간) ===",
                prefix, "완료" if not _shutdown else "", total_uploaded, days_done, elapsed / 3600)

    return {"total_uploaded": total_uploaded, "days_done": days_done, "elapsed_hours": elapsed / 3600}


def run_bronze_koreainvest(
    target_date: str | date | None = None,
    pdf_workers: int = 25,
    s3_workers: int = 15,
    dry_run: bool = False,
) -> dict:
    """Airflow PythonOperator 엔트리포인트."""
    target = (
        datetime.strptime(target_date, "%Y-%m-%d").date()
        if isinstance(target_date, str)
        else target_date
        if isinstance(target_date, date)
        else date.today()
    )

    if dry_run:
        from collectors.koreainvest import fetch_all_async

        async def _dry_run():
            if target == date.today():
                metas = await asyncio.to_thread(_collect_today)
            else:
                from_str = target.strftime("%Y.%m.%d")
                to_str = target.strftime("%Y.%m.%d")
                all_items = await fetch_all_async(
                    page_batch=15,
                    max_conn=20,
                    search_date="all",
                    from_date=from_str,
                    to_date=to_str,
                )
                metas = [m for m in all_items if m.get("발행일") == target.isoformat()]
            logger.info("DRY RUN %s → %d건", target.isoformat(), len(metas))
            return {"date": target.isoformat(), "total": len(metas), "dry_run": True}

        return asyncio.run(_dry_run())

    return run_single_day(target, pdf_workers, s3_workers)


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
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        max_active_runs=1,
        tags=["opik", "bronze", "koreainvest", "reports"],
    ) as dag_obj:
        PythonOperator(
            task_id="upload_koreainvest_reports_to_bronze",
            python_callable=run_bronze_koreainvest,
            op_kwargs={
                "target_date": "{{ ds }}",
                "pdf_workers": 25,
                "s3_workers": 15,
            },
        )

    return dag_obj


dag = build_dag()


# ── CLI ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="OPIK Bronze 적재 (한국투자증권, Async 병렬 백필)")
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
        from collectors.koreainvest import KoreaInvestCollector, fetch_all_async

        collector = KoreaInvestCollector()
        if args.date:
            target = datetime.strptime(args.date, "%Y-%m-%d").date()
            all_items = asyncio.run(fetch_all_async(page_batch=15, max_conn=20))
            metas = [m for m in all_items if m.get("발행일") == target.isoformat()]
        else:
            metas = _collect_today()

        print(f"\n=== Dry Run → {len(metas)}건 ===\n")
        for m in metas[:20]:
            print(f"  [{m.get('category_head','')}] {m['title'][:70]}")
            print(f"    분석가: {m.get('analyst','')}, 날짜: {m.get('발행일','')}")
            print(f"    PDF: {m.get('pdf_url','')[:100]}")
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
