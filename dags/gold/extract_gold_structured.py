"""OPIK — Gold Structured 추출 (정규식 기반, boto3 + PyArrow)

Silver JSON 텍스트 → 정규식으로 투자의견·목표주가·종목코드 추출 → Gold Parquet 적재
"""

from __future__ import annotations

from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
import asyncio
import io
import json
import logging
import os
import random
import re
import tempfile
import time
from collections import defaultdict

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError


OPIK_ROOT = Path(__file__).resolve().parents[2]


def load_local_env() -> None:
    """Load .env without depending on project-local helper modules."""
    candidates: list[Path] = []
    if root := os.getenv("OPIK_ROOT"):
        candidates.append(Path(root) / ".env")
    candidates.extend([
        OPIK_ROOT / ".env",
        OPIK_ROOT.parent / ".env",
        Path(__file__).resolve().parents[1] / ".env",
        Path(__file__).parent / ".env",
        Path.cwd() / ".env",
    ])

    for env_path in candidates:
        if not env_path.exists():
            continue
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("'\""))
        return


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
s3 = boto3.client("s3", region_name=S3_REGION)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("opik.gold.structured")

GOLD_STRUCTURED_PREFIX = "gold/structured"
SILVER_KEYS_CACHE = Path(__file__).parent / ".silver_keys_cache.json"

# ── 정규식 패턴 ──────────────────────────────────────────────

OPINION_PATTERNS = [
    (re.compile(r"매수\s*[\[\(\]]?\s*(?:유지|신규|강력|적극)?\s*[\)\]]?"), "BUY"),
    (re.compile(r"(?<!Not\s)(?<!Under\s)BUY\s*[\[\(\]]?\s*(?:유지|신규|Maintain)?\s*[\)\]]?", re.IGNORECASE), "BUY"),
    (re.compile(r"(?:Trading|Strong)\s*Buy", re.IGNORECASE), "BUY"),
    (re.compile(r"Outperform", re.IGNORECASE), "BUY"),
    (re.compile(r"중립|보유|시장수익률"), "HOLD"),
    (re.compile(r"NEUTRAL|HOLD|MARKET\s*PERFORM|MARKET\s*WEIGHT", re.IGNORECASE), "HOLD"),
    (re.compile(r"매도|비중축소"), "SELL"),
    (re.compile(r"SELL|UNDER\s*PERFORM|UNDER\s*WEIGHT|REDUCE", re.IGNORECASE), "SELL"),
    (re.compile(r"Not\s*Rated|NR\b|N/?R\b|N\.R\b|미제시", re.IGNORECASE), "NOT_RATED"),
]

TP_PATTERN = re.compile(
    r"(?:목표|적정)(?:주가|가격|가)\s*(?:\(\d{1,2}M\)\s*:?\s*)?[^\d]*?([\d,]+\.?\d*)\s*(?:만)?\s*원|"
    r"\bTP\b\s*(?:\(\d{1,2}M\)\s*:?\s*)?[^\d]*?([\d,]+\.?\d*)\s*(?:만)?\s*원|"
    r"(?:목표|적정)(?:주가|가격)\s*(?:\(\d{1,2}M\)\s*:?\s*)?[^\d]*?([\d,]+\.?\d*)\s*(?:만)?\s*(?:원)?|"
    r"\bTP\b\s*(?:\(\d{1,2}M\)\s*:?\s*)?[^\d]*?([\d,]+\.?\d*)|"
    r"Target\s*Price[^\d]*?([\d,]+\.?\d*)|"
    r"적정(?:주가|가치)[^\d]*?([\d,]+\.?\d*)\s*(?:만)?\s*(?:원)?",
    re.IGNORECASE,
)

CP_PATTERN = re.compile(
    r"현재(?:주가|가격?)\s*(?:\([^)]*\))?\s*:?\s*([\d,]+\.?\d*)\s*(?:만)?\s*원",
    re.IGNORECASE,
)

STOCK_CODE_PATTERN = re.compile(r"\((\d{6})(?:/[^)]*)?(?:[.,]\s*[A-Z]{1,3})?\)")
STOCK_CODE_BODY_PATTERN = re.compile(r"(?:^|\s)(\d{6})\s*(?:기업분석|종목분석|기업\s*Report|종목\s*Report|기업\s*Update)")
STOCK_CODE_SLASH_PATTERN = re.compile(r"(\d{6})\s*/\s*(?:KONEX|KOSPI|KOSDAQ|KS|KQ|Not\s*Rated)\b")
STOCK_CODE_KONEX_REV_PATTERN = re.compile(r"(?:KONEX|코넥스)\s*:?\s*(\d{6})\b", re.IGNORECASE)
STOCK_CODE_SUFFIX_PATTERN = re.compile(r"\b(\d{6})\s*\.?\s*(?:KS|KQ)\b")
STOCK_CODE_STANDALONE_PATTERN = re.compile(r"^(\d{6})\s*$", re.MULTILINE)
STOCK_CODE_BARE_PATTERN = re.compile(r"(?<=[A-Za-z가-힣])\s+(\d{6})\b")
STOCK_CODE_BRACKET_PATTERN = re.compile(r"\[[^\]]*?(\d{6})\s*\]")



def extract_opinion(text: str) -> str | None:
    if not text:
        return None
    head = text[:2500]
    for pattern, label in OPINION_PATTERNS:
        if pattern.search(head):
            return label
    if "코넥스기업 분석보고서" in head or "KONEX Research Project" in head:
        return "NOT_RATED"
    if "기술분석보고서" in head:
        return "NOT_RATED"
    return None


def _validate_tp_context(ctx: str, raw_val: str) -> bool:
    """Return True if the captured number looks like a real target price, False if it's a year/month/PER etc."""
    if not ctx or not raw_val:
        return False
    esc = re.escape(raw_val)
    # Reject if followed by 년, 개월, 배, 분기, 일, 월, 주
    if re.search(r'\b' + esc + r'\s*(?:년|개월|배|분기|일|월|주)\b', ctx):
        return False
    # % needs separate check — \b after % fails when followed by space then Korean (e.g. "100% 전환")
    if re.search(r'\b' + esc + r'\s*%', ctx):
        return False
    # Reject if directly followed by "M" (e.g. 12M = 12개월, no word boundary between 12 and M)
    if re.search(r'\b' + esc + r'M\b', ctx):
        return False
    # Reject abbreviated year like '24년 or '26E
    if re.search(r"['‘’]\s*" + esc + r"\s*(?:년|E\b)", ctx):
        return False
    # Reject date-like: YY.MM.DD or YYYY.MM
    if re.search(r'\b' + esc + r'\s*[\./]\s*\d{1,2}\s*[\./]\s*\d{1,2}', ctx):
        return False
    # Reject exact 0 (graph axis labels, never a real TP)
    if raw_val.replace(',', '').strip() == '0':
        return False
    # Reject if preceded by X or × (formula footer like "목표주가 X 100")
    if re.search(r'[X×]\s*' + esc + r'\b', ctx):
        return False
    # Reject if followed by another number with only whitespace/newline/separator — table cell split
    # e.g. "105\n2,000" or "170,\n700" — the real TP is split across table columns
    if re.search(r'\b' + esc + r'\s*[,;:]?\s*\n\s*[\d,]+', ctx):
        return False
    return True


def extract_target_price(text: str) -> int | None:
    if not text:
        return None
    for m in TP_PATTERN.finditer(text):
        raw = (m.group(1) or m.group(2) or m.group(3) or m.group(4) or m.group(5) or m.group(6) or "").replace(",", "")
        if not raw:
            continue
        # Reject stock codes: Korean stock codes are exactly 6 digits starting with 0
        # (e.g. 005930, 000660). Real TPs are never formatted this way —
        # a TP of 100,000원 becomes "100000" (starts with 1), not "0100000".
        # Edge case: "095000" could be stock code 095000 or TP 95,000원 with
        # leading zero. We check surrounding context for TP indicators to resolve.
        if len(raw) == 6 and raw.startswith('0'):
            ctx_check = text[max(0, m.start() - 30):m.start()]
            if not any(kw in ctx_check for kw in ("목표", "TP", "target", "적정", "상승")):
                continue
        price = float(raw)
        ctx_start = max(0, m.start() - 10)
        ctx_end = min(len(text), m.end() + 15)
        ctx = text[ctx_start:ctx_end]

        after_keyword = text[m.start():m.start() + 20]
        if re.search(r'목표주가\s*[\[\(\]]?\s*(?:괴리율|추이|변동추이|변동내역|평균|X\s*\d|×\s*\d)', after_keyword):
            continue
        if re.search(r'목표주가(변동추이|괴리율|추이)', after_keyword):
            continue
        before_kw = text[max(0, m.start() - 20):m.start()]
        if re.search(r'(달러|USD|달러기준|52주최저|52주최고)\b', before_kw, re.IGNORECASE):
            continue

        if not _validate_tp_context(ctx, raw):
            continue

        if price < 10000:
            if re.search(r'만\s*원', ctx):
                price *= 10000

        if price < 100:
            continue

        return int(price)
    return None


def extract_current_price(text: str) -> int | None:
    if not text:
        return None
    m = CP_PATTERN.search(text)
    if not m:
        m = re.search(r"현재(?:주가|가)\s*:?\s*([\d,]+\.?\d*)\s*(?:만)?\s*(?:원)?", text, re.IGNORECASE)
    if not m:
        return None
    raw = m.group(1).replace(",", "")
    if not raw:
        return None
    price = float(raw)
    ctx = text[max(0, m.start() - 5):m.end() + 5]
    if price < 10000 and re.search(r'만\s*원', ctx):
        price *= 10000
    return int(price)



def extract_stock_codes(text: str) -> list[str]:
    if not text:
        return []
    head_early = text[:800] if text else ""
    seen = set()
    result = []

    for c in STOCK_CODE_PATTERN.findall(text):
        if c not in seen and c != "000000":
            seen.add(c)
            result.append(c)
    for m in STOCK_CODE_BODY_PATTERN.finditer(text):
        c = m.group(1)
        if c not in seen and c != "000000":
            seen.add(c)
            result.append(c)
    for c in STOCK_CODE_SLASH_PATTERN.findall(text):
        if c not in seen and c != "000000":
            seen.add(c)
            result.append(c)
    for c in STOCK_CODE_KONEX_REV_PATTERN.findall(text):
        if c not in seen and c != "000000":
            seen.add(c)
            result.append(c)
    for c in STOCK_CODE_SUFFIX_PATTERN.findall(text):
        if c not in seen and c != "000000":
            seen.add(c)
            result.append(c)
    for m in STOCK_CODE_STANDALONE_PATTERN.finditer(head_early):
        c = m.group(1)
        if c not in seen and c != "000000":
            if 1900 <= int(c) <= 2099:
                continue
            seen.add(c)
            result.append(c)
    head_mid = text[:2000] if text else ""
    for c in STOCK_CODE_BARE_PATTERN.findall(head_mid):
        if c not in seen and c != "000000":
            if 1900 <= int(c) <= 2099:
                continue
            seen.add(c)
            result.append(c)
    for c in STOCK_CODE_BRACKET_PATTERN.findall(text):
        if c not in seen and c != "000000":
            seen.add(c)
            result.append(c)

    return result


def extract_estimates(text: str) -> dict | None:
    if not text:
        return None
    estimates = {}
    rev_pattern = re.compile(
        r"매출(?:액)?[^\d]*?(\d{1,3}(?:[.,]\d{1,2})?)\s*(?:조|십억|억|백만|만)?\s*(?:원)?",
        re.IGNORECASE,
    )
    rev_matches = rev_pattern.findall(text[:5000])
    if rev_matches:
        estimates["revenue_mentions"] = rev_matches[:3]
    op_pattern = re.compile(
        r"영업이익[^\d]*?(\d{1,3}(?:[.,]\d{1,2})?)\s*(?:조|십억|억|백만|만)?\s*(?:원)?",
        re.IGNORECASE,
    )
    op_matches = op_pattern.findall(text[:5000])
    if op_matches:
        estimates["op_profit_mentions"] = op_matches[:3]
    return estimates if estimates else None


def extract_from_silver(silver_json: dict) -> dict:
    text = silver_json.get("text", "")
    title = silver_json.get("title", "")

    result = {
        "report_id": silver_json.get("report_id", ""),
        "증권사": silver_json.get("증권사", ""),
        "종목명": silver_json.get("종목명", ""),
        "발행일": silver_json.get("발행일", ""),
        "title": title,
        "source": silver_json.get("source", ""),
        "text_len": silver_json.get("text_len", 0),
        "pages_total": silver_json.get("pages_total", 0),
    }

    codes = extract_stock_codes(f"{title}\n{text}")
    result["종목코드"] = codes[0] if codes else None
    result["종목코드_list"] = json.dumps(codes, ensure_ascii=False)

    opinion_source = f"{title}\n{text[:2500]}" if text else title
    result["투자의견"] = extract_opinion(opinion_source)
    result["목표주가"] = extract_target_price(f"{title}\n{text}")
    result["현재주가"] = extract_current_price(f"{title}\n{text}")

    estimates = extract_estimates(text)
    if estimates:
        result["실적추정_raw"] = json.dumps(estimates, ensure_ascii=False)

    if result["목표주가"] and result["현재주가"] and result["현재주가"] > 0:
        result["상승여력_pct"] = round(
            (result["목표주가"] - result["현재주가"]) / result["현재주가"] * 100, 1
        )

    발행일 = result.get("발행일", "")
    if 발행일 and len(발행일) >= 7:
        result["year"] = int(발행일[:4])
        result["month"] = int(발행일[5:7])

    return result


# ── S3 helpers ───────────────────────────────────────────────

async def list_silver_keys(force_refresh: bool = False) -> list[str]:
    if not force_refresh and SILVER_KEYS_CACHE.exists():
        try:
            data = json.loads(SILVER_KEYS_CACHE.read_text(encoding="utf-8"))
            keys = data.get("keys", [])
            logger.info("Loaded %d silver keys from cache", len(keys))
            return keys
        except Exception:
            pass
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="silver/"):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".json") and "_ocr_needed" not in k and "_manifest" not in k:
                keys.append(k)
    SILVER_KEYS_CACHE.write_text(
        json.dumps({"keys": keys, "count": len(keys), "updated": time.strftime("%Y-%m-%dT%H:%M:%S")}),
        encoding="utf-8",
    )
    return keys


async def s3_download_json(key: str, max_retries: int = 3) -> dict | None:
    for attempt in range(max_retries):
        try:
            body = await asyncio.to_thread(
                lambda: s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
            )
            return json.loads(body.decode("utf-8"))
        except ClientError as e:
            if "NoSuchKey" in str(e):
                return None
            raise
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = (2 ** attempt) + random.uniform(0, 1)
            logger.warning("S3 download retry %d/%d for %s after %.1fs: %s",
                           attempt + 1, max_retries, key[:80], delay, e)
            await asyncio.sleep(delay)


def gold_parquet_key(year: int, month: int) -> str:
    return f"gold/structured/year={year}/month={month:02d}/data.parquet"


def upload_parquet_to_s3(local_path: str, s3_key: str) -> bool:
    try:
        with open(local_path, "rb") as f:
            s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=f.read(),
                          ContentType="application/octet-stream")
        return True
    except Exception as e:
        logger.error("S3 upload fail %s: %s", s3_key, e)
        return False


# ── Parquet schema ───────────────────────────────────────────

GOLD_SCHEMA = pa.schema([
    ("report_id", pa.string()),
    ("증권사", pa.string()),
    ("종목명", pa.string()),
    ("종목코드", pa.string()),
    ("발행일", pa.string()),
    ("title", pa.string()),
    ("source", pa.string()),
    ("text_len", pa.int64()),
    ("pages_total", pa.int64()),
    ("투자의견", pa.string()),
    ("목표주가", pa.int64()),
    ("현재주가", pa.int64()),
    ("상승여력_pct", pa.float64()),
    ("종목코드_list", pa.string()),
    ("실적추정_raw", pa.string()),
])


def rows_to_parquet(rows: list[dict], local_path: str):
    columns = {field.name: [] for field in GOLD_SCHEMA}
    for r in rows:
        for name in columns:
            val = r.get(name)
            columns[name].append(val)
    table = pa.table(columns, schema=GOLD_SCHEMA)
    pq.write_table(table, local_path, compression="snappy")


# ── Sampling (per-firm test) ─────────────────────────────────

async def _run_sample_firms(by_year_month):
    t0 = time.perf_counter()
    by_firm = defaultdict(list)
    for keys in by_year_month.values():
        for k in keys:
            parts = k.split("/")
            if len(parts) >= 2:
                by_firm[parts[1]].append(k)

    firms = sorted(by_firm.keys())
    logger.info("===== SAMPLE-FIRMS: %d firms =====", len(firms))

    ok = 0
    total = 0
    issues = []

    for firm in firms:
        sample_key = by_firm[firm][0]
        data = await s3_download_json(sample_key)
        if not data:
            continue

        total += 1
        result = extract_from_silver(data)

        opinion_ok = result.get("투자의견") is not None
        tp_ok = result.get("목표주가") is not None
        code_ok = result.get("종목코드") is not None
        all_ok = opinion_ok and tp_ok and code_ok

        label = "OK" if all_ok else "KO"
        missing = []
        if not opinion_ok:
            missing.append("opinion")
        if not tp_ok:
            missing.append("TP")
        if not code_ok:
            missing.append("code")
        if not all_ok:
            issues.append((firm, result.get("report_id", "?")[:12],
                          result.get("종목명", ""), ",".join(missing)))

        logger.info("%s %-18s  %-8s  TP=%-8s  CP=%-8s  code=%-6s  up=%-6s  %s%s",
                   label, firm,
                   result.get("투자의견", "-"),
                   f"{result['목표주가']:,}" if result.get("목표주가") else "-",
                   f"{result['현재주가']:,}" if result.get("현재주가") else "-",
                   result.get("종목코드", "-"),
                   f"{result.get('상승여력_pct', '-')}%" if result.get("상승여력_pct") is not None else "-",
                   result.get("종목명", "")[:14],
                   f"  [{','.join(missing)}]" if not all_ok else "")

        if all_ok:
            ok += 1

    elapsed = time.perf_counter() - t0
    logger.info("===== SAMPLE-FIRMS: %d/%d OK (%.0f%%) | %d issues | %.1fs =====",
               ok, total, ok / total * 100 if total else 0, len(issues), elapsed)

    if issues:
        logger.info("── Issues detail ──")
        for firm, rid, stock, why in sorted(issues):
            sample_key = by_firm[firm][0]
            data = await s3_download_json(sample_key)
            if not data:
                continue
            text = data.get("text", "")
            title = data.get("title", "")
            head = f"{title}\n{text[:2000]}"
            logger.info("  %s / %s / %s -> %s", firm, rid, stock, why)
            logger.info("    text: %s", head[:500].replace("\n", "\\n"))


# ── Main ─────────────────────────────────────────────────────

async def run_gold_structured(start_date=None, end_date=None,
                              year=None, dry_run=False,
                              sample_firms=False, workers=20,
                              force_refresh=False):
    t0 = time.perf_counter()

    logger.info("Listing Silver JSON keys...")
    all_keys = await list_silver_keys(force_refresh=force_refresh)
    logger.info("Total silver objects: %d", len(all_keys))

    by_year_month = defaultdict(list)
    skipped_filter = 0
    for k in all_keys:
        parts = k.split("/")
        if len(parts) < 4:
            continue
        date_str = parts[2]
        date_ym = date_str[:7]
        if start_date and date_ym < start_date[:7]:
            skipped_filter += 1
            continue
        if end_date and date_ym > end_date[:7]:
            skipped_filter += 1
            continue
        if year and not date_str.startswith(str(year)):
            skipped_filter += 1
            continue
        try:
            y, m = int(date_str[:4]), int(date_str[5:7])
        except (ValueError, IndexError):
            continue
        by_year_month[(y, m)].append(k)

    total_filtered = sum(len(v) for v in by_year_month.values())
    logger.info("After filter: %d rows in %d partitions (skipped %d)",
                total_filtered, len(by_year_month), skipped_filter)

    if dry_run:
        logger.info("===== DRY RUN =====")
        logger.info("Total silver: %d", len(all_keys))
        logger.info("Filtered: %d", total_filtered)
        for (y, m), keys in sorted(by_year_month.items())[:1]:
            sample_key = keys[0]
            data = await s3_download_json(sample_key)
            if data:
                result = extract_from_silver(data)
                logger.info("Sample: %s", sample_key)
                for k, v in result.items():
                    logger.info("  %s: %s", k, str(v)[:80])
        return

    if sample_firms:
        await _run_sample_firms(by_year_month)
        return

    # Partition processing with batching
    total_extracted = 0
    opinion_matched = 0
    tp_matched = 0
    code_matched = 0
    partitions_done = 0
    BATCH_SIZE = 200

    for (y, m), keys in sorted(by_year_month.items()):
        month_rows = []
        month_total = len(keys)

        for batch_start in range(0, month_total, BATCH_SIZE):
            batch_keys = keys[batch_start:batch_start + BATCH_SIZE]
            sem = asyncio.Semaphore(min(workers, len(batch_keys)))

            async def process_one(k):
                async with sem:
                    data = await s3_download_json(k)
                    if data:
                        parts = k.split("/")
                        if len(parts) >= 4:
                            data.setdefault("발행일", parts[2])
                            data.setdefault("증권사", parts[1])
                        return extract_from_silver(data)
                    return None

            tasks = [process_one(k) for k in batch_keys]
            results = await asyncio.gather(*tasks)

            batch_rows = [r for r in results if r is not None]
            month_rows.extend(batch_rows)

            for r in batch_rows:
                total_extracted += 1
                if r.get("투자의견"):
                    opinion_matched += 1
                if r.get("목표주가"):
                    tp_matched += 1
                if r.get("종목코드"):
                    code_matched += 1

            elapsed = time.perf_counter() - t0
            logger.info("  batch %d-%d/%d | %d/%d rows | elapsed %.0fs",
                       batch_start + 1, min(batch_start + BATCH_SIZE, month_total),
                       month_total, total_extracted, total_filtered, elapsed)

        partitions_done += 1

        if month_rows:
            tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
            tmp_path = tmp.name
            tmp.close()
            try:
                rows_to_parquet(month_rows, tmp_path)
                s3_key = gold_parquet_key(y, m)
                upload_parquet_to_s3(tmp_path, s3_key)
                logger.info("[%d-%02d] %d rows -> %s", y, m, len(month_rows), s3_key)
            finally:
                os.unlink(tmp_path)

        elapsed = time.perf_counter() - t0
        remaining = elapsed / total_extracted * (total_filtered - total_extracted) if total_extracted else 0
        logger.info("progress: %d/%d | %d/%d rows | opinion=%.1f%% tp=%.1f%% code=%.1f%% | ~%.0fm",
                    partitions_done, len(by_year_month), total_extracted, total_filtered,
                    opinion_matched / total_extracted * 100 if total_extracted else 0,
                    tp_matched / total_extracted * 100 if total_extracted else 0,
                    code_matched / total_extracted * 100 if total_extracted else 0,
                    remaining / 60)

    elapsed = time.perf_counter() - t0
    logger.info("=== Gold structured: %d rows in %.1f min ===", total_extracted, elapsed / 60)
    logger.info("opinion=%.1f%% | tp=%.1f%% | code=%.1f%%",
                opinion_matched / total_extracted * 100 if total_extracted else 0,
                tp_matched / total_extracted * 100 if total_extracted else 0,
                code_matched / total_extracted * 100 if total_extracted else 0)


def main():
    parser = argparse.ArgumentParser(description="OPIK Gold Structured extraction")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sample-firms", action="store_true")
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    parser.add_argument("--year", type=int)
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--force-refresh", action="store_true")
    args = parser.parse_args()
