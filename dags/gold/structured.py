"""Silver 일배치 결과를 정규식 기반 Gold structured Parquet로 변환한다."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import tempfile
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

try:
    import pendulum
    from airflow import DAG
    from airflow.datasets import Dataset
    from airflow.operators.python import PythonOperator
    from airflow.sensors.external_task import ExternalTaskSensor
except ImportError:  # 로컬 CLI 실행 환경에는 Airflow가 없을 수 있다.
    DAG = None
    Dataset = None
    PythonOperator = None
    ExternalTaskSensor = None


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
AWS_REGION = (
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
logger = logging.getLogger("opik.gold.structured")

GOLD_STRUCTURED_PREFIX = "gold/structured"
RAW_SILVER_PREFIX = "silver/"
# Dataset URI — dag_maintenance_delta_faiss가 이 URI로 본 DAG 완료를 구독한다.
# 변경 시 dags/maintenance/dag_maintenance_delta_faiss.py의 동일 URI도 같이 수정할 것.
GOLD_STRUCTURED_DATASET_URI = "s3://s3-opik-bucket/gold/structured/"
REPORT_PIPELINE_SCHEDULE = os.getenv("OPIK_REPORT_PIPELINE_SCHEDULE", "0 0 * * *")
KST_TARGET_DATE_TEMPLATE = (
    "{{ data_interval_end.in_timezone('Asia/Seoul').subtract(days=1).to_date_string() }}"
)


def upstream_logical_date_for_target(logical_date, data_interval_end=None, **_):
    """Map this run to the scheduled upstream run that owns the same KST target date."""
    interval_end = pendulum.instance(data_interval_end or logical_date).in_timezone("Asia/Seoul")
    return interval_end.start_of("day").subtract(days=1)

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


def _normalize_stock_code(value: Any) -> str | None:
    match = re.search(r"(\d{6})", str(value or ""))
    if not match or match.group(1) == "000000":
        return None
    code = match.group(1)
    return None if 1900 <= int(code) <= 2099 else code


def extract_from_silver(silver_json: dict, source_key: str = "") -> dict:
    parts = source_key.split("/")
    broker_from_key = parts[1] if len(parts) >= 4 else ""
    date_from_key = parts[2] if len(parts) >= 4 else ""
    text = str(silver_json.get("text") or silver_json.get("본문") or "")
    title = str(silver_json.get("title") or silver_json.get("제목") or "")

    result = {
        "report_id": str(silver_json.get("report_id") or Path(source_key).stem),
        "증권사": str(silver_json.get("증권사") or silver_json.get("source") or broker_from_key),
        "종목명": str(silver_json.get("종목명") or ""),
        "발행일": str(silver_json.get("발행일") or date_from_key).replace(".", "-"),
        "title": title,
        "source": str(silver_json.get("source") or silver_json.get("증권사") or broker_from_key),
        "text_len": int(silver_json.get("text_len") or len(text)),
        "pages_total": int(silver_json.get("pages_total") or 0),
    }

    codes = extract_stock_codes(f"{title}\n{text}")
    explicit_code = _normalize_stock_code(
        silver_json.get("종목코드") or silver_json.get("stock_code")
    )
    if not codes and explicit_code:
        codes = [explicit_code]
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


# ── Daily S3 and Parquet pipeline ─────────────────────────────

def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def parse_date(value: str | date | None) -> date:
    if value is None:
        return date.today() - timedelta(days=1)
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"날짜는 YYYY-MM-DD 형식이어야 합니다: {value}") from exc


def list_common_prefixes(s3, prefix: str) -> list[str]:
    prefixes: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/"):
        prefixes.extend(item["Prefix"] for item in page.get("CommonPrefixes", []))
    return prefixes


def list_daily_silver_keys(s3, target_date: str) -> list[str]:
    keys: list[str] = []
    for broker_prefix in list_common_prefixes(s3, RAW_SILVER_PREFIX):
        broker = broker_prefix.rstrip("/").split("/")[-1]
        if broker == "embedding_input" or broker.startswith("_") or broker.endswith("_수정"):
            continue
        prefix = f"{broker_prefix}{target_date}/"
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".json") and key.count("/") == 3:
                    keys.append(key)
    return sorted(set(keys))


def load_json_from_s3(s3, key: str) -> dict[str, Any]:
    body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return json.loads(body)


def object_exists(s3, key: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def gold_parquet_key(target: date) -> str:
    return (
        f"{GOLD_STRUCTURED_PREFIX}/year={target.year:04d}/"
        f"month={target.month:02d}/data.parquet"
    )


def parquet_schema():
    import pyarrow as pa

    return pa.schema([
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


def read_existing_rows(s3, key: str) -> list[dict[str, Any]]:
    if not object_exists(s3, key):
        return []
    import pyarrow.parquet as pq

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = Path(tmp_dir) / "existing.parquet"
        s3.download_file(S3_BUCKET, key, str(local_path))
        table = pq.read_table(local_path)
        expected = parquet_schema().names
        if table.schema.names != expected:
            raise ValueError(
                f"기존 structured Parquet 스키마가 다릅니다: "
                f"expected={expected}, actual={table.schema.names}, key={key}"
            )
        return table.to_pylist()


def merge_rows(existing_rows: list[dict], new_rows: list[dict]) -> list[dict]:
    """Idempotent append: report_id가 같으면 이번 일배치 결과가 기존 행을 대체한다."""
    merged = {
        str(row["report_id"]): row
        for row in existing_rows
        if row.get("report_id")
    }
    for row in new_rows:
        if row.get("report_id"):
            merged[str(row["report_id"])] = row
    return sorted(
        merged.values(),
        key=lambda row: (
            str(row.get("발행일") or ""),
            str(row.get("증권사") or ""),
            str(row.get("report_id") or ""),
        ),
    )


def write_rows_to_s3(s3, key: str, rows: list[dict[str, Any]]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = Path(tmp_dir) / "data.parquet"
        table = pa.Table.from_pylist(rows, schema=parquet_schema())
        pq.write_table(table, local_path, compression="snappy")
        s3.upload_file(
            str(local_path),
            S3_BUCKET,
            key,
            ExtraArgs={"ContentType": "application/octet-stream"},
        )


def run_daily_structured(
    target_date: str | date | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    target = parse_date(target_date)
    target_str = target.isoformat()
    s3 = s3_client()
    silver_keys = list_daily_silver_keys(s3, target_str)
    skipped = Counter()
    rows: list[dict[str, Any]] = []

    logger.info("Gold structured target=%s silver=%d", target_str, len(silver_keys))
    for key in silver_keys:
        doc = load_json_from_s3(s3, key)
        row = extract_from_silver(doc, key)
        if not row["report_id"]:
            skipped["missing_report_id"] += 1
            continue
        if row["발행일"] != target_str:
            skipped["date_mismatch"] += 1
            continue
        rows.append(row)

    counts = {
        "opinion": sum(row.get("투자의견") is not None for row in rows),
        "target_price": sum(row.get("목표주가") is not None for row in rows),
        "stock_code": sum(row.get("종목코드") is not None for row in rows),
    }
    output_key = gold_parquet_key(target)
    existing_count = 0
    merged_count = 0

    if rows and not dry_run:
        existing_rows = read_existing_rows(s3, output_key)
        merged_rows = merge_rows(existing_rows, rows)
        existing_count = len(existing_rows)
        merged_count = len(merged_rows)
        write_rows_to_s3(s3, output_key, merged_rows)
        logger.info(
            "Gold structured upsert complete: existing=%d daily=%d merged=%d s3://%s/%s",
            existing_count,
            len(rows),
            merged_count,
            S3_BUCKET,
            output_key,
        )
    elif not rows:
        logger.info("Gold structured no-op: %s Silver JSON이 없습니다.", target_str)

    return {
        "target_date": target_str,
        "source_count": len(silver_keys),
        "daily_rows": len(rows),
        "existing_rows": existing_count,
        "merged_rows": merged_count,
        "matched": counts,
        "skipped": dict(skipped),
        "output": f"s3://{S3_BUCKET}/{output_key}",
        "dry_run": dry_run,
    }


def build_dag():
    if DAG is None:
        return None

    default_args = {
        "owner": "opik",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    }
    with DAG(
        dag_id="opik_gold_structured",
        description="Silver 일배치에서 정형 투자지표를 추출해 월별 Gold Parquet에 upsert",
        default_args=default_args,
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
        schedule=REPORT_PIPELINE_SCHEDULE,
        catchup=False,
        max_active_runs=1,
        tags=["opik", "gold", "structured", "reports"],
    ) as dag_obj:
        wait_for_silver = ExternalTaskSensor(
            task_id="wait_for_silver_extract",
            external_dag_id="opik_silver_extract",
            external_task_id="extract_bronze_pdf_to_silver_json",
            execution_date_fn=upstream_logical_date_for_target,
            allowed_states=["success"],
            failed_states=["failed", "skipped"],
            check_existence=True,
            mode="reschedule",
            poll_interval=60,
            timeout=6 * 60 * 60,
        )
        structured_task = PythonOperator(
            task_id="upsert_daily_structured_to_monthly_parquet",
            python_callable=run_daily_structured,
            op_kwargs={"target_date": KST_TARGET_DATE_TEMPLATE},
            # 성공 시 Dataset 업데이트 → maintenance DAG 트리거(embeddings와 AND 조건).
            outlets=[Dataset(GOLD_STRUCTURED_DATASET_URI)],
        )
        wait_for_silver >> structured_task
    return dag_obj


dag = build_dag()


def main():
    parser = argparse.ArgumentParser(description="OPIK daily Gold structured builder")
    parser.add_argument("--date", help="처리일 YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = run_daily_structured(args.date, dry_run=args.dry_run)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
