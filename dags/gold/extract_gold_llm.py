"""OPIK — Gold LLM/Embedding daily extraction.

Silver JSON text -> Claude Haiku reason/risks/keywords -> local embedding
-> gold/embeddings/year=YYYY/month=MM/data.parquet

일배치 전제:
    - 하루 처리량은 보통 수십 건 수준
    - Bedrock Batch Inference는 사용하지 않음
    - Claude 3 Haiku는 Bedrock Runtime Converse API로 동기 호출
    - 임베딩은 intfloat/multilingual-e5-small 로컬 모델 사용

Usage:
    python extract_gold_llm.py --date 2026-06-12
    python extract_gold_llm.py --date 2026-06-12 --workers 4
    python extract_gold_llm.py --date 2026-06-12 --dry-run
"""

from __future__ import annotations

from pathlib import Path
import sys

OPIK_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(OPIK_ROOT))

import argparse
import concurrent.futures
import json
import logging
import os
import re
import tempfile
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from opik_config import (
    S3_BUCKET as CONFIG_S3_BUCKET,
    S3_REGION as CONFIG_S3_REGION,
    load_dotenv,
)


load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET", CONFIG_S3_BUCKET)
S3_REGION = os.getenv("S3_REGION", CONFIG_S3_REGION)
s3 = boto3.client("s3", region_name=S3_REGION)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("opik.gold.llm")

RAW_SILVER_PREFIX = "silver/"
EMBEDDING_INPUT_PREFIX = "silver/embedding_input/"
GOLD_EMBEDDING_PREFIX = "gold/embeddings/"

BEDROCK_REGION = os.getenv("BEDROCK_REGION", S3_REGION)
LLM_MODEL_ID = os.getenv(
    "BEDROCK_LLM_MODEL_ID",
    "anthropic.claude-3-haiku-20240307-v1:0",
)
BEDROCK_API_KEY = (
    os.getenv("AWS_BEARER_TOKEN_BEDROCK")
    or os.getenv("BEDROCK_API_KEY")
    or os.getenv("AWS_BEDROCK_API_KEY")
)

EMBEDDING_MODEL_NAME = os.getenv(
    "EMBEDDING_MODEL_NAME",
    "intfloat/multilingual-e5-small",
)
EMBEDDING_DIM = 384

MAX_RETRIES = 5
RETRY_BASE_DELAY = 1.0

BOILERPLATE_MARKERS = [
    r"Compliance\s*Notice",
    r"고지\s*사항",
    r"Disclaimer",
    r"투자의견\s*변동\s*내역",
    r"투자의견\s*및\s*목표주가\s*추이",
    r"투자등급\s*비율",
    r"종목추천\s*투자등급",
    r"투자의견\s*분류",
    r"본\s*조사분석자료",
    r"본\s*분석자료는",
    r"무단\s*전재",
    r"무단으로\s*인용",
]

CONTROL_CHAR_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
ESCAPED_HEX_PATTERN = re.compile(r"\\x([0-9a-fA-F]{2})")
ESCAPED_UNICODE_PATTERN = re.compile(r"\\u([0-9a-fA-F]{4})")
NUMERIC_VALUE_PATTERN = re.compile(
    r"[-+]?\d[\d,]*(?:\.\d+)?\s*(?:%|원|억원|십억원|조원|배|x|X)?|N/A|n/a|적자|흑자"
)
FINANCIAL_METRIC_PATTERN = re.compile(
    r"("
    r"매출|매출액|영업수익|영업이익|영업손익|세전이익|당기순이익|순이익|"
    r"지배주주|EBITDA|EPS|BPS|DPS|PER|PBR|ROE|ROA|EV/EBITDA|"
    r"영업이익률|순이익률|부채비율|순차입금|현금흐름|CAPEX|FCF|"
    r"자산총계|부채총계|자본총계|Revenue|Sales|Operating Profit|Net Profit|Margin"
    r")",
    re.IGNORECASE,
)

GOLD_SCHEMA = pa.schema([
    ("report_id", pa.string()),
    ("종목코드", pa.string()),
    ("reason", pa.string()),
    ("risks", pa.list_(pa.string())),
    ("keywords", pa.list_(pa.string())),
    ("embedding", pa.list_(pa.float32())),
])


def bedrock_runtime_client():
    if BEDROCK_API_KEY and not os.getenv("AWS_BEARER_TOKEN_BEDROCK"):
        os.environ["AWS_BEARER_TOKEN_BEDROCK"] = BEDROCK_API_KEY.strip("'\"")
    return boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)


def parse_date(value: str | date, label: str = "date") -> date:
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{label} must be YYYY-MM-DD: {value}") from exc


def iter_dates(start_date: str | date, end_date: str | date):
    current = parse_date(start_date, "start_date")
    end = parse_date(end_date, "end_date")
    if current > end:
        raise ValueError(f"start_date is after end_date: {current} > {end}")
    while current <= end:
        yield current.isoformat()
        current += timedelta(days=1)


def default_target_date(lag_days: int = 1) -> str:
    return (date.today() - timedelta(days=lag_days)).isoformat()


def s3_uri(key: str) -> str:
    return f"s3://{S3_BUCKET}/{key}"


def s3_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def s3_load_json(key: str) -> dict[str, Any]:
    body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return json.loads(body)


def s3_put_json(key: str, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )


def list_common_prefixes(prefix: str) -> list[str]:
    prefixes = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/"):
        prefixes.extend(item["Prefix"] for item in page.get("CommonPrefixes", []))
    return prefixes


def list_json_keys(prefix: str) -> list[str]:
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                keys.append(key)
    return sorted(keys)


def list_daily_silver_keys(start_date: str, end_date: str) -> list[str]:
    """List silver/{broker}/{date}/*.json for the target dates only."""
    keys = []
    for broker_prefix in list_common_prefixes(RAW_SILVER_PREFIX):
        broker = broker_prefix.rstrip("/").split("/")[-1]
        if broker == "embedding_input" or broker.startswith("_") or broker.endswith("_수정"):
            continue
        for day in iter_dates(start_date, end_date):
            keys.extend(list_json_keys(f"{broker_prefix}{day}/"))
    return sorted(keys)


def normalize_stock_code(value: Any) -> str:
    if value is None:
        return ""
    match = re.search(r"(\d{6})", str(value))
    return match.group(1) if match else ""


def extract_stock_code(title: str, text: str) -> str:
    source = f"{title}\n{text[:3000]}"
    patterns = [
        r"\((\d{6})\)",
        r"\[(\d{6})\]",
        r"\bA(\d{6})\b",
        r"(?:종목코드|Code)\s*[:：]?\s*(\d{6})",
        r"\b(\d{6})\s*(?:\.KS|\.KQ|KS|KQ|KOSPI|KOSDAQ|KONEX)\b",
        r"\b(\d{6})\s*(?:기업분석|종목분석|기업\s*Report|종목\s*Report)\b",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, source, flags=re.IGNORECASE):
            code = match.group(1)
            if 1900 <= int(code) <= 2099:
                continue
            return code
    return ""


def infer_stock_name(title: str) -> str:
    match = re.match(r"\s*([^(:\[]+?)\s*(?:\(|\[)?\d{6}", title or "")
    return match.group(1).strip() if match else ""


def clean_text(value: Any) -> str:
    """Remove escape artifacts/control characters before LLM and embedding."""
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ")
    text = text.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")
    text = text.replace("\\t", " ").replace('\\"', '"').replace("\\/", "/")
    text = ESCAPED_UNICODE_PATTERN.sub(lambda m: chr(int(m.group(1), 16)), text)
    text = ESCAPED_HEX_PATTERN.sub(lambda m: chr(int(m.group(1), 16)), text)
    text = text.replace("\\", " ")
    text = CONTROL_CHAR_PATTERN.sub(" ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def strip_boilerplate(text: str) -> str:
    cut_points = []
    for marker in BOILERPLATE_MARKERS:
        match = re.search(marker, text, flags=re.IGNORECASE)
        if match:
            cut_points.append(match.start())
    return text[:min(cut_points)].strip() if cut_points else text


def normalize_silver_doc(doc: dict[str, Any], key: str) -> dict[str, Any]:
    parts = key.split("/")
    broker_from_key = parts[1] if len(parts) > 1 else ""
    date_from_key = parts[2] if len(parts) > 2 else ""

    title = clean_text(doc.get("title") or doc.get("제목") or doc.get("report_title") or "")
    text = clean_text(doc.get("text") or doc.get("본문") or doc.get("content") or "")
    if not title and text:
        title = next((line.strip() for line in text.splitlines() if line.strip()), "")[:200]

    stock_code = (
        normalize_stock_code(doc.get("종목코드"))
        or normalize_stock_code(doc.get("stock_code"))
        or extract_stock_code(title, text)
    )

    return {
        "source_s3_key": key,
        "report_id": str(doc.get("report_id") or Path(key).stem),
        "증권사": str(doc.get("증권사") or doc.get("source") or broker_from_key),
        "종목명": str(
            doc.get("종목명")
            or doc.get("stock_name")
            or doc.get("company")
            or infer_stock_name(title)
            or ""
        ),
        "종목코드": stock_code,
        "발행일": str(doc.get("발행일") or doc.get("date") or date_from_key).replace(".", "-"),
        "title": title,
        "text": text,
    }


def embedding_input_key(row: dict[str, Any]) -> str:
    return (
        f"{EMBEDDING_INPUT_PREFIX}"
        f"{row['증권사']}/{row['발행일']}/{row['report_id']}.json"
    )


def is_numeric_line(line: str) -> bool:
    stripped = line.strip()
    return bool(stripped and len(stripped) <= 40 and NUMERIC_VALUE_PATTERN.fullmatch(stripped))


def is_metric_line(line: str) -> bool:
    stripped = line.strip()
    return bool(stripped and len(stripped) <= 80 and FINANCIAL_METRIC_PATTERN.search(stripped))


def report_lines(text: str) -> list[str]:
    text = strip_boilerplate(clean_text(text))
    lines = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if re.fullmatch(r"\d{1,2}\s*/\s*\d{1,2}", line):
            continue
        lines.append(line)
    return lines


def narrative_context(lines: list[str], max_chars: int = 6000) -> str:
    return "\n".join(line for line in lines if not is_numeric_line(line))[:max_chars].strip()


def table_signal(lines: list[str], max_chars: int = 2500) -> str:
    rows = []
    seen = set()
    total_len = 0
    for idx, line in enumerate(lines):
        if not is_metric_line(line):
            continue
        values = NUMERIC_VALUE_PATTERN.findall(line)
        for next_line in lines[idx + 1: idx + 8]:
            if is_metric_line(next_line) and values:
                break
            if is_numeric_line(next_line):
                values.append(next_line)
            if len(values) >= 6:
                break
        values = [value.strip() for value in values if value.strip()][:6]
        if len(values) < 2:
            continue
        row = f"{line.strip(' :：')}: {', '.join(values)}"
        row_key = row.lower()
        if row_key in seen:
            continue
        if total_len + len(row) + 1 > max_chars:
            break
        rows.append(row)
        seen.add(row_key)
        total_len += len(row) + 1
    return "\n".join(rows).strip()


def build_prompt(row: dict[str, Any], max_chars: int = 10000) -> str:
    body = strip_boilerplate(row["text"])[:max_chars]
    return f"""
너는 국내 증권사 리포트를 구조화하는 금융 데이터 추출기다.
본문에 명시적으로 근거가 있는 내용만 추출한다.
애매하거나 본문에 없는 내용은 null 또는 빈 배열로 둔다.
반드시 JSON만 출력한다. 마크다운, 코드블록, 설명문은 붙이지 않는다.

출력 스키마:
{{
  "reason": "핵심 투자 논리 1~2문장 또는 null",
  "risks": ["리스크 요인 최대 5개"],
  "keywords": ["핵심 키워드 최대 8개"]
}}

제목: {row["title"]}
종목명: {row["종목명"]}
종목코드: {row["종목코드"]}
증권사: {row["증권사"]}
발행일: {row["발행일"]}

본문:
{body}
""".strip()


def first_json_object(text: str) -> dict[str, Any]:
    text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.IGNORECASE | re.MULTILINE).strip()
    decoder = json.JSONDecoder()
    for idx, char in enumerate(text):
        if char != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(text[idx:])
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            continue
    raise ValueError(f"JSON object not found: {text[:300]}")


def parse_llm_output(text: str) -> dict[str, Any]:
    try:
        parsed = first_json_object(text)
    except ValueError:
        parsed = {}

    risks = parsed.get("risks") or []
    keywords = parsed.get("keywords") or []
    if isinstance(risks, str):
        risks = [risks]
    if isinstance(keywords, str):
        keywords = [keywords]

    reason = parsed.get("reason")
    return {
        "reason": clean_text(reason) if reason else None,
        "risks": [clean_text(item) for item in risks if clean_text(item)][:5],
        "keywords": [clean_text(item) for item in keywords if clean_text(item)][:8],
    }


def call_haiku(prompt: str, client=None, max_tokens: int = 800) -> dict[str, Any]:
    client = client or bedrock_runtime_client()
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            response = client.converse(
                modelId=LLM_MODEL_ID,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={"maxTokens": max_tokens, "temperature": 0},
            )
            text = response["output"]["message"]["content"][0]["text"]
            return parse_llm_output(text)
        except Exception as exc:
            last_error = exc
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning("Haiku retry %d/%d after %.1fs: %s", attempt + 1, MAX_RETRIES, delay, exc)
                time.sleep(delay)
    raise RuntimeError(f"Haiku call failed: {last_error}") from last_error


def build_embedding_text(row: dict[str, Any], extracted: dict[str, Any]) -> str:
    lines = report_lines(row["text"])
    return clean_text(f"""
종목명: {row["종목명"]}
종목코드: {row["종목코드"]}
증권사: {row["증권사"]}
발행일: {row["발행일"]}
제목: {row["title"]}

핵심논리: {extracted.get("reason") or ""}
리스크: {", ".join(extracted.get("risks") or [])}
키워드: {", ".join(extracted.get("keywords") or [])}

본문서술:
{narrative_context(lines)}

재무표_수치표_압축:
{table_signal(lines)}
""")


def build_embedding_input_payload(
    row: dict[str, Any],
    extracted: dict[str, Any],
    embedding_text: str,
    llm_status: str,
    llm_error: str | None,
) -> dict[str, Any]:
    return {
        "report_id": row["report_id"],
        "source": row["증권사"],
        "증권사": row["증권사"],
        "종목명": row["종목명"],
        "종목코드": row["종목코드"] or None,
        "발행일": row["발행일"],
        "제목": row["title"],
        "reason": extracted.get("reason"),
        "risks": extracted.get("risks") or [],
        "keywords": extracted.get("keywords") or [],
        "embedding_text": embedding_text,
        "llm_model": LLM_MODEL_ID,
        "llm_status": llm_status,
        "llm_error": llm_error,
        "source_s3_key": row["source_s3_key"],
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


def process_one_silver_key(key: str, overwrite_embedding_input: bool) -> tuple[str, dict[str, Any] | None]:
    doc = s3_load_json(key)
    row = normalize_silver_doc(doc, key)
    if not row["text"]:
        return "empty_text", None

    out_key = embedding_input_key(row)
    if not overwrite_embedding_input and s3_exists(out_key):
        payload = s3_load_json(out_key)
        return "reused", payload

    llm_status = "ok"
    llm_error = None
    try:
        extracted = call_haiku(build_prompt(row))
    except Exception as exc:
        llm_status = "failed"
        llm_error = str(exc)[:1000]
        extracted = {"reason": None, "risks": [], "keywords": []}

    embedding_text = build_embedding_text(row, extracted)
    payload = build_embedding_input_payload(row, extracted, embedding_text, llm_status, llm_error)
    s3_put_json(out_key, payload)
    return "written" if llm_status == "ok" else "llm_failed", payload


def gold_row_from_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
    embedding_text = clean_text(payload.get("embedding_text") or "")
    if not embedding_text:
        return None
    return {
        "report_id": str(payload.get("report_id") or ""),
        "종목코드": payload.get("종목코드"),
        "reason": payload.get("reason"),
        "risks": [str(item) for item in (payload.get("risks") or []) if item],
        "keywords": [str(item) for item in (payload.get("keywords") or []) if item],
        "_embedding_input": embedding_text,
        "_month": payload.get("발행일", "")[:7],
    }


def gold_key_for_month(yyyymm: str) -> str:
    year, month = yyyymm.split("-")
    return f"{GOLD_EMBEDDING_PREFIX}year={year}/month={month}/data.parquet"


def read_existing_gold_rows(key: str) -> list[dict[str, Any]]:
    if not s3_exists(key):
        return []
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = Path(tmp_dir) / "existing.parquet"
        s3.download_file(S3_BUCKET, key, str(local_path))
        return pq.read_table(local_path).to_pylist()


def upload_gold_rows(key: str, rows: list[dict[str, Any]]) -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = Path(tmp_dir) / "data.parquet"
        table = pa.Table.from_pylist(rows, schema=GOLD_SCHEMA)
        pq.write_table(table, local_path, compression="snappy")
        with open(local_path, "rb") as f:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=f.read(),
                ContentType="application/octet-stream",
            )


def embed_and_upload(gold_rows: list[dict[str, Any]], batch_size: int = 16) -> dict[str, str]:
    if not gold_rows:
        return {}

    import numpy as np
    from sentence_transformers import SentenceTransformer

    logger.info("Loading embedding model: %s", EMBEDDING_MODEL_NAME)
    embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)
    vectors = embedder.encode(
        [row["_embedding_input"] for row in gold_rows],
        batch_size=batch_size,
        normalize_embeddings=True,
        show_progress_bar=True,
    )

    rows_by_month = defaultdict(list)
    for row, vector in zip(gold_rows, vectors):
        rows_by_month[row["_month"]].append({
            "report_id": row["report_id"],
            "종목코드": row["종목코드"],
            "reason": row["reason"],
            "risks": row["risks"],
            "keywords": row["keywords"],
            "embedding": np.asarray(vector, dtype=np.float32).tolist(),
        })

    uploaded = {}
    for yyyymm, new_rows in sorted(rows_by_month.items()):
        key = gold_key_for_month(yyyymm)
        existing_rows = read_existing_gold_rows(key)
        new_ids = {row["report_id"] for row in new_rows}
        merged_rows = [row for row in existing_rows if row.get("report_id") not in new_ids]
        merged_rows.extend(new_rows)
        upload_gold_rows(key, merged_rows)
        uploaded[yyyymm] = s3_uri(key)
        logger.info("[%s] uploaded %d new rows, total %d rows -> %s",
                    yyyymm, len(new_rows), len(merged_rows), uploaded[yyyymm])
    return uploaded


def run_gold_llm(
    target_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    workers: int = 4,
    limit: int = 0,
    overwrite_embedding_input: bool = False,
    include_failed_llm: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    if target_date:
        start = end = parse_date(target_date, "date").isoformat()
    else:
        start = parse_date(start_date or default_target_date(), "start").isoformat()
        end = parse_date(end_date or start, "end").isoformat()

    t0 = time.perf_counter()
    keys = list_daily_silver_keys(start, end)
    if limit:
        keys = keys[:limit]

    broker_counts = Counter(k.split("/")[1] for k in keys if len(k.split("/")) > 1)
    logger.info("Gold LLM target: %s ~ %s | silver keys=%d | workers=%d",
                start, end, len(keys), workers)

    if dry_run:
        return {
            "start_date": start,
            "end_date": end,
            "source_count": len(keys),
            "broker_counts": dict(broker_counts),
            "dry_run": True,
        }

    statuses = Counter()
    gold_rows = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        future_map = {
            executor.submit(process_one_silver_key, key, overwrite_embedding_input): key
            for key in keys
        }
        for idx, future in enumerate(concurrent.futures.as_completed(future_map), 1):
            key = future_map[future]
            try:
                status, payload = future.result()
            except Exception as exc:
                logger.error("process failed %s: %s", key, exc)
                statuses["failed"] += 1
                continue

            statuses[status] += 1
            if payload:
                if payload.get("llm_status") == "failed" and not include_failed_llm:
                    statuses["skip_failed_llm_for_embedding"] += 1
                else:
                    row = gold_row_from_payload(payload)
                    if row:
                        gold_rows.append(row)
                    else:
                        statuses["empty_embedding_text"] += 1

            if idx % 25 == 0 or idx == len(keys):
                logger.info("progress %d/%d | %s", idx, len(keys), dict(statuses))

    uploaded = embed_and_upload(gold_rows)
    elapsed = time.perf_counter() - t0

    result = {
        "start_date": start,
        "end_date": end,
        "source_count": len(keys),
        "gold_rows": len(gold_rows),
        "uploaded": uploaded,
        "statuses": dict(statuses),
        "broker_counts": dict(broker_counts),
        "elapsed_sec": round(elapsed, 1),
        "dry_run": False,
    }
    logger.info("Gold LLM done: %s", result)
    return result


def parse_args():
    parser = argparse.ArgumentParser(description="OPIK Gold LLM/Embedding daily extraction")
    parser.add_argument("--date", help="Target date YYYY-MM-DD")
    parser.add_argument("--start", help="Start date YYYY-MM-DD. Ignored when --date is set.")
    parser.add_argument("--end", help="End date YYYY-MM-DD. Ignored when --date is set.")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--overwrite-embedding-input", action="store_true")
    parser.add_argument("--include-failed-llm", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    result = run_gold_llm(
        target_date=args.date,
        start_date=args.start,
        end_date=args.end,
        workers=args.workers,
        limit=args.limit,
        overwrite_embedding_input=args.overwrite_embedding_input,
        include_failed_llm=args.include_failed_llm,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
