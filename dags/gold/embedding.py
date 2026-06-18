"""Airflow 일배치용 gold/embeddings 생성 모듈.

하루 50건 내외의 증권사 리포트를 처리한다는 전제에서 Bedrock Batch Inference를
사용하지 않고 동기 호출만 사용한다.

처리 흐름:
1. s3://{bucket}/silver/{증권사}/{YYYY-MM-DD}/*.json 읽기
2. Claude 3 Haiku로 reason / risks / keywords 추출
3. silver/embedding_input/{증권사}/{YYYY-MM-DD}/{report_id}.json 저장
4. intfloat/multilingual-e5-small로 embedding 생성
5. gold/embeddings/year=YYYY/month=MM/data.parquet에 월 단위 merge 저장

"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import tempfile
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

try:
    import pendulum
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.sensors.external_task import ExternalTaskSensor
except ImportError:  # 로컬 CLI 실행 환경에는 Airflow가 없을 수 있다.
    DAG = None
    PythonOperator = None
    ExternalTaskSensor = None


BASE_DIR = Path(__file__).resolve().parent
OPIK_ROOT = Path(__file__).resolve().parents[2]


def load_local_env() -> None:
    """프로젝트 helper 모듈이나 python-dotenv 없이 로컬 실행용 .env를 읽는다."""
    candidates: list[Path] = []
    if root := os.getenv("OPIK_ROOT"):
        candidates.append(Path(root) / ".env")
    candidates.extend([
        OPIK_ROOT / ".env",
        OPIK_ROOT.parent / ".env",
        BASE_DIR.parent / ".env",
        BASE_DIR / ".env",
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
    os.getenv("AWS_REGION")
    or os.getenv("AWS_DEFAULT_REGION")
    or "ap-northeast-2"
).strip("'\"")
BEDROCK_REGION = (os.getenv("BEDROCK_REGION") or AWS_REGION).strip("'\"")
LLM_MODEL_ID = (
    os.getenv("BEDROCK_LLM_MODEL_ID")
    or "anthropic.claude-3-haiku-20240307-v1:0"
).strip("'\"")
BEDROCK_API_KEY = (
    os.getenv("AWS_BEARER_TOKEN_BEDROCK")
    or os.getenv("BEDROCK_API_KEY")
    or os.getenv("AWS_BEDROCK_API_KEY")
)

if BEDROCK_API_KEY and not os.getenv("AWS_BEARER_TOKEN_BEDROCK"):
    os.environ["AWS_BEARER_TOKEN_BEDROCK"] = BEDROCK_API_KEY.strip("'\"")

RAW_SILVER_PREFIX = "silver/"
EMBEDDING_INPUT_PREFIX = "silver/embedding_input/"
GOLD_EMBEDDING_PREFIX = "gold/embeddings/"
REPORT_PIPELINE_SCHEDULE = os.getenv("OPIK_REPORT_PIPELINE_SCHEDULE", "0 0 * * *")
KST_TARGET_DATE_TEMPLATE = (
    "{{ data_interval_end.in_timezone('Asia/Seoul').to_date_string() }}"
)


def upstream_logical_date_for_target(logical_date, data_interval_end=None, **_):
    """Map this run to the scheduled upstream run that owns the same KST target date."""
    interval_end = pendulum.instance(data_interval_end or logical_date).in_timezone("Asia/Seoul")
    return interval_end.start_of("day").subtract(days=1)

EMBEDDING_MODEL_NAME = "intfloat/multilingual-e5-small"
EMBEDDING_DIM = 384

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("opik.gold.embeddings")

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


@dataclass
class DailyEmbeddingResult:
    start_date: str
    end_date: str
    source_count: int
    embedding_input_written: int
    embedding_input_reused: int
    llm_failed: int
    gold_rows: int
    uploaded: dict[str, Any]
    skipped: dict[str, int]
    broker_counts: dict[str, int]
    dry_run: bool = False


def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def bedrock_runtime_client():
    return boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)


def parse_date(value: str | date, label: str = "date") -> date:
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{label}는 YYYY-MM-DD 형식이어야 합니다: {value}") from exc


def date_range(start_date: str | date, end_date: str | date):
    current = parse_date(start_date, "start_date")
    end = parse_date(end_date, "end_date")
    if current > end:
        raise ValueError(f"start_date가 end_date보다 늦습니다: {current} > {end}")
    while current <= end:
        yield current.isoformat()
        current += timedelta(days=1)


def daily_target_date(logical_date: str | date | None = None, lag_days: int = 1) -> str:
    base = parse_date(logical_date) if logical_date else date.today()
    return (base - timedelta(days=lag_days)).isoformat()


def s3_uri(key: str) -> str:
    return f"s3://{S3_BUCKET}/{key}"


def load_json_from_s3(s3, key: str) -> dict[str, Any]:
    body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return json.loads(body)


def put_json_to_s3(s3, key: str, payload: dict[str, Any]):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )


def object_exists(s3, key: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def list_common_prefixes(s3, prefix: str):
    prefixes = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/"):
        prefixes.extend(item["Prefix"] for item in page.get("CommonPrefixes", []))
    return prefixes


def list_json_keys(s3, prefix: str):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                keys.append(key)
    return sorted(keys)


def list_daily_silver_keys(s3, start_date: str | date, end_date: str | date):
    """silver/{증권사}/{일자}/ 아래의 원천 리포트 JSON만 가져온다."""
    keys = []
    broker_prefixes = list_common_prefixes(s3, RAW_SILVER_PREFIX)
    for broker_prefix in broker_prefixes:
        broker = broker_prefix.rstrip("/").split("/")[-1]
        if broker == "embedding_input" or broker.startswith("_") or broker.endswith("_수정"):
            continue
        for day in date_range(start_date, end_date):
            keys.extend(list_json_keys(s3, f"{broker_prefix}{day}/"))
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
        r"\b(\d{6})\s*(?:기업분석|종목분석)\b",
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
    """JSON 내부에 남아 있는 이스케이프 문자와 제어문자를 임베딩 전 정리한다."""
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


def normalize_silver(doc: dict[str, Any], key: str):
    parts = key.split("/")
    broker_from_key = parts[1] if len(parts) > 1 else ""
    date_from_key = parts[2] if len(parts) > 2 else ""

    title = clean_text(doc.get("title") or doc.get("제목") or doc.get("report_title") or "")
    text = clean_text(doc.get("text") or doc.get("본문") or doc.get("content") or "")
    if not title and text:
        title = next((line.strip() for line in text.splitlines() if line.strip()), "")[:200]

    published_at = str(doc.get("발행일") or doc.get("date") or date_from_key).replace(".", "-")
    stock_code = (
        normalize_stock_code(doc.get("종목코드"))
        or normalize_stock_code(doc.get("stock_code"))
        or extract_stock_code(title, text)
    )
    stock_name = (
        doc.get("종목명")
        or doc.get("stock_name")
        or doc.get("company")
        or infer_stock_name(title)
        or ""
    )

    return {
        "source_s3_key": key,
        "report_id": str(doc.get("report_id") or Path(key).stem),
        "증권사": str(doc.get("증권사") or doc.get("source") or broker_from_key),
        "종목명": str(stock_name),
        "종목코드": stock_code,
        "발행일": published_at,
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
    if not stripped or len(stripped) > 80:
        return False
    return bool(FINANCIAL_METRIC_PATTERN.search(stripped))


def report_lines(text: str):
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


def narrative_context(lines: list[str], max_chars: int):
    selected = [line for line in lines if not is_numeric_line(line)]
    return "\n".join(selected)[:max_chars].strip()


def table_signal(lines: list[str], max_chars: int):
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
        add_len = len(row) + 1
        if total_len + add_len > max_chars:
            break
        rows.append(row)
        seen.add(row_key)
        total_len += add_len
    return "\n".join(rows).strip()


def build_llm_prompt(row: dict[str, Any], max_chars: int = 10000) -> str:
    body = strip_boilerplate(row["text"])[:max_chars]
    return f"""
너는 국내 증권사 리포트를 구조화하는 금융 데이터 추출기다.
아래 리포트에서 본문에 명시적으로 근거가 있는 내용만 추출한다.
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


def first_json_object(text: str):
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
    raise ValueError(f"JSON 객체를 찾지 못했습니다: {text[:300]}")


def parse_llm_output(text: str):
    try:
        parsed = first_json_object(text)
    except ValueError:
        parsed = {}

    reason = parsed.get("reason")
    risks = parsed.get("risks") or []
    keywords = parsed.get("keywords") or []
    if isinstance(risks, str):
        risks = [risks]
    if isinstance(keywords, str):
        keywords = [keywords]

    return {
        "reason": clean_text(reason) if reason else None,
        "risks": [clean_text(item) for item in risks if clean_text(item)][:5],
        "keywords": [clean_text(item) for item in keywords if clean_text(item)][:8],
    }


def call_haiku(prompt: str, model_id: str = LLM_MODEL_ID, max_tokens: int = 800, retries: int = 2):
    client = bedrock_runtime_client()
    last_error = None
    for attempt in range(retries + 1):
        try:
            response = client.converse(
                modelId=model_id,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={"maxTokens": max_tokens, "temperature": 0},
            )
            text = response["output"]["message"]["content"][0]["text"]
            return parse_llm_output(text)
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"Haiku 호출 실패: {last_error}") from last_error


def build_embedding_text(
    row: dict[str, Any],
    reason: str | None,
    risks: list[str],
    keywords: list[str],
    narrative_max_chars: int = 6000,
    table_max_chars: int = 2500,
):
    lines = report_lines(row["text"])
    return clean_text(f"""
종목명: {row["종목명"]}
종목코드: {row["종목코드"]}
증권사: {row["증권사"]}
발행일: {row["발행일"]}
제목: {row["title"]}

핵심논리: {reason or ""}
리스크: {", ".join(risks or [])}
키워드: {", ".join(keywords or [])}

본문서술:
{narrative_context(lines, narrative_max_chars)}

재무표_수치표_압축:
{table_signal(lines, table_max_chars)}
""")


def build_embedding_input_payload(
    row: dict[str, Any],
    extracted: dict[str, Any],
    embedding_text: str,
    llm_status: str,
    llm_error: str | None = None,
):
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


def row_from_embedding_input(doc: dict[str, Any], key: str):
    embedding_text = clean_text(doc.get("embedding_text") or "")
    if not embedding_text:
        return None
    return {
        "report_id": str(doc.get("report_id") or Path(key).stem),
        "종목코드": doc.get("종목코드"),
        "reason": doc.get("reason"),
        "risks": [str(item) for item in (doc.get("risks") or []) if item],
        "keywords": [str(item) for item in (doc.get("keywords") or []) if item],
        "_embedding_input": embedding_text,
    }


def gold_key_for_month(yyyymm: str):
    year, month = yyyymm.split("-")
    return f"{GOLD_EMBEDDING_PREFIX}year={year}/month={month}/data.parquet"


def month_key(date_value: str | date):
    parsed = parse_date(date_value, "발행일")
    return f"{parsed.year:04d}-{parsed.month:02d}"


def parquet_schema():
    import pyarrow as pa

    return pa.schema([
        pa.field("report_id", pa.string()),
        pa.field("종목코드", pa.string()),
        pa.field("reason", pa.string()),
        pa.field("risks", pa.list_(pa.string())),
        pa.field("keywords", pa.list_(pa.string())),
        pa.field("embedding", pa.list_(pa.float32())),
    ])


def read_existing_gold_rows(s3, key: str):
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
                f"기존 embedding Parquet 스키마가 다릅니다: "
                f"expected={expected}, actual={table.schema.names}, key={key}"
            )
        return table.to_pylist()


def write_gold_rows(s3, key: str, rows: list[dict[str, Any]]):
    import pyarrow as pa
    import pyarrow.parquet as pq

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = Path(tmp_dir) / "data.parquet"
        table = pa.Table.from_pylist(rows, schema=parquet_schema())
        pq.write_table(table, local_path, compression="snappy")
        s3.upload_file(str(local_path), S3_BUCKET, key)


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
    return [merged[report_id] for report_id in sorted(merged)]


def merge_and_upload_monthly_gold(
    s3,
    rows_by_month: dict[str, list[dict[str, Any]]],
):
    uploaded = {}
    for yyyymm, new_rows in sorted(rows_by_month.items()):
        out_key = gold_key_for_month(yyyymm)
        existing_rows = read_existing_gold_rows(s3, out_key)
        merged_rows = merge_rows(existing_rows, new_rows)
        write_gold_rows(s3, out_key, merged_rows)
        uploaded[yyyymm] = {
            "uri": s3_uri(out_key),
            "existing_rows": len(existing_rows),
            "daily_rows": len(new_rows),
            "merged_rows": len(merged_rows),
        }
    return uploaded


def run_daily_embedding(
    target_date: str | date | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    logical_date: str | date | None = None,
    lag_days: int = 1,
    limit: int = 0,
    overwrite_embedding_input: bool = False,
    write_embedding_input: bool = True,
    include_failed_llm: bool = False,
    fail_on_llm_error: bool = True,
    dry_run: bool = False,
) -> DailyEmbeddingResult:
    """
    Airflow PythonOperator에서 호출할 일배치 엔트리포인트.

    target_date를 넘기면 그 하루만 처리한다.
    start_date/end_date를 넘기면 해당 기간을 처리한다. 일배치에서는 보통 target_date만 쓴다.
    target_date가 없으면 logical_date - lag_days, logical_date도 없으면 오늘 - lag_days를 쓴다.
    """
    if target_date:
        start = end = parse_date(target_date, "target_date").isoformat()
    elif start_date and end_date:
        start = parse_date(start_date, "start_date").isoformat()
        end = parse_date(end_date, "end_date").isoformat()
    else:
        start = end = daily_target_date(logical_date, lag_days=lag_days)

    s3 = s3_client()
    keys = list_daily_silver_keys(s3, start, end)
    if limit:
        keys = keys[:limit]

    broker_counts = Counter()
    skipped = Counter()
    embedding_rows = []
    embedding_input_written = 0
    embedding_input_reused = 0
    llm_failed = 0

    if dry_run:
        for key in keys:
            parts = key.split("/")
            if len(parts) > 1:
                broker_counts[parts[1]] += 1
        return DailyEmbeddingResult(
            start_date=start,
            end_date=end,
            source_count=len(keys),
            embedding_input_written=0,
            embedding_input_reused=0,
            llm_failed=0,
            gold_rows=0,
            uploaded={},
            skipped=dict(skipped),
            broker_counts=dict(broker_counts),
            dry_run=True,
        )

    for idx, key in enumerate(keys, 1):
        doc = load_json_from_s3(s3, key)
        row = normalize_silver(doc, key)
        broker_counts[row["증권사"]] += 1

        if row["발행일"] < start or row["발행일"] > end:
            skipped["date_mismatch"] += 1
            continue
        if not row["text"]:
            skipped["empty_text"] += 1
            continue
        if not row["종목코드"]:
            skipped["missing_stock_code"] += 1

        out_key = embedding_input_key(row)
        existing_payload = None
        if not overwrite_embedding_input and object_exists(s3, out_key):
            existing_payload = load_json_from_s3(s3, out_key)

        if (
            existing_payload
            and existing_payload.get("llm_status") != "failed"
            and clean_text(existing_payload.get("embedding_text") or "")
        ):
            payload = existing_payload
            embedding_input_reused += 1
        else:
            llm_status = "ok"
            llm_error = None
            try:
                extracted = call_haiku(build_llm_prompt(row))
            except Exception as exc:
                llm_failed += 1
                llm_status = "failed"
                llm_error = str(exc)[:1000]
                extracted = {"reason": None, "risks": [], "keywords": []}

            embedding_text = build_embedding_text(
                row,
                extracted["reason"],
                extracted["risks"],
                extracted["keywords"],
            )
            payload = build_embedding_input_payload(row, extracted, embedding_text, llm_status, llm_error)
            if write_embedding_input:
                put_json_to_s3(s3, out_key, payload)
                embedding_input_written += 1

        if payload.get("llm_status") == "failed" and not include_failed_llm:
            skipped["llm_failed"] += 1
            continue

        gold_row = row_from_embedding_input(payload, out_key)
        if gold_row is None:
            skipped["empty_embedding_text"] += 1
            continue
        gold_row["_month"] = month_key(payload["발행일"])
        embedding_rows.append(gold_row)

        if idx % 10 == 0 or idx == len(keys):
            logger.info(
                "Embedding progress %d/%d (input_write=%d reuse=%d llm_failed=%d)",
                idx,
                len(keys),
                embedding_input_written,
                embedding_input_reused,
                llm_failed,
            )

    uploaded = {}
    if embedding_rows:
        import numpy as np
        from sentence_transformers import SentenceTransformer

        logger.info("Loading embedding model: %s", EMBEDDING_MODEL_NAME)
        embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)
        vectors = embedder.encode(
            [row["_embedding_input"] for row in embedding_rows],
            batch_size=16,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        if vectors.ndim != 2 or vectors.shape[1] != EMBEDDING_DIM:
            raise ValueError(
                f"임베딩 차원이 예상과 다릅니다: expected={EMBEDDING_DIM}, actual={vectors.shape}"
            )

        rows_by_month = defaultdict(list)
        for row, vector in zip(embedding_rows, vectors):
            rows_by_month[row.pop("_month")].append({
                "report_id": row["report_id"],
                "종목코드": row["종목코드"],
                "reason": row["reason"],
                "risks": row["risks"],
                "keywords": row["keywords"],
                "embedding": np.asarray(vector, dtype=np.float32).tolist(),
            })

        uploaded = merge_and_upload_monthly_gold(s3, rows_by_month)

    if llm_failed and fail_on_llm_error:
        raise RuntimeError(
            f"Haiku 추출 실패 {llm_failed}건. 성공 행은 저장했으며 Airflow 재시도에서 "
            "실패한 embedding_input만 다시 호출합니다."
        )

    return DailyEmbeddingResult(
        start_date=start,
        end_date=end,
        source_count=len(keys),
        embedding_input_written=embedding_input_written,
        embedding_input_reused=embedding_input_reused,
        llm_failed=llm_failed,
        gold_rows=len(embedding_rows),
        uploaded=uploaded,
        skipped=dict(skipped),
        broker_counts=dict(broker_counts),
        dry_run=False,
    )


def build_dag():
    if DAG is None:
        return None

    default_args = {
        "owner": "opik",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    }
    with DAG(
        dag_id="opik_gold_embeddings",
        description="Silver 일배치에서 Haiku 의미 추출과 E5 임베딩을 생성해 월별 Gold에 upsert",
        default_args=default_args,
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
        schedule=REPORT_PIPELINE_SCHEDULE,
        catchup=False,
        max_active_runs=1,
        tags=["opik", "gold", "embeddings", "bedrock", "reports"],
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
        embedding_task = PythonOperator(
            task_id="upsert_daily_embeddings_to_monthly_parquet",
            python_callable=run_daily_embedding,
            op_kwargs={
                "target_date": KST_TARGET_DATE_TEMPLATE,
                "overwrite_embedding_input": False,
                "write_embedding_input": True,
                "include_failed_llm": False,
                "fail_on_llm_error": True,
            },
            execution_timeout=timedelta(hours=1),
        )
        wait_for_silver >> embedding_task
    return dag_obj


dag = build_dag()


def parse_args():
    parser = argparse.ArgumentParser(description="Daily gold/embeddings builder")
    parser.add_argument("--date", help="처리할 하루. 예: 2026-06-16")
    parser.add_argument("--start-date", help="기간 시작일. --date가 있으면 무시")
    parser.add_argument("--end-date", help="기간 종료일. --date가 있으면 무시")
    parser.add_argument("--lag-days", type=int, default=1)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--overwrite-embedding-input", action="store_true")
    parser.add_argument("--no-write-embedding-input", action="store_true")
    parser.add_argument("--include-failed-llm", action="store_true")
    parser.add_argument("--allow-llm-failures", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    result = run_daily_embedding(
        target_date=args.date,
        start_date=args.start_date,
        end_date=args.end_date,
        lag_days=args.lag_days,
        limit=args.limit,
        overwrite_embedding_input=args.overwrite_embedding_input,
        write_embedding_input=not args.no_write_embedding_input,
        include_failed_llm=args.include_failed_llm,
        fail_on_llm_error=not args.allow_llm_failures,
        dry_run=args.dry_run,
    )
    print(json.dumps(asdict(result), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
