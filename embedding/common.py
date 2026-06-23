"""
Batch Inference 전환용 공통 유틸리티.

이 폴더는 기존 finanace_AI_Agent 작업 폴더를 수정하지 않고 Batch Inference를
준비하기 위한 별도 스캐폴딩이다.
"""

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import boto3
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
SOURCE_PROJECT_DIR = BASE_DIR.parent

load_dotenv(BASE_DIR / ".env")
if (SOURCE_PROJECT_DIR / ".env").exists():
    load_dotenv(SOURCE_PROJECT_DIR / ".env", override=False)

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
BEDROCK_BATCH_ROLE_ARN = (os.getenv("BEDROCK_BATCH_ROLE_ARN") or "").strip("'\"")
DEFAULT_LLM_MODEL_ID = (
    os.getenv("BEDROCK_LLM_MODEL_ID")
    or "anthropic.claude-3-haiku-20240307-v1:0"
).strip("'\"")

BEDROCK_API_KEY = (
    os.getenv("AWS_BEARER_TOKEN_BEDROCK")
    or os.getenv("BEDROCK_API_KEY")
    or os.getenv("AWS_BEDROCK_API_KEY")
    or os.getenv("AWS_BEDROCK_CLAUDE_API_KEY")
)

# Batch Inference의 job 생성/조회는 Bedrock control plane 호출이며 iam:PassRole이 필요하다.
# Bedrock API key principal은 일반적으로 iam:PassRole을 갖지 않으므로, batch 전환 폴더에서는
# 기본적으로 IAM access key를 사용한다. 정말 API key로 Bedrock client를 만들고 싶을 때만 켠다.
USE_BEDROCK_API_KEY_FOR_BATCH = (
    os.getenv("USE_BEDROCK_API_KEY_FOR_BATCH", "false").strip("'\"").lower()
    in {"1", "true", "yes", "y"}
)
if USE_BEDROCK_API_KEY_FOR_BATCH and BEDROCK_API_KEY and not os.getenv("AWS_BEARER_TOKEN_BEDROCK"):
    os.environ["AWS_BEARER_TOKEN_BEDROCK"] = BEDROCK_API_KEY.strip("'\"")

RAW_SILVER_PREFIX = "silver/"
EMBEDDING_INPUT_PREFIX = "silver/embedding_input/"

NARRATIVE_MAX_CHARS_DEFAULT = 3500
TABLE_MAX_CHARS_DEFAULT = 1800
BATCH_PREFIX = "batch/embedding_input/jobs/"

BOILERPLATE_MARKERS = [
    r"Compliance\s*Notice",
    r"고지\s*사항",
    r"Disclaimer",
    r"투자의견\s*변동\s*내역",
    r"투자의견\s*및\s*목표주가\s*추이",
    r"투자등급\s*비율",
    r"종목추천\s*투자등급",
    r"투자비율\s*등급",
    r"투자의견\s*및\s*적용\s*기준",
    r"투자의견\s*분류",
    r"본\s*조사분석자료",
    r"본\s*분석자료는",
    r"무단\s*전재",
    r"무단으로\s*인용",
    r"시장경보제도",
]

FINANCIAL_METRIC_PATTERN = re.compile(
    r"("
    r"매출|매출액|영업수익|순영업수익|매출총이익|영업이익|영업손익|세전이익|"
    r"당기순이익|순이익|지배주주|EBITDA(?![A-Za-z])|EPS(?![A-Za-z])|"
    r"BPS(?![A-Za-z])|DPS(?![A-Za-z])|PER(?![A-Za-z])|PBR(?![A-Za-z])|"
    r"ROE(?![A-Za-z])|ROA(?![A-Za-z])|EV/EBITDA|영업이익률|순이익률|"
    r"부채비율|순차입금|순차입금비율|현금흐름|영업활동|투자활동|재무활동|"
    r"CAPEX|FCF|자산총계|부채총계|자본총계|현금의증가|기초현금|기말현금|"
    r"이자보상배율|Revenue|Sales|Operating Profit|Net Profit|Margin"
    r")",
    re.IGNORECASE,
)
NUMERIC_VALUE_PATTERN = re.compile(
    r"[-+]?\d[\d,]*(?:\.\d+)?\s*(?:%|원|억원|십억원|조원|배|x|X)?|N/A|n/a|적자|흑자"
)


class LLMParseError(ValueError):
    pass


def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def bedrock_client():
    return boto3.client("bedrock", region_name=BEDROCK_REGION)


def parse_date(value, label="date"):
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"{label} 형식이 잘못됐습니다. YYYY-MM-DD로 입력하세요: {value}") from e


def prompt_date(label):
    while True:
        value = input(f"{label} (YYYY-MM-DD): ").strip()
        try:
            return parse_date(value, label)
        except ValueError as e:
            print(e)


# NOTE: In Airflow/batch mode, always pass --start-date and --end-date to avoid prompt_date() blocking.
def resolve_date_range(args):
    start = parse_date(args.start_date, "start-date") if args.start_date else prompt_date("start date")
    end = parse_date(args.end_date, "end-date") if args.end_date else prompt_date("end date")
    if start > end:
        raise ValueError(f"start-date가 end-date보다 늦습니다: {start} > {end}")
    return start, end


def s3_uri(key):
    return f"s3://{S3_BUCKET}/{key}"


def load_json_from_s3(s3, key):
    body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return json.loads(body)


def put_json_to_s3(s3, key, payload):
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )


def put_text_to_s3(s3, key, text, content_type="application/jsonl; charset=utf-8"):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType=content_type,
    )


def object_exists(s3, key):
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=key, MaxKeys=1)
    return any(obj["Key"] == key for obj in response.get("Contents", []))


def parse_raw_silver_key(key):
    parts = key.split("/")
    if len(parts) != 4:
        return None
    _, broker, date_part, filename = parts
    if broker == "embedding_input" or not filename.endswith(".json"):
        return None
    try:
        published_at = parse_date(date_part, "silver date")
    except ValueError:
        return None
    return broker, published_at


def list_raw_silver_keys(s3, start_date, end_date):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=RAW_SILVER_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parsed = parse_raw_silver_key(key)
            if not parsed:
                continue
            _, published_at = parsed
            if start_date <= published_at <= end_date:
                keys.append(key)
    return sorted(keys)


def normalize_silver(doc, key):
    parts = key.split("/")
    broker = parts[1] if len(parts) > 1 else doc.get("증권사", "")
    date_from_key = parts[2] if len(parts) > 2 else ""
    report_id = doc.get("report_id") or Path(key).stem
    text = doc.get("text") or doc.get("본문") or ""
    title = doc.get("title") or doc.get("제목") or ""
    if not title and text:
        title = next((line.strip() for line in text.splitlines() if line.strip()), "")[:200]
    published_at = (doc.get("발행일") or doc.get("date") or date_from_key or "").replace(".", "-")
    return {
        "key": key,
        "report_id": str(report_id),
        "증권사": doc.get("증권사") or broker,
        "종목명": doc.get("종목명") or "",
        "발행일": published_at,
        "title": title,
        "text": text,
        "text_len": int(doc.get("text_len") or len(text)),
    }


def extract_stock_codes(title, text):
    source = f"{title}\n{text}"
    patterns = [
        r"\((\d{6})\)",
        r"\b(\d{6})\s*(?:기업분석|종목분석)\b",
        r"\b(\d{6})\s*/\s*(?:KOSPI|KOSDAQ|KONEX|KS|KQ)\b",
        r"\bKONEX\s*[:：]\s*(\d{6})\b",
        r"\b(\d{6})\s*(?:\.KS|\.KQ|KS|KQ)\b",
        r"(?m)^\s*(\d{6})\s*$",
        r"[가-힣A-Za-z0-9&().\-\s]{1,30}\s+(\d{6})\b",
        r"\[(\d{6})\]",
    ]
    codes = []
    for pat in patterns:
        for match in re.finditer(pat, source, flags=re.IGNORECASE):
            code = match.group(1)
            if 1900 <= int(code) <= 2099:
                continue
            if code not in codes:
                codes.append(code)
    return codes


def normalize_report_text(text):
    if not text:
        return ""
    normalized = text.replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ")
    if "\\n" in normalized and normalized.count("\n") < normalized.count("\\n"):
        normalized = normalized.replace("\\n", "\n")
    normalized = normalized.replace("\\t", " ")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def strip_boilerplate_sections(text):
    cut_points = []
    for marker in BOILERPLATE_MARKERS:
        match = re.search(marker, text, flags=re.IGNORECASE)
        if match:
            cut_points.append(match.start())
    return text[:min(cut_points)].strip() if cut_points else text


def is_page_noise(line):
    stripped = line.strip()
    if not stripped:
        return True
    return bool(re.fullmatch(r"\d{1,2}\s*/\s*\d{1,2}", stripped))


def is_year_token(line):
    return bool(re.fullmatch(r"(?:19|20)\d{2}[EFP]?", line.strip()))


def is_numeric_value_line(line):
    stripped = line.strip()
    if not stripped or len(stripped) > 40:
        return False
    return bool(NUMERIC_VALUE_PATTERN.fullmatch(stripped))


def is_financial_metric_line(line):
    stripped = line.strip()
    if not stripped or len(stripped) > 60:
        return False
    if re.match(r"^[-+]?\d|^(억원|십억원|조원|원)\b", stripped):
        return False
    if ":" not in stripped and "：" not in stripped and len(stripped) > 35:
        return False
    if re.search(r"(예상|전망|성장|개선|악화|축소|확대|감소|증가|기대|판단|때문|것으로|것이다|했다|한다)", stripped):
        return False
    return bool(FINANCIAL_METRIC_PATTERN.search(stripped))


def prepare_report_lines(text):
    cleaned = strip_boilerplate_sections(normalize_report_text(text))
    return [ln.strip() for ln in cleaned.splitlines() if not is_page_noise(ln)]


def extract_narrative_context(lines, max_chars):
    out = [ln for ln in lines if not is_numeric_value_line(ln)]
    return "\n".join(out)[:max_chars].strip()


def extract_table_signal(lines, max_chars):
    rows = []
    seen = set()
    running_len = 0
    for idx, line in enumerate(lines):
        if not is_financial_metric_line(line):
            continue
        values = [
            value.strip()
            for value in NUMERIC_VALUE_PATTERN.findall(line)
            if value.strip() and not is_year_token(value)
        ][:6]
        for next_line in lines[idx + 1: idx + 10]:
            if is_financial_metric_line(next_line) and values:
                break
            if values and re.match(r"^\([^\d]", next_line):
                break
            if is_numeric_value_line(next_line) and not is_year_token(next_line):
                values.append(next_line)
            if len(values) >= 6:
                break
        if len(values) < 2:
            continue
        metric = re.sub(r"\s+", " ", line).strip(" :：")
        row = f"{metric}: {', '.join(values)}"
        row_key = row.lower()
        if row_key in seen:
            continue
        add_len = len(row) + (1 if rows else 0)
        if running_len + add_len > max_chars:
            break
        seen.add(row_key)
        rows.append(row)
        running_len += add_len
    return "\n".join(rows).strip()


def build_llm_prompt(row, max_chars):
    text = row["text"][:max_chars]
    return f"""
너는 국내 증권사 리포트를 구조화하는 금융 데이터 추출기다.
아래 리포트에서 명시적으로 근거가 있는 내용만 추출한다.
애매하거나 본문에 없는 내용은 null 또는 빈 배열로 둔다.
반드시 JSON만 출력한다.
앞뒤 설명문, 마크다운, 코드블록을 절대 붙이지 않는다.
reason은 2문장 이내, risks는 최대 5개, keywords는 최대 8개로 간결하게 작성한다.

출력 스키마:
{{
  "reason": "핵심 투자 논리 1~2문장 또는 null",
  "risks": ["리스크 요인"],
  "keywords": ["핵심 키워드"]
}}

제목: {row["title"]}
종목명: {row["종목명"]}
종목코드 후보: {row.get("종목코드", "")}
증권사: {row["증권사"]}
발행일: {row["발행일"]}

본문:
{text}
""".strip()


def build_embedding_text(row, reason, risks, keywords, max_chars, table_max_chars):
    lines = prepare_report_lines(row["text"])
    narrative = extract_narrative_context(lines, max_chars)
    table_signal = extract_table_signal(lines, table_max_chars)
    return f"""
제목: {row["title"]}
종목코드: {row.get("종목코드", "")}
핵심논리: {reason or ""}
리스크: {", ".join(risks or [])}
키워드: {", ".join(keywords or [])}
본문서술: {narrative}
재무표_수치표_압축: {table_signal}
""".strip()


def parse_json_string_field(text, field):
    null_match = re.search(rf'"{re.escape(field)}"\s*:\s*null', text)
    if null_match:
        return None

    match = re.search(rf'"{re.escape(field)}"\s*:\s*"', text)
    if not match:
        return None

    idx = match.end()
    raw_chars = []
    escaped = False
    closed = False
    while idx < len(text):
        ch = text[idx]
        if escaped:
            raw_chars.append(ch)
            escaped = False
        elif ch == "\\":
            raw_chars.append(ch)
            escaped = True
        elif ch == '"':
            closed = True
            break
        else:
            raw_chars.append(ch)
        idx += 1

    raw = "".join(raw_chars)
    if escaped and raw.endswith("\\"):
        raw = raw[:-1]

    try:
        return json.loads(f'"{raw}"')
    except json.JSONDecodeError:
        # Batch output can be truncated in the middle of a string when maxTokens is too low.
        # In that case, keep the readable prefix instead of marking the whole record failed.
        return raw.strip() or None


def parse_json_array_field(text, field):
    match = re.search(rf'"{re.escape(field)}"\s*:\s*\[', text)
    if not match:
        return []
    idx = match.end()
    decoder = json.JSONDecoder()
    values = []
    while idx < len(text):
        while idx < len(text) and text[idx] in " \t\r\n,":
            idx += 1
        if idx >= len(text) or text[idx] == "]":
            break
        if text[idx] != '"':
            idx += 1
            continue
        try:
            value, next_idx = decoder.raw_decode(text[idx:])
        except json.JSONDecodeError:
            break
        if isinstance(value, str) and value:
            values.append(value)
        idx += next_idx
    return values


def parse_lenient_llm_json_text(text):
    reason = parse_json_string_field(text, "reason")
    risks = parse_json_array_field(text, "risks")
    keywords = parse_json_array_field(text, "keywords")
    if reason is None and not risks and not keywords:
        raise LLMParseError(f"JSON 필드를 복구하지 못했습니다: {text[:500]}")
    return {
        "reason": reason,
        "risks": [str(item) for item in risks if item],
        "keywords": [str(item) for item in keywords if item],
    }


def parse_llm_json_text(text):
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1:
            raise LLMParseError(f"JSON 객체를 찾지 못했습니다: {text[:300]}")
        candidate = text[start:end + 1] if end > start else text[start:]
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            return parse_lenient_llm_json_text(candidate)
    risks = parsed.get("risks") or []
    keywords = parsed.get("keywords") or []
    if isinstance(risks, str):
        risks = [risks]
    if isinstance(keywords, str):
        keywords = [keywords]
    return {
        "reason": parsed.get("reason"),
        "risks": [str(item) for item in risks if item],
        "keywords": [str(item) for item in keywords if item],
    }


def embedding_input_key(row):
    return f"{EMBEDDING_INPUT_PREFIX}{row['증권사']}/{row['발행일']}/{row['report_id']}.json"


def build_embedding_input_payload(row, extracted, embedding_text, llm_model, llm_status="ok", llm_error=None):
    return {
        "report_id": row["report_id"],
        "source": row["증권사"],
        "증권사": row["증권사"],
        "종목명": row["종목명"],
        "종목코드": row.get("종목코드") or None,
        "발행일": row["발행일"],
        "title": row["title"],
        "reason": extracted["reason"],
        "risks": extracted["risks"],
        "keywords": extracted["keywords"],
        "llm_status": llm_status,
        "llm_error": llm_error,
        "embedding_text": embedding_text,
        "source_s3_key": row["key"],
        "llm_model": llm_model,
        "embedding_text_strategy": {
            "narrative_max_chars": NARRATIVE_MAX_CHARS_DEFAULT,
            "table_max_chars": TABLE_MAX_CHARS_DEFAULT,
            "boilerplate_removed": T