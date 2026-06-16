"""
silver/embedding_input JSON -> gold/embeddings Parquet 생성.

입력:
  s3://{S3_BUCKET}/silver/embedding_input/{증권사}/{YYYY-MM-DD}/{report_id}.json

출력:
  s3://{S3_BUCKET}/gold/embeddings/year={YYYY}/month={MM}/data.parquet

실행:
  python3 embedding.py
  python3 embedding.py --start-date 2021-01-01 --end-date 2021-01-31
  python3 embedding.py --start-date 2021-01-01 --end-date 2021-01-31 --dry-run
"""

import argparse
import json
import re
import tempfile
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sentence_transformers import SentenceTransformer

from common import (
    AWS_REGION,
    EMBEDDING_INPUT_PREFIX,
    S3_BUCKET,
    load_json_from_s3,
    object_exists,
    parse_date,
    prompt_date,
    resolve_date_range,
    s3_client,
    s3_uri,
)


GOLD_EMBEDDING_PREFIX = "gold/embeddings/"
DEFAULT_EMBEDDING_MODEL = "intfloat/multilingual-e5-small"

CONTROL_CHAR_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
ESCAPED_CONTROL_PATTERN = re.compile(
    r"\\(?:[rntbf]|x[0-9a-fA-F]{2}|u00[0-9a-fA-F]{2})"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="silver/embedding_input JSON -> gold/embeddings Parquet"
    )
    parser.add_argument("--start-date", help="시작일 YYYY-MM-DD")
    parser.add_argument("--end-date", help="종료일 YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=0, help="앞에서 N건만 처리. 0이면 전체")
    parser.add_argument("--dry-run", action="store_true", help="임베딩/S3 업로드 없이 대상만 확인")
    parser.add_argument("--overwrite", action="store_true", help="기존 월별 Parquet 덮어쓰기")
    parser.add_argument("--output-prefix", default=GOLD_EMBEDDING_PREFIX, help="Gold S3 prefix")
    parser.add_argument(
        "--embedding-model",
        default=DEFAULT_EMBEDDING_MODEL,
        help="sentence-transformers 모델명",
    )
    parser.add_argument("--batch-size", type=int, default=16, help="임베딩 batch size")
    parser.add_argument(
        "--schema",
        choices=["minimal", "extended"],
        default="minimal",
        help="minimal은 조인용 최소 스키마, extended는 메타 컬럼 포함",
    )
    parser.add_argument(
        "--include-failed-llm",
        action="store_true",
        help="llm_status=failed인 embedding_input도 임베딩에 포함",
    )
    return parser.parse_args()


def parse_embedding_input_key(key):
    parts = key.split("/")
    if len(parts) != 5:
        return None
    layer, input_dir, broker, date_part, filename = parts
    if layer != "silver" or input_dir != "embedding_input" or not filename.endswith(".json"):
        return None
    try:
        published_at = parse_date(date_part, "embedding_input date")
    except ValueError:
        return None
    return broker, published_at, filename[:-5]


def list_embedding_input_keys(s3, start_date, end_date):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=EMBEDDING_INPUT_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parsed = parse_embedding_input_key(key)
            if not parsed:
                continue
            _, published_at, _ = parsed
            if start_date <= published_at <= end_date:
                keys.append(key)
    return sorted(keys)


def gold_key_for_month(output_prefix, yyyymm):
    year, month = yyyymm.split("-")
    prefix = output_prefix.strip("/")
    return f"{prefix}/year={year}/month={month}/data.parquet"


def month_key(date_value):
    dt = parse_date(str(date_value), "발행일")
    return f"{dt.year:04d}-{dt.month:02d}"


def clean_embedding_text(value):
    """Remove raw control characters and literal escape artifacts before embedding."""
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ")
    text = text.replace("\\n", "\n").replace("\\r", "\n").replace("\\t", " ")
    text = ESCAPED_CONTROL_PATTERN.sub(" ", text)
    text = CONTROL_CHAR_PATTERN.sub(" ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def build_final_embedding_input(doc):
    prefix = f"""
종목명: {doc.get("종목명") or ""}
증권사: {doc.get("증권사") or doc.get("source") or ""}
발행일: {doc.get("발행일") or ""}
""".strip()
    return clean_embedding_text(f"{prefix}\n{doc.get('embedding_text') or ''}")


def normalize_doc(doc, key, include_failed_llm):
    if not include_failed_llm and doc.get("llm_status") == "failed":
        return None

    embedding_input = build_final_embedding_input(doc)
    if not embedding_input:
        return None

    return {
        "report_id": str(doc.get("report_id") or Path(key).stem),
        "종목코드": doc.get("종목코드") or None,
        "종목명": doc.get("종목명") or None,
        "증권사": doc.get("증권사") or doc.get("source") or None,
        "발행일": doc.get("발행일") or "",
        "reason": doc.get("reason"),
        "risks": [str(item) for item in (doc.get("risks") or []) if item],
        "keywords": [str(item) for item in (doc.get("keywords") or []) if item],
        "llm_status": doc.get("llm_status") or "unknown",
        "source_s3_key": key,
        "_embedding_input": embedding_input,
        "_embedding_input_len": len(embedding_input),
        "_embedding_model": None,
        "_embedding_dim": None,
    }


def load_rows(s3, keys, include_failed_llm):
    rows = []
    skipped = Counter()
    for idx, key in enumerate(keys, 1):
        try:
            doc = load_json_from_s3(s3, key)
        except Exception as e:
            skipped["s3_json_error"] += 1
            if idx <= 5 or skipped["s3_json_error"] <= 3:
                print(f"  [WARN] S3/JSON error at {key}: {e}")
            continue
        row = normalize_doc(doc, key, include_failed_llm)
        if row is None:
            if doc.get("llm_status") == "failed":
                skipped["llm_failed"] += 1
            else:
                skipped["empty_embedding_text"] += 1
            continue
        rows.append(row)
        if idx % 1000 == 0:
            print(f"  로드: {idx:,}/{len(keys):,} (rows {len(rows):,}, skip {sum(skipped.values()):,})")
    return rows, skipped


def print_summary(rows, skipped, start_date, end_date, args):
    brokers = Counter(row["증권사"] or "" for row in rows)
    months = Counter(month_key(row["발행일"]) for row in rows if row["발행일"])
    empty_stock_code = sum(1 for row in rows if not row["종목코드"])
    print("\n[embedding 대상 요약]")
    print(f"기간: {start_date} ~ {end_date}")
    print(f"대상 건수: {len(rows):,}")
    print(f"skip: {dict(skipped)}")
    print(f"월별 분포: {dict(sorted(months.items()))}")
    print(f"증권사 분포: {dict(brokers)}")
    print(f"종목코드 빈 값: {empty_stock_code:,}")
    print(f"임베딩 모델: {args.embedding_model}")
    print(f"batch size: {args.batch_size}")
    print(f"schema: {args.schema}")
    print(f"출력 prefix: s3://{S3_BUCKET}/{args.output_prefix.strip('/')}/")


def encode_rows(rows, model_name, batch_size):
    print(f"\n임베딩 모델 로드: {model_name}")
    embedder = SentenceTransformer(model_name)
    texts = [row["_embedding_input"] for row in rows]
    embeddings = embedder.encode(
        texts,
        batch_size=batch_size,
        normalize_embeddings=True,
        show_progress_bar=True,
    )
    for row, embedding in zip(rows, embeddings):
        vector = np.asarray(embedding, dtype=np.float32)
        row["embedding"] = vector.tolist()
        row["_embedding_model"] = model_name
        row["_embedding_dim"] = int(vector.shape[0])


def output_row(row, schema):
    base = {
        "report_id": row["report_id"],
        "종목코드": row["종목코드"],
        "reason": row["reason"],
        "risks": row["risks"],
        "keywords": row["keywords"],
        "embedding": row["embedding"],
    }
    if schema == "extended":
        base.update({
            "종목명": row["종목명"],
            "증권사": row["증권사"],
            "발행일": row["발행일"],
            "llm_status": row["llm_status"],
            "embedding_model": row["_embedding_model"],
            "embedding_dim": row["_embedding_dim"],
            "embedding_input_len": row["_embedding_input_len"],
            "source_s3_key": row["source_s3_key"],
        })
    return base


def parquet_schema(schema):
    fields = [
        pa.field("report_id", pa.string()),
        pa.field("종목코드", pa.string()),
        pa.field("reason", pa.string()),
        pa.field("risks", pa.list_(pa.string())),
        pa.field("keywords", pa.list_(pa.string())),
        pa.field("embedding", pa.list_(pa.float32())),
    ]
    if schema == "extended":
        fields.extend([
            pa.field("종목명", pa.string()),
            pa.field("증권사", pa.string()),
            pa.field("발행일", pa.string()),
            pa.field("llm_status", pa.string()),
            pa.field("embedding_model", pa.string()),
            pa.field("embedding_dim", pa.int32()),
            pa.field("embedding_input_len", pa.int32()),
            pa.field("source_s3_key", pa.string()),
        ])
    return pa.schema(fields)


def write_parquet(rows, path, schema):
    table = pa.Table.from_pylist(rows, schema=parquet_schema(schema))
    pq.write_table(table, path, compression="snappy")


def main():
    args = parse_args()
    start_date, end_date = resolve_date_range(args)
    s3 = s3_client()

    keys = list_embedding_input_keys(s3, start_date, end_date)
    if args.limit:
        keys = keys[:args.limit]

    rows, skipped = load_rows(s3, keys, args.include_failed_llm)
    print_summary(rows, skipped, start_date, end_date, args)

    if args.dry_run:
        print("dry-run이므로 임베딩/S3 업로드는 수행하지 않았습니다.")
        return
    if not rows:
        print("대상 embedding_input이 없습니다.")
        return

    rows_by_month = defaultdict(list)
    for row in rows:
        rows_by_month[month_key(row["발행일"])].append(row)

    for yyyymm in sorted(rows_by_month):
        out_key = gold_key_for_month(args.output_prefix, yyyymm)
        if not args.overwrite and object_exists(s3, out_key):
            raise RuntimeError(f"이미 존재합니다. 덮어쓰려면 --overwrite 사용: {s3_uri(out_key)}")

    encode_rows(rows, args.embedding_model, args.batch_size)

    for yyyymm, month_rows in sorted(rows_by_month.items()):
        print(f"\n[{yyyymm}] Parquet 생성: {len(month_rows):,}건")
        out_rows = [output_row(row, args.schema) for row in month_rows]
        local_tmp = tempfile.NamedTemporaryFile(
            suffix=f"gold_embeddings_{yyyymm}.parquet", delete=False
        )
        local_path = Path(local_tmp.name)
        local_tmp.close()  # close so write_parquet can write to it
        write_parquet(out_rows, local_path, args.schema)

        out_key = gold_key_for_month(args.output_prefix, yyyymm)
        s3.upload_file(str(local_path), S3_BUCKET, out_key)
       