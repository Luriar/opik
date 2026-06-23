"""
Bedrock Batch Inference output을 silver/embedding_input JSON으로 변환.

실행:
  python collect_batch_output.py --job-name embedding-input-...
"""

import argparse
import json
import time

from common import (
    BATCH_PREFIX,
    DEFAULT_LLM_MODEL_ID,
    NARRATIVE_MAX_CHARS_DEFAULT,
    S3_BUCKET,
    TABLE_MAX_CHARS_DEFAULT,
    build_embedding_input_payload,
    build_embedding_text,
    embedding_input_key,
    load_json_from_s3,
    normalize_silver,
    object_exists,
    parse_llm_json_text,
    put_json_to_s3,
    s3_client,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Collect Bedrock Batch output into silver/embedding_input")
    parser.add_argument("--job-name", required=True)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--overwrite-failed", action="store_true", help="기존 llm_status=failed JSON만 재파싱 결과로 덮어쓰기")
    parser.add_argument("--embed-max-chars", type=int, default=NARRATIVE_MAX_CHARS_DEFAULT)
    parser.add_argument("--table-max-chars", type=int, default=TABLE_MAX_CHARS_DEFAULT)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def list_output_keys(s3, job_name):
    prefix = f"{BATCH_PREFIX}{job_name}/output/"
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("manifest.json.out"):
                continue
            if key.endswith(".jsonl") or key.endswith(".out") or key.endswith(".jsonl.out"):
                keys.append(key)
    return sorted(keys)


def iter_jsonl_from_s3(s3, key):
    body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode("utf-8")
    buffer = ""
    start_line_no = None

    for line_no, line in enumerate(body.splitlines(), 1):
        if not line.strip() and not buffer:
            continue

        if not buffer:
            start_line_no = line_no
            buffer = line
        else:
            # Some Batch outputs contain literal line breaks inside model text.
            # Join continuation lines without adding a raw newline so the JSON string can recover.
            buffer += line

        try:
            yield start_line_no, json.loads(buffer)
            buffer = ""
            start_line_no = None
        except json.JSONDecodeError:
            continue

    if buffer.strip():
        raise json.JSONDecodeError(
            f"JSONL record did not complete from line {start_line_no}",
            buffer,
            0,
        )


def extract_text_from_model_output(record):
    if record.get("error"):
        raise ValueError(str(record["error"]))
    output = record.get("modelOutput") or record.get("output") or {}

    # Converse output shape
    try:
        return output["output"]["message"]["content"][0]["text"]
    except (KeyError, IndexError, TypeError):
        pass

    # Some batch examples wrap message directly.
    try:
        return output["message"]["content"][0]["text"]
    except (KeyError, IndexError, TypeError):
        pass

    # Anthropic invoke_model style fallback.
    try:
        return output["content"][0]["text"]
    except (KeyError, IndexError, TypeError):
        pass

    raise ValueError(f"modelOutput에서 text를 찾지 못했습니다: {str(output)[:500]}")


def main():
    args = parse_args()
    s3 = s3_client()
    manifest_key = f"{BATCH_PREFIX}{args.job_name}/manifest.json"
    manifest = load_json_from_s3(s3, manifest_key)
    records_meta = manifest.get("records", {})
    llm_model = manifest.get("llm_model") or DEFAULT_LLM_MODEL_ID

    output_keys = list_output_keys(s3, args.job_name)
    # S3 eventual consistency: listing may miss recently written outputs
    if not output_keys:
        print("  output files not found yet, waiting 2s for S3 eventual consistency...")
        time.sleep(2)
        output_keys = list_output_keys(s3, args.job_name)
    print("\n[Batch output 수집]")
    print(f"job_name: {args.job_name}")
    print(f"manifest: s3://{S3_BUCKET}/{manifest_key}")
    print(f"output files: {len(output_keys):,}")
    for key in output_keys:
        print(f"  - s3://{S3_BUCKET}/{key}")

    if not output_keys:
        raise SystemExit("output JSONL 파일이 없습니다. Batch job 완료 여부를 먼저 확인하세요.")

    written = 0
    skipped = 0
    failed = 0
    missing_meta = 0
    overwritten_failed = 0

    for output_key in output_keys:
        for _, record in iter_jsonl_from_s3(s3, output_key):
            record_id = record.get("recordId")
            meta = records_meta.get(record_id)
            if not meta:
                missing_meta += 1
                continue

            source_doc = load_json_from_s3(s3, meta["source_s3_key"])
            row = normalize_silver(source_doc, meta["source_s3_key"])
            row["종목코드"] = meta.get("종목코드") or ""

            out_key = meta.get("embedding_input_s3_key") or embedding_input_key(row)
            if not args.overwrite and object_exists(s3, out_key):
                if args.overwrite_failed:
                    existing = load_json_from_s3(s3, out_key)
                    if existing.get("llm_status") != "failed":
                        skipped += 1
                        continue
                    overwritten_failed += 1
                else:
                    skipped += 1
                    continue

            llm_status = "ok"
            llm_error = None
            try:
                text = extract_text_from_model_output(record)
                extracted = parse_llm_json_text(text)
            except Exception as e:
                failed += 1
                llm_status = "failed"
                llm_error = str(e)[:1000]
                extracted = {"reason": None, "risks": [], "keywords": []}

            embedding_text = build_embedding_text(
                row,
                extracted["reason"],
                extracted["risks"],
                extracted["keywords"],
                args.embed_max_chars,
                args.table_max_chars,
            )
            payload = build_embedding_input_payload(
                row,
                extracted,
                embedding_text,
                llm_model,
                llm_status=llm_status,
                llm_error=llm_error,
            )
            if not args.dry_run:
                put_json_to_s3(s3, out_key, payload)
            written += 1
            if written % 500 == 0:
                print(f"  저장: {written:,} (skip {skipped:,}, failed {failed: