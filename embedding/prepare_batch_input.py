"""
Bedrock Batch Inference 입력 JSONL 생성.

원본:
  s3://{bucket}/silver/{증권사}/{YYYY-MM-DD}/{report_id}.json

생성:
  s3://{bucket}/batch/embedding_input/jobs/{job_name}/input/input.jsonl
  s3://{bucket}/batch/embedding_input/jobs/{job_name}/manifest.json

실행:
  python prepare_batch_input.py --start-date 2026-01-01 --end-date 2026-06-11
  python prepare_batch_input.py
"""

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime

from common import (
    BATCH_PREFIX,
    DEFAULT_LLM_MODEL_ID,
    BEDROCK_BATCH_ROLE_ARN,
    S3_BUCKET,
    build_llm_prompt,
    embedding_input_key,
    extract_stock_codes,
    list_raw_silver_keys,
    load_json_from_s3,
    normalize_silver,
    object_exists,
    put_json_to_s3,
    put_text_to_s3,
    resolve_date_range,
    s3_client,
    s3_uri,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Prepare Bedrock Batch Inference JSONL")
    parser.add_argument("--start-date", help="시작일 YYYY-MM-DD")
    parser.add_argument("--end-date", help="종료일 YYYY-MM-DD")
    parser.add_argument("--job-name", help="Batch job 이름. 생략 시 날짜 기반 자동 생성")
    parser.add_argument("--limit", type=int, default=0, help="앞에서 N건만 준비. 0이면 전체")
    parser.add_argument("--llm-model", default=DEFAULT_LLM_MODEL_ID)
    parser.add_argument("--llm-max-chars", type=int, default=10000)
    parser.add_argument("--max-tokens", type=int, default=800, help="Batch LLM 출력 토큰 상한")
    parser.add_argument("--include-existing", action="store_true", help="이미 embedding_input이 있어도 batch input에 포함")
    parser.add_argument("--dry-run", action="store_true", help="S3 쓰기 없이 대상만 확인")
    return parser.parse_args()


def make_job_name(start_date, end_date):
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"embedding-input-{start_date}-{end_date}-{now}"


def batch_input_record(row, args):
    prompt = build_llm_prompt(row, args.llm_max_chars)
    return {
        "recordId": row["report_id"],
        "modelInput": {
            "messages": [
                {"role": "user", "content": [{"text": prompt}]}
            ],
            "inferenceConfig": {
                "maxTokens": args.max_tokens,
                "temperature": 0,
            },
        },
    }


def main():
    args = parse_args()
    start_date, end_date = resolve_date_range(args)
    job_name = args.job_name or make_job_name(start_date, end_date)
    base_key = f"{BATCH_PREFIX}{job_name}/"
    input_key = f"{base_key}input/input.jsonl"
    manifest_key = f"{base_key}manifest.json"

    s3 = s3_client()
    keys = list_raw_silver_keys(s3, start_date, end_date)
    if args.limit:
        keys = keys[:args.limit]

    records = []
    manifest = {
        "job_name": job_name,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "llm_model": args.llm_model,
        "input_s3_key": input_key,
        "records": {},
    }
    brokers = Counter()
    missing_by_broker = defaultdict(int)
    skipped_existing = 0

    for idx, key in enumerate(keys, 1):
        doc = load_json_from_s3(s3, key)
        row = normalize_silver(doc, key)
        codes = extract_stock_codes(row["title"], row["text"])
        row["종목코드"] = codes[0] if codes else ""
        if not row["종목코드"]:
            missing_by_broker[row["증권사"]] += 1

        out_key = embedding_input_key(row)
        if not args.include_existing and object_exists(s3, out_key):
            skipped_existing += 1
            continue

        brokers[row["증권사"]] += 1
        records.append(batch_input_record(row, args))
        manifest["records"][row["report_id"]] = {
            "source_s3_key": key,
            "embedding_input_s3_key": out_key,
            "report_id": row["report_id"],
            "증권사": row["증권사"],
            "종목명": row["종목명"],
            "종목코드": row["종목코드"],
            "발행일": row["발행일"],
            "title": row["title"],
            "text_len": row["text_len"],
        }
        if idx % 1000 == 0:
            print(f"  준비: {idx:,}/{len(keys):,}")

    print("\n[Batch input 준비 요약]")
    print(f"기간: {start_date} ~ {end_date}")
    print(f"job_name: {job_name}")
    print(f"원본 후보: {len(keys):,}건")
    print(f"이미 embedding_input 존재로 skip: {skipped_existing:,}건")
    print(f"batch 대상: {len(records):,}건")
    print(f"증권사 분포: {dict(brokers)}")
    print(f"종목코드 실패 증권사별: {dict(missing_by_broker)}")
    print(f"input: {s3_uri(input_key)}")
    print(f"manifest: {s3_uri(manifest_key)}")

    if args.dry_run:
        print("dry-run이므로 S3에 쓰지 않았습니다.")
        return
    if not records:
        print("batch 대상이 없습니다.")
        return

    jsonl = "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n"
    put_text_to_s3(s3, input_key, jsonl)
    put_json_to_s3(s3, manifest_key, manifest)
    print("\n완료")
    if BEDROCK_BATCH_ROLE_ARN:
        print(f"다음 단계: python submit_batch_job.py --job-name {job_name}")
    else:
        print(f"다음 단계: python submit_batch_job.py --job-name {job_name} --role-arn <BedrockBatchRoleArn>")


if __name__ == "__main__":
    main()
