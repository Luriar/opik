"""
Bedrock Batch Inference job 제출.

사전 조건:
  1. prepare_batch_input.py로 input.jsonl/manifest.json 생성
  2. Bedrock Batch service role ARN 확보

실행:
  python submit_batch_job.py --job-name embedding-input-... --role-arn arn:aws:iam::...:role/...
"""

import argparse
import json
from datetime import datetime, timezone

from common import (
    BATCH_PREFIX,
    BEDROCK_BATCH_ROLE_ARN,
    BEDROCK_REGION,
    DEFAULT_LLM_MODEL_ID,
    S3_BUCKET,
    bedrock_client,
    put_json_to_s3,
    s3_client,
    s3_uri,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Submit Bedrock Batch Inference job")
    parser.add_argument("--job-name", required=True)
    parser.add_argument("--role-arn", default=BEDROCK_BATCH_ROLE_ARN)
    parser.add_argument("--llm-model", default=DEFAULT_LLM_MODEL_ID)
    parser.add_argument("--timeout-hours", type=int, default=24)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.role_arn:
        raise SystemExit("role ARN이 필요합니다. --role-arn 또는 BEDROCK_BATCH_ROLE_ARN을 설정하세요.")

    base_key = f"{BATCH_PREFIX}{args.job_name}/"
    input_uri = s3_uri(f"{base_key}input/")
    output_uri = s3_uri(f"{base_key}output/")
    metadata_key = f"{base_key}job.json"

    request = {
        "jobName": args.job_name,
        "roleArn": args.role_arn,
        "modelId": args.llm_model,
        "inputDataConfig": {
            "s3InputDataConfig": {
                "s3Uri": input_uri,
                "s3InputFormat": "JSONL",
            }
        },
        "outputDataConfig": {
            "s3OutputDataConfig": {
                "s3Uri": output_uri,
            }
        },
        "timeoutDurationInHours": args.timeout_hours,
        "modelInvocationType": "Converse",
    }

    print("\n[Batch job 제출 요청]")
    print(json.dumps({
        "region": BEDROCK_REGION,
        "jobName": args.job_name,
        "modelId": args.llm_model,
        "inputUri": input_uri,
        "outputUri": output_uri,
        "roleArn": args.role_arn,
        "modelInvocationType": "Converse",
    }, ensure_ascii=False, indent=2))

    if args.dry_run:
        print("dry-run이므로 job을 생성하지 않았습니다.")
        return

    bedrock = bedrock_client()
    response = bedrock.create_model_invocation_job(**request)
    job_arn = response["jobArn"]

    metadata = {
        "job_name": args.job_name,
        "job_arn": job_arn,
        "model_id": args.llm_model,
        "role_arn": args.role_arn,
        "input_uri": input_uri,
        "output_uri": output_uri,
        "request": request,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    put_json_to_s3(s3_client(), metadata_key, metadata)

    print("\n제출 완료")
    print(f"jobArn: {job_arn}")
    print(f"metadata: s3://{S3_BUCKET}/{metadata_key}")
    print(f"상태 확인: python status_batch_job.py --job-arn {job_arn}")


if __name__ == "__main__":
    main()
