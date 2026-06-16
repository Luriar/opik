"""
Bedrock Batch Inference job 상태 확인.

실행:
  python status_batch_job.py --job-arn arn:aws:bedrock:...
  python status_batch_job.py --job-name embedding-input-...
"""

import argparse
import json

from botocore.exceptions import ClientError

from common import BATCH_PREFIX, S3_BUCKET, bedrock_client, load_json_from_s3, s3_client


def parse_args():
    parser = argparse.ArgumentParser(description="Check Bedrock Batch Inference job status")
    parser.add_argument("--job-arn")
    parser.add_argument("--job-name")
    return parser.parse_args()


def resolve_job_identifier(args):
    if args.job_arn:
        return args.job_arn
    if not args.job_name:
        raise SystemExit("--job-arn 또는 --job-name 중 하나가 필요합니다.")
    key = f"{BATCH_PREFIX}{args.job_name}/job.json"
    try:
        metadata = load_json_from_s3(s3_client(), key)
        return metadata["job_arn"]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"NoSuchKey", "404"}:
            raise

    response = bedrock_client().list_model_invocation_jobs(
        nameContains=args.job_name,
        maxResults=10,
        sortBy="CreationTime",
        sortOrder="Descending",
    )
    matches = [
        job for job in response.get("invocationJobSummaries", [])
        if job.get("jobName") == args.job_name
    ]
    if not matches:
        raise SystemExit(
            f"job metadata와 Bedrock job을 찾지 못했습니다: {args.job_name}\n"
            f"먼저 submit_batch_job.py --job-name {args.job_name} 을 실행했는지 확인하세요."
        )
    return matches[0]["jobArn"]


def main():
    args = parse_args()
    job_identifier = resolve_job_identifier(args)
    response = bedrock_client().get_model_invocation_job(jobIdentifier=job_identifier)
    printable = {
        "jobArn": response.get("jobArn"),
        "jobName": response.get("jobName"),
        "status": response.get("status"),
        "message": response.get("message"),
        "modelId": response.get("modelId"),
        "submitTime": response.get("submitTime").isoformat() if response.get("submitTime") else None,
        "lastModifiedTime": response.get("lastModifiedTime").isoformat() if response.get("lastModifiedTime") else None,
        "totalRecordCount": response.get("totalRecordCount"),
        "processedRecordCount": response.get("processedRecordCount"),
        "successRecordCount": response.get("successRecordCount"),
        "errorRecordCount": response.get("errorRecordCount"),
        "outputDataConfig": response.get("outputDataConfig"),
    }
    print(json.dumps(printable, ensure_ascii=False, indent=2, default=str))
    if args.job_name:
        print(f"\nmetadata: s3://{S3_BUCKET}/{BATCH_PREFIX}{args.job_name}/job.json")


if __name__ == "__main__":
    main()
