"""
한국투자증권 silver 2차 전처리 — '주식주문' 꼬리 노이즈 제거
- 입력:  s3://{BUCKET}/silver/한국투자증권_수정/  (1차 전처리 완료본: 종목명 추가 + \n 제거 + 원문보기 제거)
- 출력:  s3://{BUCKET}/silver/한국투자증권/      (1차 수집 원본은 비워둔 상태)
처리 내용:
  - 본문에서 '주식주문'부터 끝까지 제거 (관련리포트 목록 등 꼬리 노이즈 통째로 제거)
  - 그 외 필드는 그대로 유지
실행: python extract/preprocess_한국투자증권.py
사전 설치: pip install boto3 python-dotenv
"""
import os
import json
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

S3_BUCKET  = os.getenv("S3_BUCKET", "s3-opik-bucket")
SRC_PREFIX = "silver/한국투자증권_수정/"
DST_PREFIX = "silver/한국투자증권/"


def remove_tail(body):
    """'주식주문'부터 끝까지 제거. 없으면 원본 유지."""
    idx = body.find("주식주문")
    return body[:idx].rstrip() if idx != -1 else body


def main():
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    processed, trimmed = 0, 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=SRC_PREFIX):
        for obj in page.get("Contents", []):
            src_key = obj["Key"]
            if not src_key.endswith(".json"):
                continue

            doc = json.loads(
                s3.get_object(Bucket=S3_BUCKET, Key=src_key)["Body"].read()
            )

            before = doc.get("본문", "")
            doc["본문"] = remove_tail(before)
            if len(doc["본문"]) < len(before):
                trimmed += 1

            dst_key = src_key.replace(SRC_PREFIX, DST_PREFIX, 1)
            s3.put_object(
                Bucket=S3_BUCKET, Key=dst_key,
                Body=json.dumps(doc, ensure_ascii=False).encode(),
            )
            processed += 1
            if processed % 200 == 0:
                print(f"  {processed}건 처리...")

    print(f"\n완료: 총 {processed}건 → s3://{S3_BUCKET}/{DST_PREFIX}")
    print(f"꼬리 제거 적용: {trimmed}건 / 변화 없음: {processed - trimmed}건")


if __name__ == "__main__":
    main()
