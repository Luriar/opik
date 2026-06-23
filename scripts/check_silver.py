from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

"""Silver 추출 결과 확인"""
import json
from collections import defaultdict

from opik_config import S3_BUCKET, S3_REGION, load_dotenv
from opik_s3 import get_s3_client

load_dotenv()
s3 = get_s3_client()
BUCKET = S3_BUCKET

# silver 아래 키 수집
by_firm = defaultdict(lambda: {"count": 0, "total_text_len": 0, "ocr_needed": 0, "sample_text": ""})
by_date = defaultdict(int)
total = 0

paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=BUCKET, Prefix="silver/"):
    for obj in page.get("Contents", []):
        k = obj["Key"]
        if k.endswith(".json") and "_ocr_needed" not in k and "_manifest" not in k and "/2026-" in k:
            parts = k.split("/")
            if len(parts) >= 4:
                firm = parts[1]
                date = parts[2]
                by_firm[firm]["count"] += 1
                by_date[date] += 1
                total += 1

# 샘플 로드 (각 증권사 첫 파일)
for firm in list(by_firm.keys())[:5]:
    sample_key = f"silver/{firm}/"
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=sample_key, MaxKeys=1)
    if resp.get("Contents"):
        k = resp["Contents"][0]["Key"]
        data = json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read().decode("utf-8"))
        by_firm[firm]["sample_text"] = data.get("text", "")[:80]
        by_firm[firm]["total_text_len"] += data.get("text_len", 0)
        if data.get("needs_ocr"):
            by_firm[firm]["ocr_needed"] += 1

print(f"\n=== Silver 2026년 추출 결과 ===\n")
print(f"총 {total}건")
print(f"\n증권사별:")
for firm, info in sorted(by_firm.items(), key=lambda x: -x[1]["count"])[:15]:
    print(f"  {firm}: {info['count']}건", end="")
    if info["sample_text"]:
        print(f" | text: {info['sample_text'][:60]}...")
    else:
        print()

# OCR 필요한 건 확인
ocr_count = 0
try:
    ocr_resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="silver/_ocr_needed/2026", MaxKeys=50)
    for obj in ocr_resp.get("Contents", []):
        data = json.loads(s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read().decode("utf-8"))
        ocr_count += len(data)
except:
    pass
print(f"\nOCR 필요: {ocr_count}")
