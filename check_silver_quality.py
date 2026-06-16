"""Silver 텍스트 품질 전수 검증 + blocks 모드 실험"""
import json, io
from collections import defaultdict

from opik_config import S3_BUCKET, S3_REGION, load_dotenv
from opik_s3 import get_s3_client

load_dotenv()
s3 = get_s3_client()
B = S3_BUCKET

# ── 1. 모든 증권사 Silver 샘플링 ──
print("=" * 60)
print("1. 증권사별 Silver 텍스트 품질")
print("=" * 60)

firms_stats = defaultdict(lambda: {"count": 0, "total_chars": 0, "min_chars": 1e9, "max_chars": 0, "samples": []})
total_2026 = 0

paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=B, Prefix="silver/"):
    for obj in page.get("Contents", []):
        k = obj["Key"]
        if not k.endswith(".json") or "_ocr_needed" in k or "_manifest" in k:
            continue
        if "/2026-" not in k:
            continue
        parts = k.split("/")
        if len(parts) < 4:
            continue
        firm = parts[1]
        total_2026 += 1

        # 20개까지만 다운로드해서 통계
        if firms_stats[firm]["count"] >= 20:
            continue

        try:
            data = json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read().decode("utf-8"))
            text_len = data.get("text_len", 0)
            pages = data.get("pages_total", 0)
            pages_text = data.get("pages_with_text", 0)
            firms_stats[firm]["count"] += 1
            firms_stats[firm]["total_chars"] += text_len
            firms_stats[firm]["min_chars"] = min(firms_stats[firm]["min_chars"], text_len)
            firms_stats[firm]["max_chars"] = max(firms_stats[firm]["max_chars"], text_len)
            firms_stats[firm]["samples"].append((text_len, pages, pages_text, data.get("title", "")[:30]))
        except:
            pass

print(f"\n총 {total_2026}건 (2026년)\n")
print(f"{'증권사':<20s} {'샘플':>5s} {'평균글자':>8s} {'최소':>6s} {'최대':>6s} {'페이지':>6s} {'상태'}")
print("-" * 70)

for firm, st in sorted(firms_stats.items(), key=lambda x: x[1]["total_chars"] / max(x[1]["count"], 1)):
    if st["count"] == 0:
        continue
    avg = st["total_chars"] // st["count"]
    min_c = st["min_chars"] if st["min_chars"] != 1e9 else 0
    max_c = st["max_chars"]
    # 평균 페이지 수
    avg_pages = sum(s[1] for s in st["samples"]) // len(st["samples"]) if st["samples"] else 0
    status = "OK"
    if avg < 500:
        status = "매우적음"
    elif avg < 1500:
        status = "적음"
    elif avg < 3000:
        status = "보통"
    print(f"{firm:<20s} {st['count']:>5d} {avg:>8,d} {min_c:>6,d} {max_c:>6,d} {avg_pages:>6.0f} {status}")

# ── 2. 가장 글자 적은 증권사 PDF 원본으로 blocks 실험 ──
print("\n" + "=" * 60)
print("2. get_text('blocks') vs get_text() 비교 실험")
print("=" * 60)

import fitz

worst_firms = sorted(firms_stats.items(), key=lambda x: x[1]["total_chars"] / max(x[1]["count"], 1))[:3]

for firm, st in worst_firms:
    # bronze PDF 하나 찾기
    resp = s3.list_objects_v2(Bucket=B, Prefix=f"bronze/{firm}/2026-", MaxKeys=3)
    for obj in resp.get("Contents", []):
        k = obj["Key"]
        if not k.endswith(".pdf"):
            continue
        pdf_data = s3.get_object(Bucket=B, Key=k)["Body"].read()

        # 기존 방식
        doc = fitz.open(stream=pdf_data, filetype="pdf")
        plain_text = "\n\n".join(p.get_text() for p in doc)
        doc.close()

        # blocks 방식
        doc = fitz.open(stream=pdf_data, filetype="pdf")
        blocks_text = []
        for page in doc:
            blocks = page.get_text("blocks")
            for b in sorted(blocks, key=lambda x: (x[1], x[0])):  # y, x 순 정렬
                if b[6] == 0:  # text block
                    txt = b[4].strip()
                    if txt:
                        blocks_text.append(txt)
        doc.close()
        blocks_joined = "\n".join(blocks_text)

        print(f"\n[{firm}] {k.split('/')[-1]}")
        print(f"  get_text():      {len(plain_text)}자")
        print(f"  get_text(blocks): {len(blocks_joined)}자 (차이: {len(blocks_joined) - len(plain_text):+d})")
        if plain_text:
            print(f"  get_text() 샘플: {plain_text[:120]}...")
        if blocks_joined:
            print(f"  blocks()   샘플: {blocks_joined[:120]}...")
        break

# ── 3. needs_ocr 건수 요약 ──
print("\n" + "=" * 60)
print("3. OCR 필요 건수")
print("=" * 60)
ocr_total = 0
try:
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=B, Prefix="silver/_ocr_needed/"):
        for obj in page.get("Contents", []):
            try:
                data = json.loads(s3.get_object(Bucket=B, Key=obj["Key"])["Body"].read().decode("utf-8"))
                ocr_total += len(data) if isinstance(data, list) else 0
            except:
                pass
except:
    pass
print(f"OCR 필요: {ocr_total}건")
print(f"전체 대비: {ocr_total / total_2026 * 100:.1f}%" if total_2026 e