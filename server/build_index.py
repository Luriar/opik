"""Build FAISS index and report info cache from S3 - 증권사 + DART.
Run directly on EC2, not through SSM timeout-prone commands.

Usage: python build_index.py

Reads:
  - gold/embeddings/year=YYYY/month=MM/data.parquet (증권사)
  - gold/dart/rag/embedding/... (DART 공시)

Saves:
  - /data/opik/faiss_index.bin
  - /data/opik/report_ids.json
  - /data/opik/report_info.json
"""
import io, json, os, sys, time, re
import boto3, numpy as np, faiss, pyarrow.parquet as pq
from source_links import source_url_from_metadata

S3_BUCKET = os.environ.get("S3_BUCKET", "s3-opik-bucket")
OUT_DIR = "/data/opik"
s3 = boto3.client("s3")

all_embeddings = []
all_ids = []
all_info = {}

# ============================================================
# Phase 1: 증권사 애널리스트 리포트
# ============================================================
print("=" * 60)
print("Phase 1: 증권사 embeddings")
print("=" * 60)

keys_sec = []
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix="gold/embeddings/"):
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            keys_sec.append(obj["Key"])

print(f"Found {len(keys_sec)} parquet files", flush=True)

for i, key in enumerate(keys_sec):
    try:
        ym = re.search(r"year=(\d{4})/month=(\d{2})", key)
        year = int(ym.group(1)) if ym else None
        month = int(ym.group(2)) if ym else None
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        table = pq.read_table(buf)
        df = table.to_pandas()
        for _, row in df.iterrows():
            emb = row["embedding"]
            if emb is None or len(emb) == 0:
                continue
            all_embeddings.append(np.array(emb, dtype=np.float32))
            rid = str(row["report_id"])
            all_ids.append(rid)
            kw = row.get("keywords")
            rs = row.get("risks")
            reason = row.get("reason")
            if reason is None or (hasattr(reason, "isna") and reason.isna()):
                reason = None
            all_info[rid] = {
                "종목코드": str(row.get("종목코드")) if row.get("종목코드") is not None else None,
                "reason": str(reason) if reason is not None else None,
                "keywords": [str(x) for x in kw] if kw is not None and len(kw) > 0 else None,
                "risks": [str(x) for x in rs] if rs is not None and len(rs) > 0 else None,
                "year": year,
                "month": month,
            }
    except Exception as e:
        print(f"  Error {key}: {e}", flush=True)
    if (i + 1) % 10 == 0:
        print(f"  증권사 {i+1}/{len(keys_sec)} files, {len(all_embeddings)} vectors", flush=True)

n_sec = len(all_embeddings)
print(f"증권사 loaded: {n_sec} embeddings", flush=True)

# ============================================================
# Phase 2: DART 공시 embeddings
# ============================================================
print("\n" + "=" * 60)
print("Phase 2: DART embeddings")
print("=" * 60)

DART_PREFIX = "gold/dart/rag/embedding/model=intfloat_multilingual-e5-small/version=v1/"

keys_dart = []
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix=DART_PREFIX):
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            keys_dart.append(obj["Key"])

print(f"Found {len(keys_dart)} parquet files", flush=True)

dart_loaded = 0
dart_skipped_amended = 0
dart_skipped_expired = 0
dart_error = 0

for i, key in enumerate(keys_dart):
    try:
        ym = re.search(r"rcept_year=(\d{4})/rcept_month=(\d{2})", key)
        year = int(ym.group(1)) if ym else None
        month = int(ym.group(2)) if ym else None
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        table = pq.read_table(buf)
        df = table.to_pandas()

        for _, row in df.iterrows():
            is_latest = row.get("is_latest")
            if is_latest is None or not bool(is_latest):
                dart_skipped_amended += 1
                continue
            valid_to = row.get("valid_to")
            if valid_to is not None and str(valid_to) != "None":
                dart_skipped_expired += 1
                continue

            emb = row["embedding"]
            if emb is None or len(emb) == 0:
                continue

            all_embeddings.append(np.array(emb, dtype=np.float32))
            chunk_id = str(row["chunk_id"])
            all_ids.append(chunk_id)

            stock_code = str(row.get("stock_code", "")) if row.get("stock_code") is not None else None
            rcept_no = str(row.get("rcept_no", ""))
            rcept_dt = str(row.get("rcept_dt", ""))
            base_type = str(row.get("base_report_type", ""))
            dart_kw = row.get("keywords")
            source_url = source_url_from_metadata(row)

            reason_parts = [f"[DART {base_type}]"]
            if rcept_dt:
                reason_parts.append(f"접수일: {rcept_dt}")
            if rcept_no:
                reason_parts.append(f"공시번호: {rcept_no}")
            reason = " ".join(reason_parts)

            all_info[chunk_id] = {
                "종목코드": stock_code,
                "reason": reason,
                "keywords": [str(x) for x in dart_kw] if dart_kw is not None and len(dart_kw) > 0 else None,
                "risks": None,
                "year": year,
                "month": month,
                "source_type": "dart",
                "rcept_no": rcept_no or None,
                "rcept_dt": rcept_dt or None,
                "corp_code": str(row.get("corp_code", "")) if row.get("corp_code") is not None else None,
                "corp_name": str(row.get("corp_name", "")) if row.get("corp_name") is not None else None,
                "report_nm": str(row.get("report_nm", "")) if row.get("report_nm") is not None else None,
                "base_report_type": base_type or None,
                "dart_view_url": source_url,
                "source_url": source_url,
                "source_uri": str(row.get("source_uri", "")) if row.get("source_uri") is not None else None,
            }
            dart_loaded += 1

    except Exception as e:
        dart_error += 1
        if dart_error <= 5:
            print(f"  Error {key}: {e}", flush=True)
    if (i + 1) % 20 == 0:
        print(f"  DART {i+1}/{len(keys_dart)} files, +{dart_loaded} vectors "
              f"(skipped: {dart_skipped_amended} not-latest, {dart_skipped_expired} expired)",
              flush=True)

print(f"DART loaded: {dart_loaded} embeddings "
      f"(skipped: {dart_skipped_amended} not-latest, {dart_skipped_expired} expired, "
      f"{dart_error} errors)",
      flush=True)

# ============================================================
# Phase 3: Build FAISS index
# ============================================================
n = len(all_embeddings)
print(f"\nTotal: {n} embeddings (증권사 {n_sec} + DART {n - n_sec})", flush=True)

if n == 0:
    print("ERROR: No embeddings found", flush=True)
    sys.exit(1)

dim = all_embeddings[0].shape[0]
print(f"Dimension: {dim}", flush=True)

index = faiss.IndexIDMap(faiss.IndexFlatIP(dim))
vectors = np.array(all_embeddings, dtype=np.float32)
ids = np.arange(n, dtype=np.int64)
index.add_with_ids(vectors, ids)
print(f"FAISS built: {index.ntotal} vectors", flush=True)

# ============================================================
# Phase 4: Save
# ============================================================
os.makedirs(OUT_DIR, exist_ok=True)
faiss.write_index(index, f"{OUT_DIR}/faiss_index.bin")
print(f"Saved faiss_index.bin ({index.ntotal} vectors)", flush=True)

with open(f"{OUT_DIR}/report_ids.json", "w") as f:
    json.dump(all_ids, f, ensure_ascii=False)
print(f"Saved report_ids.json ({len(all_ids)} entries)", flush=True)

with open(f"{OUT_DIR}/report_info.json", "w") as f:
    json.dump(all_info, f, ensure_ascii=False, default=str)
print(f"Saved report_info.json ({len(all_info)} entries)", flush=True)

# Verify
with open(f"{OUT_DIR}/report_ids.json") as f:
    loaded_ids = json.load(f)
with open(f"{OUT_DIR}/report_info.json") as f:
    loaded_info = json.load(f)
print(f"Verification: {len(loaded_ids)} ids, {len(loaded_info)} infos - OK", flush=True)

# Sample DART entries
dart_sample = [rid for rid in all_ids if rid.startswith("dart:")][:3]
if dart_sample:
    print(f"\nSample DART entries:")
    for rid in dart_sample:
        info = all_info.get(rid, {})
        print(f"  {rid}")
        print(f"    종목코드: {info.get('종목코드')}")
        print(f"    reason: {info.get('reason')}")
        print(f"    keywords: {info.get('keywords')}")
        print(f"    year/month: {info.get('year')}/{info.get('month')}")

print("\nDone.")
