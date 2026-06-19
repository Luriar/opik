"""Build FAISS index and report info cache from S3, save to disk.
Run directly on EC2, not through SSM timeout-prone commands.
"""
import io, json, os, sys, time
import boto3, numpy as np, faiss, pyarrow.parquet as pq

S3_BUCKET = os.environ.get("S3_BUCKET", "s3-opik-bucket")
OUT_DIR = "/data/opik"

s3 = boto3.client("s3")

# List all parquet files
keys = []
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="gold/embeddings/"):
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            keys.append(obj["Key"])

print(f"Found {len(keys)} parquet files", flush=True)

all_embeddings = []
all_ids = []
all_info = {}

for i, key in enumerate(keys):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        table = pq.read_table(buf)
        df = table.to_pandas()
        for _, row in df.iterrows():
            emb = row["embedding"]
            if emb is None or len(emb) == 0:
                continue
            all_embeddings.append(np.array(emb, dtype=np.float32))
            rid = row["report_id"]
            all_ids.append(rid)
            # Convert numpy/pandas types to native Python for JSON
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
            }
    except Exception as e:
        print(f"  Error {key}: {e}", flush=True)
    if (i + 1) % 10 == 0:
        print(f"  {i+1}/{len(keys)} files, {len(all_embeddings)} vectors", flush=True)

n = len(all_embeddings)
print(f"Loaded {n} embeddings", flush=True)

# Build FAISS index
dim = len(all_embeddings[0])
index = faiss.IndexIDMap(faiss.IndexFlatIP(dim))
vectors = np.array(all_embeddings, dtype=np.float32)
ids = np.arange(n, dtype=np.int64)
index.add_with_ids(vectors, ids)
print(f"FAISS built: {index.ntotal} vectors", flush=True)

# Save
os.makedirs(OUT_DIR, exist_ok=True)
faiss.write_index(index, f"{OUT_DIR}/faiss_index.bin")

# Save IDs
with open(f"{OUT_DIR}/report_ids.json", "w") as f:
    json.dump(all_ids, f, ensure_ascii=False)
print(f"Saved {len(all_ids