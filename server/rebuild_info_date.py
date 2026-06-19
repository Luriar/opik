"""Rebuild report_info.json with year/month from S3 keys. Does NOT rebuild FAISS."""
import boto3, json, io, re, pyarrow.parquet as pq

BUCKET = "s3-opik-bucket"
s3 = boto3.client("s3")

# Load existing info
with open("/data/opik/report_info.json") as f:
    info = json.load(f)
print("Loaded {} existing entries".format(len(info)))

# Scan S3 keys for year/month
keys = []
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=BUCKET, Prefix="gold/embeddings/"):
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            keys.append(obj["Key"])

print("Found {} parquet files".format(len(keys)))

updated = 0
for i, key in enumerate(keys):
    ym = re.search(r"year=(\d{4})/month=(\d{2})", key)
    if not ym:
        continue
    y = int(ym.group(1))
    m = int(ym.group(2))
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        table = pq.read_table(buf)
        df = table.to_pandas()
        for _, row in df.iterrows():
            rid = row["report_id"]
            if rid in info:
                info[rid]["year"] = y
                info[rid]["month"] = m
                updated += 1
    except Exception as e:
        print("Error {}: {}".format(key, e))
    if (i + 1) % 20 == 0:
        print("  {}/{} files, {} updated".format(i + 1, len(keys), updated))

# Save
with open("/data/opik/report_info.json", "w") as f:
    json.dump(info, f, ensure_ascii=False)

# Verify
with open("/data/opik/report_info.json") as f:
    verify = json.load(f)
sample = list(verify.values())[:3]
print("Done: {} entries".format(len(verify)))
for s in sample:
    print("  year={}, month={}".format(s.get("year"), s.get("month")))
