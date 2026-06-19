import boto3, json, io, pyarrow.parquet as pq

B = "s3-opik-bucket"
s3 = boto3.client("s3")

with open("/data/opik/report_ids.json") as f:
    report_ids = json.load(f)

unique_ids = set(report_ids)
all_info = {}
keys = []
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=B, Prefix="gold/embeddings/"):
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            keys.append(obj["Key"])

total = len(keys)
print("Processing {} files, {} unique ids".format(total, len(unique_ids)))

for i, key in enumerate(keys):
    try:
        obj = s3.get_object(Bucket=B, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        table = pq.read_table(buf)
        df = table.to_pandas()
        for _, row in df.iterrows():
            rid = row["report_id"]
            if rid in all_info:
                continue
            kw = row.get("keywords")
            rs = row.get("risks")
            reason = row.get("reason")
            if reason is None or (hasattr(reason, "isna") and reason.isna()):
                reason = None
            code = row.get("종목코드")
            all_info[rid] = {
                "종목코드": str(code) if code is not None else None,
                "reason": str(reason) if reason is not None else None,
                "keywords": [str(x) for x in kw] if kw is not None and len(kw) > 0 else None,
                "risks": [str(x) for x in rs] if rs is not None and len(rs) > 0 else None,
            }
    except Exception as e:
        print("  Error: {}".format(e))
    if (i + 1) % 20 == 0:
        print("  {}/{} files, {} infos".format(i + 1, total, len(all_info)))

with open("/data/opik/report_info.json", "w") as f:
    json.dump(all_info, f, ensure_ascii=False)

with open("/data/opik/report_info.json") as f:
    loaded = json.load(f)
print("Done: {} entries, verified".format(len(loaded)))
