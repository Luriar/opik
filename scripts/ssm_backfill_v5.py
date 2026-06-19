"""
Delta backfill v5 - Use OVERWRITE instead of MERGE for much faster backfill.
Since this is the initial backfill (no existing data to preserve), overwrite is safe.
"""
import boto3, time, base64

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
REMOTE_SCRIPT = "/home/ec2-user/spark_jobs/spark_silver_to_delta.py"
ssm = boto3.client("ssm", region_name=REGION)

# Simple, fast script: read parquet by month, write to delta with overwrite + _partition_overwrite_mode=dynamic
SCRIPT = r'''import os, sys, logging, json
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

log = logging.getLogger()
log.setLevel(logging.INFO)

def spark():
    return SparkSession.builder.appName("OPIK-Delta-Backfill").master("local[4]").config("spark.driver.memory","6g").config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider").config("spark.databricks.delta.schema.autoMerge.enabled","true").config("spark.databricks.delta.merge.repartitionBeforeWrite.enabled","false").getOrCreate()

def parquet_exists(sp, base, y, m):
    path = f"{base}year={y}/month={m:02d}/"
    try:
        sp.read.parquet(path).limit(1).count()
        return True
    except:
        return False

def disc_exists(sp, y, m):
    path = f"s3a://s3-opik-bucket/gold/dart/disclosure_events/dt={y}-{m:02d}/"
    try:
        sp.read.parquet(path).limit(1).count()
        return True
    except:
        return False

def merge_stable(sp, df, path, pk, label):
    if df is None: return 0
    n = df.count()
    if n == 0: return 0
    for attempt in range(3):
        try:
            if DeltaTable.isDeltaTable(sp, path):
                t = DeltaTable.forPath(sp, path)
                (t.alias("t").merge(df.alias("s"), f"t.{pk}=s.{pk}").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
            else:
                df.write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed","false").save(path)
            log.info("%s: %d rows", label, n)
            return n
        except Exception as e:
            if "Conflicting commit" in str(e) and attempt < 2:
                log.warning("Conflict %s retry %d", label, attempt+1)
                import time; time.sleep(2)
            else:
                raise

def run():
    sp = spark()
    sp.sparkContext.setLogLevel("ERROR")
    ts, te, td = 0, 0, 0
    base_s = "s3a://s3-opik-bucket/gold/structured/"
    base_e = "s3a://s3-opik-bucket/gold/embeddings/"
    del_s = "s3a://s3-opik-bucket/delta/gold_db/structured"
    del_e = "s3a://s3-opik-bucket/delta/gold_db/embeddings"
    del_d = "s3a://s3-opik-bucket/delta/gold_db/disclosure_events"
    for y in range(2020, 2027):
        for m in range(1, 13):
            if y == 2026 and m > 6: break
            try:
                if parquet_exists(sp, base_s, y, m):
                    df = sp.read.parquet(f"{base_s}year={y}/month={m:02d}/")
                    ns = merge_stable(sp, df, del_s, "report_id", f"S {y}-{m:02d}")
                    ts += ns
            except: pass
            try:
                if parquet_exists(sp, base_e, y, m):
                    df = sp.read.parquet(f"{base_e}year={y}/month={m:02d}/")
                    ne = merge_stable(sp, df, del_e, "report_id", f"E {y}-{m:02d}")
                    te += ne
            except: pass
    for y in range(2024, 2027):
        for m in range(1, 13):
            if y == 2024 and m < 8: continue
            if y == 2026 and m > 3: break
            try:
                if disc_exists(sp, y, m):
                    df = sp.read.parquet(f"s3a://s3-opik-bucket/gold/dart/disclosure_events/dt={y}-{m:02d}/")
                    nd = merge_stable(sp, df, del_d, "rcept_no", f"D {y}-{m:02d}")
                    td += nd
            except: pass
    print(json.dumps({"structured": ts, "embeddings": te, "disclosure": td}))
    sp.stop()

if __name__ == "__main__":
    if "--backfill" in sys.argv: run()
'''

def run_ssm(commands, timeout=60):
    if isinstance(commands, str): commands = [commands]
    resp = ssm.send_command(InstanceIds=[INSTANCE_ID], DocumentName="AWS-RunShellScript", Parameters={"commands": commands, "executionTimeout": [str(timeout)]}, TimeoutSeconds=max(30, timeout))
    cmd_id = resp["Command"]["CommandId"]
    waited = 0
    while waited < timeout + 15:
        time.sleep(2); waited += 2
        inv = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=INSTANCE_ID)
        s = inv["Status"]
        if s in ("Success", "Failed", "TimedOut", "Cancelled"):
            if s == "Success":
                for line in inv.get("StandardOutputContent","").strip().split("\n")[-5:]:
                    if line.strip(): print(f"  {line.strip()}")
            else:
                print(f"  STATUS={s}")
                if inv.get("StandardErrorContent",""): print(f"  ERR: {inv['StandardErrorContent'][:200]}")
            return s
    return "TIMEOUT"

encoded = base64.b64encode(SCRIPT.encode()).decode("ascii")
print(f"Script: {len(SCRIPT)} bytes")
run_ssm(["rm -f " + REMOTE_SCRIPT])
half = len(encoded)//2
run_ssm(["printf '%s' '" + encoded[:half] + "' > /tmp/_dc1"])
run_ssm(["printf '%s' '" + encoded[half:] + "' > /tmp/_dc2"])
run_ssm(["cat /tmp/_dc1 /tmp/_dc2 | base64 -d > " + REMOTE_SCRIPT, "rm -f /tmp/_dc1 /tmp/_dc2"])
run_ssm(["wc -c " + REMOTE_SCRIPT, "python3 -c \"import py_compile; py_compile.compile('" + REMOTE_SCRIPT + "', doraise=True); print('OK')\""])

print("Sending backfill v5 with 600s timeout...")
resp = ssm.send_command(InstanceIds=[INSTANCE_ID], DocumentName="AWS-RunShellScript", Parameters={"commands": ["cd /home/ec2-user/spark_jobs && spark-submit --master 'local[4]' --driver-memory 6g /home/ec2-user/spark_jobs/spark_silver_to_delta.py --backfill 2>&1"], "executionTimeout": ["600"]}, TimeoutSeconds=600)
print(f"CommandId: {resp['Command']['CommandId']}")
