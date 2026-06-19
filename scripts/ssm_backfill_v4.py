"""
Upload Delta backfill v4 with S3 path existence check before Spark read.
"""
import boto3, time, base64

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
REMOTE_SCRIPT = "/home/ec2-user/spark_jobs/spark_silver_to_delta.py"
ssm = boto3.client("ssm", region_name=REGION)

SCRIPT = r'''import os, sys, logging, time as _time, subprocess
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("spark_delta")

S3_BASE = f"s3a://{os.environ.get('S3_BUCKET','s3-opik-bucket')}"
DELTA_S = f"{S3_BASE}/delta/gold_db/structured"
DELTA_E = f"{S3_BASE}/delta/gold_db/embeddings"
DELTA_D = f"{S3_BASE}/delta/gold_db/disclosure_events"

def s3_path_exists(path):
    s3path = path.replace("s3a://", "s3://")
    try:
        r = subprocess.run(["aws", "s3", "ls", s3path + "/"], capture_output=True, timeout=10)
        return r.returncode == 0 and len(r.stdout) > 0
    except:
        return False

def spark():
    return SparkSession.builder.appName("OPIK-Delta").master("local[4]").config("spark.driver.memory","6g").config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider").getOrCreate()

def load_structured(sp, y, m):
    path = f"s3a://s3-opik-bucket/gold/structured/year={y}/month={m:02d}/"
    if not s3_path_exists(f"s3://s3-opik-bucket/gold/structured/year={y}/month={m:02d}"):
        return None
    return sp.read.parquet(path)

def load_embeddings(sp, y, m):
    path = f"s3a://s3-opik-bucket/gold/embeddings/year={y}/month={m:02d}/"
    if not s3_path_exists(f"s3://s3-opik-bucket/gold/embeddings/year={y}/month={m:02d}"):
        return None
    return sp.read.parquet(path)

def load_disclosure(sp, y, m):
    path = f"s3a://s3-opik-bucket/gold/dart/disclosure_events/dt={y}-{m:02d}/"
    if not s3_path_exists(f"s3://s3-opik-bucket/gold/dart/disclosure_events/dt={y}-{m:02d}"):
        return None
    return sp.read.parquet(path)

def ensure(sp, df, path, pk):
    try:
        return DeltaTable.forPath(sp, path)
    except:
        df.limit(0).write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed","false").save(path)
        return DeltaTable.forPath(sp, path)

def merge(sp, df, path, pk):
    if df is None: return 0
    n = df.count()
    if n == 0: return 0
    t = ensure(sp, df, path, pk)
    for attempt in range(5):
        try:
            (t.alias("t").merge(df.alias("s"), f"t.{pk}=s.{pk}").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
            return n
        except Exception as e:
            if "Conflicting commit" in str(e) and attempt < 4:
                log.warning("Conflict on %s, retry %d", path, attempt + 1)
                _time.sleep(3)
                t = ensure(sp, df, path, pk)
            else:
                raise

def run():
    sp = spark()
    sp.sparkContext.setLogLevel("WARN")
    ts, te, td = 0, 0, 0
    for y in range(2020, 2027):
        for m in range(1, 13):
            if y == 2026 and m > 6: break
            ns = merge(sp, load_structured(sp, y, m), DELTA_S, "report_id")
            ne = merge(sp, load_embeddings(sp, y, m), DELTA_E, "report_id")
            ts += ns; te += ne
            if ns+ne > 0: log.info("%04d-%02d s=%d e=%d", y, m, ns, ne)
    for y in range(2024, 2027):
        for m in range(1, 13):
            if y == 2024 and m < 8: continue
            if y == 2026 and m > 3: break
            nd = merge(sp, load_disclosure(sp, y, m), DELTA_D, "rcept_no")
            td += nd
            if nd > 0: log.info("disc %04d-%02d: %d", y, m, nd)
    print(f"BACKFILL OK: structured={ts} embeddings={te} disclosure={td}")
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

# Send backfill with 600s timeout
print("Sending backfill with 600s timeout...")
resp = ssm.send_command(InstanceIds=[INSTANCE_ID], DocumentName="AWS-RunShellScript", Parameters={"commands": ["cd /home/ec2-user/spark_jobs && spark-submit --master 'local[4]' --driver-memory 6g /home/ec2-user/spark_jobs/spark_silver_to_delta.py --backfill 2>&1"], "executionTimeout": ["600"]}, TimeoutSeconds=600)
print(f"CommandId: {resp['Command']['CommandId']}")
print("Backfill launched with S3 path pre-check.")
