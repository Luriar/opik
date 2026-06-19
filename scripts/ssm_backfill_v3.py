"""
Upload Delta backfill script v3 with retry logic, then run it.
Script is embedded as a base64 string in this Python file.
"""
import boto3, time, base64, sys

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
REMOTE_SCRIPT = "/home/ec2-user/spark_jobs/spark_silver_to_delta.py"
ssm = boto3.client("ssm", region_name=REGION)

SCRIPT = '''import os, sys, logging, time as _time
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("spark_delta")

S3_BASE = f"s3a://{os.environ.get('S3_BUCKET','s3-opik-bucket')}"
GOLD_S = f"{S3_BASE}/gold/structured/"
GOLD_E = f"{S3_BASE}/gold/embeddings/"
GOLD_D = f"{S3_BASE}/gold/dart/disclosure_events/"
DELTA_S = f"{S3_BASE}/delta/gold_db/structured"
DELTA_E = f"{S3_BASE}/delta/gold_db/embeddings"
DELTA_D = f"{S3_BASE}/delta/gold_db/disclosure_events"

def spark():
    return SparkSession.builder.appName("OPIK-Delta").master("local[4]").config("spark.driver.memory","6g").config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider").getOrCreate()

def load(sp, prefix, y, m):
    p = f"{prefix}year={y}/month={m:02d}/"
    try:
        return sp.read.parquet(p)
    except:
        return None

def load_d(sp, y, m):
    p = f"{GOLD_D}dt={y}-{m:02d}/"
    try:
        return sp.read.parquet(p)
    except:
        return None

def ensure(sp, df, path, pk):
    try:
        return DeltaTable.forPath(sp, path)
    except:
        df.limit(0).write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed","false").save(path)
        return DeltaTable.forPath(sp, path)

def merge(sp, df, path, pk):
    if df is None:
        return 0
    n = df.count()
    if n == 0:
        return 0
    t = ensure(sp, df, path, pk)
    for attempt in range(5):
        try:
            (t.alias("t").merge(df.alias("s"), f"t.{pk}=s.{pk}").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
            return n
        except Exception as e:
            if "Conflicting commit" in str(e) and attempt < 4:
                log.warning("Conflict on %s, retry %d", path, attempt + 1)
                _time.sleep(2 ** attempt)
                t = ensure(sp, df, path, pk)
            else:
                raise

def run():
    sp = spark()
    sp.sparkContext.setLogLevel("WARN")
    ts, te, td = 0, 0, 0
    for y in range(2020, 2027):
        for m in range(1, 13):
            if y == 2026 and m > 6:
                break
            ns = merge(sp, load(sp, GOLD_S, y, m), DELTA_S, "report_id")
            ne = merge(sp, load(sp, GOLD_E, y, m), DELTA_E, "report_id")
            ts += ns
            te += ne
            if ns + ne > 0:
                log.info("%04d-%02d s=%d e=%d", y, m, ns, ne)
    for y in range(2024, 2027):
        for m in range(1, 13):
            if y == 2024 and m < 8:
                continue
            if y == 2026 and m > 3:
                break
            nd = merge(sp, load_d(sp, y, m), DELTA_D, "rcept_no")
            td += nd
            if nd > 0:
                log.info("disc %04d-%02d: %d", y, m, nd)
    print(f"BACKFILL OK: structured={ts} embeddings={te} disclosure={td}")
    sp.stop()

if __name__ == "__main__":
    if "--backfill" in sys.argv:
        run()
'''

def run_ssm(commands, timeout=60):
    if isinstance(commands, str):
        commands = [commands]
    resp = ssm.send_command(
        InstanceIds=[INSTANCE_ID],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands, "executionTimeout": [str(timeout)]},
        TimeoutSeconds=max(30, timeout),
    )
    cmd_id = resp["Command"]["CommandId"]
    waited = 0
    while waited < timeout + 15:
        time.sleep(2)
        waited += 2
        inv = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=INSTANCE_ID)
        s = inv["Status"]
        if s in ("Success", "Failed", "TimedOut", "Cancelled"):
            out = inv.get("StandardOutputContent","").strip()
            err = inv.get("StandardErrorContent","").strip()
            if s == "Success":
                for line in out.split("\n")[-5:]:
                    if line.strip(): print(f"  {line.strip()}")
            else:
                print(f"  STATUS={s}")
                if err: print(f"  ERR: {err[:200]}")
            return s
    return "TIMEOUT"

# Upload
encoded = base64.b64encode(SCRIPT.encode("utf-8")).decode("ascii")
print(f"Script: {len(SCRIPT)} bytes")

run_ssm(["rm -f " + REMOTE_SCRIPT])
half = len(encoded) // 2
run_ssm(["printf '%s' '" + encoded[:half] + "' > /tmp/_dc1"])
run_ssm(["printf '%s' '" + encoded[half:] + "' > /tmp/_dc2"])
run_ssm(["cat /tmp/_dc1 /tmp/_dc2 | base64 -d > " + REMOTE_SCRIPT, "rm -f /tmp/_dc1 /tmp/_dc2"])
run_ssm(["wc -c " + REMOTE_SCRIPT])

# Send the backfill command (non-blocking)
print("Sending backfill command (300s timeout)...")
BACKFILL_CMD = (
    "cd /home/ec2-user/spark_jobs && "
    "spark-submit --master 'local[4]' --driver-memory 6g "
    + REMOTE_SCRIPT + " --backfill 2>&1"
)
resp = ssm.send_command(
    InstanceIds=[INSTANCE_ID],
    DocumentName="AWS-RunShellScript",
    Parameters={"commands": [BACKFILL_CMD], "executionTimeout": ["300"]},
    TimeoutSeconds=300,
)
backfill_cmd_id = resp["Command"]["CommandId"]
print(f"Backfill CommandId: {backfill_cmd_id}")
print("Backfill is running. Check status with get_command_invocation.")
