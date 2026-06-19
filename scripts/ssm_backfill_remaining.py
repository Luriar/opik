"""
Backfill remaining months (2024-2026) + disclosure events
"""
import boto3, time, base64

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
REMOTE_SCRIPT = "/home/ec2-user/spark_jobs/spark_silver_to_delta.py"
ssm = boto3.client("ssm", region_name=REGION)

SCRIPT = r'''import os, sys, logging, subprocess
from pyspark.sql import SparkSession, DataFrame as DF
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger()

S3_B = "s3a://s3-opik-bucket"

def spark():
    return SparkSession.builder.appName("OPIK-BF-Remaining").master("local[2]").config("spark.driver.memory","4g").config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider").config("spark.databricks.delta.schema.autoMerge.enabled","true").getOrCreate()

def s3_exists(path):
    r = subprocess.run(["aws", "s3", "ls", path + "/"], capture_output=True, timeout=10)
    return r.returncode == 0 and len(r.stdout) > 0

def load(sp, base, y, m):
    p = f"{base}year={y}/month={m:02d}/"
    if not s3_exists(f"s3://s3-opik-bucket/gold/structured/year={y}/month={m:02d}" if "structured" in base else f"s3://s3-opik-bucket/gold/embeddings/year={y}/month={m:02d}"):
        return None
    return sp.read.parquet(p)

def load_disc(sp, y, m):
    p = f"{S3_B}/gold/dart/disclosure_events/dt={y}-{m:02d}/"
    if not s3_exists(f"s3://s3-opik-bucket/gold/dart/disclosure_events/dt={y}-{m:02d}"):
        return None
    return sp.read.parquet(p)

def merge(sp, df, path, pk):
    if df is None: return 0
    n = df.count()
    if n == 0: return 0
    t = DeltaTable.forPath(sp, path)
    for a in range(3):
        try:
            (t.alias("t").merge(df.alias("s"), f"t.{pk}=s.{pk}").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
            return n
        except Exception as e:
            if "Conflicting" in str(e) and a < 2:
                import time as _t; _t.sleep(2)
            else:
                raise

def run():
    sp = spark()
    sp.sparkContext.setLogLevel("WARN")
    base_s = f"{S3_B}/gold/structured/"
    base_e = f"{S3_B}/gold/embeddings/"
    delta_s = f"{S3_B}/delta/gold_db/structured"
    delta_e = f"{S3_B}/delta/gold_db/embeddings"
    delta_d = f"{S3_B}/delta/gold_db/disclosure_events"
    ts, te, td = 0, 0, 0

    # Remaining structured + embeddings (2024-2026/06)
    for y in range(2024, 2027):
        for m in range(1, 13):
            if y == 2026 and m > 6: break
            ns = merge(sp, load(sp, base_s, y, m), delta_s, "report_id")
            ne = merge(sp, load(sp, base_e, y, m), delta_e, "report_id")
            ts += ns; te += ne
            if ns+ne > 0: log.info("S %04d-%02d: %d | E %04d-%02d: %d", y, m, ns, y, m, ne)

    # Disclosure events (2024-08 to 2026-03)
    for y in range(2024, 2027):
        for m in range(1, 13):
            if y == 2024 and m < 8: continue
            if y == 2026 and m > 3: break
            nd = merge(sp, load_disc(sp, y, m), delta_d, "rcept_no")
            td += nd
            if nd > 0: log.info("D %04d-%02d: %d", y, m, nd)

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

print("Sending remaining backfill (2024-2026 + disclosure)...")
resp = ssm.send_command(InstanceIds=[INSTANCE_ID], DocumentName="AWS-RunShellScript", Parameters={"commands": ["cd /home/ec2-user/spark_jobs && spark-submit --master 'local[2]' --driver-memory 4g /home/ec2-user/spark_jobs/spark_silver_to_delta.py --backfill 2>&1"], "executionTimeout": ["600"]}, TimeoutSeconds=600)
print(f"CommandId: {resp['Command']['CommandId']}")
