"""
Simple backfill - read each month, write with Delta mergeSchema.
"""
import boto3, time, base64

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
REMOTE_SCRIPT = "/home/ec2-user/spark_jobs/spark_silver_to_delta.py"
ssm = boto3.client("ssm", region_name=REGION)

SCRIPT = r'''import os, sys, logging, subprocess
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger()

def spark():
    return SparkSession.builder.appName("OPIK-Delta").master("local[4]").config("spark.driver.memory","6g").config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider").config("spark.databricks.delta.schema.autoMerge.enabled","true").getOrCreate()

def write_one(sp, base, delta_path, pk, y, m, label):
    s3path = f"s3://s3-opik-bucket{base.split('s3-opik-bucket')[1]}year={y}/month={m:02d}/"
    r = subprocess.run(["aws", "s3", "ls", s3path], capture_output=True, timeout=10)
    if r.returncode != 0 or len(r.stdout) == 0:
        return 0
    parquet_path = f"{base}year={y}/month={m:02d}/"
    df = sp.read.parquet(parquet_path)
    n = df.count()
    if n == 0:
        return 0
    try:
        if DeltaTable.isDeltaTable(sp, delta_path):
            t = DeltaTable.forPath(sp, delta_path)
            for a in range(3):
                try:
                    (t.alias("t").merge(df.alias("s"), f"t.{pk}=s.{pk}").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
                    log.info("%s %04d-%02d: %d rows", label, y, m, n)
                    return n
                except Exception as e2:
                    if "Conflicting" in str(e2) and a < 2:
                        import time as _t; _t.sleep(3)
                    else:
                        raise
        else:
            df.write.format("delta").mode("overwrite").option("mergeSchema","true").save(delta_path)
            log.info("%s %04d-%02d (NEW): %d rows", label, y, m, n)
            return n
    except Exception as e:
        log.warning("%s %04d-%02d FAILED: %s", label, y, m, str(e)[:200])
        return 0

def run():
    sp = spark()
    sp.sparkContext.setLogLevel("WARN")
    base_s = "s3a://s3-opik-bucket/gold/structured/"
    base_e = "s3a://s3-opik-bucket/gold/embeddings/"
    delta_s = "s3a://s3-opik-bucket/delta/gold_db/structured"
    delta_e = "s3a://s3-opik-bucket/delta/gold_db/embeddings"
    delta_d = "s3a://s3-opik-bucket/delta/gold_db/disclosure_events"
    ts, te, td = 0, 0, 0
    for y in range(2020, 2027):
        for m in range(1, 13):
            if y == 2026 and m > 6: break
            ts += write_one(sp, base_s, delta_s, "report_id", y, m, "S")
            te += write_one(sp, base_e, delta_e, "report_id", y, m, "E")
    for y in range(2024, 2027):
        for m in range(1, 13):
            if y == 2024 and m < 8: continue
            if y == 2026 and m > 3: break
            s3path = f"s3://s3-opik-bucket/gold/dart/disclosure_events/dt={y}-{m:02d}/"
            r = subprocess.run(["aws", "s3", "ls", s3path], capture_output=True, timeout=10)
            if r.returncode != 0 or len(r.stdout) == 0: continue
            df = sp.read.parquet(f"s3a://s3-opik-bucket/gold/dart/disclosure_events/dt={y}-{m:02d}/")
            n = df.count()
            if n > 0:
                try:
                    if DeltaTable.isDeltaTable(sp, delta_d):
                        t = DeltaTable.forPath(sp, delta_d)
                        for a in range(3):
                            try:
                                (t.alias("t").merge(df.alias("s"), "t.rcept_no=s.rcept_no").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
                                break
                            except:
                                if a < 2: import time as _t; _t.sleep(3)
                                else: raise
                    else:
                        df.write.format("delta").mode("overwrite").option("mergeSchema","true").save(delta_d)
                    td += n
                    log.info("D %04d-%02d: %d rows", y, m, n)
                except Exception as e2:
                    log.warning("D %04d-%02d FAILED: %s", y, m, str(e2)[:200])
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

print("Sending simple backfill (mergeSchema, per-month MERGE)...")
resp = ssm.send_command(InstanceIds=[INSTANCE_ID], DocumentName="AWS-RunShellScript", Parameters={"commands": ["cd /home/ec2-user/spark_jobs && spark-submit --master 'local[4]' --driver-memory 6g /home/ec2-user/spark_jobs/spark_silver_to_delta.py --backfill 2>&1"], "executionTimeout": ["600"]}, TimeoutSeconds=600)
print(f"CommandId: {resp['Command']['CommandId']}")
