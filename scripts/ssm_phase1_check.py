"""
Phase 1: Upload check script and send spark-submit async.
"""
import boto3, time, base64, sys

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
ssm = boto3.client("ssm", region_name=REGION)

CHECK_SCRIPT = r"""import sys, logging, json
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, col
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("check")
spark = SparkSession.builder.appName("Check").master("local[4]").config("spark.driver.memory","4g").config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider").config("spark.hadoop.fs.s3a.endpoint","s3.ap-northeast-2.amazonaws.com").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
source = "s3a://s3-opik-bucket/gold/dart_delta/disclosure_events"
log.info("Reading: %s", source)
df = spark.read.format("delta").load(source)
total = df.count(); log.info("Total rows: %d", total)
log.info("Schema:")
for f in df.schema.fields: log.info("  %s: %s null=%s", f.name, f.dataType, f.nullable)
log.info("Columns: %s", df.columns)
for row in df.limit(3).collect():
    d = {k: str(v)[:60] for k, v in row.asDict().items()}
    log.info("  Sample: %s", json.dumps(d, ensure_ascii=False))
if "rcept_dt" in df.columns:
    df2 = df.withColumn("dt", substring(col("rcept_dt"), 1, 7))
    dts = sorted([r["dt"] for r in df2.select("dt").distinct().collect()])
    log.info("Months: %d, first: %s, last: %s", len(dts), dts[:3], dts[-3:])
    for r in df2.groupBy("dt").count().orderBy("dt").collect(): log.info("  %s: %d", r["dt"], r["count"])
for c in ["rcept_no","corp_code","stock_code","report_nm","text","event_category","corp_name"]:
    if c in df.columns:
        if c == "text":
            log.info("text len samples: %s", [len(str(r["text"]or"")) for r in df.select("text").limit(5).collect()])
        else:
            log.info("%s samples: %s", c, [str(r[c])[:30] for r in df.select(c).limit(5).collect()])
spark.stop()
print(f"DELTA_CHECK_OK total={total}")
"""

def run_ssm_wait(commands, timeout=30):
    if isinstance(commands, str): commands = [commands]
    resp = ssm.send_command(InstanceIds=[INSTANCE_ID], DocumentName="AWS-RunShellScript",
                            Parameters={"commands": commands, "executionTimeout": [str(timeout)]},
                            TimeoutSeconds=max(30, timeout))
    cmd_id = resp["Command"]["CommandId"]
    waited = 0
    while waited < timeout + 10:
        time.sleep(1); waited += 1
        inv = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=INSTANCE_ID)
        s = inv["Status"]
        if s in ("Success", "Failed", "TimedOut", "Cancelled"):
            return s, inv.get("StandardOutputContent",""), inv.get("StandardErrorContent","")
    return "TIMEOUT", "", ""

CHK = "/home/ec2-user/spark_jobs/check_dd.py"
encoded = base64.b64encode(CHECK_SCRIPT.encode()).decode("ascii")
print(f"Script: {len(CHECK_SCRIPT)} bytes")
run_ssm_wait(["rm -f " + CHK])
half = len(encoded)//2
run_ssm_wait(["printf '%s' '" + encoded[:half] + "' > /tmp/_c1"])
run_ssm_wait(["printf '%s' '" + encoded[half:] + "' > /tmp/_c2"])
run_ssm_wait(["cat /tmp/_c1 /tmp/_c2 | base64 -d > " + CHK, "rm -f /tmp/_c1 /tmp/_c2"])

print("Sending spark-submit CHECK (async)...")
resp = ssm.send_command(InstanceIds=[INSTANCE_ID], DocumentName="AWS-RunShellScript",
    Parameters={"commands": [f"spark-submit --master 'local[4]' --driver-memory 4g {CHK} 2>&1"], "executionTimeout": ["120"]},
    TimeoutSeconds=120)
print(f"Check CommandId: {resp['Command']['CommandId']}")
print("Waiting 90s for result...")
time.sleep(90)
inv = ssm.get_command_invocation(CommandId=resp['Command']['CommandId'], InstanceId=INSTANCE_ID)
s = inv["Status"]
out = inv.get("StandardOutputContent","")
err = inv.get("StandardErrorContent","")
print(f"Status: {s}")
for line in out.split("\n")[-40:]:
    if line.strip(): print(f"  {line.strip()}")
if err: print(f"STDERR: {err[:300]}")
