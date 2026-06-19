"""
SSM: Check gold/dart_delta/disclosure_events Delta table schema and content.
This is where the disclosure events data actually lives.
"""
import boto3, time, base64

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
REMOTE_SCRIPT = "/home/ec2-user/spark_jobs/check_gold_dart_delta_disclosure.py"
ssm = boto3.client("ssm", region_name=REGION)

SCRIPT = r"""import sys, logging, json
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, col, desc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("check_delta")

spark = SparkSession.builder.appName("Check-Delta").master("local[4]").config("spark.driver.memory","4g").config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider").config("spark.hadoop.fs.s3a.endpoint","s3.ap-northeast-2.amazonaws.com").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

source = "s3a://s3-opik-bucket/gold/dart_delta/disclosure_events"
log.info("Reading Delta table: %s", source)
df = spark.read.format("delta").load(source)

total_count = df.count()
log.info("Total rows: %d", total_count)

log.info("Schema:")
for f in df.schema.fields:
    log.info("  %s: %s (nullable=%s)", f.name, f.dataType, f.nullable)

log.info("All columns: %s", df.columns)

# Show sample
log.info("Sample 3 rows (all columns):")
for row in df.limit(3).collect():
    d = {k: str(v)[:80] for k, v in row.asDict().items()}
    log.info("  %s", json.dumps(d, ensure_ascii=False))

# Check rcept_dt
if "rcept_dt" in df.columns:
    df_months = df.withColumn("dt", substring(col("rcept_dt"), 1, 7))
    dts = sorted([r["dt"] for r in df_months.select("dt").distinct().collect()])
    log.info("Distinct months (dt from rcept_dt): %d", len(dts))
    log.info("First 10: %s", dts[:10])
    log.info("Last 10: %s", dts[-10:])

    # Count by month
    month_counts = df_months.groupBy("dt").count().orderBy("dt").collect()
    log.info("Row counts by month:")
    for r in month_counts:
        log.info("  dt=%s: %d rows", r["dt"], r["count"])
else:
    # Find which column can be used for date partitioning
    for c in df.columns:
        lower = c.lower()
        if "date" in lower or "dt" in lower or "year" in lower or "month" in lower or "rcept" in lower:
            log.info("Candidate date column: %s, sample: %s", c, [str(r[c])[:20] for r in df.select(c).limit(5).collect()])

    # Also try rcept_no which has format YYYYMMDD
    if "rcept_no" in df.columns:
        # rcept_no format: YYYYMMDDNNNNNN (date + serial)
        log.info("rcept_no samples: %s", [str(r["rcept_no"])[:20] for r in df.select("rcept_no").limit(5).collect()])
        df_months = df.withColumn("dt", substring(col("rcept_no"), 1, 7))
        dts = sorted([r["dt"] for r in df_months.select("dt").distinct().collect()])
        log.info("Distinct months (dt from rcept_no): %d", len(dts))
        log.info("First 10: %s", dts[:10])
        log.info("Last 10: %s", dts[-10:])

spark.stop()
print(f"RESULT: {{\"total_rows\": {total_count}}}")
"""

def run_ssm(commands, timeout=60):
    if isinstance(commands, str): commands = [commands]
    resp = ssm.send_command(InstanceIds=[INSTANCE_ID], DocumentName="AWS-RunShellScript",
                            Parameters={"commands": commands, "executionTimeout": [str(timeout)]},
                            TimeoutSeconds=max(30, timeout))
    cmd_id = resp["Command"]["CommandId"]
    waited = 0
    while waited < timeout + 15:
        time.sleep(2); waited += 2
        inv = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=INSTANCE_ID)
        s = inv["Status"]
        if s in ("Success", "Failed", "TimedOut", "Cancelled"):
            out = inv.get("StandardOutputContent", "").strip()
            err = inv.get("StandardErrorContent", "").strip()
            if s == "Success":
                for line in out.split("\n")[-40:]:
                    if line.strip(): print(f"  {line.strip()}")
            else:
                print(f"  STATUS={s}")
                if err: print(f"  ERR: {err[:500]}")
            return s
    return "TIMEOUT"

encoded = base64.b64encode(SCRIPT.encode()).decode("ascii")
print(f"Script: {len(SCRIPT)} bytes")

run_ssm(["rm -f " + REMOTE_SCRIPT])
half = len(encoded) // 2
run_ssm(["printf '%s' '" + encoded[:half] + "' > /tmp/_gd1"])
run_ssm(["printf '%s' '" + encoded[half:] + "' > /tmp/_gd2"])
run_ssm(["cat /tmp/_gd1 /tmp/_gd2 | base64 -d > " + REMOTE_SCRIPT, "rm -f /tmp/_gd1 /tmp/_gd2"])

print("Running Delta schema check (120s timeout)...")
resp = ssm.send_command(InstanceIds=[INSTANCE_ID], DocumentName="AWS-RunShellScript",
                        Parameters={"commands": [f"spark-submit --master 'local[4]' --driver-memory 4g {REMOTE_SCRIPT} 2>&1"], "executionTimeout": ["120"]},
                        TimeoutSeconds=120)
cmd_id = resp["Command"]["CommandId"]
waited = 0
while waited < 135:
    time.sleep(3); waited += 3
    inv = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=INSTANCE_ID)
    s = inv["Status"]
    if s in ("Success", "Failed", "TimedOut", "Cancelled"):
        out = inv.get("StandardOutputContent", "").strip()
        err = inv.get("StandardErrorContent", "").strip()
        print(f"\nSTATUS: {s}")
        if out:
            for line in out.split("\n")[-50:]:
                if line.strip(): print(f"  {line.strip()}")
        if err:
            print(f"\nSTDERR: {err[:500]}")
        break
