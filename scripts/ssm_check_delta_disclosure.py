"""
SSM: Check Delta disclosure_events table schema and content.
Reads from s3a://s3-opik-bucket/delta/gold_db/disclosure_events/
"""
import boto3, time, base64, sys

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
REMOTE_SCRIPT = "/home/ec2-user/spark_jobs/check_disclosure_delta.py"
ssm = boto3.client("ssm", region_name=REGION)

SCRIPT = r"""import sys, logging, json
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, col

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("check_delta")

spark = SparkSession.builder.appName("Check-Delta").master("local[4]").config("spark.driver.memory","4g").config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider").config("spark.hadoop.fs.s3a.endpoint","s3.ap-northeast-2.amazonaws.com").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

source = "s3a://s3-opik-bucket/delta/gold_db/disclosure_events"
log.info("Reading Delta table: %s", source)
df = spark.read.format("delta").load(source)

total_count = df.count()
log.info("Total rows: %d", total_count)

log.info("Schema:")
for f in df.schema.fields:
    log.info("  %s: %s (%s)", f.name, f.dataType, f.nullable)

log.info("Columns: %s", df.columns)
log.info("Sample 3 rows:")
for row in df.limit(3).collect():
    d = row.asDict()
    # Show first 10 columns
    sample = {k: d[k] for k in list(d.keys())[:10]}
    log.info("  %s", json.dumps(sample, ensure_ascii=False))

# Check rcept_dt and derive months
if "rcept_dt" in df.columns:
    df_months = df.withColumn("dt", substring(col("rcept_dt"), 1, 7))
    dts = sorted([r["dt"] for r in df_months.select("dt").distinct().collect()])
    log.info("Distinct months (dt) in rcept_dt: %d", len(dts))
    log.info("First 10: %s", dts[:10])
    log.info("Last 10: %s", dts[-10:])

    # Count by month
    from pyspark.sql.functions import desc
    month_counts = df_months.groupBy("dt").count().orderBy("dt").collect()
    log.info("Row counts by month:")
    for r in month_counts:
        log.info("  dt=%s: %d rows", r["dt"], r["count"])

    # Count how many rcept_dt are NULL
    null_count = df.filter(col("rcept_dt").isNull()).count()
    log.info("Null rcept_dt: %d", null_count)

# Also check alternative date columns
for date_col in ["rcept_date", "rcept_dttm", "bsns_year", "report_date"]:
    if date_col in df.columns:
        log.info("Column %s sample (5):", date_col)
        for r in df.select(date_col).limit(5).collect():
            log.info("  %s: %s", date_col, r[date_col])

spark.stop()
result = {"total_rows": total_count, "months_found": len(dts) if "rcept_dt" in df.columns else None}
print(f"RESULT: {json.dumps(result)}")
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
                for line in out.split("\n")[-20:]:
                    if line.strip(): print(f"  {line.strip()}")
            else:
                print(f"  STATUS={s}")
                if err: print(f"  ERR: {err[:500]}")
            return s
    return "TIMEOUT"

# Upload script
encoded = base64.b64encode(SCRIPT.encode()).decode("ascii")
print(f"Script: {len(SCRIPT)} bytes")

run_ssm(["rm -f " + REMOTE_SCRIPT])
half = len(encoded) // 2
run_ssm(["printf '%s' '" + encoded[:half] + "' > /tmp/_dc1"])
run_ssm(["printf '%s' '" + encoded[half:] + "' > /tmp/_dc2"])
run_ssm(["cat /tmp/_dc1 /tmp/_dc2 | base64 -d > " + REMOTE_SCRIPT, "rm -f /tmp/_dc1 /tmp/_dc2"])
run_ssm(["wc -c " + REMOTE_SCRIPT])

# Run check
print("Running Delta schema check (120s timeout)...")
BACKFILL_CMD = f"spark-submit --master 'local[4]' --driver-memory 4g {REMOTE_SCRIPT} 2>&1"
resp = ssm.send_command(InstanceIds=[INSTANCE_ID], DocumentName="AWS-RunShellScript",
                        Parameters={"commands": [BACKFILL_CMD], "executionTimeout": ["120"]},
                        TimeoutSeconds=120)
cmd_id = resp["Command"]["CommandId"]
print(f"Check CommandId: {cmd_id}")

# Wait for completion
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
            for line in out.split("\n")[-30:]:
                if line.strip(): print(f"  {line.strip()}")
        if err:
            print(f"\nSTDERR (last 500): {err[:500]}")
        break
