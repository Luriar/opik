"""
SSM: Check gold/dart_delta/disclosure_events Delta table, then convert to partitioned Parquet.
Two-phase: first check schema (Phase 1), then convert (Phase 2).
Uses async send_command pattern (no blocking wait for spark-submit result).
"""
import boto3, time, base64, sys, json

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
ssm = boto3.client("ssm", region_name=REGION)

# --- PHASE 1: Check script ---
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
total = df.count()
log.info("Total rows: %d", total)
log.info("Schema:")
for f in df.schema.fields:
    log.info("  %s: %s null=%s", f.name, f.dataType, f.nullable)
log.info("Columns: %s", df.columns)
# Show sample
for row in df.limit(3).collect():
    d = {k: str(v)[:60] for k, v in row.asDict().items()}
    log.info("  Sample: %s", json.dumps(d, ensure_ascii=False))
# rcept_dt analysis
if "rcept_dt" in df.columns:
    df2 = df.withColumn("dt", substring(col("rcept_dt"), 1, 7))
    dts = sorted([r["dt"] for r in df2.select("dt").distinct().collect()])
    log.info("Months from rcept_dt: %d", len(dts))
    log.info("First 5: %s", dts[:5])
    log.info("Last 5: %s", dts[-5:])
    for r in df2.groupBy("dt").count().orderBy("dt").collect():
        log.info("  %s: %d", r["dt"], r["count"])
if "rcept_no" in df.columns:
    log.info("rcept_no samples: %s", [str(r["rcept_no"])[:15] for r in df.select("rcept_no").limit(5).collect()])
if "corp_code" in df.columns:
    log.info("corp_code samples: %s", [str(r["corp_code"]) for r in df.select("corp_code").limit(5).collect()])
if "stock_code" in df.columns:
    log.info("stock_code samples: %s", [str(r["stock_code"]) for r in df.select("stock_code").limit(5).collect()])
if "report_nm" in df.columns:
    log.info("report_nm samples: %s", [str(r["report_nm"])[:40] for r in df.select("report_nm").limit(5).collect()])
if "text" in df.columns:
    log.info("text length samples: %s", [len(str(r["text"] or "")) for r in df.select("text").limit(5).collect()])
spark.stop()
print(f"RESULT: {json.dumps({'total': total})}")
"""

def run_ssm_nowait(commands, timeout=120):
    """Send SSM command and return CommandId immediately (non-blocking)."""
    if isinstance(commands, str): commands = [commands]
    resp = ssm.send_command(InstanceIds=[INSTANCE_ID], DocumentName="AWS-RunShellScript",
                            Parameters={"commands": commands, "executionTimeout": [str(timeout)]},
                            TimeoutSeconds=max(30, timeout))
    return resp["Command"]["CommandId"]

def run_ssm_wait(commands, timeout=60):
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

def upload_script(script_content, remote_path):
    encoded = base64.b64encode(script_content.encode()).decode("ascii")
    print(f"  Uploading {len(script_content)} bytes -> {remote_path}")
    run_ssm_wait(["rm -f " + remote_path])
    half = len(encoded) // 2
    run_ssm_wait(["printf '%s' '" + encoded[:half] + "' > /tmp/_dc1"])
    run_ssm_wait(["printf '%s' '" + encoded[half:] + "' > /tmp/_dc2"])
    run_ssm_wait(["cat /tmp/_dc1 /tmp/_dc2 | base64 -d > " + remote_path, "rm -f /tmp/_dc1 /tmp/_dc2"])
    status, out, err = run_ssm_wait(["wc -c " + remote_path])
    print(f"  Verify: {out.strip()}")

# ====== PHASE 1: Upload and run check ======
print("=" * 60)
print("PHASE 1: Upload check script")
print("=" * 60)
CHECK_REMOTE = "/home/ec2-user/spark_jobs/check_dd.py"
upload_script(CHECK_SCRIPT, CHECK_REMOTE)

print("\nRunning check (async)...")
cmd_id = run_ssm_nowait([f"spark-submit --master 'local[4]' --driver-memory 4g {CHECK_REMOTE} 2>&1"], timeout=120)
print(f"  Check CommandId: {cmd_id}")
print(f"  Check status with: aws ssm get-command-invocation --command-id {cmd_id} --instance-id {INSTANCE_ID}")

# Wait for check to complete (up to 2 minutes)
waited = 0
while waited < 130:
    time.sleep(5); waited += 5
    inv = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=INSTANCE_ID)
    s = inv["Status"]
    if s in ("Success", "Failed", "TimedOut", "Cancelled"):
        out = inv.get("StandardOutputContent", "").strip()
        err = inv.get("StandardErrorContent", "").strip()
        print(f"\n  Check STATUS: {s}")
        if out:
            for line in out.split("\n")[-40:]:
                if line.strip(): print(f"    {line.strip()}")
        if err:
            print(f"  STDERR: {err[:300]}")
        break

# ====== PHASE 2: Convert script ======
print("\n" + "=" * 60)
print("PHASE 2: Upload conversion script")
print("=" * 60)

CONVERT_SCRIPT = r"""import sys, logging, json, subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, col, concat, lit

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("convert")
spark = SparkSession.builder.appName("Convert-DD").master("local[4]").config("spark.driver.memory","6g").config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider").config("spark.hadoop.fs.s3a.endpoint","s3.ap-northeast-2.amazonaws.com").config("spark.sql.adaptive.enabled","true").config("spark.sql.sources.partitionOverwriteMode","dynamic").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

SRC = "s3a://s3-opik-bucket/gold/dart_delta/disclosure_events"
DST = "s3a://s3-opik-bucket/gold/dart/disclosure_events"

log.info("Reading Delta: %s", SRC)
df = spark.read.format("delta").load(SRC)
total = df.count()
log.info("Delta total rows: %d", total)

# Derive dt from rcept_dt (YYYYMMDD -> YYYY-MM)
df = df.withColumn("dt", substring(col("rcept_dt"), 1, 7))
dts = sorted([r["dt"] for r in df.select("dt").distinct().collect()])
log.info("Distinct months: %d, range: %s ~ %s", len(dts), dts[0] if dts else "N/A", dts[-1] if dts else "N/A")

# Filter to our range and write
allowed = [d for d in dts if "2024-08" <= d <= "2026-03"]
log.info("Months in range 2024-08~2026-03: %d", len(allowed))
if allowed:
    df_filtered = df.filter(col("dt").isin(allowed))
    n = df_filtered.count()
    log.info("Rows to write: %d", n)
    df_filtered.write.mode("overwrite").partitionBy("dt").option("compression","snappy").parquet(DST)
    log.info("Write complete! Verifying...")
    dv = spark.read.parquet(DST)
    vn = dv.count()
    vdts = sorted([r["dt"] for r in dv.select("dt").distinct().collect()])
    log.info("Verified: %d rows, %d partitions", vn, len(vdts))
    log.info("Partitions: %s", vdts)
    # Show row counts per partition
    for r in dv.groupBy("dt").count().orderBy("dt").collect():
        log.info("  %s: %d", r["dt"], r["count"])
    print(json.dumps({"rows": n, "partitions": len(allowed), "verified_rows": vn}))
else:
    log.warning("No partitions in range!")
spark.stop()
"""

CONVERT_REMOTE = "/home/ec2-user/spark_jobs/convert_dd.py"
upload_script(CONVERT_SCRIPT, CONVERT_REMOTE)

print("\nRunning conversion (async, 300s timeout)...")
cmd_id = run_ssm_nowait([f"spark-submit --master 'local[4]' --driver-memory 6g {CONVERT_REMOTE} 2>&1"], timeout=300)
print(f"  Convert CommandId: {cmd_id}")
print(f"  Monitor with: aws ssm get-command-invocation --command-id {cmd_id} --instance-id {INSTANCE_ID}")

# Wait for conversion to complete (up to 5 minutes)
waited = 0
while waited < 310:
    time.sleep(10); waited += 10
    inv = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=INSTANCE_ID)
    s = inv["Status"]
    if s in ("Success", "Failed", "TimedOut", "Cancelled"):
        out = inv.get("StandardOutputContent", "").strip()
        err = inv.get("StandardErrorContent", "").strip()
        print(f"\n  Convert STATUS: {s}")
        if out:
            for line in out.split("\n")[-50:]:
                if line.strip(): print(f"    {line.strip()}")
        if err:
            print(f"  STDERR: {err[:500]}")
        break
