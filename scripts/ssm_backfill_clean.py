"""
Clean start backfill - delete existing delta tables and recreate with OVERWRITE.
"""
import boto3, time, base64

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
REMOTE_SCRIPT = "/home/ec2-user/spark_jobs/spark_silver_to_delta.py"
ssm = boto3.client("ssm", region_name=REGION)

SCRIPT = r'''import os, sys, logging
from pyspark.sql import SparkSession

log = logging.getLogger()
log.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def spark():
    return SparkSession.builder.appName("OPIK-Delta-Fresh").master("local[4]").config("spark.driver.memory","6g").config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider").config("spark.databricks.delta.schema.autoMerge.enabled","true").getOrCreate()

def collect_partitions(sp, base, label):
    months = []
    import subprocess
    for y in range(2020, 2027):
        for m in range(1, 13):
            if y == 2026 and m > 6: break
            path = f"{base}year={y}/month={m:02d}/"
            s3path = path.replace("s3a://", "s3://")
            r = subprocess.run(["aws", "s3", "ls", s3path], capture_output=True, timeout=5)
            if r.returncode == 0 and len(r.stdout) > 0:
                df = sp.read.parquet(path)
                n = df.count()
                if n > 0:
                    months.append((y, m, df, n))
                    log.info("%s %04d-%02d: %d rows", label, y, m, n)
    return months

def run():
    sp = spark()
    sp.sparkContext.setLogLevel("WARN")

    # Delete existing delta tables
    log.info("Deleting existing delta tables...")
    import subprocess
    subprocess.run(["aws", "s3", "rm", "--recursive", "s3://s3-opik-bucket/delta/gold_db/structured/"], timeout=30)
    subprocess.run(["aws", "s3", "rm", "--recursive", "s3://s3-opik-bucket/delta/gold_db/embeddings/"], timeout=30)
    subprocess.run(["aws", "s3", "rm", "--recursive", "s3://s3-opik-bucket/delta/gold_db/disclosure_events/"], timeout=30)
    log.info("Deleted old delta tables.")

    # Structured + Embeddings: collect all parquet files
    structured = collect_partitions(sp, "s3a://s3-opik-bucket/gold/structured/", "S")
    embeddings = collect_partitions(sp, "s3a://s3-opik-bucket/gold/embeddings/", "E")

    if structured:
        s_df = structured[0][3]
        for _, _, df, _ in structured[1:]:
            s_df = s_df.unionByName(df)
        log.info("Writing structured: %d rows", s_df.count())
        s_df.write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed","false").save("s3a://s3-opik-bucket/delta/gold_db/structured")
        log.info("Structured written OK")

    if embeddings:
        e_df = embeddings[0][3]
        for _, _, df, _ in embeddings[1:]:
            e_df = e_df.unionByName(df)
        log.info("Writing embeddings: %d rows", e_df.count())
        e_df.write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed","false").save("s3a://s3-opik-bucket/delta/gold_db/embeddings")
        log.info("Embeddings written OK")

    # Disclosure
    disc_months = []
    for y in range(2024, 2027):
        for m in range(1, 13):
            if y == 2024 and m < 8: continue
            if y == 2026 and m > 3: break
            path = f"s3a://s3-opik-bucket/gold/dart/disclosure_events/dt={y}-{m:02d}/"
            s3path = f"s3://s3-opik-bucket/gold/dart/disclosure_events/dt={y}-{m:02d}/"
            import subprocess
            r = subprocess.run(["aws", "s3", "ls", s3path], capture_output=True, timeout=5)
            if r.returncode == 0 and len(r.stdout) > 0:
                df = sp.read.parquet(path)
                n = df.count()
                if n > 0:
                    disc_months.append(df)
                    log.info("D %04d-%02d: %d rows", y, m, n)

    if disc_months:
        d_df = disc_months[0]
        for df in disc_months[1:]:
            d_df = d_df.unionByName(df)
        log.info("Writing disclosure: %d rows", d_df.count())
        d_df.write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed","false").save("s3a://s3-opik-bucket/delta/gold_db/disclosure_events")
        log.info("Disclosure written OK")

    totals = {
        "structured": sum(n for _,_,_,n in structured),
        "embeddings": sum(n for _,_,_,n in embeddings),
        "disclosure": sum(d.count() for d in disc_months) if disc_months else 0,
    }
    print(f"BACKFILL OK: {totals}")
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

print("Sending clean backfill (delete old + union all months + overwrite)...")
resp = ssm.send_command(InstanceIds=[INSTANCE_ID], DocumentName="AWS-RunShellScript", Parameters={"commands": ["cd /home/ec2-user/spark_jobs && spark-submit --master 'local[4]' --driver-memory 6g /home/ec2-user/spark_jobs/spark_silver_to_delta.py --backfill 2>&1"], "executionTimeout": ["600"]}, TimeoutSeconds=600)
print(f"CommandId: {resp['Command']['CommandId']}")
