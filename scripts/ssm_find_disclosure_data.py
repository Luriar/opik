"""
SSM: Find where disclosure_events data actually lives in S3.
Checks multiple possible paths.
"""
import boto3, time, base64

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
ssm = boto3.client("ssm", region_name=REGION)

SCRIPT = r"""import sys, logging, subprocess, json

log = logging.getLogger()
log.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BUCKET = "s3-opik-bucket"

def s3_ls(path):
    r = subprocess.run(["aws", "s3", "ls", path], capture_output=True, timeout=15, text=True)
    return r.stdout.strip()

# Check various possible paths
paths = [
    "s3://s3-opik-bucket/gold/dart/disclosure_events/",
    "s3://s3-opik-bucket/gold/dart_delta/disclosure_events/",
    "s3://s3-opik-bucket/delta/gold_db/disclosure_events/",
    "s3://s3-opik-bucket/delta/dart/disclosure_events/",
    "s3://s3-opik-bucket/delta/dart/disclosure_scores/",
    "s3://s3-opik-bucket/gold/dart_delta/",
    "s3://s3-opik-bucket/gold/dart/",
    "s3://s3-opik-bucket/delta/gold_db/",
    "s3://s3-opik-bucket/gold/dart/document_text/report_type=DISCLOSURE/",
]

for path in paths:
    log.info("Checking: %s", path)
    out = s3_ls(path)
    for line in out.split("\n")[:5]:
        if line.strip():
            log.info("  -> %s", line.strip())
    if not out.strip():
        log.info("  -> (empty)")

# Also try to find anything named disclosure_events
log.info("Searching for disclosure_events in gold/...")
r = subprocess.run(["aws", "s3", "ls", "--recursive", "s3://s3-opik-bucket/gold/dart/"], capture_output=True, timeout=30, text=True)
disclosure_files = [l for l in r.stdout.split("\n") if "disclosure" in l.lower()]
log.info("Found %d disclosure-related files in gold/dart/:", len(disclosure_files))
for f in disclosure_files[:30]:
    log.info("  %s", f.strip())

# Also check deep listing of gold/dart/document_text top levels
log.info("Checking gold/dart/document_text/ partitions...")
r2 = subprocess.run(["aws", "s3", "ls", "s3://s3-opik-bucket/gold/dart/document_text/"], capture_output=True, timeout=15, text=True)
for line in r2.stdout.strip().split("\n")[:10]:
    if line.strip():
        log.info("  %s", line.strip())
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
                for line in out.split("\n")[-30:]:
                    if line.strip(): print(f"  {line.strip()}")
            else:
                print(f"  STATUS={s}")
                if err: print(f"  ERR: {err[:500]}")
            return s
    return "TIMEOUT"

REMOTE_SCRIPT = "/home/ec2-user/spark_jobs/find_disclosure_data.py"
encoded = base64.b64encode(SCRIPT.encode()).decode("ascii")
print(f"Script: {len(SCRIPT)} bytes")

run_ssm(["rm -f " + REMOTE_SCRIPT])
half = len(encoded) // 2
run_ssm(["printf '%s' '" + encoded[:half] + "' > /tmp/_fd1"])
run_ssm(["printf '%s' '" + encoded[half:] + "' > /tmp/_fd2"])
run_ssm(["cat /tmp/_fd1 /tmp/_fd2 | base64 -d > " + REMOTE_SCRIPT, "rm -f /tmp/_fd1 /tmp/_fd2"])
run_ssm(["wc -c " + REMOTE_SCRIPT])

print("Running S3 discovery (60s timeout)...")
resp = ssm.send_command(InstanceIds=[INSTANCE_ID], DocumentName="AWS-RunShellScript",
                        Parameters={"commands": [f"python3 {REMOTE_SCRIPT} 2>&1"], "executionTimeout": ["60"]},
                        TimeoutSeconds=60)
cmd_id = resp["Command"]["CommandId"]
waited = 0
while waited < 75:
    time.sleep(3); waited += 3
    inv = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=INSTANCE_ID)
    s = inv["Status"]
    if s in ("Success", "Failed", "TimedOut", "Cancelled"):
        out = inv.get("StandardOutputContent", "").strip()
        err = inv.get("StandardErrorContent", "").strip()
        print(f"\nSTATUS: {s}")
        if out:
            for line in out.split("\n"):
                if line.strip(): print(f"  {line.strip()}")
        if err:
            print(f"\nSTDERR: {err[:500]}")
        break
