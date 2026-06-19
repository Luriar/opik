"""
Upload spark_silver_to_delta.py to EC2 via SSM
"""
import boto3, base64, time, sys

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
REMOTE_SCRIPT = "/home/ec2-user/spark_jobs/spark_silver_to_delta.py"
LOCAL_SCRIPT = "/sessions/keen-laughing-carson/mnt/opik/spark_jobs/spark_silver_to_delta.py"

ssm = boto3.client("ssm", region_name=REGION)

def run_ssm(commands, timeout=30):
    label = commands[0][:60]
    print(f"  SSM [{label}] timeout={timeout}")
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
                for line in out.split("\n")[:10]:
                    if line.strip(): print(f"    out: {line.strip()}")
            else:
                print(f"    STATUS={s} err={err[:300]}")
            return s, out, err
    return "TIMEOUT", "", ""

print("=== mkdir ===")
run_ssm(["mkdir -p /home/ec2-user/spark_jobs"], 30)

print("=== Upload script ===")
with open(LOCAL_SCRIPT, "r", encoding="utf-8") as f:
    content = f.read()

encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
print(f"  Script: {len(content)} bytes, b64: {len(encoded)} chars")

import math
chunk_size = 25000
nchunks = math.ceil(len(encoded) / chunk_size)
tmpfile = "/tmp/_spark_delta_b64.txt"

if nchunks == 1:
    s, out, _ = run_ssm([f"echo '{encoded}' | base64 -d > {REMOTE_SCRIPT}"], 30)
    if s != "Success":
        print("FATAL: upload failed")
        sys.exit(1)
else:
    print(f"  Chunks: {nchunks}")
    for ci in range(nchunks):
        chunk = encoded[ci*chunk_size:(ci+1)*chunk_size]
        cmd = f"echo '{chunk}' > {tmpfile}" if ci == 0 else f"echo '{chunk}' >> {tmpfile}"
        s, _, _ = run_ssm([cmd], 30)
        if s != "Success":
            print(f"FATAL: chunk {ci+1} failed")
            sys.exit(1)
    s, _, _ = run_ssm([f"base64 -d {tmpfile} > {REMOTE_SCRIPT}; rm -f {tmpfile}"], 30)
    if s != "Success":
        print("FATAL: decode failed")
        sys.exit(1)

print("=== Verify ===")
run_ssm([f"wc -l {REMOTE_SCRIPT} && head -3 {REMOTE_SCRIPT}"], 30)
print("=== UPLOAD DONE ===")
