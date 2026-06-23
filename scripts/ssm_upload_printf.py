"""
Upload spark_silver_to_delta.py to EC2 via SSM using printf + base64.
printf doesn't add trailing newlines like echo does.
"""
import boto3, time, base64, sys

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
REMOTE_SCRIPT = "/home/ec2-user/spark_jobs/spark_silver_to_delta.py"

ssm = boto3.client("ssm", region_name=REGION)

def run_ssm(commands, timeout=30):
    label = commands[0][:60]
    print(f"  SSM [{label}] t={timeout}s")
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
            if s != "Success":
                print(f"    STATUS={s} err={err[:200]}")
            return s, out, err
    return "TIMEOUT", "", ""

with open("/sessions/keen-laughing-carson/mnt/opik/spark_jobs/spark_silver_to_delta.py", "r", encoding="utf-8") as f:
    content = f.read()

print(f"File: {len(content)} bytes, {len(content.splitlines())} lines")

# Delete existing
run_ssm(["rm -f " + REMOTE_SCRIPT], 15)

# Encode the ENTIRE file as base64 (this should be safe)
# Use printf instead of echo to avoid trailing newlines
encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
print(f"Base64: {len(encoded)} chars")

# Check if base64 has any problematic chars inside SSM
problematic = [c for c in encoded if c not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="]
print(f"Problematic chars in base64: {problematic}")

# printf doesn't process escape sequences in single quotes
# So printf '%s' 'BASE64_STRING' | base64 -d > file
if len(encoded) < 30000:
    cmd = "printf '%s' '" + encoded + "' | base64 -d > " + REMOTE_SCRIPT
    s, _, _ = run_ssm([cmd], 30)
    if s != "Success":
        print("FATAL: single printf failed")
        sys.exit(1)
else:
    chunk_size = 24000
    n = (len(encoded) + chunk_size - 1) // chunk_size
    for i in range(n):
        chunk = encoded[i*chunk_size:(i+1)*chunk_size]
        if i == 0:
            cmd = "printf '%s' '" + chunk + "' > " + REMOTE_SCRIPT
        else:
            cmd = "printf '%s' '" + chunk + "' >> " + REMOTE_SCRIPT
        run_ssm([cmd], 30)

import os
expected = len(content)
print(f"\nExpected size: {expected} bytes")

s, out = run_ssm("wc -c " + REMOTE_SCRIPT, 30)
actual = int(out.split()[0]) if out and out[0].isdigit() else 0
print(f"Actual size: {actual} bytes")

if actual == expected:
    print("SIZE MATCH!")
else:
    print(f"SIZE MISMATCH: expected {expected}, got {actual}")
    sys.exit(1)

# Verify content
run_ssm("wc -l " + REMOTE_SCRIPT, 30)
run_ssm("head -3 " + REMOTE_SCRIPT, 30)
run_ssm("tail -5 " + REMOTE_SCRIPT, 30)

# Syntax check via explicit temp file (avoids quoting issues)
check_script = "python3 << 'EOF'\nimport py_compile\ntry:\n    py_compile.compile('" + REMOTE_SCRIPT + "', doraise=True)\n    print('SYNTAX OK')\nexcept py_compile.PyCompileError as e:\n    print(f'SYNTAX ERROR: {e}')\nEOF"
run_ssm([check_script], 30)
