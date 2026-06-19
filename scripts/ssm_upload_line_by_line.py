"""
Upload spark_silver_to_delta.py to EC2 via SSM, one line at a time.
Each line is base64 encoded, so special chars are not an issue.
Uses append (>>) to build the file.
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

# Read file content
with open("/sessions/keen-laughing-carson/mnt/opik/spark_jobs/spark_silver_to_delta.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

print(f"File: {len(lines)} lines")

# Delete existing file first
run_ssm(["rm -f " + REMOTE_SCRIPT], 15)

# Write each line base64 encoded
# echo 'BASE64' | base64 -d >> file
# Each base64 line decoded appends to the file without extra newlines (base64 decodes exactly)
# But we need newlines between lines. So echo 'BASE64' | base64 -d >> file && echo >> file

batch_size = 10
for batch_start in range(0, len(lines), batch_size):
    batch = lines[batch_start:batch_start + batch_size]
    cmds = []
    for line in batch:
        b64 = base64.b64encode(line.encode("utf-8")).decode("ascii")
        cmds.append("echo '" + b64 + "' | base64 -d >> " + REMOTE_SCRIPT)

    # Combine into one SSM command
    combined = " && ".join(cmds)
    s, _, _ = run_ssm([combined], 30)
    if s != "Success":
        print(f"FATAL: batch {batch_start} failed")
        sys.exit(1)
    if (batch_start + batch_size) % 50 == 0 or batch_start + batch_size >= len(lines):
        print(f"  Wrote lines {batch_start+1}-{min(batch_start+batch_size, len(lines))}/{len(lines)}")

print("All lines written. Verifying...")
run_ssm(["wc -l " + REMOTE_SCRIPT], 30)
run_ssm(["tail -5 " + REMOTE_SCRIPT], 30)
syn_cmd = "python3 -c \"import py_compile; py_compile.compile('" + REMOTE_SCRIPT + "', doraise=True); print('SYNTAX OK')\""
run_ssm([syn_cmd], 30)

# Also check total size matches
run_ssm(["wc -c " + REMOTE_SCRIPT], 30)
