"""
Upload spark_silver_to_delta.py to EC2 via SSM using base64 + echo.
base64 chars (A-Za-z0-9+/=) are safe inside single quotes in bash.
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
            if s == "Success":
                for line in out.split("\n")[-5:]:
                    if line.strip(): print(f"    {line.strip()}")
            else:
                print(f"    STATUS={s} err={err[:300]}")
            return s, out, err
    return "TIMEOUT", "", ""

# Read file
with open("/sessions/keen-laughing-carson/mnt/opik/spark_jobs/spark_silver_to_delta.py", "r", encoding="utf-8") as f:
    content = f.read()

print(f"File: {len(content)} bytes")

# base64 encode
encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
print(f"Base64: {len(encoded)} chars")

chunk_size = 25000
n = (len(encoded) + chunk_size - 1) // chunk_size
tmpfile = "/tmp/_delta_b64.txt"

if n == 1:
    cmd = "echo '" + encoded + "' | base64 -d > " + REMOTE_SCRIPT
    run_ssm([cmd], 30)
else:
    print(f"Chunks: {n}")
    for i in range(n):
        chunk = encoded[i*chunk_size:(i+1)*chunk_size]
        op = ">" if i == 0 else ">>"
        cmd = "echo '" + chunk + "' " + op + " " + tmpfile
        run_ssm([cmd], 30)
    decode_cmd = "base64 -d " + tmpfile + " > " + REMOTE_SCRIPT + "; rm -f " + tmpfile
    run_ssm([decode_cmd], 30)

# Verify with wc -l and simple python syntax check
print("\n=== Verify ===")
wc_cmd = "wc -l " + REMOTE_SCRIPT
run_ssm([wc_cmd], 30)
check_cmd = "python3 -c \"import py_compile; py_compile.compile('" + REMOTE_SCRIPT + "', doraise=True); print('SYNTAX OK')\""
run_ssm([check_cmd], 30)

# Full cat to see content
print("\n=== Full file ===")
cat_cmd = "cat -n " + REMOTE_SCRIPT
run_ssm([cat_cmd], 30)
