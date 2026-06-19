"""
Upload spark_silver_to_delta.py to EC2 via SSM in 2 chunks.
"""
import boto3, time, base64, sys

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
REMOTE_SCRIPT = "/home/ec2-user/spark_jobs/spark_silver_to_delta.py"

ssm = boto3.client("ssm", region_name=REGION)

def run_ssm(commands, timeout=30):
    if isinstance(commands, str):
        commands = [commands]
    label = commands[0][:60]
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
                print(f"    STATUS={s}")
                if err: print(f"    ERR: {err[:300]}")
            return s, out, err
    return "TIMEOUT", "", ""

with open("/sessions/keen-laughing-carson/mnt/opik/spark_jobs/spark_silver_to_delta.py", "r", encoding="utf-8") as f:
    content = f.read()

print(f"File: {len(content)} bytes")

# Delete existing
run_ssm("rm -f " + REMOTE_SCRIPT + "; rm -f /tmp/_delta_chunk*")

# base64 encode
encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
print(f"Base64: {len(encoded)} chars")

# Split in half
half = len(encoded) // 2 + 1
chunk1 = encoded[:half]
chunk2 = encoded[half:]

# Write chunk 1
print("Writing chunk 1...")
run_ssm("echo '" + chunk1 + "' > /tmp/_delta_chunk1")
# Write chunk 2
print("Writing chunk 2...")
run_ssm("echo '" + chunk2 + "' > /tmp/_delta_chunk2")

# Concatenate and decode
print("Concatenating and decoding...")
cmd = "cat /tmp/_delta_chunk1 /tmp/_delta_chunk2 | base64 -d > " + REMOTE_SCRIPT
run_ssm(cmd)

# Cleanup temp files
run_ssm("rm -f /tmp/_delta_chunk1 /tmp/_delta_chunk2")

# Verify
s, out, _ = run_ssm("wc -c " + REMOTE_SCRIPT)
expected = len(content)
actual = int(out.split()[0]) if out and out.split()[0].isdigit() else 0
print(f"Expected: {expected}, Got: {actual}")
if actual != expected:
    print("SIZE MISMATCH!")
    sys.exit(1)
else:
    print("SIZE MATCH!")

run_ssm("wc -l " + REMOTE_SCRIPT)
run_ssm("head -3 " + REMOTE_SCRIPT)
