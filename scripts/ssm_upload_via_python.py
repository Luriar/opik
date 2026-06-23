"""
Upload spark_silver_to_delta.py to EC2 via SSM using a Python helper script.
Instead of base64 encoding + shell echo, write a Python script on the EC2 that
writes the file directly. This avoids shell escaping issues.
"""
import boto3, time, sys

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
REMOTE_SCRIPT = "/home/ec2-user/spark_jobs/spark_silver_to_delta.py"

ssm = boto3.client("ssm", region_name=REGION)

def run_ssm(commands, timeout=30):
    label = commands[0][:60]
    print(f"  SSM [{label}] timeout={timeout}s")
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
                for line in out.split("\n")[-10:]:
                    if line.strip(): print(f"    {line.strip()}")
            else:
                print(f"    STATUS={s} err={err[:300]}")
            return s, out, err
    return "TIMEOUT", "", ""

# Read the local file content
with open("/sessions/keen-laughing-carson/mnt/opik/spark_jobs/spark_silver_to_delta.py", "r", encoding="utf-8") as f:
    content = f.read()

# Escape the content for embedding in a Python string
# We need to escape: backslashes, single quotes, newlines
escaped = content.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")

# Create a Python helper script that writes the file
helper_script = (
    "import os\n"
    f"content = '{escaped}'\n"
    f"os.makedirs(os.path.dirname('{REMOTE_SCRIPT}'), exist_ok=True)\n"
    f"with open('{REMOTE_SCRIPT}', 'w', encoding='utf-8') as f:\n"
    f"    f.write(content)\n"
    "print(f'Wrote {len(content)} bytes')\n"
)

print(f"Helper script size: {len(helper_script)} chars")

# Upload the helper script to a temp file
encoded = helper_script.encode("utf-8").hex()
print(f"Hex encoded: {len(encoded)} chars")

# Check if helper is small enough for single SSM command
if len(helper_script) < 30000:
    print("Using single hex decode approach")
    # Write helper to /tmp/helper.py using python3 -c
    write_cmd = f"python3 -c 'import binascii; open(\"/tmp/_write_delta.py\",\"wb\").write(binascii.unhexlify(\"{encoded}\"))'"
    s, out, _ = run_ssm([write_cmd], 30)
    if s != "Success":
        print("FATAL: helper upload failed")
        sys.exit(1)
else:
    # Chunk the hex
    chunk_size = 25000
    n = (len(encoded) + chunk_size - 1) // chunk_size
    print(f"Chunked: {n} parts")
    tmpfile = "/tmp/_delta_hex.txt"
    for i in range(n):
        chunk = encoded[i*chunk_size:(i+1)*chunk_size]
        cmd = f"echo -n '{chunk}' > {tmpfile}" if i == 0 else f"echo -n '{chunk}' >> {tmpfile}"
        run_ssm([cmd], 30)
    run_ssm([f"python3 -c 'import binascii; open(\"/tmp/_write_delta.py\",\"wb\").write(binascii.unhexlify(open(\"{tmpfile}\").read().strip()))'"], 30)

# Run the helper
print("Running helper script to write spark_silver_to_delta.py...")
run_ssm(["python3 /tmp/_write_delta.py"], 30)

# Verify
print("Verifying...")
run_ssm(["wc -l /home/ec2-user/spark_jobs/spark_silver_to_delta.py && head -5 /home/ec2-user/spark_jobs/spark_silver_to_delta.py"], 30)

# Clean up
run_ssm(["rm -f /tmp/_write_delta.py /tmp/_delta_hex.txt"], 15)

print("=== UPLOAD DONE ===")
