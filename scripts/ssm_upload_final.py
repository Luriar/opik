"""
Upload and run the Delta backfill on EC2.

Strategy: Write the spark_silver_to_delta.py content as a Python script on EC2
that gets its content via base64 from the SSM command itself.
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

# Read the actual disk version
with open("/sessions/keen-laughing-carson/mnt/opik/spark_jobs/spark_silver_to_delta.py", "r", encoding="utf-8") as f:
    content = f.read()

print(f"Source file: {len(content)} bytes, {len(content.splitlines())} lines")

# base64 encode - safe for echo with single quotes
encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")

# Delete any old file
run_ssm("rm -f " + REMOTE_SCRIPT)

# Write in 2 chunks to avoid SSM truncation
half = len(encoded) // 2 + 1
chunk1, chunk2 = encoded[:half], encoded[half:]

print("Writing chunk 1...")
run_ssm("printf '%s' '" + chunk1 + "' > /tmp/_delta_c1")

print("Writing chunk 2...")
run_ssm("printf '%s' '" + chunk2 + "' > /tmp/_delta_c2")

print("Assembling and decoding...")
run_ssm("cat /tmp/_delta_c1 /tmp/_delta_c2 | base64 -d > " + REMOTE_SCRIPT)

# Cleanup temps
run_ssm("rm -f /tmp/_delta_c1 /tmp/_delta_c2")

# Verify size
s, out, _ = run_ssm("wc -c " + REMOTE_SCRIPT)
actual_size = int(out.split()[0]) if out and out.split()[0].isdigit() else 0
expected_size = len(content)
print(f"Expected: {expected_size}, Got: {actual_size}")

if actual_size != expected_size:
    print(f"SIZE MISMATCH by {abs(actual_size - expected_size)} bytes - retrying with 3 chunks")
    # Try 3 chunks
    run_ssm("rm -f " + REMOTE_SCRIPT)
    third = len(encoded) // 3 + 1
    c1, c2, c3 = encoded[:third], encoded[third:2*third], encoded[2*third:]
    run_ssm("printf '%s' '" + c1 + "' > /tmp/_delta_c1")
    run_ssm("printf '%s' '" + c2 + "' > /tmp/_delta_c2")
    run_ssm("printf '%s' '" + c3 + "' > /tmp/_delta_c3")
    run_ssm("cat /tmp/_delta_c1 /tmp/_delta_c2 /tmp/_delta_c3 | base64 -d > " + REMOTE_SCRIPT)
    run_ssm("rm -f /tmp/_delta_c1 /tmp/_delta_c2 /tmp/_delta_c3")

    s, out, _ = run_ssm("wc -c " + REMOTE_SCRIPT)
    actual_size = int(out.split()[0]) if out and out.split()[0].isdigit() else 0
    print(f"After retry: Expected: {expected_size}, Got: {actual_size}")

if actual_size == expected_size:
    print("SIZE MATCH! Running syntax check...")
    s, out, _ = run_ssm("python3 -c \"import py_compile; py_compile.compile('" + REMOTE_SCRIPT + "', doraise=True); print('SYNTAX OK')\"")
    if "SYNTAX OK" in out:
        print("SYNTAX OK!")
    else:
        print(f"Syntax check result: {s} - {out[:200]}")
        print("Running tail to diagnose...")
        run_ssm("tail -20 " + REMOTE_SCRIPT)
else:
    print("Upload failed. Aborting.")
    sys.exit(1)
