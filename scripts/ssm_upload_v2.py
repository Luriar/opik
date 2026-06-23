"""
Upload spark_silver_to_delta.py to EC2 via SSM
Write a helper Python script on EC2 that creates the target file using hex encoding.
Hex chars (0-9a-f) are completely safe in bash single quotes.
"""
import boto3, time, sys

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

# Read file content
with open("/sessions/keen-laughing-carson/mnt/opik/spark_jobs/spark_silver_to_delta.py", "r", encoding="utf-8") as f:
    content = f.read()

print(f"File: {len(content)} bytes, {len(content.splitlines())} lines")

# Hex encode - completely safe in bash single quotes
hex_encoded = content.encode("utf-8").hex()
print(f"Hex: {len(hex_encoded)} chars")

# Create helper script content that writes the file
helper_lines = []
helper_lines.append("import os, binascii")
helper_lines.append("hex_data = " + repr(hex_encoded))
helper_lines.append("data = binascii.unhexlify(hex_data)")
helper_lines.append("os.makedirs('/home/ec2-user/spark_jobs', exist_ok=True)")
helper_lines.append("with open('/home/ec2-user/spark_jobs/spark_silver_to_delta.py', 'wb') as f:")
helper_lines.append("    f.write(data)")
helper_lines.append("print(f'Wrote {len(data)} bytes')")

helper_content = "\n".join(helper_lines)
print(f"Helper script: {len(helper_content)} chars")

# Write helper to EC2 /tmp directly using hex
helper_hex = helper_content.encode("utf-8").hex()
print(f"Helper hex: {len(helper_hex)} chars")

write_helper = "python3 -c \"exec(binascii.unhexlify('" + helper_hex + "').decode())\""
# This is too long. Let me use a different approach: write hex to a temp file, then decode

# Actually, let me split the hex into chunks using SSM echo (hex is safe in single quotes)
chunk_size = 24000
n = (len(helper_hex) + chunk_size - 1) // chunk_size
tmpfile = "/tmp/_helper_hex.txt"

print(f"Writing helper hex to {tmpfile} in {n} chunks...")
for i in range(n):
    chunk = helper_hex[i*chunk_size:(i+1)*chunk_size]
    op = ">" if i == 0 else ">>"
    cmd = "echo '" + chunk + "' " + op + " " + tmpfile
    run_ssm([cmd], 30)

# Now decode the hex into the helper script and run it
decoder_cmd = "python3 -c \"import binascii; open('/tmp/_write_delta.py','wb').write(binascii.unhexlify(open('" + tmpfile + "').read().strip()))\""
run_ssm([decoder_cmd], 30)

# Run the helper
run_ssm(["python3 /tmp/_write_delta.py"], 30)

# Verify
print("\n=== Verify ===")
run_ssm(["wc -l " + REMOTE_SCRIPT], 30)
run_ssm(["head -5 " + REMOTE_SCRIPT], 30)
run_ssm(["tail -5 " + REMOTE_SCRIPT], 30)
syn_cmd = "python3 -c \"import py_compile; py_compile.compile('" + REMOTE_SCRIPT + "', doraise=True); print('SYNTAX OK')\""
run_ssm([syn_cmd], 30)

# Cleanup
run_ssm(["rm -f /tmp/_helper_hex.txt /tmp/_write_delta.py"], 15)
