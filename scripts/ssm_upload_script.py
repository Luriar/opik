"""
Upload a Python script to EC2 via base64+SSM.
Usage: python3 ssm_upload_script.py <script_content> <remote_path>
"""
import boto3, time, base64, sys

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
ssm = boto3.client("ssm", region_name=REGION)

def run_ssm(commands, timeout=30):
    if isinstance(commands, str): commands = [commands]
    resp = ssm.send_command(InstanceIds=[INSTANCE_ID], DocumentName="AWS-RunShellScript",
                            Parameters={"commands": commands, "executionTimeout": [str(timeout)]},
                            TimeoutSeconds=max(30, timeout))
    cmd_id = resp["Command"]["CommandId"]
    waited = 0
    while waited < timeout + 10:
        time.sleep(1); waited += 1
        inv = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=INSTANCE_ID)
        s = inv["Status"]
        if s in ("Success", "Failed", "TimedOut", "Cancelled"):
            return s, inv.get("StandardOutputContent",""), inv.get("StandardErrorContent","")
    return "TIMEOUT", "", ""

if __name__ == "__main__":
    # Read script content from first arg, remote path from second arg
    script_content = sys.argv[1]
    remote_path = sys.argv[2]

    encoded = base64.b64encode(script_content.encode()).decode("ascii")
    print(f"Script: {len(script_content)} bytes, {len(encoded)} base64")

    status, out, err = run_ssm(["rm -f " + remote_path])
    print(f"rm: {status}")

    half = len(encoded) // 2
    status, out, err = run_ssm(["printf '%s' '" + encoded[:half] + "' > /tmp/_dc1"])
    print(f"Write part1: {status}")
    if status != "Success": print(f"  ERR: {err[:200]}")

    status, out, err = run_ssm(["printf '%s' '" + encoded[half:] + "' > /tmp/_dc2"])
    print(f"Write part2: {status}")
    if status != "Success": print(f"  ERR: {err[:200]}")

    status, out, err = run_ssm(["cat /tmp/_dc1 /tmp/_dc2 | base64 -d > " + remote_path, "rm -f /tmp/_dc1 /tmp/_dc2"])
    print(f"Decode: {status}")
    if status != "Success": print(f"  ERR: {err[:200]}")

    status, out, err = run_ssm(["wc -c " + remote_path])
    print(f"Verify: {status}")
    if out: print(f"  {out.strip()}")
