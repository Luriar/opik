"""Run briefing pipeline on EC2 and capture output."""
import boto3
import time
import sys

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
DATE = sys.argv[1] if len(sys.argv) > 1 else "20260319"

ssm = boto3.client("ssm", region_name=REGION)

# Single-command approach: download + run
command = (
    f"aws s3 cp s3://s3-opik-bucket/deploy/scripts/run_briefing_ec2.py /tmp/run_briefing_ec2.py --region ap-northeast-2 && "
    f"cd /home/ec2-user/opik-server && python3 /tmp/run_briefing_ec2.py {DATE} 2>&1"
)

resp = ssm.send_command(
    InstanceIds=[INSTANCE_ID],
    DocumentName="AWS-RunShellScript",
    Parameters={"commands": [command]},
    TimeoutSeconds=120,
)

cmd_id = resp["Command"]["CommandId"]
print(f"SSM Command: {cmd_id}")
print(f"Date: {DATE}")
print("Waiting for result...")

for i in range(60):
    time.sleep(2)
    inv = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=INSTANCE_ID)
    status = inv.get("Status", "InProgress")
    if status in ("Success", "Failed", "TimedOut", "Cancelled"):
        print(f"\nStatus: {status} ({i*2}s)")
        stdout = inv.get("StandardOutputContent", "") or ""
        stderr = inv.get("StandardErrorContent", "") or ""
        if stdout:
            print(f"\n=== STDOUT ({len(stdout)} chars) ===\n{stdout}")
        if stderr:
            print(f"\n=== STDERR ===\n{stderr}")
        break
    if i % 10 == 0:
        print(f"  [{i*2}s] {status}")
