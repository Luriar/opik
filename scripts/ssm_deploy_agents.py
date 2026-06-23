"""Deploy agent files to EC2 via SSM (no quote-escaping issues)."""
import boto3
import time

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"

ssm = boto3.client("ssm", region_name=REGION)

commands = [
    "aws s3 cp s3://s3-opik-bucket/deploy/agents/response_composer.py /home/ec2-user/opik-server/agents/response_composer.py --region ap-northeast-2",
    "cp /home/ec2-user/opik-server/agents/response_composer.py /root/opik-server/agents/response_composer.py 2>/dev/null || true",
    "echo deployed: lines=$(wc -l < /home/ec2-user/opik-server/agents/response_composer.py) bytes=$(wc -c < /home/ec2-user/opik-server/agents/response_composer.py)",
    "cd /home/ec2-user/opik-server && python3 -c 'from agents.response_composer import ResponseComposer; print(\"import ok\")'",
    "sudo systemctl restart opik-server",
    "sleep 2",
    "sudo systemctl status opik-server --no-pager | head -5",
]

resp = ssm.send_command(
    InstanceIds=[INSTANCE_ID],
    DocumentName="AWS-RunShellScript",
    Parameters={"commands": commands},
    TimeoutSeconds=60,
)

cmd_id = resp["Command"]["CommandId"]
print(f"SSM Command: {cmd_id}")

for i in range(20):
    time.sleep(1)
    inv = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=INSTANCE_ID)
    if inv.get("Status") in ("Success", "Failed"):
        print(f"Status: {inv['Status']}")
        out = inv.get("StandardOutputContent", "")
        err = inv.get("StandardErrorContent", "")
        if out:
            print(out.strip())
        if err:
            print("ERR:", err.strip()[:500])
        break
    if i % 5 == 0:
        print(f"  [{i}s] {inv.get('Status')}")
