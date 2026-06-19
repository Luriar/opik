"""Trigger briefing pipeline on EC2 via SSM for a given date."""
import boto3
import time
import sys

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
DATE = sys.argv[1] if len(sys.argv) > 1 else "20260319"

ssm = boto3.client("ssm", region_name=REGION)

script = f"""
import sys
sys.path.insert(0, '/home/ec2-user/opik-server')
from agents.briefing_graph import BriefingGraph
import json

graph = BriefingGraph()
state = graph.run("{DATE}")

print("=== BRIEFING COMPLETE ===")
print(f"Stars: {{len(state.star_candidates)}}")
print(f"Excl: {{len(state.exclamation_items)}}")
print(f"Error: {{state.error}}")
print(f"Briefing length: {{len(state.final_briefing)}}")
print("")

if state.final_briefing:
    print("=== BRIEFING TEXT ===")
    print(state.final_briefing)
else:
    print("No briefing text generated.")
""".strip()

resp = ssm.send_command(
    InstanceIds=[INSTANCE_ID],
    DocumentName="AWS-RunShellScript",
    Parameters={"commands": [f"python3 -c '{script}'"]},
    TimeoutSeconds=120,
)

cmd_id = resp["Command"]["CommandId"]
print(f"SSM Command: {cmd_id}")
print(f"Date: {DATE}")

for i in range(45):
    time.sleep(2)
    inv = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=INSTANCE_ID)
    status = inv.get("Status", "InProgress")
    if status in ("Success", "Failed", "TimedOut", "Cancelled"):
        print(f"\nStatus: {status}\n")
        stdout = inv.get("StandardOutputContent", "") or ""
        stderr = inv.get("StandardErrorContent", "") or ""
        if stdout:
            print(stdout.strip())
        if stderr:
            print(f"--- STDERR ---\n{stderr.strip()}")
        break
    if i % 5 == 0:
        print(f"  [{i*2}s] {status}")
