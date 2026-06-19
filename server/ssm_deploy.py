"""
SSM Deploy — upload OPIK server files to EC2 without SSH key.
Uses AWS SSM Run Command to write files and restart service.
"""
import boto3
import base64
import os
import sys
import time

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
BASE_DIR = "/home/ec2-user/opik-server"

FILES = [
    ("intent_parser.py", f"{BASE_DIR}/intent_parser.py"),
    ("opik_server.py", f"{BASE_DIR}/opik_server.py"),
    ("agent_integration.py", f"{BASE_DIR}/agent_integration.py"),
    ("conversation_store.py", f"{BASE_DIR}/conversation_store.py"),
    ("dart_query.py", f"{BASE_DIR}/dart_query.py"),
    ("prompts/system.md", f"{BASE_DIR}/prompts/system.md"),
    ("prompts/intent_parser.md", f"{BASE_DIR}/prompts/intent_parser.md"),
    ("prompts/answer_generator.md", f"{BASE_DIR}/prompts/answer_generator.md"),
]

AGENT_FILES = [
    "__init__.py",
    "safety_agent.py",
    "intent_agent.py",
    "report_agent.py",
    "dart_agent.py",
    "dart_sentiment_agent.py",
    "analysis_agent.py",
    "response_composer.py",
    "supervisor.py",
    "briefing_graph.py",
]

ssm = boto3.client("ssm", region_name=REGION)

def run_command(commands, label=""):
    """Send SSM command and wait for result."""
    if label:
        print(f"  [{label}] ", end="", flush=True)
    resp = ssm.send_command(
        InstanceIds=[INSTANCE_ID],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands},
        TimeoutSeconds=60,
    )
    cmd_id = resp["Command"]["CommandId"]

    for attempt in range(30):
        time.sleep(1)
        inv = ssm.get_command_invocation(
            CommandId=cmd_id, InstanceId=INSTANCE_ID
        )
        if inv["Status"] in ("Success", "Failed", "TimedOut", "Cancelled"):
            status = inv["Status"]
            if status == "Success":
                stdout = inv.get("StandardOutputContent", "").strip()
                if stdout:
                    # Print relevant lines
                    lines = [l for l in stdout.split("\n") if l.strip()]
                    for l in lines[:5]:
                        print(f"  {l}")
                else:
                    print("OK")
                return True
            else:
                stderr = inv.get("StandardErrorContent", "").strip()
                stdout = inv.get("StandardOutputContent", "").strip()
                print(f"FAILED: {stderr[:300] or stdout[:300]}")
                return False
    print("TIMEOUT")
    return False

def upload_file(local_name, remote_path):
    """Upload a single file via SSM."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    local_path = os.path.join(script_dir, local_name)

    try:
        with open(local_path, "r", encoding="utf-8") as f:
            content = f.read()
    except FileNotFoundError:
        print(f"  SKIP {local_name} — not found")
        return False
    except Exception as e:
        print(f"  ERROR reading {local_name}: {e}")
        return False

    size_kb = len(content) / 1024
    encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")

    # SSM command limit ~64KB; for large files, chunk via temp file
    if len(encoded) < 40000:
        cmds = [f"echo '{encoded}' | base64 -d > {remote_path}"]
        label = f"{local_name} ({size_kb:.1f}KB)"
        return run_command(cmds, label)
    else:
        # Chunked upload
        chunk_size = 30000
        chunks = [encoded[i:i+chunk_size] for i in range(0, len(encoded), chunk_size)]
        tmpfile = f"/tmp/_opik_deploy_{os.path.basename(local_name)}.b64"

        for ci, chunk in enumerate(chunks):
            if ci == 0:
                cmds = [f"echo '{chunk}' > {tmpfile}"]
            else:
                cmds = [f"echo '{chunk}' >> {tmpfile}"]
            if not run_command(cmds, f"{local_name} chunk {ci+1}/{len(chunks)}"):
                return False

        return run_command(
            [f"base64 -d {tmpfile} > {remote_path}", f"rm {tmpfile}"],
            f"{local_name} decode"
        )


# ─── MAIN ───
print(f"=== OPIK SSM Deploy → {INSTANCE_ID} ===\n")

# Step 1: Ensure directories
print("[1/4] mkdir prompts/ agents/")
run_command([f"mkdir -p {BASE_DIR}/prompts {BASE_DIR}/agents"], "mkdir")

# Step 2: Upload main files
print("[2/5] Uploading core files...")
ok = 0
for local_name, remote_path in FILES:
    if upload_file(local_name, remote_path):
        ok += 1
print(f"  Core files: {ok}/{len(FILES)}")

# Step 3: Upload agent files
print("[3/5] Uploading agent files...")
ok2 = 0
for name in AGENT_FILES:
    if upload_file(f"agents/{name}", f"{BASE_DIR}/agents/{name}"):
        ok2 += 1
print(f"  Agent files: {ok2}/{len(AGENT_FILES)}")

# Step 4: Verify files
print("[4/5] Verifying...")
run_command([
    f"ls -la {BASE_DIR}/opik_server.py {BASE_DIR}/agent_integration.py {BASE_DIR}/conversation_store.py {BASE_DIR}/prompts/",
    f"ls {BASE_DIR}/agents/*.py | wc -l",
    f"grep -c 'def v2_chat' {BASE_DIR}/opik_server.py",
    f"grep -c 'init_agents' {BASE_DIR}/agent_integration.py",
], "verify")

# Step 5: Restart service
print("[5/5] Restarting opik-server...")
run_command(["sudo systemctl restart opik-server"], "restart")
time.sleep(3)
run_command(["sudo systemctl status opik-server --no-pager | head -7"], "status")

# Health check
print("\n--- Health Check ---")
run_command(["curl -s http://localhost:8000/health"], "health")

print("\n=== Deploy Complete ===")
print(f"Test: curl http://54.180.246.253:8000/health")
