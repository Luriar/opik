"""Deploy updated opik_server.py to EC2 — download from S3, update systemd, restart."""
import boto3, subprocess, time

s3 = boto3.client("s3")

# 1. Download updated server code
s3.download_file("s3-opik-bucket", "deploy/opik_server.py", "/root/opik-server/opik_server.py")
print("Downloaded opik_server.py")

# 2. Update systemd env to Haiku
subprocess.run([
    "sed", "-i",
    "s|Environment=BEDROCK_MODEL=.*|Environment=BEDROCK_MODEL=apac.anthropic.claude-3-haiku-20240307-v1:0|",
    "/etc/systemd/system/opik-server.service"
], check=True)
print("Updated service file BEDROCK_MODEL -> Haiku")

# 3. Reload and restart
subprocess.run(["systemctl", "daemon-reload"], check=True)
subprocess.run(["systemctl", "restart", "opik-server"], check=True)
print("Service restarted, waiting...")
time.sleep(6)

# 4. Verify
result = subprocess.run(["systemctl", "is-active", "opik-server"], capture_output=True, text=True)
print("Service active:", result.stdout.strip())

# 5. Health check
import urllib.request
try:
    resp = urllib.request.urlopen("http://localhost:8000/health", timeout=5)
    print("Health:", resp.read().decode()[:500])
except Exception as e:
    print("Health check failed:", e)

# 6. Quick chat test
import json
data = json.dumps({"message": "SK하이닉스 목표주가 알려줘", "top_k": 3}).encode()
req = urllib.request.Request("http://localhost:8000/chat", data=data,
    headers={"Content-Type": "application/json"}, method="POST")
try:
    resp = urllib.request.urlopen(req, timeout=30)
    body = json.loads(resp.read().decode())
    print("Chat answer (first 300 chars):", body.get("answer", "")[:300])
    print("Elapsed:", body.get("elapsed_ms"), "ms")
except Exception as e:
    print("Chat test failed:", e)
