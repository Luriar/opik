#!/bin/bash
# OPIK Server Deploy Script (v3 — multi-agent framework + conversation_store)
# Usage: ./deploy.sh <EC2_PUBLIC_IP> [--skip-index]
#   OPIK_AGENT_ENABLED=true  → enables /v2/chat (Phase 2a agents)
#   OPIK_AGENT_ENABLED=false → disables agents, uses /chat only

set -e

EC2_IP="${1:?Usage: $0 <EC2_PUBLIC_IP> [--skip-index]}"
SKIP_INDEX="${2:-}"
SSH_KEY="${SSH_KEY:-~/.ssh/key-ju.pem}"
REMOTE_USER="ec2-user"

echo "=== Deploying OPIK Server to $EC2_IP ==="
echo "  OPIK_AGENT_ENABLED=${OPIK_AGENT_ENABLED:-true}"

echo "[1/6] Copying server files..."
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$REMOTE_USER@$EC2_IP" \
  "mkdir -p ~/opik-server/prompts ~/opik-server/agents"

scp -i "$SSH_KEY" opik_server.py agent_integration.py requirements.txt opik-server.service \
  "$REMOTE_USER@$EC2_IP:~/opik-server/"
scp -i "$SSH_KEY" conversation_store.py dart_query.py intent_parser.py \
  "$REMOTE_USER@$EC2_IP:~/opik-server/"
scp -i "$SSH_KEY" prompts/system.md prompts/intent_parser.md prompts/answer_generator.md \
  "$REMOTE_USER@$EC2_IP:~/opik-server/prompts/"
scp -i "$SSH_KEY" agents/*.py "$REMOTE_USER@$EC2_IP:~/opik-server/agents/"

echo "[2/6] Installing Python dependencies..."
ssh -i "$SSH_KEY" "$REMOTE_USER@$EC2_IP" \
  "cd ~/opik-server && pip3 install --upgrade pip && pip3 install -r requirements.txt"

echo "[3/6] Setting up systemd service..."
ssh -i "$SSH_KEY" "$REMOTE_USER@$EC2_IP" \
  "sudo cp ~/opik-server/opik-server.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable opik-server"

echo "[4/6] Restarting service..."
if [ "$SKIP_INDEX" = "--skip-index" ]; then
    echo "  --skip-index: restart only"
    ssh -i "$SSH_KEY" "$REMOTE_USER@$EC2_IP" "sudo systemctl restart opik-server && sleep 3"
else
    ssh -i "$SSH_KEY" "$REMOTE_USER@$EC2_IP" << 'ENDSSH'
cd ~/opik-server
python3 << 'ENDPY'
import sys; sys.path.insert(0, ".")
from opik_server import build_index_from_s3
build_index_from_s3()
ENDPY
sudo systemctl restart opik-server
sleep 3
ENDSSH
fi

echo "[5/6] Checking service status..."
ssh -i "$SSH_KEY" "$REMOTE_USER@$EC2_IP" "sudo systemctl status opik-server --no-pager" || true

echo "[6/6] Verifying deployed files on EC2..."
ssh -i "$SSH_KEY" "$REMOTE_USER@$EC2_IP" << 'ENDVERIFY'
echo '--- Agents ---'
ls -la ~/opik-server/agents/
echo '--- Agent count ---'
ls ~/opik-server/agents/*.py | wc -l
echo '--- Prompts ---'
wc -l ~/opik-server/prompts/*.md
echo '--- Key files ---'
ls -la ~/opik-server/opik_server.py ~/opik-server/agent_integration.py ~/opik-server/conversation_store.py
ENDVERIFY

echo ""
echo "=== Deploy Complete ==="
echo "Health check:     curl http://$EC2_IP:8000/health"
echo "v1 Chat (stable): curl -X POST http://$EC2_IP:8000/chat -H 'Content-Type: application/json' -d '{\"message\":\"삼성전자 리포트\"}'"
echo "v2 Chat (agents): curl -X POST http://$EC2_IP:8000/v2/chat -H 'Content-Type: application/json' -d '{\"message\":\"삼성전자 리포트\"}'"
