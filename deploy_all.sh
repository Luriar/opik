#!/usr/bin/env bash
# =============================================================================
# OPIK Delta ?•ë³¸ ?„í™˜ ???µí•© ë°°í¬ ?¤í¬ë¦½íŠ¸ (2026-06-22)
#
#   ./deploy_all.sh preflight   # ?½ê¸°?„ìš©: ?¸ìŠ¤???¤ì œ ê²½ë¡œ/ì»¨í…Œ?´ë„ˆ/ë²„ì „ ?•ì¸
#   ./deploy_all.sh deploy      # ?¤ì œ ë°°í¬: ë°±í•„?’ì„œë²„â†’DAG?’ì¬?Œì‹±?’unpause?’ìŠ¤ëª¨í¬
#
# ??ë°˜ë“œ??"ë³¸ì¸ ?˜ê²½"?ì„œ ?¤í–‰??ê²????•ìƒ git + aws CLI + ?¬ë°”ë¥?ë¡œì»¬ ?Œì¼???ˆëŠ” ê³?
#   (Cowork ?Œë“œë°•ìŠ¤??git ê¹¨ì§ + ?Œì¼ ë§ˆìš´??ë¶ˆì•ˆ?•ìœ¼ë¡?ë°°í¬ ë¶ˆê?)
#
# ?¬ì „: ?„ë˜ CONFIGê°€ ?¤ì œ ?¸ìŠ¤?¸ì? ë§ëŠ”ì§€ `preflight`ë¡?ë¨¼ì? ?•ì¸?˜ê³  ?˜ì •?˜ë¼.
#       ?¹íˆ compose ?Œì¼????.yaml/.yml)?´ë¼ DAGS_DIR/SCHEDULERê°€ ?˜ê²½ë§ˆë‹¤ ?¤ë? ???ˆë‹¤.
# =============================================================================
set -euo pipefail

# ?€?€ .env ë¡œë“œ(AWS ?ê²©ì¦ëª…/ë¦¬ì „) ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$REPO_ROOT/.env" ]]; then set -a; source "$REPO_ROOT/.env"; set +a; fi

# ?€?€ CONFIG (preflight ê²°ê³¼ë¡??•ì¸/?˜ì •) ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€
INSTANCE_ID="i-0395d9432acf6630d"                 # ?œë²„+Airflow+Spark ?¨ì¼ EC2
REGION="${S3_REGION:-ap-northeast-2}"
EC2_IP="54.180.246.253"
DEPLOY_BUCKET="${S3_BUCKET:-s3-opik-bucket}"       # S3 ë¦´ë ˆ??ë²„í‚· (deploy/ prefix)
DEPLOY_PREFIX="deploy/delta-migration"
SERVER_RUNTIME="/home/ec2-user/opik-server"        # FastAPI ?°í???ssm_deploy.pyê°€ ì²˜ë¦¬)
AIRFLOW_REPO="/home/ec2-user/airflow/opik"         # ?¸ìŠ¤???ˆí¬(=/opt/airflow/opik ë§ˆìš´??
DAGS_DIR="/home/ec2-user/airflow/dags"             # preflight ?•ì¸: ?¤ì œ DAG ?„ì¹˜
SPARK_DIR="/home/ec2-user/spark_jobs"              # maintenance DAG??cd ?˜ëŠ” ê²½ë¡œ
SCHEDULER="opik-airflow-airflow-scheduler-1"       # ???€?? llm-financial-airflow-scheduler
SPARK_SUBMIT="spark-submit --master local[4] --driver-memory 6g"
export AWS_DEFAULT_REGION="$REGION"

# ?€?€ SSM ?™ê¸° ?¤í–‰ ?¬í¼ ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€
ssm_run() {  # ssm_run "<remote shell>"  (?¨ì¼ ëª…ë ¹ ë¬¸ì?? ?´ë????‘ì??°ì˜´?œë§Œ ?¬ìš©)
  local cmd="$1" cid st i=0
  cid=$(aws ssm send-command --region "$REGION" --instance-ids "$INSTANCE_ID" \
        --document-name AWS-RunShellScript \
        --parameters "commands=[\"$cmd\"]" \
        --query Command.CommandId --output text)
  while (( i++ < 600 )); do   # 20 min timeout for long-running spark jobs
    sleep 2
    st=$(aws ssm get-command-invocation --region "$REGION" --command-id "$cid" \
         --instance-id "$INSTANCE_ID" --query Status --output text 2>/dev/null || echo Pending)
    case "$st" in
      Success) aws ssm get-command-invocation --region "$REGION" --command-id "$cid" \
                 --instance-id "$INSTANCE_ID" --query StandardOutputContent --output text; return 0;;
      Failed|TimedOut|Cancelled)
        echo "  SSM $st:"; aws ssm get-command-invocation --region "$REGION" --command-id "$cid" \
          --instance-id "$INSTANCE_ID" --query StandardErrorContent --output text; return 1;;
    esac
  done
  echo "  SSM timeout"; return 1
}

relay() {  # relay <local_file> <remote_abs_path>  : S3 ë¦´ë ˆ???…ë¡œ????SSM ?¤ìš´ë¡œë“œ
  local local_f="$1" remote="$2" base; base="$(basename "$remote")"
  [[ -f "$local_f" ]] || { echo "  MISSING local: $local_f"; return 1; }
  aws s3 cp "$local_f" "s3://$DEPLOY_BUCKET/$DEPLOY_PREFIX/$base" --region "$REGION" >/dev/null
  ssm_run "mkdir -p $(dirname "$remote") && aws s3 cp s3://$DEPLOY_BUCKET/$DEPLOY_PREFIX/$base $remote --region $REGION && echo synced $remote \$(wc -l < $remote)L"
}

# ?€?€ PREFLIGHT (?½ê¸°?„ìš©) ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€
preflight() {
  echo "== ë¡œì»¬ git ?íƒœ =="; git -C "$REPO_ROOT" status --short 2>/dev/null || echo "(git ?•ì¸ ë¶ˆê?)"
  echo "== ?¸ìŠ¤???¤ì œ ê²½ë¡œ/ì»¨í…Œ?´ë„ˆ ?•ì¸ (CONFIG?€ ?€ì¡? =="
  ssm_run "echo '[server runtime]'; ls -la $SERVER_RUNTIME/agents/briefing_graph.py 2>&1; echo '[airflow repo server]'; ls -la $AIRFLOW_REPO/server/agents/briefing_graph.py 2>&1; echo '[dags cand1] '$DAGS_DIR; ls $DAGS_DIR/maintenance/ 2>&1; echo '[dags cand2] /home/ec2-user/airflow/dags'; ls /home/ec2-user/airflow/dags/maintenance/ 2>&1; echo '[spark]'; ls -la $SPARK_DIR/gold_to_delta.py 2>&1; echo '[scheduler containers]'; docker ps --format '{{.Names}}' 2>&1 | grep -i sched"
  echo "== ?•ì¸ ??CONFIGë¥?ë§ì¶”ê³? ./deploy_all.sh deploy =="
}

# ?€?€ DEPLOY (?œì„œ: ë°±í•„ë¡?Delta ë¨¼ì? ì±„ìš°ê³????½ê¸° ?„í™˜) ?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€?€
deploy() {
  echo "[1/7] Spark ??ë°°í¬ ??$SPARK_DIR"
  relay "$REPO_ROOT/spark_jobs/gold_to_delta.py" "$SPARK_DIR/gold_to_delta.py"

  echo "[2/7] Delta ì´ˆê¸° ?ì¬(material_event ?ì„±, PK=event_id) ???½ê¸° ?„í™˜ ?„ì— ë¨¼ì?"
  ssm_run "cd $SPARK_DIR && $SPARK_SUBMIT gold_to_delta.py --date 20260622 2>&1 | tail -25"

  echo "[3/7] ?œë²„ ?°í???ë°°í¬ (ê¸°ì¡´ ssm_deploy.py ?¬ì‚¬?? opik_server/dart_query/agents/* ???¬ì‹œ??"
  ( cd "$REPO_ROOT/server" && python ssm_deploy.py )

  echo "[4/7] DAG 3ì¢?+ Airflowì¸?briefing_graph + Briefing runner ë°°í¬"
  relay "$REPO_ROOT/dags/gold/structured.py"                         "$DAGS_DIR/gold/structured.py"
  relay "$REPO_ROOT/dags/gold/embedding.py"                          "$DAGS_DIR/gold/embedding.py"
  relay "$REPO_ROOT/dags/maintenance/dag_maintenance_delta_faiss.py" "$DAGS_DIR/maintenance/dag_maintenance_delta_faiss.py"
  relay "$REPO_ROOT/server/agents/briefing_graph.py"                 "$AIRFLOW_REPO/server/agents/briefing_graph.py"

  echo "[5/7] ?¤ì?ì¤„ëŸ¬ DAG ?¬íŒŒ???ë™ ~30s, ?•ì • ?„í•´ reserialize ?¸ë¦¬ê±?"
  ssm_run "docker exec $SCHEDULER airflow dags reserialize 2>&1 | tail -3 || true"

  echo "[6/7] maintenance DAG unpause (??gold Dataset ?¸ë¦¬ê±??œì„±??"
  ssm_run "docker exec $SCHEDULER airflow dags unpause dag_maintenance_delta_faiss 2>&1 | tail -3"

  echo "[7/7] ?¤ëª¨?? ?¼ë°°ì¹?ë¨¸ì? + ?œë²„ ?¬ìŠ¤/ì§ˆì˜"
  ssm_run "cd $SPARK_DIR && $SPARK_SUBMIT gold_to_delta.py --date \$(date +%Y%m%d) 2>&1 | tail -10"
  echo "--- /health ---"; curl -s "http://$EC2_IP:8000/health" || true; echo
  echo "--- /chat (ê³µì‹œ) ---"
  curl -s -XPOST "http://$EC2_IP:8000/v2/chat" -H 'Content-Type: application/json' \
       -d '{"message":"ìµœê·¼ ????ì£¼ìš” ê³µì‹œ ?Œë ¤ì¤?,"top_k":5}' | head -c 600; echo
  echo
  echo "=== DONE ==="
  echo "?•ì¸: ë¡œê·¸??'material_event loaded via Delta', 'Date scan complete (Delta)' ?œì‹œ ?? ?±ê³µ."
  echo "?•ìƒ ?•ì¸ ?? êµ??Œì´ë¸?delta/gold_db/disclosure_events(1ì»¤ë°‹ ê³ ì•„) ?ê¸° ê¶Œì¥."
}

case "${1:-}" in
  preflight) preflight ;;
  deploy)    deploy ;;
  *) echo "usage: $0 {preflight|deploy}"; exit 1 ;;
esac
