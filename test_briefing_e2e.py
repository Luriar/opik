#!/usr/bin/env python3
"""E2E briefing test — runs directly in the Airflow container."""
import sys, os, logging, json

sys.path.insert(0, '/opt/airflow/opik')
sys.path.insert(0, '/opt/airflow/opik/server')

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='%(asctime)s %(name)s %(levelname)s %(message)s')

from server.agents.briefing_graph import run_briefing_pipeline

DATE = sys.argv[1] if len(sys.argv) > 1 else "20260617"
print(f"Starting E2E briefing test for date={DATE}", flush=True)

result = run_briefing_pipeline(DATE)

safe = {k: v for k, v in result.items() if k != "final_briefing"}
print(f"RESULT: {json.dumps(safe, default=str, ensure_ascii=False)}", flush=True)

brief = result.get("final_briefing", "")
if brief:
    print(f"BRIEFING ({len(brief)} chars):", flush=True)
    print(brief[:2000], flush=True)
    if len(brief) > 2000:
        print(f"... ({len(brief) - 2000} more chars)", flush=True)

if result.get("error"):
    print(f"ERROR: {result['error']}", flush=True)
else:
    print("E2E SUCCESS - all 9 steps completed", flush=True)
