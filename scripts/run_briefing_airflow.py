#!/usr/bin/env python3
"""Airflow Briefing runner — called from BashOperator in daily_briefing.py.

Usage: python run_briefing_airflow.py <YYYYMMDD>

Expects: /opt/airflow/opik/.env (sourced for S3/AWS credentials)
         /opt/airflow/opik/server/agents/briefing_graph.py

Deployed to: /home/ec2-user/airflow/opik/scripts/ (mounted as /opt/airflow/opik/scripts/)
"""
import sys
import os

sys.path.insert(0, "/opt/airflow/opik")

# ── Load .env (Airflow-style: key=value, no export needed) ──
env_path = "/opt/airflow/opik/.env"
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("\"'"))

from server.agents.briefing_graph import run_briefing_pipeline

ds = sys.argv[1]
print(f"Starting briefing for {ds}")
result = run_briefing_pipeline(ds)
print(f"Complete: star={result['star_count']} excl={result['exclamation_count']} "
      f"reports={result['report_count']} dart={result['dart_count']} error={result['error']}")

if result.get("error") and "Telegram" not in str(result.get("error", "")):
    raise SystemExit(f"Briefing failed: {result['error']}")
