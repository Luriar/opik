#!/usr/bin/env python3
"""Wrapper script: runs the briefing pipeline for a given date argument."""
import sys
sys.path.insert(0, "/opt/airflow/opik")

from server.agents.briefing_graph import run_briefing_pipeline

if len(sys.argv) < 2:
    print("Usage: run_briefing.py YYYYMMDD", file=sys.stderr)
    sys.exit(2)

date_str = sys.argv[1]
print(f"Starting briefing for {date_str}")
result = run_briefing_pipeline(date_str)
star = result["star_count"]
excl = result["exclamation_count"]
reports = result["report_count"]
dart = result["dart_count"]
error = result["error"]
print(f"Complete: star={star} excl={excl} reports={reports} dart={dart} error={error}")

if error and "Telegram" not in str(error):
    raise SystemExit(f"Briefing failed: {error}")
