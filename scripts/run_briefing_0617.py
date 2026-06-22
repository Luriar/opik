"""Run briefing pipeline for 2026-06-17."""
import sys
sys.path.insert(0, '/opt/airflow/opik')
from server.agents.briefing_graph import run_briefing_pipeline

r = run_briefing_pipeline("20260617")
print(f'Stars: {r["star_count"]}')
print(f'Exclamations: {r["exclamation_count"]}')
print(f'Reports: {r["report_count"]}')
print(f'DART: {r["dart_count"]}')
print(f'Error: {r["error"]}')
fb = r.get("final_briefing", "")
print(f'Briefing ({len(fb)} chars):')
print(fb[:3000])
