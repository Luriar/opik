"""Run briefing pipeline on EC2 and send via Telegram. Upload to EC2 then execute via SSM."""
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("run_briefing")

sys.path.insert(0, "/home/ec2-user/opik-server")

from agents.briefing_graph import BriefingGraph

date = sys.argv[1] if len(sys.argv) > 1 else "20260319"
logger.info("Running briefing for date=%s", date)

graph = BriefingGraph()
state = graph.run(date)

print("=== BRIEFING COMPLETE ===")
print(f"Stars: {len(state.star_candidates)}")
print(f"Excl: {len(state.exclamation_items)}")
print(f"Reports loaded: {len(state.structured)}")
print(f"LLM rows loaded: {len(state.llm_data)}")
dart_rows = len(state.dart_events_df) if state.dart_events_df is not None else 0
print(f"DART rows loaded: {dart_rows}")
print(f"Error: {state.error}")
print(f"Briefing length: {len(state.final_briefing)} chars")
print("")

if state.final_briefing:
    print("=== BRIEFING TEXT ===")
    print(state.final_briefing)
else:
    print("No briefing text generated.")
