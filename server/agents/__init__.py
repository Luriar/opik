"""
OPIK LangGraph Multi-Agent Framework — Phase 2a.

Agent roster:
  SafetyAgent       — Zone C/D filtering, investment advice refusal
  IntentAgent       — Intent classification + parameter extraction
  ReportAgent       — FAISS semantic search + report summarisation (Haiku)
  DartAgent         — DART Gold Parquet queries + disclosure interpretation (Haiku)
  DartSentimentAgent— Batch sentiment classification of DART disclosures (Haiku)
  AnalysisAgent     — Cross-source synthesis / comparison / cause-tracing (Sonnet)
  ResponseComposer  — Format + source citations + safety re-check
  SupervisorAgent   — Routing decisions + graph orchestration

Briefing:
  BriefingGraph     — Daily ★/! briefing pipeline (Pandas in-process, LangGraph)

All agents share a common Bedrock invocation helper (`_call_bedrock`).
LangGraph is optional — all agents expose plain callable APIs that work
without it, and a LangGraph StateGraph wrapper is composed in supervisor.py.
"""

from .safety_agent import SafetyAgent
from .intent_agent import IntentAgent
from .report_agent import ReportAgent
from .dart_agent import DartAgent
from .dart_sentiment_agent import DartSentimentAgent
from .analysis_agent import AnalysisAgent
from .response_composer import ResponseComposer
from .supervisor import SupervisorAgent
from .briefing_graph import BriefingGraph, BriefingState

__all__ = [
    "SafetyAgent",
    "IntentAgent",
    "ReportAgent",
    "DartAgent",
    "DartSentimentAgent",
    "AnalysisAgent",
    "ResponseComposer",
    "SupervisorAgent",
    "BriefingGraph",
    "BriefingState",
]
