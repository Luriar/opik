"""
Briefing Graph — Daily ★/! briefing pipeline.

Runs at 07:00 KST via Airflow dags/briefing/daily_briefing.py.
Implements Steps 1-9 of the briefing pipeline (Section 5.5):

  Step 1: Load Gold Structured (today's reports)
  Step 2: Load Gold LLM (reason/risks/keywords)
  Step 3: Load DART Disclosure Events (1 month)
  Step 4: Run DART Sentiment Agent (Haiku batch)
  Step 5: Load Model Predictions (Chanho, S3)
  Step 6: ★ Triple Consensus check (Pandas in-process)
  Step 7: ! Major Disclosure filtering
  Step 8: Compose Briefing
  Step 9: Telegram Send

Design decisions:
  - No Spark — all Pandas in-process (2-6 seconds total)
  - No composite score — triple consensus binary check
  - Single PythonOperator in Airflow (LangGraph orchestrates internals)
  - DART data: OPIK disclosure_events as Phase 2 primary source
  - ! tier: B-type major disclosures only
"""

import io
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import boto3
import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger("opik.briefing")

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
S3_BUCKET = os.environ.get("S3_BUCKET", "s3-opik-bucket")

# BriefingState — matches the TypedDict from the design doc (Section 5.6)
class BriefingState:
    """State object for the briefing pipeline (plain class — no LangGraph dependency)."""

    def __init__(self, date: str):
        self.date = date  # YYYYMMDD
        self.structured: List[dict] = []       # today's Gold Structured rows
        self.llm_data: List[dict] = []          # today's Gold LLM rows
        self.dart_events_df: Optional[pd.DataFrame] = None  # 1-month DART w/ sentiment
        self.model_preds: List[dict] = []       # Chanho 348-stock predictions
        self.intersection_tickers: List[str] = []
        self.star_candidates: List[dict] = []
        self.exclamation_items: List[dict] = []
        self.final_briefing: str = ""
        self.error: Optional[str] = None


# ── S3 Helpers ──

def _get_s3():
    return boto3.client("s3", region_name=AWS_REGION)


def _read_parquet_s3(key: str) -> pd.DataFrame:
    """Read a single Parquet file from S3."""
    s3 = _get_s3()
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    return pq.read_table(buf).to_pandas()


def _list_parquet_keys(prefix: str) -> List[str]:
    """List all Parquet keys under an S3 prefix."""
    s3 = _get_s3()
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


# ── Step Implementations ──

def load_gold_structured(state: BriefingState) -> BriefingState:
    """Step 1: Load today's Gold Structured data from S3."""
    date = state.date  # YYYYMMDD
    year = date[:4]
    month = date[4:6]
    key = f"gold/structured/year={year}/month={month}/data.parquet"

    try:
        df = _read_parquet_s3(key)
        # Filter to today's report date
        # 발행일 is the report publication date (stored as YYYYMMDD or YYYY-MM-DD)
        today_str = f"{year}-{month}-{date[6:8]}"
        today_compact = date

        if "발행일" in df.columns:
            df["발행일_str"] = df["발행일"].astype(str).str.replace("-", "")
            df = df[df["발행일_str"] == today_compact]
        elif "report_date" in df.columns:
            df["report_date_str"] = df["report_date"].astype(str).str.replace("-", "")
            df = df[df["report_date_str"] == today_compact]

        state.structured = df.to_dict("records")
        logger.info("Step 1: Gold Structured loaded — %d rows for %s", len(state.structured), date)
    except Exception as e:
        logger.warning("Step 1: Gold Structured load failed: %s", e)
        state.structured = []

    return state


def load_gold_llm(state: BriefingState) -> BriefingState:
    """Step 2: Load today's Gold LLM (reason/risks/keywords) from S3."""
    date = state.date
    year = date[:4]
    month = date[4:6]
    key = f"gold/embeddings/year={year}/month={month}/data.parquet"

    try:
        df = _read_parquet_s3(key)
        state.llm_data = df.to_dict("records")
        logger.info("Step 2: Gold LLM loaded — %d rows", len(state.llm_data))
    except Exception as e:
        logger.warning("Step 2: Gold LLM load failed: %s", e)
        state.llm_data = []

    return state


def load_dart_events(state: BriefingState) -> BriefingState:
    """Step 3: Load 1 quarter of DART disclosure events.

    Primary: Delta dart/disclosure_events (fast single read)
    Fallback: DartCollector Gold facts/material_event (new partitioned path)
    Legacy fallback: gold/dart/disclosure_events/dt={ym}/ (may be deleted by compaction)
    """
    target_dt = datetime.strptime(state.date, "%Y%m%d")
    start_dt = target_dt - timedelta(days=92)  # ~1 quarter for \u2605 consensus window

    combined = None

    # Path 1: Delta (primary, fast)
    try:
        from agents.data_helper import read_gold_data
        df_delta = read_gold_data("dart/disclosure_events")
        if df_delta is not None and len(df_delta) > 0:
            if "rcept_dt" in df_delta.columns:
                df_delta = df_delta[
                    (df_delta["rcept_dt"] >= start_dt.strftime("%Y%m%d")) &
                    (df_delta["rcept_dt"] <= state.date)
                ]
            combined = df_delta
            logger.info("Step 3: DART loaded via Delta \u2014 %d rows", len(combined))
    except Exception as e:
        logger.debug("Step 3: Delta read skipped: %s", e)

    # Path 2: DartCollector Gold material_event facts (new)
    if combined is None or len(combined) == 0:
        try:
            prefix = "gold/dart/facts/material_event/"
            all_keys = _list_parquet_keys(prefix)

            # Filter by date partitions
            months = set()
            d = start_dt
            while d <= target_dt:
                months.add((str(d.year), f"{d.month:02d}"))
                if d.month == 12:
                    d = datetime(d.year + 1, 1, 1)
                else:
                    d = datetime(d.year, d.month + 1, 1)

            relevant_keys = []
            for key in all_keys:
                m = re.search(r'rcept_year=(\d{4})/rcept_month=(\d{2})/', key)
                if m and (m.group(1), m.group(2)) in months:
                    relevant_keys.append(key)
                elif not m:
                    relevant_keys.append(key)

            if not relevant_keys and all_keys:
                relevant_keys = all_keys

            df_facts = _read_parquet_keys(relevant_keys, limit_rows=50000)
            if len(df_facts) > 0:
                if "rcept_dt" in df_facts.columns:
                    df_facts = df_facts[
                        (df_facts["rcept_dt"].astype(str) >= start_dt.strftime("%Y%m%d")) &
                        (df_facts["rcept_dt"].astype(str) <= state.date)
                    ]
                combined = df_facts
                logger.info("Step 3: DART loaded via material_event facts \u2014 %d rows", len(combined))
        except Exception as e:
            logger.debug("Step 3: material_event read skipped: %s", e)

    # Path 3: Legacy gold/dart/disclosure_events/dt= (may be deleted by compaction)
    if combined is None or len(combined) == 0:
        months_set = set()
        d = start_dt
        while d <= target_dt:
            months_set.add(d.strftime("%Y-%m"))
            d = datetime(d.year, d.month, 1) + timedelta(days=32)
            d = datetime(d.year, d.month, 1)

        dfs = []
        for ym in sorted(months_set):
            key = f"gold/dart/disclosure_events/dt={ym}/data.parquet"
            try:
                df = _read_parquet_s3(key)
                dfs.append(df)
                logger.info("Step 3 legacy: Loaded dt=%s: %d rows", ym, len(df))
            except Exception:
                pass

        if dfs:
            combined = pd.concat(dfs, ignore_index=True)
            if "rcept_dt" in combined.columns:
                combined = combined[
                    (combined["rcept_dt"] >= start_dt.strftime("%Y%m%d")) &
                    (combined["rcept_dt"] <= state.date)
                ]

    if combined is not None and len(combined) > 0:
        state.dart_events_df = combined
        logger.info("Step 3: DART events loaded \u2014 %d rows", len(combined))
    else:
        logger.warning("Step 3: No DART events found for the period")
        state.dart_events_df = pd.DataFrame()

    return state


# ── Main Pipeline ──

class BriefingGraph:
    """Runs the full briefing pipeline without LangGraph dependency.

    Usage (from Airflow DAG):
        graph = BriefingGraph()
        state = graph.run("20260619")

    Usage (from LangGraph, if installed):
        graph = build_briefing_graph()
        result = graph.invoke({"date": "20260619"})
    """

    def run(self, date: str) -> BriefingState:
        """Execute all 9 steps in sequence. Returns final BriefingState."""
        state = BriefingState(date)

        steps = [
            ("Step 1: Load Gold Structured", load_gold_structured),
            ("Step 2: Load Gold LLM", load_gold_llm),
            ("Step 3: Load DART Events", load_dart_events),
            ("Step 4: DART Sentiment Agent", run_dart_sentiment),
            ("Step 5: Load Model Predictions", load_model_predictions),
            ("Step 6: Triple Consensus", check_triple_consensus),
            ("Step 7: Major Disclosures", filter_major_disclosures),
            ("Step 8: Compose Briefing", compose_briefing),
            ("Step 9: Send Telegram", send_telegram),
        ]

        for label, fn in steps:
            logger.info("%s starting...", label)
            try:
                state = fn(state)
                if state.error:
                    logger.error("%s failed: %s", label, state.error)
                    break
            except Exception as e:
                logger.exception("%s crashed: %s", label, e)
                state.error = f"{label}: {e}"
                break

        logger.info("Briefing pipeline complete. Error: %s", state.error)
        return state


def build_briefing_graph():
    """Build LangGraph StateGraph for the briefing pipeline (optional).

    Returns compiled graph if langgraph is installed, otherwise None.
    Falls back to BriefingGraph().run() for plain execution.
    """
    try:
        from langgraph.graph import StateGraph, END
    except ImportError:
        logger.warning("langgraph not installed — using plain pipeline")
        return None

    from typing import TypedDict, Annotated, Sequence, Optional as Opt
    from langgraph.graph.message import add_messages

    class BriefingStateTyped(TypedDict):
        date: str
        structured: list
        llm_data: list
        dart_events_df: Any
        model_preds: list
        intersection_tickers: list
        star_candidates: list
        exclamation_items: list
        final_briefing: str
        error: Opt[str]

    # Wrappers that convert BriefingStateTyped → BriefingState → BriefingStateTyped
    def _wrap(fn):
        def wrapper(state: BriefingStateTyped) -> BriefingStateTyped:
            bs = BriefingState(state["date"])
            bs.structured = state.get("structured", [])
            bs.llm_data = state.get("llm_data", [])
            bs.dart_events_df = state.get("dart_events_df")
            bs.model_preds = state.get("model_preds", [])
            bs.intersection_tickers = state.get("intersection_tickers", [])
            bs.star_candidates = state.get("star_candidates", [])
            bs.exclamation_items = state.get("exclamation_items", [])
            bs.final_briefing = state.get("final_briefing", "")
            bs.error = state.get("error")

            bs = fn(bs)

            return {
                "date": state["date"],
                "structured": bs.structured,
                "llm_data": bs.llm_data,
                "dart_events_df": bs.dart_events_df,
                "model_preds": bs.model_preds,
                "intersection_tickers": bs.intersection_tickers,
                "star_candidates": bs.star_candidates,
                "exclamation_items": bs.exclamation_items,
                "final_briefing": bs.final_briefing,
                "error": bs.error,
            }
        return wrapper

    g = StateGraph(BriefingStateTyped)

    g.add_node("load_structured", _wrap(load_gold_structured))
    g.add_node("load_llm", _wrap(load_gold_llm))
    g.add_node("load_dart", _wrap(load_dart_events))
    g.add_node("sentiment", _wrap(run_dart_sentiment))
    g.add_node("load_model", _wrap(load_model_predictions))
    g.add_node("consensus", _wrap(check_triple_consensus))
    g.add_node("filter_excl", _wrap(filter_major_disclosures))
    g.add_node("compose", _wrap(compose_briefing))
    g.add_node("send", _wrap(send_telegram))

    g.set_entry_point("load_structured")
    g.add_edge("load_structured", "load_llm")
    g.add_edge("load_llm", "load_dart")
    g.add_edge("load_dart", "sentiment")
    g.add_edge("sentiment", "load_model")
    g.add_edge("load_model", "consensus")
    g.add_edge("consensus", "filter_excl")
    g.add_edge("filter_excl", "compose")
    g.add_edge("compose", "send")
    g.set_finish_point("send")

    return g.compile()


# ── Airflow entrypoint ──

def run_briefing_pipeline(date: str) -> dict:
    """Entrypoint for Airflow PythonOperator.

    Args:
        date: YYYYMMDD string (from Airflow {{ ds_nodash }})

    Returns:
        dict with summary of results
    """
    graph = BriefingGraph()
    state = graph.run(date)

    return {
        "date": date,
        "star_count": len(state.star_candidates),
        "exclamation_count": len(state.exclamation_items),
        "report_count": len(state.structured),
        "dart_count": len(state.dart_events_df) if state.dart_events_df is not None else 0,
        "briefing_length": len(state.final_briefing),
        "error": state.error,
    }
