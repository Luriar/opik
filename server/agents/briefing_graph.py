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
    """Step 3: Load 1 month of DART disclosure events from S3 Gold.

    Uses OPIK Gold disclosure_events as Phase 2 primary source.
    """
    target_dt = datetime.strptime(state.date, "%Y%m%d")
    start_dt = target_dt - timedelta(days=30)

    # Collect all monthly partitions in range
    months = set()
    d = start_dt
    while d <= target_dt:
        months.add(d.strftime("%Y-%m"))
        d = datetime(d.year, d.month, 1) + timedelta(days=32)
        d = datetime(d.year, d.month, 1)

    dfs = []
    s3 = _get_s3()
    for ym in sorted(months):
        key = f"gold/dart/disclosure_events/dt={ym}/data.parquet"
        try:
            df = _read_parquet_s3(key)
            dfs.append(df)
            logger.info("Step 3: Loaded disclosure_events dt=%s: %d rows", ym, len(df))
        except Exception:
            logger.debug("Step 3: No disclosure_events for dt=%s", ym)

    if dfs:
        combined = pd.concat(dfs, ignore_index=True)

        # Filter to date range
        if "rcept_dt" in combined.columns:
            combined = combined[
                (combined["rcept_dt"] >= start_dt.strftime("%Y%m%d")) &
                (combined["rcept_dt"] <= state.date)
            ]

        state.dart_events_df = combined
        logger.info("Step 3: DART events loaded — %d rows", len(combined))
    else:
        logger.warning("Step 3: No DART events found for the period")
        state.dart_events_df = pd.DataFrame()

    return state


def run_dart_sentiment(state: BriefingState) -> BriefingState:
    """Step 4: Assign default neutral sentiment to all DART events.

    Full LLM sentiment classification (Haiku batch) is too slow for daily
    briefing — 12K+ events would take 60+ minutes. Sentiment is pre-computed
    offline or deferred to a separate batch job.

    For the daily briefing pipeline, all events default to "neutral" so that
    DART data is still loaded and queryable without blocking the pipeline.
    """
    if state.dart_events_df is None or state.dart_events_df.empty:
        logger.info("Step 4: No DART events to classify — skipping")
        return state

    # Assign neutral sentiment without LLM calls
    state.dart_events_df["sentiment"] = "neutral"
    total = len(state.dart_events_df)
    logger.info("Step 4: DART sentiment defaulted to neutral for %d events", total)

    return state

def load_model_predictions(state: BriefingState) -> BriefingState:
    """Step 5: Load Chanho model predictions from S3 Gold."""
    key = f"gold/model/predictions/dt={state.date}/predictions.parquet"

    try:
        df = _read_parquet_s3(key)
        state.model_preds = df.to_dict("records")
        logger.info("Step 5: Model predictions loaded — %d stocks", len(state.model_preds))
    except Exception as e:
        logger.warning("Step 5: Model predictions not found at %s: %s", key, e)
        state.model_preds = []

    return state


def check_triple_consensus(state: BriefingState) -> BriefingState:
    """Step 6: ★ Triple Consensus check (Pandas in-process).

    Algorithm:
      1. INNER JOIN: report tickers ∩ model tickers
      2. If DART sentiment available: filter to positive, remove negatives
      3. Within intersection, verify: BUY opinion + upside > 0%

    When DART sentiment is not available (all neutral), DART acts as a
    data source for reference rather than a filter — the consensus uses
    reports ∩ model only.
    """
    if not state.structured:
        logger.info("Step 6: No structured data — skipping consensus")
        return state

    # Extract unique tickers from reports
    report_tickers = set()
    for r in state.structured:
        code = str(r.get("종목코드", "")).zfill(6)
        if code and code != "000000":
            report_tickers.add(code)

    # Extract model tickers with positive ranking_score
    model_tickers = set()
    for m in state.model_preds:
        code = str(m.get("ticker", "")).zfill(6)
        if code and m.get("ranking_score", 0) > 0:
            model_tickers.add(code)

    # Extract DART sentiment tickers
    dart_positive_tickers = set()
    dart_negative_tickers = set()
    if state.dart_events_df is not None and not state.dart_events_df.empty:
        for _, row in state.dart_events_df.iterrows():
            code = str(row.get("stock_code", "")).zfill(6)
            if not code or code == "000000":
                continue
            sentiment = row.get("sentiment", "neutral")
            if sentiment == "positive":
                dart_positive_tickers.add(code)
            elif sentiment == "negative":
                dart_negative_tickers.add(code)

    # INNER JOIN: reports ∩ model (always); add DART filter if sentiment available
    intersection = report_tickers & model_tickers
    has_dart_sentiment = bool(dart_positive_tickers or dart_negative_tickers)

    if has_dart_sentiment:
        # Narrow to tickers with positive DART sentiment
        intersection &= dart_positive_tickers
        # Remove tickers with negative DART events
        intersection -= dart_negative_tickers

    logger.info(
        "Step 6: reports=%d model+=%d dart_pos=%d dart_neg=%d sentiment_available=%s → intersection=%d",
        len(report_tickers), len(model_tickers),
        len(dart_positive_tickers), len(dart_negative_tickers),
        has_dart_sentiment, len(intersection),
    )

    # Within intersection: verify BUY opinion + upside > 0
    stars = []
    for ticker in intersection:
        reports = [
            r for r in state.structured
            if str(r.get("종목코드", "")).zfill(6) == ticker
            and str(r.get("투자의견", "")).upper() == "BUY"
            and (r.get("상승여력_pct", 0) or 0) > 0
        ]
        if not reports:
            continue

        # Find matching model prediction
        model_pred = next(
            (m for m in state.model_preds
             if str(m.get("ticker", "")).zfill(6) == ticker),
            {},
        )

        # Find matching DART events
        dart_events = []
        if state.dart_events_df is not None:
            dart_mask = (
                (state.dart_events_df["stock_code"].astype(str).str.zfill(6) == ticker) &
                (state.dart_events_df["sentiment"] == "positive")
            )
            dart_events = state.dart_events_df[dart_mask].to_dict("records")

        # Get 종목명 from the first report
        종목명 = reports[0].get("종목명", "")
        if not 종목명 and dart_events:
            종목명 = dart_events[0].get("corp_name", ticker)

        stars.append({
            "ticker": ticker,
            "종목명": 종목명,
            "reports": reports,
            "dart_events": dart_events,
            "model_pred": model_pred,
        })

    state.intersection_tickers = list(intersection)
    state.star_candidates = stars
    logger.info("Step 6: ★ candidates = %d", len(stars))

    return state


def filter_major_disclosures(state: BriefingState) -> BriefingState:
    """Step 7: ! Major Disclosure filtering.

    Filters DART events to:
      - B-type major disclosures only
      - Positive sentiment
      - NOT already covered in ★ (star_candidates)
    """
    if state.dart_events_df is None or state.dart_events_df.empty:
        logger.info("Step 7: No DART events — skipping ! filter")
        state.exclamation_items = []
        return state

    df = state.dart_events_df

    # Filter to B-type (주요사항보고) major disclosures
    b_type_keywords = [
        "주요사항보고", "유상증자", "무상증자", "감자", "부도발생",
        "회생절차", "합병", "분할", "영업양도", "주요자산처분",
        "타법인출자", "타법인취득", "최대주주변경", "자기주식취득",
        "단일판매공급계약", "소송제기", "CB발행", "BW발행", "EB발행",
    ]

    # Match B-type by event_category
    if "event_category" in df.columns:
        df = df[df["event_category"].isin(["B", "B-type"]) |
                df["report_nm"].str.contains("|".join(b_type_keywords), na=False)]

    # Only positive sentiment (skip if all neutral — no LLM sentiment available)
    if "sentiment" in df.columns and (df["sentiment"] == "positive").any():
        df = df[df["sentiment"] == "positive"]

    # Exclude tickers already in ★
    star_tickers = {s["ticker"] for s in state.star_candidates}
    if "stock_code" in df.columns:
        df = df[~df["stock_code"].astype(str).str.zfill(6).isin(star_tickers)]

    # Sort by market cap proxy (rcept_dt recency as secondary)
    if "rcept_dt" in df.columns:
        df = df.sort_values("rcept_dt", ascending=False)

    items = df.to_dict("records")

    # Deduplicate by ticker (keep first — most recent)
    seen = set()
    deduped = []
    for item in items:
        code = str(item.get("stock_code", "")).zfill(6)
        if code in seen or code == "000000":
            continue
        seen.add(code)
        deduped.append(item)

    state.exclamation_items = deduped
    logger.info("Step 7: ! candidates = %d", len(deduped))

    return state


def compose_briefing(state: BriefingState) -> BriefingState:
    """Step 8: Compose the final briefing text."""
    try:
        from .response_composer import ResponseComposer
    except ImportError:
        from response_composer import ResponseComposer

    composer = ResponseComposer()

    try:
        target_dt = datetime.strptime(state.date, "%Y%m%d")
        date_display = target_dt.strftime("%Y-%m-%d")
    except ValueError:
        date_display = state.date

    report_count = len(state.structured)
    dart_count = len(state.dart_events_df) if state.dart_events_df is not None else 0

    state.final_briefing = composer.compose_briefing(
        date=date_display,
        star_items=state.star_candidates,
        exclamation_items=state.exclamation_items,
        report_count=report_count,
        dart_count=dart_count,
    )

    logger.info("Step 8: Briefing composed — %d chars", len(state.final_briefing))
    return state


def send_telegram(state: BriefingState) -> BriefingState:
    """Step 9: Send briefing to Telegram."""
    if not state.final_briefing:
        logger.warning("Step 9: No briefing content to send")
        return state

    # Import telegram sender (from existing pipeline)
    telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

    if not telegram_token or not telegram_chat_id:
        logger.warning("Step 9: Telegram credentials not configured — skipping send")
        # Still log the briefing content
        logger.info("Briefing content:\n%s", state.final_briefing[:500])
        return state

    try:
        import requests

        # Split long messages (Telegram limit: 4096 chars)
        max_len = 4000
        text = state.final_briefing

        for i in range(0, len(text), max_len):
            chunk = text[i:i + max_len]
            url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
            payload = {
                "chat_id": telegram_chat_id,
                "text": chunk,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code != 200:
                logger.error(
                    "Telegram send failed (chunk %d): %s",
                    i // max_len, resp.text,
                )
            else:
                logger.info("Telegram chunk %d sent", i // max_len)

        logger.info("Step 9: Briefing sent to Telegram")
    except Exception as e:
        logger.error("Step 9: Telegram send error: %s", e)
        state.error = f"Telegram send failed: {e}"

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
