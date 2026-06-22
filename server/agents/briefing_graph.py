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


def _read_parquet_keys(keys: List[str], limit_rows: int = 50000) -> pd.DataFrame:
    """Read multiple Parquet keys from S3 and concatenate into a DataFrame.

    Args:
        keys: S3 key paths to read
        limit_rows: stop reading after accumulating this many rows

    Returns:
        Concatenated DataFrame (empty if no keys or all reads fail)
    """
    s3 = _get_s3()
    frames = []
    total = 0
    for key in keys:
        if total >= limit_rows:
            break
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            buf = io.BytesIO(obj["Body"].read())
            table = pq.read_table(buf)
            df = table.to_pandas()
            frames.append(df)
            total += len(df)
        except Exception as e:
            logger.warning("Error reading %s: %s", key, e)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ── Step Implementations ──

def load_gold_structured(state: BriefingState) -> BriefingState:
    """Step 1: Load recent Gold Structured data from S3 (3-day lookback).

    Uses a 3-day window to catch Friday reports on Monday briefings.
    Reports are rarely published on weekends.
    """
    date = state.date  # YYYYMMDD
    year = date[:4]
    month = date[4:6]
    key = f"gold/structured/year={year}/month={month}/data.parquet"

    # Build 3-day lookback window: [date-3d, date]
    from datetime import datetime as dt_dt, timedelta
    target_dt = dt_dt.strptime(date, "%Y%m%d")
    lookback_dt = target_dt - timedelta(days=3)
    lookback_compat = lookback_dt.strftime("%Y%m%d")

    try:
        from agents.data_helper import read_gold_data
        df = read_gold_data("structured")        # Delta-first(델타 정본), 실패 시 parquet
        if df is None or len(df) == 0:
            df = _read_parquet_s3(key)            # 레거시 단일 월 파티션 폴백
        # 발행일 is the report publication date (stored as YYYYMMDD or YYYY-MM-DD)
        if "발행일" in df.columns:
            df["발행일_str"] = df["발행일"].astype(str).str.replace("-", "").str.replace(".", "")
            df = df[(df["발행일_str"] >= lookback_compat) & (df["발행일_str"] <= date)]
        elif "report_date" in df.columns:
            df["report_date_str"] = df["report_date"].astype(str).str.replace("-", "")
            df = df[(df["report_date_str"] >= lookback_compat) & (df["report_date_str"] <= date)]

        state.structured = df.to_dict("records")
        logger.info("Step 1: Gold Structured loaded — %d rows for %s (lookback >= %s)",
                     len(state.structured), date, lookback_compat)
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

    Primary: Delta material_event (fast single read, PK=event_id)
    Fallback: DartCollector Gold facts/material_event Parquet (new partitioned path)
    Legacy fallback: gold/dart/disclosure_events/dt={ym}/ (may be deleted by compaction)
    """
    target_dt = datetime.strptime(state.date, "%Y%m%d")
    start_dt = target_dt - timedelta(days=92)  # ~1 quarter for ★ consensus window

    combined = None

    # Path 1: Delta (primary, fast)
    try:
        from agents.data_helper import read_gold_data
        df_delta = read_gold_data("material_event")
        if df_delta is not None and len(df_delta) > 0:
            if "rcept_dt" in df_delta.columns:
                # Normalize: rcept_dt may have hyphens (2026-06-16 from Gold facts)
                df_delta["_rcept_dt_norm"] = df_delta["rcept_dt"].astype(str).str.replace("-", "")
                df_delta = df_delta[
                    (df_delta["_rcept_dt_norm"] >= start_dt.strftime("%Y%m%d")) &
                    (df_delta["_rcept_dt_norm"] <= target_dt.strftime("%Y%m%d"))
                ]
                df_delta = df_delta.drop(columns=["_rcept_dt_norm"])
            combined = df_delta
            logger.info("Step 3: DART loaded via Delta — %d rows", len(combined))
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
                    # Normalize: Gold facts use "2026-06-16" format (with hyphens)
                    df_facts["_rcept_dt_norm"] = df_facts["rcept_dt"].astype(str).str.replace("-", "")
                    df_facts = df_facts[
                        (df_facts["_rcept_dt_norm"] >= start_dt.strftime("%Y%m%d")) &
                        (df_facts["_rcept_dt_norm"] <= target_dt.strftime("%Y%m%d"))
                    ]
                    df_facts = df_facts.drop(columns=["_rcept_dt_norm"])
                combined = df_facts
                logger.info("Step 3: DART loaded via material_event facts — %d rows", len(combined))
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
                # Normalize: legacy rcept_dt may have hyphens
                combined["_rcept_dt_norm"] = combined["rcept_dt"].astype(str).str.replace("-", "")
                combined = combined[
                    (combined["_rcept_dt_norm"] >= start_dt.strftime("%Y%m%d")) &
                    (combined["_rcept_dt_norm"] <= target_dt.strftime("%Y%m%d"))
                ]
                combined = combined.drop(columns=["_rcept_dt_norm"])

    if combined is not None and len(combined) > 0:
        state.dart_events_df = combined
        logger.info("Step 3: DART events loaded — %d rows", len(combined))
    else:
        logger.warning("Step 3: No DART events found for the period")
        state.dart_events_df = pd.DataFrame()

    return state


def run_dart_sentiment(state: BriefingState) -> BriefingState:
    """Step 4: Run Haiku sentiment on today's DART events + quarter 정기보고서.

    Classifies:
      - All today's events (~600-700)
      - 정기보고서 (사업/반기/분기) filings in last ~92 days (~1000-2000)
    Total ~1600-2600 events, ~60-100 Haiku batches, ~1-2 min. Acceptable.
    정기보고서 sentiment is needed for ★ quarter-level DART filter.
    Non-정기보고서 events from previous days stay neutral (not classified).
    """
    if state.dart_events_df is None or state.dart_events_df.empty:
        logger.info("Step 4: No DART events to classify — skipping")
        return state

    df = state.dart_events_df

    # Default all events to neutral (safe fallback)
    if "sentiment" not in df.columns:
        df["sentiment"] = "neutral"

    # Build classification mask: today's events + quarter 정기보고서
    from datetime import datetime as _dt, timedelta as _td
    _target_dt = _dt.strptime(state.date, "%Y%m%d")
    _quarter_ago_str = (_target_dt - _td(days=92)).strftime("%Y%m%d")
    _정기보고서_pat = "사업보고서|반기보고서|분기보고서|감사보고서제출|사업연도|기업가치제고계획"
    n_today = 0
    n_q_reports = 0

    if "rcept_dt" in df.columns:
        today_mask = df["rcept_dt"] == state.date
        n_today = int(today_mask.sum())
        # 정기보고서 in last quarter
        q_report_mask = (
            df["report_nm"].str.contains(_정기보고서_pat, na=False) &
            (df["rcept_dt"] >= _quarter_ago_str) &
            (df["rcept_dt"] <= state.date)
        )
        n_q_reports = int(q_report_mask.sum())
        classify_mask = today_mask | q_report_mask
    else:
        classify_mask = pd.Series(True, index=df.index)
        n_today = len(classify_mask)

    classify_df = df[classify_mask].copy()

    if classify_df.empty:
        logger.info("Step 4: No events to classify — skipping sentiment")
        return state

    logger.info(
        "Step 4: Classifying sentiment for %d events (today=%d, q-reports=%d)",
        len(classify_df), n_today, n_q_reports,
    )

    try:
        # Ensure agents dir is importable
        import sys as _sys
        _agents_dir = os.path.dirname(os.path.abspath(__file__))
        if _agents_dir not in _sys.path:
            _sys.path.insert(0, _agents_dir)
        from dart_sentiment_agent import DartSentimentAgent  # noqa: E402
        agent = DartSentimentAgent()
        classify_df = agent.classify_sync(classify_df)

        # Merge results back
        for idx in classify_df.index:
            sent = classify_df.at[idx, "sentiment"] if "sentiment" in classify_df.columns else "neutral"
            df.at[idx, "sentiment"] = sent
            reason = classify_df.at[idx, "sentiment_reason"] if "sentiment_reason" in classify_df.columns else ""
            if reason:
                df.at[idx, "sentiment_reason"] = reason

        n_pos = (classify_df.get("sentiment") == "positive").sum() if "sentiment" in classify_df.columns else 0
        n_neg = (classify_df.get("sentiment") == "negative").sum() if "sentiment" in classify_df.columns else 0
        n_neu = len(classify_df) - n_pos - n_neg
        logger.info("Step 4: Sentiment complete — %d pos, %d neg, %d neutral", n_pos, n_neg, n_neu)

    except Exception as e:
        logger.warning("Step 4: Sentiment classification failed, using neutral: %s", e)

    state.dart_events_df = df
    return state

def load_model_predictions(state: BriefingState) -> BriefingState:
    """Step 5: Load Chanho model predictions from S3 Gold."""
        # state.date is YYYYMMDD; S3 key uses YYYY-MM-DD (see s3_upload.py line 64)
    dt_formatted = f"{state.date[:4]}-{state.date[4:6]}-{state.date[6:8]}"
    key = f"gold/model/predictions/dt={dt_formatted}/predictions.parquet"

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
      2. DART filter (1-quarter window): positive event OR 정기보고서 proxy
         (감사보고서제출/사업연도 등) within last ~92 days → narrows intersection
      3. If filtered intersection empty → fallback to report∩model (no DART signal today)
      4. Within remaining: verify BUY opinion + upside > 0%
      정기보고서 filing in last quarter counts as positive signal.
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

    # Extract all model tickers (full universe, no score threshold)
    # Score filtering happens at intersection level (best-ranked among overlap)
    model_tickers = set()
    model_score_map = {}
    if state.model_preds:
        for m in state.model_preds:
            code = str(m.get("ticker", "")).zfill(6)
            if code and code != "000000":
                model_tickers.add(code)
                model_score_map[code] = m.get("ranking_score", 0)

    # Calculate 1-quarter boundary (last ~92 days)
    from datetime import datetime as _dt, timedelta as _td
    _target_dt = _dt.strptime(state.date, "%Y%m%d")
    _quarter_ago_str = (_target_dt - _td(days=92)).strftime("%Y%m%d")

    # Build DART qualified set: tickers with positive event OR positive quarterly report
    # in the last 1 quarter. 정기보고서 filing itself is a positive signal.
    _정기보고서_pat = "사업보고서|반기보고서|분기보고서|감사보고서제출|사업연도|기업가치제고계획"
    dart_q_positive = set()   # tickers with positive-sentiment events in last quarter
    dart_q_report = set()     # tickers with 정기보고서 filings in last quarter
    dart_negative_q = set()   # tickers with negative events in last quarter (for removal)

    if state.dart_events_df is not None and not state.dart_events_df.empty:
        df = state.dart_events_df
        if "rcept_dt" in df.columns:
            q_df = df[df["rcept_dt"] >= _quarter_ago_str]
        else:
            q_df = df

        for _, row in q_df.iterrows():
            code = str(row.get("stock_code", "")).zfill(6)
            if not code or code == "000000":
                continue
            sentiment = str(row.get("sentiment", "neutral"))
            rn = str(row.get("report_nm", ""))

            if sentiment == "positive":
                dart_q_positive.add(code)
            elif sentiment == "negative":
                dart_negative_q.add(code)

            # 정기보고서 filing in last quarter counts as positive signal
            # (사업보고서/반기보고서/분기보고서 contain substantial financial data)
            import re as _re
            if _re.search(_정기보고서_pat, rn):
                dart_q_report.add(code)

    # INNER JOIN: reports ∩ model
    intersection = report_tickers & model_tickers

    # Apply DART 1-quarter filter: must have (positive event OR quarterly report) in last quarter
    _dart_qualified = dart_q_positive | dart_q_report
    _original_intersection = report_tickers & model_tickers
    if _dart_qualified:
        _filtered = _original_intersection & _dart_qualified
        # Remove tickers with ONLY negative events and no positive/quarterly offsets
        _filtered -= (dart_negative_q - dart_q_positive - dart_q_report)
        if _filtered:
            intersection = _filtered
            logger.info(
                "Step 6: DART quarter filter PASS — %d pos, %d report, %d neg; intersection=%d",
                len(dart_q_positive), len(dart_q_report), len(dart_negative_q), len(intersection),
            )
        else:
            # Fallback: DART filter is too strict, use report∩model with note
            logger.warning(
                "Step 6: DART quarter filter EMPTY (pos=%d report=%d neg=%d) — fallback to report∩model=%d",
                len(dart_q_positive), len(dart_q_report), len(dart_negative_q), len(_original_intersection),
            )
            intersection = _original_intersection
    else:
        logger.info("Step 6: No DART quarter data — skipping DART filter")


    # Safe scalar extraction helpers (defined at function scope)
    def _safe_str(val):
        if val is None:
            return ""
        try:
            s = str(val)
            return "" if s in ("None", "[]", "nan") else s
        except Exception:
            return ""

    def _safe_float(val, default=0.0):
        """Safely convert a value to float, handling numpy arrays."""
        if val is None:
            return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    # Build LLM lookup: report_id → {reason, risks, keywords}
    llm_lookup = {}
    for row in state.llm_data:
        rid = row.get("report_id", "")
        reason_s = _safe_str(row.get("reason"))
        risks_s = _safe_str(row.get("risks"))
        if rid and (reason_s or risks_s):
            llm_lookup[rid] = {
                "reason": reason_s,
                "risks": risks_s,
                "keywords": _safe_str(row.get("keywords")),
            }

    # Within intersection: verify BUY opinion + upside > 0
    stars = []
    for ticker in intersection:
        reports = [
            r for r in state.structured
            if str(r.get("종목코드", "")).zfill(6) == ticker
            and str(r.get("투자의견", "")).upper() == "BUY"
            and _safe_float(r.get("상승여력_pct", 0)) > 0
        ]
        if not reports:
            continue
        # Attach LLM reason/risks/keywords via report_id
        for r in reports:
            rid = r.get("report_id", "")
            if rid in llm_lookup:
                r["reason"] = llm_lookup[rid]["reason"]
                r["risks"] = llm_lookup[rid]["risks"]
                r["keywords"] = llm_lookup[rid]["keywords"]

        # Find matching model prediction
        model_pred = next(
            (m for m in state.model_preds
             if str(m.get("ticker", "")).zfill(6) == ticker),
            {},
        )

        # ★ DART content: last quarter's most recent 정기보고서 summary
        # + most recent 주요사항보고서 event title
        dart_quarterly_summary = ""
        dart_recent_event_title = ""
        if state.dart_events_df is not None and not state.dart_events_df.empty:
            df = state.dart_events_df
            ticker_mask = df["stock_code"].astype(str).str.zfill(6) == ticker
            
            # Last quarter date range
            target_dt_q = datetime.strptime(state.date, "%Y%m%d")
            q_start = (target_dt_q - timedelta(days=90)).strftime("%Y%m%d")
            date_mask = (df["rcept_dt"] >= q_start) & (df["rcept_dt"] <= state.date)
            
            ticker_df = df[ticker_mask & date_mask].copy()
            
            if not ticker_df.empty:
                # Most recent 정기보고서 (A-type: 분기/반기/사업보고서)
                regular_reports = ticker_df[
                    ticker_df["report_nm"].str.contains(
                        "분기보고서|반기보고서|사업보고서|정기공시", na=False
                    )
                ]
                if not regular_reports.empty:
                    latest = regular_reports.sort_values("rcept_dt", ascending=False).iloc[0]
                    dart_quarterly_summary = str(latest.get("report_nm", ""))
                
                # Most recent 주요사항보고서 event title
                b_type = ticker_df[
                    ticker_df["report_nm"].str.contains(
                        "주요사항보고|유상증자|무상증자|감자|부도발생|회생절차|합병|분할|"
                        "영업양도|주요자산처분|타법인|최대주주변경|자기주식취득|"
                        "단일판매|소송제기|CB발행|BW발행|EB발행", na=False
                    )
                ]
                if not b_type.empty:
                    latest_b = b_type.sort_values("rcept_dt", ascending=False).iloc[0]
                    dart_recent_event_title = str(latest_b.get("report_nm", ""))
        
        # Build dart_info dict instead of full events list
        dart_info = {
            "quarterly_summary": dart_quarterly_summary,
            "recent_event_title": dart_recent_event_title,
        }

        # Get 종목명 from the first report
        종목명 = reports[0].get("종목명", "")
        if not 종목명:
            종목명 = ticker  # fallback to ticker code

        stars.append({
            "ticker": ticker,
            "종목명": 종목명,
            "reports": reports,
            "dart_info": dart_info,
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
    
    # Filter to today only (! is daily — not 1 month)
    if "rcept_dt" in df.columns:
        df = df[df["rcept_dt"] == state.date]
        logger.info("Step 7: DART events filtered to %s — %d rows", state.date, len(df))
    
    if df.empty:
        logger.info("Step 7: No DART events for today — skipping ! filter")
        state.exclamation_items = []
        return state
    
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

    # Include positive AND negative sentiment (exclude neutral only)
    # neutral은 필요 없고 positive/negative 둘 다 보여줌
    if "sentiment" in df.columns:
        df = df[df["sentiment"] != "neutral"]

    # Exclude tickers already in ★
    star_tickers = {s["ticker"] for s in state.star_candidates}
    if "stock_code" in df.columns:
        df = df[~df["stock_code"].astype(str).str.zfill(6).isin(star_tickers)]

    # Sort by market cap proxy (rcept_dt recency as secondary)
    if "rcept_dt" in df.columns:
        df = df.sort_values("rcept_dt", ascending=False)

    items = df.to_dict("records")

    # Deduplicate by ticker (keep first — most recent)
    # Also filter out low-value disclosures
    _low_value_patterns = [
        "권리락",               # technical price adjustment, not news
        "합병등종료보고서",     # post-hoc filing, event already done
    ]
    seen = set()
    deduped = []
    for item in items:
        code = str(item.get("stock_code", "")).zfill(6)
        if code in seen or code == "000000":
            continue
        rn = str(item.get("report_nm", ""))
        # Skip low-value disclosures
        if any(p in rn for p in _low_value_patterns):
            logger.info("Step 7: Skipping low-value item: %s — %s", code, rn[:60])
            continue
        # Strip [기재정정] from report_nm for cleaner display
        if "[기재정정]" in rn:
            item["report_nm"] = rn.replace("[기재정정]", "").strip()
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
        all_ok = True

        for i in range(0, len(text), max_len):
            chunk = text[i:i + max_len]
            url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
            payload = {
                "chat_id": int(telegram_chat_id),
                "text": chunk,
                "disable_web_page_preview": True,
            }
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code != 200:
                logger.error(
                    "Telegram send failed (chunk %d): %s",
                    i // max_len, resp.text,
                )
                all_ok = False
            else:
                logger.info("Telegram chunk %d sent", i // max_len)

        if all_ok:
            logger.info("Step 9: Briefing sent to Telegram")
        else:
            state.error = "Telegram send partially failed"
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
    g.add_edge("sentimen