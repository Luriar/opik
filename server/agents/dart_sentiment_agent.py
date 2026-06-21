
"""
DART Sentiment Agent — batch sentiment classification of DART disclosures.

Used in the daily briefing pipeline (Step 4). Takes up to 1 quarter of DART disclosure
events and classifies each as positive / negative / neutral using Haiku batch mode
(25 items per call, 2 retries on JSON parse failure).

Key design decisions:
  - Keywords alone cannot distinguish context ("유상증자" can be positive or negative)
  - LLM reads disclosure title + body text to judge context
  - Batch mode: 25 items/call, sequential processing
  - Retry: up to 2 retries on empty/invalid JSON response with temperature jitter

Output: [{ticker, stock_code, sentiment, reason}]
"""

import asyncio
import io
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict

import boto3
import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger("opik.dart_sentiment")

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
S3_BUCKET = os.environ.get("S3_BUCKET", "s3-opik-bucket")
SENTIMENT_MODEL = os.environ.get(
    "SENTIMENT_MODEL",
    "apac.anthropic.claude-3-haiku-20240307-v1:0",
)
# Batch size: how many disclosures per LLM call
BATCH_SIZE = int(os.environ.get("DART_SENTIMENT_BATCH_SIZE", "25"))
# Max concurrent LLM calls
MAX_CONCURRENT = int(os.environ.get("DART_SENTIMENT_CONCURRENT", "20"))
# Max retries for JSON parse failures
MAX_RETRIES = int(os.environ.get("DART_SENTIMENT_MAX_RETRIES", "2"))
# Delay between retries (seconds)
RETRY_DELAY = float(os.environ.get("DART_SENTIMENT_RETRY_DELAY", "1.5"))

SENTIMENT_SYSTEM_PROMPT = """당신은 한국 DART 공시의 시장 영향을 평가하는 금융 AI입니다.
주어진 공시 목록을 분석하여 각각의 sentiment를 판단하세요.

출력 형식 (JSON 배열):
[{"ticker": "005930", "sentiment": "positive", "reason": "자사주 500억 매입, 주주환원 강화"},
 {"ticker": "000660", "sentiment": "negative", "reason": "유상증자 2조, 주가희석 우려"},
 {"ticker": "035720", "sentiment": "neutral",  "reason": "정기주총 소집공고, 일상적 공시"}]

판단 기준:
- positive: 주주가치 제고, 성장 모멘텀, 재무건전성 개선을 시사하는 공시
- negative: 주가 희석, 재무 리스크, 영업 악화, 법적 리스크를 시사하는 공시
- neutral: 일상적·의례적 공시, 영향 미미, 정보 부족으로 판단 불가

중요:
- 공시유형명만 보지 말고 제목과 요약의 구체적 내용을 읽고 판단할 것
- 같은 유상증자라도 목적(시설투자 vs 채무상환)에 따라 sentiment가 달라짐
- 판단이 모호하면 망설이지 말고 neutral로 분류할 것
- 각 판단의 근거를 reason에 한글로 15단어 이내로 작성할 것
- 반드시 유효한 JSON 배열만 출력하고 다른 텍스트는 포함하지 말 것"""


def _extract_json_text(raw_text: str) -> str:
    """Extract JSON array/object from LLM response, stripping markdown fences."""
    text = raw_text.strip()
    
    # Normalize escaped newlines sometimes returned in JSON strings
    text = text.replace('\\n', '\n').replace('\\r', '\r')
    
    # Remove markdown code fences
    if text.startswith("```"):
        lines = text.split("\n")
        # Find first non-fence line
        start_idx = 1
        if len(lines) > 1 and lines[1].strip().startswith("json"):
            start_idx = 2
        # Find closing fence
        end_lines = [i for i in range(len(lines)-1, start_idx-1, -1) if lines[i].strip() == "```"]
        if end_lines:
            text = "\n".join(lines[start_idx:end_lines[-1]]).strip()
        else:
            text = "\n".join(lines[start_idx:]).strip()
    
    # Try to find JSON array/object with regex if still not clean
    if not text or (not text.startswith("[") and not text.startswith("{")):
        m = re.search(r'(\[.*\]|\{.*\})', text, re.DOTALL)
        if m:
            text = m.group(1).strip()
    
    return text


def _parse_sentiment_response(text: str, batch_items: List[dict]) -> List[dict]:
    """Parse LLM response into sentiment results. Returns neutral fallback on failure."""
    text = _extract_json_text(text)
    
    if not text:
        raise ValueError("Empty response text after extraction")
    
    results = json.loads(text)
    
    if isinstance(results, dict):
        results = [results]
    
    if not isinstance(results, list):
        raise ValueError(f"Expected JSON array, got {type(results).__name__}")
    
    # Validate each result has required fields
    for r in results:
        if not isinstance(r, dict):
            raise ValueError(f"Expected dict in array, got {type(r).__name__}")
    
    return results


def _neutral_fallback(batch_items: List[dict]) -> List[dict]:
    """Return neutral sentiment for all items in a batch."""
    return [
        {"ticker": item.get("stock_code", "unknown"),
         "sentiment": "neutral",
         "reason": "분류 실패"}
        for item in batch_items
    ]


class DartSentimentAgent:
    """Batch sentiment classification for DART disclosures."""

    def __init__(
        self,
        model_id: str = SENTIMENT_MODEL,
        region: str = AWS_REGION,
        batch_size: int = BATCH_SIZE,
        max_concurrent: int = MAX_CONCURRENT,
    ):
        self.model_id = model_id
        self.region = region
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent

    def _make_client(self):
        return boto3.client("bedrock-runtime", region_name=self.region)

    def _call_bedrock_sync(self, batch_items: List[dict]) -> List[dict]:
        """Synchronous single-batch call with retry on JSON parse failure.
        
        Retries up to MAX_RETRIES times with increasing temperature jitter
        if the response is empty or not valid JSON.
        Returns [{ticker, sentiment, reason}].
        """
        # Build input: each item = {ticker, report_nm, text_snippet}
        items_json = []
        for item in batch_items:
            text_snippet = (item.get("text", "") or "")[:500]
            items_json.append({
                "ticker": item.get("stock_code", "unknown"),
                "report_nm": item.get("report_nm", ""),
                "text": text_snippet,
            })

        last_error = None
        
        for attempt in range(MAX_RETRIES + 1):  # 0 = first try, 1+ = retries
            temperature = 0.0 if attempt == 0 else min(0.1 * attempt, 0.4)
            
            body = json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 2000,
                "system": SENTIMENT_SYSTEM_PROMPT,
                "messages": [{
                    "role": "user",
                    "content": json.dumps(items_json, ensure_ascii=False),
                }],
                "temperature": temperature,
            })

            client = self._make_client()
            try:
                resp = client.invoke_model(
                    modelId=self.model_id,
                    contentType="application/json",
                    accept="application/json",
                    body=body,
                )
                resp_body = json.loads(resp["body"].read())
                text = ""
                for block in resp_body.get("content", []):
                    if block.get("type") == "text":
                        text += block["text"]

                if not text.strip():
                    raise ValueError("Empty response from model")

                results = _parse_sentiment_response(text, batch_items)
                
                if attempt > 0:
                    logger.info("Sentiment batch succeeded on retry %d", attempt)
                
                return results

            except (json.JSONDecodeError, ValueError) as e:
                last_error = e
                if attempt < MAX_RETRIES:
                    logger.warning(
                        "Sentiment batch parse failed (attempt %d/%d): %s — retrying...",
                        attempt + 1, MAX_RETRIES + 1, str(e)[:100],
                    )
                    time.sleep(RETRY_DELAY * (attempt + 1))  # exponential backoff
                else:
                    logger.error(
                        "Sentiment batch failed after %d retries: %s",
                        MAX_RETRIES + 1, str(e)[:100],
                    )
            except Exception as e:
                last_error = e
                if attempt < MAX_RETRIES:
                    logger.warning(
                        "Sentiment batch API error (attempt %d/%d): %s — retrying...",
                        attempt + 1, MAX_RETRIES + 1, str(e)[:100],
                    )
                    time.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    logger.error(
                        "Sentiment batch API error after %d retries: %s",
                        MAX_RETRIES + 1, str(e)[:100],
                    )
        
        # All retries exhausted
        logger.error("Sentiment batch call failed: all %d attempts exhausted, last error: %s",
                     MAX_RETRIES + 1, str(last_error)[:100] if last_error else "unknown")
        return _neutral_fallback(batch_items)

    async def _call_bedrock_async(self, batch_items: List[dict]) -> List[dict]:
        """Async wrapper for batch call."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._call_bedrock_sync, batch_items)

    def load_disclosures(
        self,
        target_date: str,
        lookback_days: int = 30,
        event_types: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Load DART disclosure events from S3 for sentiment analysis.

        Primary: Delta dart/disclosure_events (fast single read)
        Fallback: DartCollector Gold facts/material_event (new partitioned path)
        Legacy fallback: gold/dart/disclosure_events/dt={ym}/ (may be deleted by compaction)

        Args:
            target_date: reference date in YYYYMMDD format
            lookback_days: how many days to look back
            event_types: filter to specific event types (B-type = major disclosure)
        """
        target_dt = datetime.strptime(target_date, "%Y%m%d")
        start_dt = target_dt - timedelta(days=lookback_days)

        combined = None

        # Path 1: Delta (primary, fast)
        try:
            from agents.data_helper import read_gold_data
            df_delta = read_gold_data("dart/disclosure_events")
            if df_delta is not None and len(df_delta) > 0:
                if "rcept_dt" in df_delta.columns:
                    df_delta = df_delta[
                        (df_delta["rcept_dt"] >= start_dt.strftime("%Y%m%d")) &
                        (df_delta["rcept_dt"] <= target_date)
                    ]
                combined = df_delta
                logger.info("DART loaded via Delta — %d rows", len(combined))
        except Exception as e:
            logger.debug("Delta read skipped: %s", e)

        # Path 2: DartCollector Gold material_event facts (new)
        if combined is None or len(combined) == 0:
            try:
                s3 = boto3.client("s3", region_name=self.region)
                import re as _re

                prefix = "gold/dart/facts/material_event/"
                all_keys = []
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        if obj["Key"].endswith(".parquet"):
                            all_keys.append(obj["Key"])

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
                    m = _re.search(r'rcept_year=(\d{4})/rcept_month=(\d{2})/', key)
                    if m and (m.group(1), m.group(2)) in months:
                        relevant_keys.append(key)
                    elif not m:
                        relevant_keys.append(key)

                if not relevant_keys and all_keys:
                    relevant_keys = all_keys

                frames = []
                total_rows = 0
                for key in relevant_keys:
                    if total_rows >= 50000:
                        break
                    try:
                        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
                        buf = io.BytesIO(obj["Body"].read())
                        df = pq.read_table(buf).to_pandas()
                        frames.append(df)
                        total_rows += len(df)
                    except Exception as e2:
                        logger.warning("Error reading %s: %s", key, e2)

                if frames:
                    combined = pd.concat(frames, ignore_index=True)
                    if "rcept_dt" in combined.columns:
                        combined = combined[
                            (combined["rcept_dt"].astype(str) >= start_dt.strftime("%Y%m%d")) &
                            (combined["rcept_dt"].astype(str) <= target_date)
                        ]
                    logger.info("DART loaded via material_event facts — %d rows", len(combined))
            except Exception as e:
                logger.debug("material_event read skipped: %s", e)

        # Path 3: Legacy gold/dart/disclosure_events/dt= (may be deleted by compaction)
        if combined is None or len(combined) == 0:
            s3 = boto3.client("s3", region_name=self.region)
            months = set()
            d = start_dt
            while d <= target_dt:
                months.add(d.strftime("%Y-%m"))
                d = datetime(d.year, d.month, 1) + timedelta(days=32)
                d = datetime(d.year, d.month, 1)

            dfs = []
            for ym in sorted(months):
                key = f"gold/dart/disclosure_events/dt={ym}/data.parquet"
                try:
                    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
                    buf = io.BytesIO(obj["Body"].read())
                    df = pq.read_table(buf).to_pandas()
                    dfs.append(df)
                    logger.info("Loaded disclosure_events dt=%s: %d rows", ym, len(df))
                except Exception:
                    pass

            if dfs:
                combined = pd.concat(dfs, ignore_index=True)
                if "rcept_dt" in combined.columns:
                    combined = combined[
                        (combined["rcept_dt"].astype(str) >= start_dt.strftime("%Y%m%d")) &
                        (combined["rcept_dt"].astype(str) <= target_date)
                    ]

        if combined is not None and len(combined) > 0:
            # Filter by event type if specified
            if event_types and "event_category" in combined.columns:
                combined = combined[combined["event_category"].isin(event_types)]

            logger.info("Disclosure events loaded: %d rows after filtering", len(combined))
            return combined
        else:
            logger.warning("No disclosure events found for period %s to %s",
                           start_dt.strftime("%Y-%m-%d"), target_dt.strftime("%Y-%m-%d"))
            return pd.DataFrame()


    def classify_sync(self, df: pd.DataFrame) -> pd.DataFrame:
        """Classify sentiment for all disclosures (synchronous, for Airflow).

        Returns DataFrame with added columns: sentiment, sentiment_reason.
        """
        if df.empty:
            return df

        # Deduplicate by stock_code to avoid redundant LLM calls
        # For same ticker appearing multiple times, classify once and map back
        items = []
        seen_codes = set()
        for _, row in df.iterrows():
            code = str(row.get("stock_code", "")).zfill(6)
            if code not in seen_codes:
                seen_codes.add(code)
                items.append({
                    "stock_code": code,
                    "report_nm": str(row.get("report_nm", "")),
                    "text": str(row.get("text", "")),
                })

        # Split into batches
        batches = [
            items[i:i + self.batch_size]
            for i in range(0, len(items), self.batch_size)
        ]

        logger.info(
            "DART sentiment: %d unique tickers in %d batches (batch_size=%d)",
            len(items), len(batches), self.batch_size,
        )

        # Process sequentially (Airflow context — asyncio not needed)
        all_results = []
        failures = 0
        for i, batch in enumerate(batches):
            results = self._call_bedrock_sync(batch)
            # Check if this batch returned all neutral fallbacks
            if all(r.get("sentiment") == "neutral" and r.get("reason") == "분류 실패" for r in results):
                failures += 1
            all_results.extend(results)
            logger.info("Batch %d/%d: %d results", i + 1, len(batches), len(results))

        if failures > 0:
            logger.warning("Sentiment: %d/%d batches returned neutral fallback", failures, len(batches))

        # Merge results back into DataFrame
        result_map = {}
        for r in all_results:
            ticker = r.get("ticker", "").zfill(6)
            result_map[ticker] = {
                "sentiment": r.get("sentiment", "neutral"),
                "sentiment_reason": r.get("reason", ""),
            }

        df = df.copy()
        sentiments = []
        reasons = []
        for _, row in df.iterrows():
            code = str(row.get("stock_code", "")).zfill(6)
            mapped = result_map.get(code, {"sentiment": "neutral", "sentiment_reason": ""})
            sentiments.append(mapped["sentiment"])
            reasons.append(mapped["sentiment_reason"])

        df["sentiment"] = sentiments
        df["sentiment_reason"] = reasons

        positive = (df["sentiment"] == "positive").sum()
        negative = (df["sentiment"] == "negative").sum()
        neutral = (df["sentiment"] == "neutral").sum()
        logger.info(
            "Sentiment complete: %d positive, %d negative, %d neutral (%d batch failures)",
            positive, negative, neutral, failures,
        )
        return df

    async def classify_async(self, df: pd.DataFrame) -> pd.DataFrame:
        """Classify sentiment with asyncio concurrency (for chat use)."""
        if df.empty:
            return df

        # Deduplicate
        items = []
        seen_codes = set()
        for _, row in df.iterrows():
            code = str(row.get("stock_code", "")).zfill(6)
            if code not in seen_codes:
                seen_codes.add(code)
                items.append({
                    "stock_code": code,
                    "report_nm": str(row.get("report_nm", "")),
                    "text": str(row.get("text", "")),
                })

        batches = [
            items[i:i + self.batch_size]
            for i in range(0, len(items), self.batch_size)
        ]

        logger.info("DART sentiment async: %d items in %d batches", len(items), len(batches))

        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def process_batch(batch):
            async with semaphore:
                return await self._call_bedrock_async(batch)

        tasks = [process_batch(b) for b in batches]
        batch_results = await asyncio.gather(*tasks)
        all_results = [r for batch in batch_results for r in batch]

        result_map = {}
        for r in all_results:
            ticker = r.get("ticker", "").zfill(6)
            result_map[ticker] = {
                "sentiment": r.get("sentiment", "neutral"),
                "sentiment_reason": r.get("reason", ""),
            }

        df = df.copy()
        df["sentiment"] = [
            result_map.get(str(row.get("stock_code", "")).zfill(6), {}).get("sentiment", "neutral")
            for _, row in df.iterrows()
        ]
        df["sentiment_reason"] = [
            result_map.get(str(row.get("stock_code", "")).zfill(6), {}).get("sentiment_reason", "")
            for _, row in df.iterrows()
        ]
        return df


# Singleton
_default_sentiment: Optional[DartSentimentAgent] = None


def get_sentiment_agent() -> DartSentimentAgent:
    global _default_sentiment
    if _default_sentiment is None:
        _default_sentiment = DartSentimentAgent()
    return _default_sentiment
