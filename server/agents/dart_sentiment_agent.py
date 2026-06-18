"""
DART Sentiment Agent — batch sentiment classification of DART disclosures.

Used in the daily briefing pipeline (Step 4). Takes 1 month of DART disclosure
events and classifies each as positive / negative / neutral using Haiku batch mode
(25 items per call, asyncio 20 concurrent → 1-2 seconds total).

Key design decisions:
  - Keywords alone cannot distinguish context ("유상증자" can be positive or negative)
  - LLM reads disclosure title + body text to judge context
  - Batch mode: 25 items/call, parallel asyncio → 12 calls for 300 items

Output: [{ticker, stock_code, sentiment, reason}]
"""

import asyncio
import io
import json
import logging
import os
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
- 각 판단의 근거를 reason에 한글로 15단어 이내로 작성할 것"""


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
        """Synchronous single-batch call. Returns [{ticker, sentiment, reason}]."""
        # Build input: each item = {ticker, report_nm, text_snippet}
        items_json = []
        for item in batch_items:
            text_snippet = (item.get("text", "") or "")[:500]
            items_json.append({
                "ticker": item.get("stock_code", "unknown"),
                "report_nm": item.get("report_nm", ""),
                "text": text_snippet,
            })

        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 2000,
            "system": SENTIMENT_SYSTEM_PROMPT,
            "messages": [{
                "role": "user",
                "content": json.dumps(items_json, ensure_ascii=False),
            }],
            "temperature": 0.0,
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

            text = text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[-1].rsplit("\n```", 1)[0]
                if text.startswith("json"):
                    text = text[4:].strip()

            results = json.loads(text)
            if isinstance(results, dict):
                results = [results]
            return results

        except Exception as e:
            logger.error("Sentiment batch call failed: %s", e)
            # Return neutral fallback
            return [
                {"ticker": item.get("stock_code", "unknown"),
                 "sentiment": "neutral",
                 "reason": "분류 실패"}
                for item in batch_items
            ]

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
        """Load DART disclosure events from S3 Gold for sentiment analysis.

        Uses OPIK Gold disclosure_events (Phase 2 1차 소스).

        Args:
            target_date: reference date in YYYYMMDD format
            lookback_days: how many days to look back
            event_types: filter to specific event types (B-type = major disclosure)
        """
        target_dt = datetime.strptime(target_date, "%Y%m%d")
        start_dt = target_dt - timedelta(days=lookback_days)

        # Scan relevant monthly partitions
        s3 = boto3.client("s3", region_name=self.region)
        months = set()
        d = start_dt
        while d <= target_dt:
            months.add(d.strftime("%Y-%m"))
            # Add a day and re-normalize
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
            except s3.exceptions.NoSuchKey:
                logger.debug("No disclosure_events for dt=%s", ym)
            except Exception as e:
                logger.warning("Error loading dt=%s: %s", ym, e)

        if not dfs:
            logger.warning("No disclosure events found for period %s to %s",
                           start_dt.strftime("%Y-%m-%d"), target_dt.strftime("%Y-%m-%d"))
            return pd.DataFrame()

        combined = pd.concat(dfs, ignore_index=True)

        # Filter by date range
        if "rcept_dt" in combined.columns:
            combined = combined[
                (combined["rcept_dt"] >= start_dt.strftime("%Y%m%d")) &
                (combined["rcept_dt"] <= target_dt)
            ]

        # Filter by event type if specified
        if event_types and "event_category" in combined.columns:
            combined = combined[combined["event_category"].isin(event_types)]

        logger.info("Disclosure events loaded: %d rows after filtering", len(combined))
        return combined

    def classify_sync(self, df: pd.DataFrame) -> pd.DataFrame:
        """Classify sentiment for all disclosures (synchronous, for Airflow).

        Returns DataFrame with added columns: sentiment, sentiment_reason.
        """
        if df.empty:
            return df

        # Extract relevant fields
        items = []
        for _, row in df.iterrows():
            items.append({
                "stock_code": str(row.get("stock_code", "")).zfill(6),
                "report_nm": str(row.get("report_nm", "")),
                "text": str(row.get("text", "")),
            })

        # Split into batches
        batches = [
            items[i:i + self.batch_size]
            for i in range(0, len(items), self.batch_size)
        ]

        logger.info(
            "DART sentiment: %d items in %d batches (batch_size=%d)",
            len(items), len(batches), self.batch_size,
        )

        # Process sequentially (Airflow context — asyncio not needed)
        all_results = []
        for i, batch in enumerate(batches):
            results = self._call_bedrock_sync(batch)
            all_results.extend(results)
            logger.info("Batch %d/%d: %d results", i + 1, len(batches), len(results))

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
            "Sentiment complete: %d positive, %d negative, %d neutral",
            positive, negative, neutral,
        )
        return df

    async def classify_async(self, df: pd.DataFrame) -> pd.DataFrame:
        """Classify sentiment with asyncio concurrency (for chat use)."""
        if df.empty:
            return df

        items = []
        for _, row in df.iterrows():
            items.append({
                "stock_code": str(row.get("stock_code", "")).zfill(6),
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
