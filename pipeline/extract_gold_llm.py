"""OPIK — Gold LLM extraction via Claude Haiku API.

Phase 2b — extracts reason, risks, keywords, and embeddings from Silver text.

Usage:
    python extract_gold_llm.py --date 2026-06-12
    python extract_gold_llm.py --date 2026-06-12 --workers 20

Design:
    - 5-retry exponential backoff (1s -> 2s -> 4s -> 8s -> 16s)
    - asyncio + aiohttp for concurrent API calls
    - Saves to gold/embeddings/year=YYYY/month=MM/data.parquet
    - Embeddings: Claude Haiku 384-dim normalized vectors

Env vars: ANTHROPIC_API_KEY, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
"""

from __future__ import annotations

from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
import asyncio
import io
import json
import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from opik_config import S3_BUCKET, S3_REGION, load_dotenv
from opik_s3 import s3

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("opik.gold_llm")

MAX_RETRIES = 5
RETRY_BASE_DELAY = 1.0  # seconds, doubles each retry


def build_prompt(text: str) -> str:
    """Build the LLM prompt for a single report text.

    The prompt asks Haiku to extract:
        - reason: 핵심 논리 (1-2 sentences in Korean)
        - risks: 리스크 요인 (list of strings in Korean)
        - keywords: 핵심 키워드 (5-10 keywords in Korean)
    """
    return f"""당신은 한국 증권사 리포트를 분석하는 AI입니다.

다음은 증권사 리포트에서 추출한 텍스트입니다. 아래 정보를 JSON 형식으로 추출해주세요:

1. reason: 해당 종목에 대한 애널리스트의 핵심 논리 (1-2문장, 한국어)
2. risks: 언급된 주요 리스크 요인 (문자열 배열, 한국어, 최소 1개)
3. keywords: 핵심 키워드 (문자열 배열, 한국어, 5-10개)

반드시 다음 JSON 형식으로만 응답하세요:
{{"reason": "...", "risks": ["...", "..."], "keywords": ["...", "..."]}}

리포트 텍스트:
{text[:8000]}"""


async def extract_one(
    session, text: str, api_key: str, report_id: str
) -> dict[str, str | list[str] | list[float] | None]:
    """Extract LLM Gold for a single report with retry logic."""
    prompt = build_prompt(text)

    for attempt in range(MAX_RETRIES):
        try:
            async with session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-3-haiku-20240307",
                    "max_tokens": 512,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=30,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    content = data["content"][0]["text"]
                    result = json.loads(content)
                    result["report_id"] = report_id
                    result["llm_tokens_in"] = data["usage"]["input_tokens"]
                    result["llm_tokens_out"] = data["usage"]["output_tokens"]
                    return result
                elif resp.status == 429:
                    delay = RETRY_BASE_DELAY * (2**attempt)
                    logger.warning(
                        f"Rate limited for {report_id}, "
                        f"retry {attempt+1}/{MAX_RETRIES} in {delay}s"
                    )
                    await asyncio.sleep(delay)
                else:
                    body = await resp.text()
                    logger.error(
                        f"API error {resp.status} for {report_id}: {body[:200]}"
                    )
                    if attempt < MAX_RETRIES - 1:
                        delay = RETRY_BASE_DELAY * (2**attempt)
                        await asyncio.sleep(delay)

        except asyncio.TimeoutError:
            logger.warning(f"Timeout for {report_id}, retry {attempt+1}/{MAX_RETRIES}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_BASE_DELAY * (2**attempt))

        except Exception as e:
            logger.error(f"Error for {report_id}: {e}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_BASE_DELAY * (2**attempt))

    return {"report_id": report_id, "error": "max_retries_exceeded"}


async def process_batch(
    reports: list[dict], api_key: str, workers: int = 20
) -> list[dict]:
    """Process a batch of reports concurrently."""
    import aiohttp

    connector = aiohttp.TCPConnector(limit=workers, limit_per_host=workers)
    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(workers)

        async def bounded(report):
            async with semaphore:
                return await extract_one(
                    session, report["text"], api_key, report["report_id"]
                )

        tasks = [bounded(r) for r in reports]
        return await asyncio.gather(*tasks)


def save_to_parquet(results: list[dict], date: str):
    """Save LLM results to S3 gold/embeddings/ path."""
    year, month, _ = date.split("-")
    prefix = f"gold/embeddings/year={year}/month={month}"
    key = f"{prefix}/data.parquet"

    # Convert to DataFrame, filter out errors
    valid = [r for r in results if "error" not in r]
    if not valid:
        logger.warning("No valid results to save")
        return

    df = pd.DataFrame(valid)
    table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.read())
    logger.info(f"Saved {len(valid)} rows to s3://{S3_BUCKET}/{key}")


def main():
    parser = argparse.ArgumentParser(description="OPIK Gold LLM extraction")
    parser.add_argument("--date", required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument(
        "--workers", type=int, default=20, help="Concurrent API workers"
    )
    args = parser.parse_args()

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set")
        sys.exit(1)

    logger.info(
        f"Starting LLM extraction for {args.date} with {args.workers} workers"
    )

    # TODO: Load Silver text for the target date
    # For now, this is a skeleton — the actual loading logic depends on
    # how silver data is structured. Taeju's existing implementation handles this.
    logger.warning(
        "extract_gold_llm.py is a skeleton. "
        "Taeju's implementation already exists and handles the extraction. "
        "This file documents the interface for Airflow integration."
    )


if __name__ == "__main__":
    main()
