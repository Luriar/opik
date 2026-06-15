"""OPIK -- Telegram Daily Briefing

Gold Structured Parquet -> format -> Telegram API (HTML)

Usage:
    python telegram_briefing.py                    # latest date
    python telegram_briefing.py --date 2026-06-12  # specific date
    python telegram_briefing.py --dry-run           # print only, no send

Env vars: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from pathlib import Path

import boto3
import pandas as pd
import pyarrow.parquet as pq
import requests
from botocore.config import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("opik.telegram")

S3_BUCKET = os.getenv("S3_BUCKET", "s3-opik-bucket")
S3_REGION = os.getenv("S3_REGION", "ap-northeast-2")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_MAX_LEN = 4000

s3 = boto3.client("s3", region_name=S3_REGION, config=Config(max_pool_connections=10))


def _list_gold_prefixes(depth: int = 1) -> list[str]:
    """List gold/structured/ prefixes in S3.
    depth=1: year-level (year=2026/)
    depth=2: month-level (year=2026/month=06/)
    """
    prefixes = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=S3_BUCKET, Prefix="gold/structured/", Delimiter="/"
    ):
        for p in page.get("CommonPrefixes", []):
            if depth == 1:
                prefixes.append(p["Prefix"])
            else:
                # Go one level deeper for each year prefix
                sub = s3.get_paginator("list_objects_v2")
                for sp in sub.paginate(Bucket=S3_BUCKET, Prefix=p["Prefix"], Delimiter="/"):
                    for sp2 in sp.get("CommonPrefixes", []):
                        prefixes.append(sp2["Prefix"])
    return sorted(prefixes)


def load_gold(date: str | None = None) -> tuple[pd.DataFrame, str]:
    """Load Gold Structured Parquet for a specific or latest date.
    Returns (dataframe, resolved_date_string).
    Only loads the target month's parquet.
    """

    # Determine which month to load
    if date:
        target_prefix = f"gold/structured/year={date[:4]}/month={date[5:7]}/"
    else:
        # Find latest month
        month_prefixes = _list_gold_prefixes(depth=2)
        if not month_prefixes:
            raise RuntimeError("No Gold Parquet files found")
        target_prefix = month_prefixes[-1]

    # Load parquet for target month only
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=target_prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])

    if not keys:
        raise RuntimeError(f"No parquet files found at {target_prefix}")

    dfs = []
    for key in keys:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        buf = io.BytesIO(resp["Body"].read())
        table = pq.read_table(buf)
        df = table.to_pandas()
        dfs.append(df)
        logger.info(f"Loaded {key}: {len(df)} rows")

    full = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    if len(full) == 0:
        return pd.DataFrame(), ""

    # Normalize dates (2026.06.12 -> 2026-06-12)
    full["발행일_clean"] = full["발행일"].str.replace(".", "-", regex=False)

    # Filter to target date
    if date:
        full = full[full["발행일_clean"] == date]
        if len(full) == 0:
            logger.warning(f"No reports for {date}")
            return pd.DataFrame(), date
        result_date = date
    else:
        result_date = max(full["발행일_clean"].unique())
        full = full[full["발행일_clean"] == result_date]

    logger.info(
        f"Date {result_date}: {len(full)} reports from {full['증권사'].nunique()} firms"
    )
    return full, result_date


def format_briefing(df: pd.DataFrame, date: str) -> str:
    """Format briefing as Telegram HTML."""

    total = len(df)
    buy = int((df["투자의견"] == "BUY").sum())
    hold = int((df["투자의견"] == "HOLD").sum())
    sell = int((df["투자의견"] == "SELL").sum())
    nr = int((df["투자의견"] == "NOT_RATED").sum())
    null_op = int(df["투자의견"].isna().sum())
    firms = df["증권사"].nunique()

    tp_count = int(df["목표주가"].notna().sum())
    tp_rate = tp_count / total * 100 if total > 0 else 0

    with_upside = df[df["상승여력_pct"].notna()].sort_values(
        "상승여력_pct", ascending=False
    )

    firm_counts = df["증권사"].value_counts()

    lines = [
        f"<b>OPIK Daily Briefing / {date}</b>",
        "",
        f"<b>Summary</b>",
        f"Total <b>{total}</b> reports from {firms} firms",
        f"BUY {buy} / HOLD {hold} / SELL {sell} / NR {nr} / null {null_op}",
        f"TP extracted: {tp_count}/{total} ({tp_rate:.0f}%)",
        "",
    ]

    if len(with_upside) > 0:
        lines.append("<b>Top Upside</b>")
        for _, row in with_upside.head(10).iterrows():
            name = str(row["종목명"])
            firm = row["증권사"]
            up = row["상승여력_pct"]
            op = row["투자의견"] if pd.notna(row["투자의견"]) else "N/A"
            tp = int(row["목표주가"]) if pd.notna(row["목표주가"]) else 0
            cp = int(row["현재주가"]) if pd.notna(row["현재주가"]) else 0
            tp_str = f"{tp:,}" if tp else "-"
            cp_str = f"{cp:,}" if cp else "-"
            lines.append(
                f"  <b>{name}</b> ({firm}, {op}) TP={tp_str} CP={cp_str} upside=<b>{up:+.1f}%</b>"
            )
        lines.append("")

    lines.append("<b>By Firm</b>")
    for firm, cnt in firm_counts.items():
        lines.append(f"  {firm}: {cnt}")
    lines.append("")

    names = df[df["종목명"].notna()]["종목명"].unique()
    if len(names) <= 30:
        lines.append(f"<b>Stocks covered ({len(names)})</b>")
        names_str = ", ".join(str(n) for n in names)
        lines.append(names_str)

    return "\n".join(lines)


def send_telegram(
    text: str,
    token: str = TELEGRAM_BOT_TOKEN,
    chat_id: str = TELEGRAM_CHAT_ID,
    max_len: int = TELEGRAM_MAX_LEN,
):
    """Send HTML message via Telegram. Split if over max_len."""

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    chunks = _split_text(text, max_len)

    for i, chunk in enumerate(chunks):
        payload = {
            "chat_id": chat_id,
            "text": chunk,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        resp = requests.post(url, json=payload, timeout=10)
        data = resp.json()

        if data.get("ok"):
            logger.info(f"Sent chunk {i+1}/{len(chunks)}: msg_id={data['result']['message_id']}")
        else:
            logger.error(f"Telegram error (chunk {i+1}): {data}")
            # Fallback: plain text
            payload["parse_mode"] = ""
            resp2 = requests.post(url, json=payload, timeout=10)
            if resp2.json().get("ok"):
                logger.info(f"Sent chunk {i+1}/{len(chunks)} as plain text (fallback)")
            else:
                logger.error(f"Fallback also failed: {resp2.json()}")


def _split_text(text: str, max_len: int) -> list[str]:
    """Split text on newline boundaries, each under max_len."""
    chunks = []
    while len(text) > max_len:
        split_at = text.rfind("\n", 0, max_len)
        if split_at == -1:
            split_at = max_len
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    if text.strip():
        chunks.append(text)
    return chunks


def main():
    parser = argparse.ArgumentParser(description="OPIK Telegram Daily Briefing")
    parser.add_argument("--date", help="Target date (YYYY-MM-DD), omit for latest")
    parser.add_argument("--dry-run", action="store_true", help="Print only, no send")
    args = parser.parse_args()

    # Load .env
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k, v)

    # Load data
    try:
        df, date = load_gold(args.date)
    except RuntimeError as e:
        logger.error(str(e))
        sys.exit(1)

    if len(df) == 0:
        logger.info("No reports to send")
        return

    # Format
    briefing = format_briefing(df, date)
    print(briefing)
    print(f"\n[{len(briefing)} chars]")

    if args.dry_run:
        logger.info("Dry run -- not sending")
        return

    # Send
    send_telegram(briefing)
    logger.info("Done")


if __name__ == "__main__":
    main()
                                                              