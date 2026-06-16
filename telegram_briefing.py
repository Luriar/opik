"""OPIK -- Telegram Daily Briefing

Gold Structured Parquet + LLM Gold (embeddings) -> format -> Telegram API (HTML)

Usage:
    python telegram_briefing.py                    # latest date
    python telegram_briefing.py --date 2026-06-12  # specific date
    python telegram_briefing.py --dry-run           # print only, no send
    python telegram_briefing.py --no-llm            # structured only, skip LLM join

Env vars: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import requests

from opik_config import S3_BUCKET, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, load_dotenv
from opik_s3 import get_s3_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("opik.telegram")

TELEGRAM_MAX_LEN = 4000

s3 = get_s3_client(max_pool_connections=10)


def _load_parquet_month(prefix: str) -> pd.DataFrame:
    """Load all parquet files under a given S3 prefix."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])

    if not keys:
        raise RuntimeError(f"No parquet files found at {prefix}")

    dfs = []
    for key in keys:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        buf = io.BytesIO(resp["Body"].read())
        table = pq.read_table(buf)
        df = table.to_pandas()
        dfs.append(df)
        logger.info(f"Loaded {key}: {len(df)} rows")

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _find_latest_month(prefix_base: str) -> str:
    """Find the latest year=YYYY/month=MM/ prefix under prefix_base."""
    paginator = s3.get_paginator("list_objects_v2")
    year_prefixes = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix_base, Delimiter="/"):
        for p in page.get("CommonPrefixes", []):
            year_prefixes.append(p["Prefix"])

    if not year_prefixes:
        raise RuntimeError(f"No data found at {prefix_base}")

    latest_year = sorted(year_prefixes)[-1]
    month_prefixes = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=latest_year, Delimiter="/"):
        for p in page.get("CommonPrefixes", []):
            month_prefixes.append(p["Prefix"])

    if not month_prefixes:
        raise RuntimeError(f"No months found at {latest_year}")

    return sorted(month_prefixes)[-1]


def load_gold_structured(date: str | None = None) -> tuple[pd.DataFrame, str]:
    """Load Gold Structured Parquet, filtered to target date.

    Returns (dataframe, resolved_date_string).
    """
    if date:
        target_prefix = f"gold/structured/year={date[:4]}/month={date[5:7]}/"
    else:
        target_prefix = _find_latest_month("gold/structured/")

    full = _load_parquet_month(target_prefix)
    if len(full) == 0:
        return pd.DataFrame(), ""

    # Normalize dates
    full["_date_clean"] = full["발행일"].str.replace(".", "-", regex=False)

    if date:
        full = full[full["_date_clean"] == date]
        if len(full) == 0:
            logger.warning(f"No reports for {date}")
            return pd.DataFrame(), date
        result_date = date
    else:
        result_date = max(full["_date_clean"].unique())
        full = full[full["_date_clean"] == result_date]

    n_firms = full["증권사"].nunique()
    logger.info(f"Date {result_date}: {len(full)} reports from {n_firms} firms")
    return full, result_date


def load_gold_llm(date: str) -> pd.DataFrame:
    """Load Gold LLM (embeddings) Parquet for a specific date."""
    target_prefix = f"gold/embeddings/year={date[:4]}/month={date[5:7]}/"

    try:
        df = _load_parquet_month(target_prefix)
    except RuntimeError:
        logger.warning(f"No LLM Gold data at {target_prefix}")
        return pd.DataFrame()

    logger.info(f"LLM Gold loaded: {len(df)} rows")
    return df


def join_structured_llm(df_s, df_l):
    """Left-join structured with LLM Gold on report_id.

    Returns (merged_df, llm_stats).
    """
    if df_l.empty:
        llm_stats = {"total": len(df_s), "matched": 0, "with_reason": 0,
                      "with_risks": 0, "with_keywords": 0}
        for col in ["reason", "risks", "keywords"]:
            df_s[col] = None
        return df_s, llm_stats

    need_cols = ["report_id", "reason", "risks", "keywords"]
    avail = [c for c in need_cols if c in df_l.columns]
    merged = df_s.merge(df_l[avail], on="report_id", how="left")

    llm_stats = {
        "total": len(df_s),
        "matched": int(merged["report_id"].isin(df_l["report_id"]).sum()),
        "with_reason": int(merged["reason"].notna().sum()),
        "with_risks": int(merged["risks"].notna().sum()),
        "with_keywords": int(merged["keywords"].notna().sum()),
    }

    logger.info(
        f"LLM join: {llm_stats['matched']}/{llm_stats['total']} matched, "
        f"reason={llm_stats['with_reason']}, risks={llm_stats['with_risks']}, "
        f"keywords={llm_stats['with_keywords']}"
    )
    return merged, llm_stats


def _safe_list(val):
    """Convert risks/keywords from various formats to a Python list."""
    if val is None:
        return []
    try:
        if isinstance(val, np.ndarray):
            return val.tolist()
    except Exception:
        pass
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            result = json.loads(val)
            if isinstance(result, list):
                return result
        except Exception:
            pass
        return [val]
    try:
        return list(val)
    except Exception:
        return [str(val)]


def _escape_html(text):
    """Escape HTML special chars for Telegram HTML parse_mode."""
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _truncate(text, max_len=120):
    """Truncate text to max_len chars, adding ellipsis if cut."""
    if not text or len(text) <= max_len:
        return _escape_html(text or "")
    return _escape_html(text[:max_len].rsplit(" ", 1)[0]) + "..."


def format_briefing(df, date, llm_stats=None):
    """Format integrated briefing as Telegram HTML."""
    total = len(df)
    buy = int((df["투자의견"] == "BUY").sum())
    hold = int((df["투자의견"] == "HOLD").sum())
    sell = int((df["투자의견"] == "SELL").sum())
    nr = int((df["투자의견"] == "NOT_RATED").sum())
    null_op = int(df["투자의견"].isna().sum())
    firms = df["증권사"].nunique()

    tp_count = int(df["목표주가"].notna().sum())
    tp_rate = tp_count / total * 100 if total > 0 else 0

    has_llm = llm_stats and llm_stats.get("with_reason", 0) > 0

    upside = df[df["상승여력_pct"].notna()].sort_values(
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
    ]

    if llm_stats:
        lines.append(
            f"LLM: {llm_stats['matched']}/{total} matched "
            f"(reason {llm_stats['with_reason']}, "
            f"risks {llm_stats['with_risks']}, "
            f"kw {llm_stats['with_keywords']})"
        )

    lines.append("")

    # Top Upside with LLM insights
    if len(upside) > 0:
        lines.append("<b>Top Picks (by upside)</b>")
        for _, row in upside.head(8).iterrows():
            name = _escape_html(row["종목명"])
            firm = _escape_html(row["증권사"])
            up_pct = row["상승여력_pct"]
            op = row["투자의견"] if pd.notna(row["투자의견"]) else "N/A"
            tp_val = int(row["목표주가"]) if pd.notna(row["목표주가"]) else 0
            cp_val = int(row["현재주가"]) if pd.notna(row["현재주가"]) else 0
            tp_s = f"{tp_val:,}" if tp_val else "-"
            cp_s = f"{cp_val:,}" if cp_val else "-"

            lines.append(
                f"  <b>{name}</b> ({firm}, {op}) "
                f"TP={tp_s} CP={cp_s} upside=<b>{up_pct:+.1f}%</b>"
            )

            # Reason from LLM (full text, no truncation)
            if has_llm and pd.notna(row.get("reason")):
                reason = _escape_html(str(row["reason"]))
                if reason:
                    lines.append(f"    {reason}")

            # Risks from LLM
            risks = _safe_list(row.get("risks"))
            if risks:
                risk_s = ", ".join(_escape_html(r) for r in risks[:2])
                lines.append(f"    Risks: {risk_s}")

        lines.append("")

    # By Firm
    lines.append("<b>By Firm</b>")
    for firm, cnt in firm_counts.items():
        lines.append(f"  {_escape_html(firm)}: {cnt}")
    lines.append("")

    # Stocks covered
    names = df[df["종목명"].notna()]["종목명"].unique()
    if len(names) <= 30:
        lines.append(f"<b>Stocks covered ({len(names)})</b>")
        names_s = ", ".join(_escape_html(str(n)) for n in names if str(n).strip())
        lines.append(names_s)

    return "\n".join(lines)


def send_telegram(text, token=TELEGRAM_BOT_TOKEN, chat_id=TELEGRAM_CHAT_ID,
                  max_len=TELEGRAM_MAX_LEN):
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
            mid = data["result"]["message_id"]
            logger.info(f"Sent chunk {i+1}/{len(chunks)}: msg_id={mid}")
        else:
            logger.error(f"Telegram error (chunk {i+1}): {data}")
            payload["parse_mode"] = ""
            resp2 = requests.post(url, json=payload, timeout=10)
            if resp2.json().get("ok"):
                logger.info(f"Sent chunk {i+1}/{len(chunks)} as plain text")
            else:
                logger.error(f"Fallback failed: {resp2.json()}")


def _split_text(text, max_len):
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
    parser.add_argument("--no-llm", action="store_true",
                        help="Skip LLM Gold join (structured only)")
    args = parser.parse_args()

    load_dotenv()

    # Load structured
    try:
        df_s, date = load_gold_structured(args.date)
    except RuntimeError as e:
        logger.error(str(e))
        sys.exit(1)

    if len(df_s) == 0:
        logger.info("No reports to send")
        return

    # Load LLM Gold
    llm_stats = None
    if not args.no_llm:
        try:
            df_l = load_gold_llm(date)
            df_s, llm_stats = join_structured_llm(df_s, df_l)
        except Exception as e:
            logger.warning(f"LLM Gold join failed, proceeding without: {e}")
            for col in ["reason", "risks", "keywords"]:
                df_s[col] = None

    # Format
    briefing = format_briefing(df_s, date, llm_stats)
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
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  