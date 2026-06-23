"""
DART Query Engine v2 — reads DartCollector Gold Parquet tables from S3.

Uses DartCollector medallion architecture Gold paths:
  gold/dart/facts/material_event/       ← disclosure events
  gold/dart/facts/ownership/            ← insider + major shareholder
  gold/dart/facts/financial_statement/  ← financial statements
  gold/dart/company_master/             ← company name→code lookup (legacy OPIK)

All S3 scans use paginated list_objects_v2 and read only matching partitions.
No Spark required. PyArrow + Pandas in-process.
"""

import io
import json
import logging
import re
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

import boto3
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger("opik.dart")

S3_BUCKET = "s3-opik-bucket"
AWS_REGION = "ap-northeast-2"

_company_lookup: Optional[Dict[str, Dict]] = None
_company_lookup_ts: float = 0


def _get_s3():
    return boto3.client("s3", region_name=AWS_REGION)


def _load_company_master() -> Dict[str, Dict]:
    global _company_lookup, _company_lookup_ts
    now = datetime.now().timestamp()
    if _company_lookup is not None and (now - _company_lookup_ts) < 3600:
        return _company_lookup

    logger.info("Loading company_master...")
    s3 = _get_s3()
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="gold/dart/company_master/data.parquet")
        buf = io.BytesIO(obj["Body"].read())
        df = pq.read_table(buf).to_pandas()
    except Exception as e:
        logger.warning("company_master load failed: %s", e)
        _company_lookup = {}
        _company_lookup_ts = now
        return _company_lookup

    lookup = {}
    for _, row in df.iterrows():
        name = str(row.get("corp_name", "")).strip()
        code = str(row.get("stock_code", "")).strip()
        corp_code = str(row.get("corp_code", "")).strip()
        key = name.replace("(주)", "").replace("주식회사 ", "").strip()
        lookup[key] = {"stock_code": code, "corp_code": corp_code, "corp_name": name}
        if name != key:
            lookup[name] = lookup[key]
        if code:
            lookup[code] = lookup[key]
        if corp_code:
            lookup[corp_code] = lookup[key]

    _company_lookup = lookup
    _company_lookup_ts = now
    logger.info("Company master loaded: %d entries", len(lookup))
    return lookup


_KOR_ALIASES = {
    "sk": "에스케이", "lg": "엘지", "kt": "케이티", "skc": "에스케이씨",
}


def _normalize_name(name: str) -> str:
    lower = name.lower()
    for eng, kor in _KOR_ALIASES.items():
        if lower.startswith(eng) and len(name) > len(eng):
            return kor + name[len(eng):]
    return name


def _find_company(query_companies, query_codes):
    master = _load_company_master()
    search_names = []
    for name in query_companies:
        clean = name.replace("(주)", "").replace("주식회사 ", "").strip()
        search_names.extend([name, clean, _normalize_name(name), _normalize_name(clean)])
    for name in search_names:
        if name in master:
            return master[name]
    for code in query_codes:
        if code in master:
            return master[code]
    for name in search_names:
        name_lower = name.lower()
        for key, info in master.items():
            if key.lower().startswith(name_lower) or name_lower.startswith(key.lower()):
                return info
    return None


def _list_gold_partitions(prefix):
    s3 = _get_s3()
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


def _read_parquet_keys(keys, limit_rows=1000):
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


def _filter_by_date_col(df, col, date_from, date_to):
    if col not in df.columns or (not date_from and not date_to):
        return df
    vals = df[col].astype(str).str.replace("-", "")
    mask = pd.Series(True, index=df.index)
    if date_from:
        mask = mask & (vals >= date_from.replace("-", ""))
    if date_to:
        mask = mask & (vals <= date_to.replace("-", ""))
    return df[mask]


def _is_null(val):
    if val is None:
        return True
    try:
        if pd.isna(val):
            return True
    except (TypeError, ValueError):
        pass
    return False


def _parse_num(val, default=0):
    """Parse a numeric value handling numpy types and comma-formatted strings."""
    if _is_null(val):
        return default
    if isinstance(val, (int, float, np.integer, np.floating)):
        if default is None:
            return float(val)
        if isinstance(default, int):
            return int(val)
        return float(val)
    try:
        cleaned = str(val).replace(",", "").strip()
        if default is None:
            return float(cleaned)
        return type(default)(cleaned)
    except (ValueError, TypeError):
        return default


def _fmt_won(val):
    """Format won amount: 123456789012 -> '1,234억원' or '1.23조원'."""
    num = _parse_num(val, None)
    if num is None or num == 0:
        return "0"
    try:
        v = int(float(str(num).replace(",", "")))
    except (ValueError, TypeError):
        return str(num)
    if abs(v) >= 10000_0000_0000:
        return f"{v/1_0000_0000_0000:.2f}조원"
    return f"{v//100_000_000:,}억원"


_FINANCIAL_KEY_ACCOUNTS = [
    ("매출액", ["매출액"]),
    ("영업이익", ["영업이익(손실)"]),
    ("당기순이익", ["당기순이익(손실)"]),
    ("자산총계", ["자산총계"]),
    ("자본총계", ["자본총계"]),
]


def _find_account_value(df_report, account_labels):
    """Find account value preferring non-zero rows."""
    for label in account_labels:
        mask = df_report["account_nm"].astype(str).str.strip() == label
        if not mask.any():
            continue
        rows = df_report[mask]
        best_val = None
        for _, row in rows.iterrows():
            val = row.get("thstrm_amount")
            if _is_null(val):
                val = row.get("frmtrm_amount")
            if not _is_null(val):
                num = _parse_num(val, None)
                if num is not None and num != 0:
                    return val
                if best_val is None:
                    best_val = val
        return best_val
    return None


def _enumerate_months(date_from, date_to, default_months=6):
    months = set()
    if date_from or date_to:
        start = datetime.strptime(date_from, "%Y-%m-%d") if date_from else datetime(2020, 1, 1)
        end = datetime.strptime(date_to, "%Y-%m-%d") if date_to else datetime.now()
        d = datetime(start.year, start.month, 1)
        while d <= end:
            months.add((str(d.year), f"{d.month:02d}"))
            if d.month == 12:
                d = datetime(d.year + 1, 1, 1)
            else:
                d = datetime(d.year, d.month + 1, 1)
    else:
        now = datetime.now()
        d = datetime(now.year, now.month, 1)
        for _ in range(default_months):
            months.add((str(d.year), f"{d.month:02d}"))
            if d.month == 1:
                d = datetime(d.year - 1, 12, 1)
            else:
                d = datetime(d.year, d.month - 1, 1)
    return months


def query_financials(companies, codes, date_from=None, date_to=None,
                     is_recent=False, limit_rows=500):
    comp = _find_company(companies, codes)
    if not comp:
        return "회사 정보를 찾을 수 없습니다. 종목코드나 회사명을 확인해주세요."

    corp_code = comp["corp_code"]
    corp_name = comp["corp_name"]

    # 2-tier: Delta first, Parquet fallback
    df = pd.DataFrame()
    try:
        from agents.data_helper import read_gold_data
        df = read_gold_data("dart_financial_statement")
        if df is not None and len(df) > 0:
            logger.info("financial_statement loaded via Delta: %d rows", len(df))
            if "bsns_year" in df.columns:
                from_year = int(date_from[:4]) if date_from and len(date_from) >= 4 else None
                to_year = int(date_to[:4]) if date_to and len(date_to) >= 4 else None
                if from_year:
                    df = df[df["bsns_year"].astype(int) >= from_year]
                if to_year:
                    df = df[df["bsns_year"].astype(int) <= to_year]
            df = _filter_by_date_col(df, "rcept_dt", date_from, date_to)
    except Exception as e:
        logger.debug("Delta financial_statement read failed: %s", e)

    if len(df) == 0:
        logger.info("Delta empty, trying Gold Parquet...")
        prefix = "gold/dart/facts/financial_statement/"
        all_keys = _list_gold_partitions(prefix)
        from_year = int(date_from[:4]) if date_from and len(date_from) >= 4 else None
        to_year = int(date_to[:4]) if date_to and len(date_to) >= 4 else None
        relevant_keys = []
        for key in all_keys:
            m = re.search(r'bsns_year=(\d{4})/', key)
            if m:
                year = int(m.group(1))
                if from_year and year < from_year:
                    continue
                if to_year and year > to_year:
                    continue
                relevant_keys.append(key)
        if not relevant_keys:
            relevant_keys = all_keys
        if not relevant_keys:
            return f"{corp_name}의 재무제표 데이터가 없습니다."
        df = _read_parquet_keys(relevant_keys, limit_rows=10_000_000)
    if len(df) == 0:
        return f"{corp_name}의 재무제표 데이터가 없습니다."

    df = df[df["corp_code"].astype(str).str.lstrip("0") == corp_code.lstrip("0")]
    if len(df) == 0:
        return f"{corp_name}의 재무제표 데이터가 없습니다."

    df["rcept_dt_sort"] = df["rcept_dt"].astype(str)
    df = df.sort_values(["bsns_year", "rcept_dt_sort"], ascending=[False, False])

    report_groups = df.groupby(
        ["bsns_year", "reprt_code", "fs_div", "rcept_dt", "report_type"],
        dropna=False, sort=False
    )

    lines = [f"=== {corp_name} 재무제표 ==="]
    lines.append(f"종목코드: {comp['stock_code']}, DART 고유번호: {corp_code}")

    shown = 0
    max_reports = max(1, limit_rows // 20)
    for (year, reprt_code, fs_div, rcept_dt, report_type), grp in report_groups:
        if shown >= max_reports:
            break
        sj_div = grp["sj_div"].iloc[0] if "sj_div" in grp.columns else ""
        fs_label = f"{fs_div}" if fs_div and str(fs_div) != "nan" else ""
        if sj_div and str(sj_div) != "nan":
            fs_label = f"{fs_div}/{sj_div}" if fs_label else str(sj_div)

        lines.append(f"\n[{year}년] {reprt_code} ({fs_label})")
        lines.append(f"  접수일자: {rcept_dt}")

        for label, patterns in _FINANCIAL_KEY_ACCOUNTS:
            val = _find_account_value(grp, patterns)
            lines.append(f"  {label}: {_fmt_won(val)}")
        shown += 1

    if shown == 0:
        return f"{corp_name}의 재무제표 데이터를 표시할 수 없습니다."
    return "\n".join(lines)


def query_disclosure_events(companies, codes, date_from=None, date_to=None,
                             is_recent=False, limit_rows=500,
                             page=1, page_size=20):
    if is_recent and not date_from:
        date_from = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")

    # Merge all 3 sources: Delta material_event (active pipeline, sparse),
    # Gold facts material_event (DartCollector partitioned), legacy compacted
    # disclosure_events (historical data). Always merge all paths, deduplicate by rcept_no.
    dfs = []

    # Path 1: Delta material_event (active pipeline output, sparse backfill ~86 rows)
    try:
        from agents.data_helper import read_gold_data
        df_delta_me = read_gold_data("material_event")
        if df_delta_me is not None and len(df_delta_me) > 0:
            logger.info("material_event loaded via Delta: %d rows", len(df_delta_me))
            dfs.append(df_delta_me)
    except Exception as e:
        logger.debug("Delta material_event read failed: %s", e)

    # Path 2: Gold facts material_event (DartCollector partitioned)
    try:
        df_facts = _read_material_event_facts(date_from, date_to, limit_rows=50000)
        if len(df_facts) > 0:
            logger.info("material_event facts loaded: %d rows", len(df_facts))
            dfs.append(df_facts)
    except Exception as e:
        logger.debug("Gold facts read failed: %s", e)

    # Path 3: Legacy compacted disclosure_events (historical, most complete)
    try:
        df_legacy = _read_disclosure_events_fallback(date_from, date_to, limit_rows=50000)
        if len(df_legacy) > 0:
            logger.info("disclosure_events legacy loaded: %d rows", len(df_legacy))
            dfs.append(df_legacy)
    except Exception as e:
        logger.debug("Legacy disclosure read failed: %s", e)

    if not dfs:
        return "해당 기간의 공시 데이터가 없습니다."

    # Normalize columns across sources before merge
    for d in dfs:
        # Unify event_type -> event_category
        if "event_type" in d.columns and "event_category" not in d.columns:
            d["event_category"] = d["event_type"]
        # Fill text from report_nm if text is missing
        if "text" in d.columns:
            d["text"] = d["text"].fillna("")
            empty_mask = d["text"].astype(str).str.strip() == ""
            if empty_mask.any() and "report_nm" in d.columns:
                d.loc[empty_mask, "text"] = d.loc[empty_mask, "report_nm"].fillna("")
        elif "report_nm" in d.columns:
            d["text"] = d["report_nm"].fillna("")
        # Normalize rcept_dt format (remove hyphens for consistent dedup)
        if "rcept_dt" in d.columns:
            d["rcept_dt"] = d["rcept_dt"].astype(str).str.replace("-", "")

    # Merge and deduplicate by rcept_no (which is unique per disclosure filing)
    df = pd.concat(dfs, ignore_index=True)
    if "rcept_no" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["rcept_no"], keep="first")
        logger.info("Deduplicated disclosure events: %d -> %d", before, len(df))
    elif "rcept_dt" in df.columns and "corp_name" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["rcept_dt", "corp_name", "report_nm"], keep="first")
        logger.info("Deduplicated (fallback keys): %d -> %d", before, len(df))

    # Apply date filter
    df = _filter_by_date_col(df, "rcept_dt", date_from, date_to)

    if len(df) == 0:
        return "해당 기간의 공시 데이터가 없습니다."

    comp = _find_company(companies, codes)
    if comp:
        corp_code = comp["corp_code"]
        df = df[df["corp_code"].astype(str).str.lstrip("0") == corp_code.lstrip("0")]
        if len(df) == 0:
            return f"{comp['corp_name']}의 공시 내역이 없습니다."

    if len(df) > limit_rows:
        df = df.head(limit_rows)

    total_count = len(df)
    df_sorted = df.sort_values("rcept_dt", ascending=False)
    total_pages = max(1, (total_count + page_size - 1) // page_size)
    start = (page - 1) * page_size
    paginated = df_sorted.iloc[start:start + page_size]

    dates_series = df_sorted["rcept_dt"].astype(str)
    date_min = dates_series.min()[:10] if len(dates_series) > 0 else "?"
    date_max = dates_series.max()[:10] if len(dates_series) > 0 else "?"

    lines = ["=== 공시 이벤트 ==="]
    if comp:
        lines.append(f"회사: {comp['corp_name']} ({comp['stock_code']})")
    lines.append(f"조회 기간: {date_min} ~ {date_max}, 총 {total_count}건")
    lines.append("")

    for _, row in paginated.iterrows():
        text = str(row.get("normalized_text", row.get("text", "")))[:250]
        text = text.replace("\n", " ")
        category = str(row.get("event_type", row.get("event_category", "N/A")))
        report_nm = str(row.get("report_nm", ""))
        lines.append(
            f"[{row.get('rcept_dt', 'N/A')}] {row.get('corp_name', '')} | "
            f"{report_nm} | "
            f"유형: {category}\n"
            f"  내용: {text}"
        )

    lines.append(f"\n[페이지 {page}/{total_pages}, 총 {total_count}건]")
    return "\n".join(lines)


def _read_material_event_facts(date_from, date_to, limit_rows=500):
    prefix = "gold/dart/facts/material_event/"
    all_keys = _list_gold_partitions(prefix)
    months = _enumerate_months(date_from, date_to, default_months=6)

    relevant_keys = []
    for key in all_keys:
        m = re.search(r'rcept_year=(\d{4})/rcept_month=(\d{2})/', key)
        if m:
            if (m.group(1), m.group(2)) in months:
                relevant_keys.append(key)
        else:
            relevant_keys.append(key)

    if not relevant_keys:
        relevant_keys = all_keys

    df = _read_parquet_keys(relevant_keys, limit_rows)
    df = _filter_by_date_col(df, "rcept_dt", date_from, date_to)
    return df


def _read_disclosure_events_fallback(date_from, date_to, limit_rows=500):
    """Path 3 fallback: read legacy disclosure_events compacted parquet files."""
    prefix = "gold/dart/disclosure_events/"
    all_keys = _list_gold_partitions(prefix)
    if not all_keys:
        return pd.DataFrame()
    months = _enumerate_months(date_from, date_to, default_months=3)
    relevant_keys = []
    for key in all_keys:
        m = re.search(r'dt=(\d{4}-\d{2})/', key)
        if m:
            ym = m.group(1)
            year, month = ym.split("-")
            if (year, month) in months:
                relevant_keys.append(key)
        else:
            relevant_keys.append(key)
    if not relevant_keys:
        relevant_keys = all_keys
    logger.info("disclosure_events fallback: scanning %d keys", len(relevant_keys))
    df = _read_parquet_keys(relevant_keys, limit_rows)
    if len(df) == 0:
        return df
    # Fill empty text with report_nm placeholder (compaction may have lost text)
    if "text" in df.columns:
        empty_mask = df["text"].isna() | (df["text"].astype(str).str.strip() == "")
        if empty_mask.any():
            df.loc[empty_mask, "text"] = df.loc[empty_mask, "report_nm"].fillna("").apply(
                lambda x: f"[{x}]" if x else ""
            )
    df = _filter_by_date_col(df, "rcept_dt", date_from, date_to)
    return df


def query_insider_transactions(companies, codes, date_from=None, date_to=None,
                                is_recent=False, limit_rows=500,
                                page=1, page_size=20):
    if is_recent and not date_from:
        date_from = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    df = _read_ownership_facts("elestock", date_from, date_to, limit_rows)
    if len(df) == 0:
        return "해당 기간의 임원 거래 데이터가 없습니다."

    comp = _find_company(companies, codes)
    if comp:
        corp_code = comp["corp_code"]
        df = df[df["corp_code"].astype(str).str.lstrip("0") == corp_code.lstrip("0")]
        if len(df) == 0:
            return f"{comp['corp_name']}의 임원 거래 내역이 없습니다."

    total_count = len(df)
    df_sorted = df.sort_values("rcept_dt", ascending=False)
    total_pages = max(1, (total_count + page_size - 1) // page_size)
    start = (page - 1) * page_size
    paginated = df_sorted.iloc[start:start + page_size]

    total_buy = 0
    total_sell = 0
    for _, row in df_sorted.iterrows():
        irds = _parse_num(row.get("stkqy_irds"), 0)
        if irds > 0:
            total_buy += irds
        else:
            total_sell += irds

    dates_series = df_sorted["rcept_dt"].astype(str)
    date_min = dates_series.min()[:10] if len(dates_series) > 0 else "?"
    date_max = dates_series.max()[:10] if len(dates_series) > 0 else "?"

    lines = ["=== 임원 거래내역 ==="]
    if comp:
        lines.append(f"회사: {comp['corp_name']} ({comp['stock_code']})")
    lines.append(f"총 매수: {total_buy:,}주, 총 매도: {abs(total_sell):,}주")
    lines.append(f"조회 기간: {date_min} ~ {date_max}, 총 {total_count}건")
    lines.append("")

    for _, row in paginated.iterrows():
        irds = _parse_num(row.get("stkqy_irds"), 0)
        direction = "매수" if irds > 0 else "매도"
        stkqy = _parse_num(row.get("stkqy"), 0)
        repror = str(row.get("repror", ""))
        report_resn = str(row.get("report_resn", ""))
        lines.append(
            f"[{row.get('rcept_dt', 'N/A')}] {row.get('corp_name', '')} | "
            f"{repror} | "
            f"{direction} {abs(irds):,}주 | "
            f"보유 {stkqy:,}주 | "
            f"사유: {report_resn}"
        )

    lines.append(f"\n[페이지 {page}/{total_pages}, 총 {total_count}건]")
    return "\n".join(lines)


def query_major_shareholders(companies, codes, date_from=None, date_to=None,
                              is_recent=False, limit_rows=500,
                              page=1, page_size=20):
    if is_recent and not date_from:
        date_from = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")

    df = _read_ownership_facts("majorstock", date_from, date_to, limit_rows)
    if len(df) == 0:
        return "해당 기간의 주요주주 데이터가 없습니다."

    comp = _find_company(companies, codes)
    if comp:
        corp_code = comp["corp_code"]
        df = df[df["corp_code"].astype(str).str.lstrip("0") == corp_code.lstrip("0")]
        if len(df) == 0:
            return f"{comp['corp_name']}의 주요주주 내역이 없습니다."

    total_count = len(df)
    df_sorted = df.sort_values("rcept_dt", ascending=False)
    total_pages = max(1, (total_count + page_size - 1) // page_size)
    start = (page - 1) * page_size
    paginated = df_sorted.iloc[start:start + page_size]

    dates_series = df_sorted["rcept_dt"].astype(str)
    date_min = dates_series.min()[:10] if len(dates_series) > 0 else "?"
    date_max = dates_series.max()[:10] if len(dates_series) > 0 else "?"

    lines = ["=== 주요주주 현황 ==="]
    if comp:
        lines.append(f"회사: {comp['corp_name']} ({comp['stock_code']})")
    lines.append(f"조회 기간: {date_min} ~ {date_max}, 총 {total_count}건")
    lines.append("")

    for _, row in paginated.iterrows():
        stkqy = _parse_num(row.get("stkqy"), 0)
        stkrt = _parse_num(row.get("stkrt"), 0.0)
        stkqy_irds = _parse_num(row.get("stkqy_irds"), 0)
        stkrt_irds = _parse_num(row.get("stkrt_irds"), 0.0)
        repror = str(row.get("repror", ""))
        report_resn = str(row.get("report_resn", ""))
        report_tp = str(row.get("report_tp", ""))
        change_str = ""
        if stkqy_irds != 0:
            sign = "+" if stkqy_irds > 0 else ""
            change_str = f" (변동 {sign}{stkqy_irds:,}주, {stkrt_irds:+.2f}%)"
        lines.append(
            f"[{row.get('rcept_dt', 'N/A')}] {row.get('corp_name', '')} | "
            f"{repror} ({report_tp}) | "
            f"보유 {stkqy:,}주 ({stkrt:.2f}%){change_str} | "
            f"사유: {report_resn}"
        )

    lines.append(f"\n[페이지 {page}/{total_pages}, 총 {total_count}건]")
    return "\n".join(lines)


def _read_ownership_facts(ownership_type, date_from, date_to, limit_rows=500):
    # Delta first, Gold Parquet fallback
    df = pd.DataFrame()
    try:
        from agents.data_helper import read_gold_data
        df = read_gold_data("dart_ownership")
        if df is not None and len(df) > 0:
            logger.info("ownership loaded via Delta: %d rows", len(df))
            if "ownership_type" in df.columns:
                df = df[df["ownership_type"].astype(str) == ownership_type]
            df = _filter_by_date_col(df, "rcept_dt", date_from, date_to)
    except Exception as e:
        logger.debug("Delta ownership read failed: %s", e)

    if len(df) > 0:
        return df

    logger.info("Delta empty, trying Gold Parquet for ownership type=%s", ownership_type)
    prefix = f"gold/dart/facts/ownership/ownership_type={ownership_type}/"
    all_keys = _list_gold_partitions(prefix)
    if not all_keys:
        logger.warning("No ownership facts found for type=%s", ownership_type)
        return pd.DataFrame()

    months = _enumerate_months(date_from, date_to, default_months=12)
    relevant_keys = []
    for key in all_keys:
        m = re.search(r'rcept_year=(\d{4})/rcept_month=(\d{2})/', key)
        if m:
            if (m.group(1), m.group(2)) in months:
                relevant_keys.append(key)
        else:
            relevant_keys.append(key)

    if not relevant_keys:
        relevant_keys = all_keys

    df = _read_parquet_keys(relevant_keys, limit_rows)
    df = _filter_by_date_col(df, "rcept_dt", date_from, date_to)
    return df


def query_dart(intent, companies, codes, date_from=None, date_to=None,
               is_recent=False, page=1, page_size=20):
    logger.info("DART query: intent=%s companies=%s codes=%s date=%s~%s",
                 intent, companies, codes, date_from, date_to)

    if intent == "hybrid":
        parts = []
        for sub_intent, handler_fn in [
            ("dart_disclosure", query_disclosure_events),
            ("dart_financial", query_financials),
        ]:
            try:
                if sub_intent == "dart_financial":
                    result = handler_fn(companies, codes,
                                        date_from=date_from, date_to=date_to,
                                        is_recent=is_recent)
                else:
                    result = handler_fn(companies, codes,
                                        date_from=date_from, date_to=date_to,
                                        is_recent=is_recent,
                                        page=page, page_size=page_size)
                if result and "데이터가 없습니다" not in result:
                    parts.append(result)
            except Exception as e:
                logger.warning("Hybrid sub-query %s failed: %s", sub_intent, e)
        if parts:
            return "\n\n".join(parts)
        return "하이브리드 쿼리에서 데이터를 찾을 수 없습니다."

    handlers = {
        "dart_financial": query_financials,
        "dart_insider": query_insider_transactions,
        "dart_disclosure": query_disclosure_events,
        "dart_shareholder": query_major_shareholders,
    }

    handler_fn = handlers.get(intent)
    if not handler_fn:
        return f"DART intent '{intent}' is not supported."

    try:
        if intent == "dart_financial":
            return handler_fn(companies, codes,
                              date_from=date_from, date_to=date_to,
                              is_recent=is_recent)
        else:
            return handler_fn(companies, codes,
                              date_from=date_from, date_to=date_to,
                              is_recent=is_recent,
                              page=page, page_size=page_size)
    except Exception as e:
        logger.error("DART query error: %s", e, exc_info=True)
        return f"DART 데이터 조회 중 오류가 발생했습니다: {e}"
