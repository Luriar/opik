"""
DART Query Engine — reads DART Gold Parquet tables from S3 using PyArrow.
No Spark required. Partition pruning via S3 listing.

Supports:
  dart_financial       → financial statements (revenue, profit, assets)
  dart_insider          → insider transactions (buy/sell)
  dart_disclosure       → disclosure events (text + category)
  dart_shareholder      → major shareholder changes
  corporate_events      → corporate event summaries (bonus)
"""

import io
import json
import logging
import re
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta

import boto3
import pyarrow.parquet as pq
import pandas as pd

logger = logging.getLogger("opik.dart")

S3_BUCKET = "s3-opik-bucket"
AWS_REGION = "ap-northeast-2"

# Cache for company_master lookup (loaded once)
_company_lookup: Optional[Dict[str, Dict]] = None
_company_lookup_ts: float = 0


def _get_s3():
    return boto3.client("s3", region_name=AWS_REGION)


def _load_company_master() -> Dict[str, Dict]:
    """Load company_master as name->info and code->name lookups. Cached for 1 hour."""
    global _company_lookup, _company_lookup_ts
    now = datetime.now().timestamp()
    if _company_lookup is not None and (now - _company_lookup_ts) < 3600:
        return _company_lookup

    logger.info("Loading company_master...")
    s3 = _get_s3()
    obj = s3.get_object(Bucket=S3_BUCKET, Key="gold/dart/company_master/data.parquet")
    buf = io.BytesIO(obj["Body"].read())
    df = pq.read_table(buf).to_pandas()

    lookup = {}
    for _, row in df.iterrows():
        name = str(row.get("corp_name", "")).strip()
        code = str(row.get("stock_code", "")).strip()
        corp_code = str(row.get("corp_code", "")).strip()

        key = name.replace("(주)", "").strip()
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
    "sk": "에스케이", "lg": "엘지", "kt": "케이티",
    "skc": "에스케이씨",
}


def _normalize_name(name: str) -> str:
    """Convert English-prefixed company names to Korean (SK하이닉스 → 에스케이하이닉스)."""
    lower = name.lower()
    for eng, kor in _KOR_ALIASES.items():
        if lower.startswith(eng) and len(name) > len(eng):
            return kor + name[len(eng):]
    return name


def _find_company(query_companies: List[str], query_codes: List[str]) -> Optional[Dict]:
    """Find company info from name or code with English→Korean alias support."""
    master = _load_company_master()

    # Expand search names with aliases
    search_names = []
    for name in query_companies:
        clean = name.replace("(주)", "").strip()
        search_names.extend([name, clean, _normalize_name(name), _normalize_name(clean)])

    # Try exact matches first
    for name in search_names:
        if name in master:
            return master[name]

    # Try codes
    for code in query_codes:
        if code in master:
            return master[code]

    # Fuzzy match: company name starts with search term or vice versa
    for name in search_names:
        name_lower = name.lower()
        for key, info in master.items():
            key_lower = key.lower()
            if key_lower.startswith(name_lower) or name_lower.startswith(key_lower):
                return info

    return None


def _get_date_partitions(table_prefix, date_from, date_to, max_parts=6):
    """Get dt=YYYY-MM partitions in date range.

    Normalizes date_from/date_to to YYYY-MM for partition matching.
    Falls back to latest N if filter eliminates everything.
    """
    s3 = _get_s3()
    resp = s3.list_objects_v2(
        Bucket=S3_BUCKET, Prefix=f"{table_prefix}/dt=", Delimiter="/"
    )
    all_parts = []
    for cp in resp.get("CommonPrefixes", []):
        dt = cp["Prefix"].split("dt=")[-1].rstrip("/")
        if dt:
            all_parts.append(dt)
    all_parts.sort(reverse=True)

    if not all_parts:
        return []

    # Normalize to YYYY-MM for partition-level comparison
    p_from = date_from[:7] if date_from and len(date_from) >= 7 else date_from
    p_to = date_to[:7] if date_to and len(date_to) >= 7 else date_to

    filtered = []
    for dt in all_parts:
        if p_from and dt < p_from:
            continue
        if p_to and dt > p_to:
            continue
        filtered.append(dt)

    if not filtered and (p_from or p_to):
        logger.info("Date filter %s~%s missed all partitions, returning latest 3",
                     p_from, p_to)
        return all_parts[:3]

    return filtered[:max_parts]


def _filter_by_date_col(df, col, date_from, date_to):
    """Day-level filter on a date column (supports 'YYYYMMDD' and 'YYYY-MM-DD' formats)."""
    if col not in df.columns or (not date_from and not date_to):
        return df
    # Normalize to YYYYMMDD for comparison
    vals = df[col].astype(str).str.replace("-", "")
    mask = pd.Series(True, index=df.index)
    if date_from:
        dfrom = date_from.replace("-", "")
        mask = mask & (vals >= dfrom)
    if date_to:
        dto = date_to.replace("-", "")
        mask = mask & (vals <= dto)
    return df[mask]


def _read_parquet_partitions(table_prefix, dt_parts, limit_rows=500):
    """Read parquet from given dt partitions."""
    s3 = _get_s3()
    frames = []
    total_read = 0

    for dt in dt_parts:
        if total_read >= limit_rows:
            break
        try:
            obj = s3.get_object(
                Bucket=S3_BUCKET, Key=f"{table_prefix}/dt={dt}/data.parquet"
            )
            buf = io.BytesIO(obj["Body"].read())
            table = pq.read_table(buf)
            df = table.to_pandas()
            df["_dt_partition"] = dt
            frames.append(df)
            total_read += len(df)
        except Exception as e:
            logger.warning("Error reading %s/dt=%s: %s", table_prefix, dt, e)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ============================================================
# Query handlers
# ============================================================

def query_financials(companies, codes, date_from=None, date_to=None,
                     is_recent=False, limit_rows=500):
    """Query DART financial statements."""
    comp = _find_company(companies, codes)
    if not comp:
        return "회사 정보를 찾을 수 없습니다. 종목코드나 회사명을 확인해주세요."

    corp_code = comp["corp_code"]
    corp_name = comp["corp_name"]

    s3 = _get_s3()
    key = f"gold/dart/financials/corp_code={corp_code}/data.parquet"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        df = pq.read_table(buf).to_pandas()
    except Exception as e:
        return f"{corp_name}({corp_code})의 재무제표 데이터를 찾을 수 없습니다."

    if len(df) == 0:
        return f"{corp_name}의 재무제표 데이터가 없습니다."

    df_all = df  # save unfiltered for available-years hint

    if date_from or date_to:
        from_year = date_from[:4] if date_from else None
        to_year = date_to[:4] if date_to else None
        mask = pd.Series(True, index=df.index)
        if from_year:
            mask = mask & (df["bsns_year"].astype(str) >= from_year)
        if to_year:
            mask = mask & (df["bsns_year"].astype(str) <= to_year)
        df = df[mask]

    if len(df) == 0:
        available_years = sorted(set(str(int(y)) for y in df_all["bsns_year"].dropna().unique()))
        if available_years and (date_from or date_to):
            years_str = ", ".join(available_years)
            req_range = f"{date_from or '?'}~{date_to or '?'}"
            return (
                f"{corp_name}의 {req_range} 재무제표 데이터가 없습니다.\n"
                f"현재 이용 가능한 연도: {years_str}년"
            )
        return f"{corp_name}의 해당 기간 재무제표 데이터가 없습니다."

    df = df.head(limit_rows)

    lines = [f"=== {corp_name} 재무제표 ==="]
    lines.append(f"종목코드: {comp['stock_code']}, DART 고유번호: {corp_code}")

    for _, row in df.iterrows():
        year = row.get("bsns_year", "N/A")
        report = row.get("report_nm", "")
        consolidation = row.get("consolidation", "")
        lines.append(f"\n[{year}년] {report} ({consolidation})")
        lines.append(f"  접수일자: {row.get('rcept_dt', 'N/A')}")

        revenue = row.get("revenue")
        op_income = row.get("operating_income")
        net_income = row.get("net_income")
        total_assets = row.get("total_assets")
        total_equity = row.get("total_equity")

        def fmt_won(val, unit="억원"):
            if val is None or pd.isna(val):
                return "N/A"
            v = int(val) // 100_000_000
            if abs(v) >= 10000:
                return f"{v/10000:.2f}조원"
            return f"{v:,}억원"

        lines.append(f"  매출액: {fmt_won(revenue)}")
        lines.append(f"  영업이익: {fmt_won(op_income)}")
        lines.append(f"  당기순이익: {fmt_won(net_income)}")
        lines.append(f"  총자산: {fmt_won(total_assets)}")
        lines.append(f"  총자본: {fmt_won(total_equity)}")

    return "\n".join(lines)


def query_insider_transactions(companies, codes, date_from=None, date_to=None,
                                is_recent=False, limit_rows=500,
                                page=1, page_size=20):
    """Query insider transactions (paginated)."""
    table_prefix = "gold/dart/insider_transactions"
    if is_recent and not date_from:
        date_from = (datetime.now() - timedelta(days=90)).strftime("%Y-%m")
    dt_parts = _get_date_partitions(
        table_prefix, date_from or "2025-01", date_to or "2099-12"
    )
    if not dt_parts:
        return "해당 기간의 임원 거래 데이터가 없습니다."

    df = _read_parquet_partitions(table_prefix, dt_parts, limit_rows)
    if len(df) == 0:
        return "해당 기간의 임원 거래 데이터가 없습니다."

    # Day-level filter on rcept_dt
    df = _filter_by_date_col(df, "rcept_dt", date_from, date_to)
    if len(df) == 0:
        return "해당 기간의 임원 거래 데이터가 없습니다."

    comp = _find_company(companies, codes)
    if comp:
        corp_code = comp["corp_code"]
        df = df[
            (df["corp_code"] == corp_code) |
            (df["corp_code"] == corp_code.lstrip("0"))
        ]
        if len(df) == 0:
            return f"{comp['corp_name']}의 임원 거래 내역이 없습니다."

    total_count = len(df)
    df_sorted = df.sort_values("rcept_dt", ascending=False)
    total_pages = max(1, (total_count + page_size - 1) // page_size)
    start = (page - 1) * page_size
    paginated = df_sorted.iloc[start:start + page_size]

    total_buy = int(df_sorted[df_sorted["direction"] == "BUY"]["change_qty"].sum())
    total_sell = int(df_sorted[df_sorted["direction"] == "SELL"]["change_qty"].sum())

    lines = ["=== 임원•주요주주 거래내역 ==="]
    if comp:
        lines.append(f"회사: {comp['corp_name']} ({comp['stock_code']})")

    lines.append(f"총 매수: {total_buy:,}주, 총 매도: {abs(total_sell):,}주")
    lines.append(f"조회 기간: {dt_parts[-1]} ~ {dt_parts[0]}, 총 {total_count}건")
    lines.append("")

    for _, row in paginated.iterrows():
        direction = "매수" if row.get("direction") == "BUY" else "매도"
        lines.append(
            f"[{row.get('rcept_dt', 'N/A')}] {row.get('corp_name', '')} | "
            f"{row.get('repror', '')}({row.get('ofcps', '')}) | "
            f"{direction} {int(row.get('change_qty', 0)):,}주 | "
            f"보유 {int(row.get('holdings', 0)):,}주"
        )

    lines.append(f"\n[페이지 {page}/{total_pages}, 총 {total_count}건]")
    return "\n".join(lines)


def query_disclosure_events(companies, codes, date_from=None, date_to=None,
                             is_recent=False, limit_rows=500,
                             page=1, page_size=20):
    """Query disclosure events (paginated)."""
    table_prefix = "gold/dart/disclosure_events"
    if is_recent and not date_from:
        date_from = (datetime.now() - timedelta(days=60)).strftime("%Y-%m")
    dt_parts = _get_date_partitions(
        table_prefix, date_from or "2025-01", date_to or "2099-12"
    )
    if not dt_parts:
        return "해당 기간의 공시 데이터가 없습니다."

    df = _read_parquet_partitions(table_prefix, dt_parts, limit_rows)
    if len(df) == 0:
        return "해당 기간의 공시 데이터가 없습니다."

    # Day-level filter on rcept_dt
    df = _filter_by_date_col(df, "rcept_dt", date_from, date_to)
    if len(df) == 0:
        return "해당 기간의 공시 데이터가 없습니다."

    comp = _find_company(companies, codes)
    if comp:
        corp_code = comp["corp_code"]
        df = df[
            (df["corp_code"] == corp_code) |
            (df["corp_code"] == corp_code.lstrip("0"))
        ]
        if len(df) == 0:
            return f"{comp['corp_name']}의 공시 내역이 없습니다."

    total_count = len(df)
    df_sorted = df.sort_values("rcept_dt", ascending=False)
    total_pages = max(1, (total_count + page_size - 1) // page_size)
    start = (page - 1) * page_size
    paginated = df_sorted.iloc[start:start + page_size]

    lines = ["=== 공시 이벤트 ==="]
    if comp:
        lines.append(f"회사: {comp['corp_name']} ({comp['stock_code']})")
    lines.append(f"조회 기간: {dt_parts[-1]} ~ {dt_parts[0]}, 총 {total_count}건")
    lines.append("")

    for _, row in paginated.iterrows():
        text = str(row.get("text", ""))[:250].replace("\n", " ")
        lines.append(
            f"[{row.get('rcept_dt', 'N/A')}] {row.get('corp_name', '')} | "
            f"{row.get('report_nm', '')} | "
            f"카테고리: {row.get('event_category', 'N/A')}\n"
            f"  내용: {text}"
        )

    lines.append(f"\n[페이지 {page}/{total_pages}, 총 {total_count}건]")
    return "\n".join(lines)


def query_major_shareholders(companies, codes, date_from=None, date_to=None,
                              is_recent=False, limit_rows=500,
                              page=1, page_size=20):
    """Query major shareholder changes (paginated)."""
    table_prefix = "gold/dart/major_shareholders"
    if is_recent and not date_from:
        date_from = (datetime.now() - timedelta(days=180)).strftime("%Y-%m")
    dt_parts = _get_date_partitions(
        table_prefix, date_from or "2025-01", date_to or "2099-12"
    )
    if not dt_parts:
        return "해당 기간의 주요주주 데이터가 없습니다."

    df = _read_parquet_partitions(table_prefix, dt_parts, limit_rows)
    if len(df) == 0:
        return "해당 기간의 주요주주 데이터가 없습니다."

    # Day-level filter on rcept_dt
    df = _filter_by_date_col(df, "rcept_dt", date_from, date_to)
    if len(df) == 0:
        return "해당 기간의 주요주주 데이터가 없습니다."

    comp = _find_company(companies, codes)
    if comp:
        corp_code = comp["corp_code"]
        df = df[
            (df["corp_code"] == corp_code) |
            (df["corp_code"] == corp_code.lstrip("0"))
        ]
        if len(df) == 0:
            return f"{comp['corp_name']}의 주요주주 내역이 없습니다."

    total_count = len(df)
    df_sorted = df.sort_values("rcept_dt", ascending=False)
    total_pages = max(1, (total_count + page_size - 1) // page_size)
    start = (page - 1) * page_size
    paginated = df_sorted.iloc[start:start + page_size]

    lines = ["=== 주요주주 현황 ==="]
    if comp:
        lines.append(f"회사: {comp['corp_name']} ({comp['stock_code']})")
    lines.append(f"조회 기간: {dt_parts[-1]} ~ {dt_parts[0]}, 총 {total_count}건")
    lines.append("")

    for _, row in paginated.iterrows():
        lines.append(
            f"[{row.get('rcept_dt', 'N/A')}] {row.get('corp_name', '')} | "
            f"{row.get('repror', '')} | "
            f"보유 {int(row.get('stkqy', 0)):,}주 "
            f"({row.get('stkrt', 0):.1f}%) | "
            f"사유: {row.get('report_resn', '')}"
        )

    lines.append(f"\n[페이지 {page}/{total_pages}, 총 {total_count}건]")
    return "\n".join(lines)


# ============================================================
# Main dispatcher
# ============================================================

def query_dart(intent, companies, codes, date_from=None, date_to=None,
               is_recent=False, page=1, page_size=20):
    """Dispatch DART query based on intent.

    page/page_size control pagination for list-type results
    (disclosure_events, insider_transactions, major_shareholders).
    """
    logger.info("DART query: intent=%s companies=%s codes=%s date=%s~%s",
                 intent, companies, codes, date_from, date_to)

    # Hybrid: disclosure + financials
    if intent == "hybrid":
        parts = []
        for sub_intent, handler_fn in [
            ("dart_disclosure", query_disclosure_events),
            ("dart_financial", query_financials),
        ]:
            try:
                # Pass page/page_size only to handlers that support pagination
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
        # Pass page/page_size only to paginated handlers
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
