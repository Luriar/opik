"""latest_company_context — 기업별 최신 요약 캐시(서비스용, 정본 아님).

LLM이 매 질의마다 여러 데이터셋을 조인하지 않도록, 기업당 1 row로 최신 포인터·요약·산업·관계를 모은다.

소스(Gold 내부만):
  - dart_report_index(Service DB) : 유형별 최신 rcept_no/rcept_dt + 식별(corp_name/stock_code)
  - company_snapshot(Parquet)     : induty_code (KSIC 산업명으로 enrich)
  - rag_document(Parquet)         : 유형별 최신 요약 텍스트(재무/이벤트/지분/공시)
  - entity_relation(Parquet)      : related_corp_codes(해석된 mentioned_corp_code)
  - EntityIndex(Service DB)       : market_type(KOSPI/KOSDAQ)

순수 집계(build_context_rows)와 IO(run_gold_latest_context)를 분리해 테스트 가능하게 둔다.
"""
from __future__ import annotations

import io
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import text

from dart_agent.config import get_settings
from dart_agent.db import engine_from_url
from dart_agent.gold import ksic
from dart_agent.gold.entities import EntityIndex
from dart_agent.gold.field_roles import canonical_report_type
from dart_agent.gold.writer import _to_parquet_bytes
from dart_agent.storage import GoldPaths, build_storage

log = logging.getLogger(__name__)

# report_type → latest_company_context의 (rcept 포인터 prefix, 요약 텍스트 컬럼, 요약 rag_doc_type).
_TYPE_MAP = {
    "REGULAR": ("latest_regular", "financial_summary_text", "financial_summary_text"),
    "MATERIAL_EVENT": ("latest_material_event", "recent_event_summary_text", "summary_card"),
    "DISCLOSURE": ("latest_disclosure", "disclosure_summary_text", "raw_disclosure_text"),
    "OWNERSHIP": ("latest_ownership", "ownership_summary_text", "summary_card"),
}


def build_context_rows(*, index_rows: list[dict], induty_by_corp: dict[str, str],
                       summary_by_corp: dict[str, dict[tuple[str, str], str]],
                       related_by_corp: dict[str, list[str]], market_by_corp: dict[str, str],
                       snapshot_date: str) -> list[dict]:
    """기업별 최신 컨텍스트 row를 만든다(순수 함수)."""
    by_corp: dict[str, dict] = {}
    # 유형별 최신(rcept_dt 최대)만 남긴다.
    latest: dict[str, dict[str, dict]] = defaultdict(dict)  # corp -> report_type -> row
    for r in index_rows:
        corp = r["corp_code"]
        by_corp.setdefault(corp, r)
        rt = canonical_report_type(r.get("report_type"))
        cur = latest[corp].get(rt)
        if cur is None or str(r.get("rcept_dt") or "") > str(cur.get("rcept_dt") or ""):
            latest[corp][rt] = r

    now = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []
    for corp, ident in by_corp.items():
        induty = induty_by_corp.get(corp)
        row: dict[str, Any] = {
            "corp_code": corp,
            "stock_code": ident.get("stock_code"),
            "corp_name": ident.get("corp_name"),
            "market_type": market_by_corp.get(corp),
            "induty_code": induty,
            "induty_name": ksic.industry_name(induty),
            "induty_section": ksic.section_name(induty),
            "related_corp_codes": sorted(set(related_by_corp.get(corp, []))),
            "snapshot_date": snapshot_date,
            "updated_at": now,
        }
        summaries = summary_by_corp.get(corp, {})
        for rt, (prefix, summary_col, doc_type) in _TYPE_MAP.items():
            latest_row = latest[corp].get(rt)
            row[f"{prefix}_rcept_no"] = latest_row.get("rcept_no") if latest_row else None
            row[f"{prefix}_rcept_dt"] = str(latest_row.get("rcept_dt")) if latest_row else None
            row[summary_col] = summaries.get((rt, doc_type))
        rows.append(row)
    return rows


# ── IO ────────────────────────────────────────────────────────────────────

def _read_projected(storage, prefix: str, columns: list[str]) -> list[dict]:
    out: list[dict] = []
    for key in storage.list_keys(prefix):
        if not key.endswith(".parquet"):
            continue
        table = pq.read_table(io.BytesIO(storage.read_bytes(key)))
        cols = [c for c in columns if c in table.column_names]
        if cols:
            out.extend(table.select(cols).to_pylist())
    return out


def run_gold_latest_context(storage=None, service_engine=None) -> dict[str, Any]:
    """latest_company_context Parquet 스냅샷을 생성한다(일배치)."""
    settings = get_settings()
    storage = storage or build_storage(settings)
    engine = service_engine or engine_from_url(settings.service_db_url)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot_date = datetime.now(timezone.utc).date().isoformat()

    # 1) 식별 + 유형별 최신 포인터(dart_report_index, is_latest만).
    with engine.connect() as conn:
        index_rows = [dict(r) for r in conn.execute(text(
            "SELECT corp_code, stock_code, corp_name, report_type, rcept_no, rcept_dt "
            "FROM dart_report_index WHERE is_latest = true"
        )).mappings()]
    if not index_rows:
        return {"corps": 0, "reason": "dart_report_index empty"}

    root = GoldPaths.root()
    # 2) induty_code (company_snapshot 최신 per corp).
    induty_by_corp: dict[str, str] = {}
    seen_rcept: dict[str, str] = {}
    for s in _read_projected(storage, root + "company_snapshot/", ["corp_code", "induty_code", "source_rcept_no"]):
        corp = s.get("corp_code")
        rc = str(s.get("source_rcept_no") or "")
        if corp and rc >= seen_rcept.get(corp, ""):
            seen_rcept[corp] = rc
            if s.get("induty_code"):
                induty_by_corp[corp] = s["induty_code"]
    # 3) 유형별 최신 요약 텍스트(rag_document).
    summary_by_corp: dict[str, dict[tuple[str, str], str]] = defaultdict(dict)
    summ_dt: dict[tuple, str] = {}
    for d in _read_projected(storage, root + "rag/rag_document/",
                             ["corp_code", "report_type", "rag_doc_type", "rcept_dt", "text", "is_latest"]):
        if d.get("is_latest") is False:
            continue
        rt = canonical_report_type(d.get("report_type"))
        key = (d.get("corp_code"), rt, d.get("rag_doc_type"))
        dt = str(d.get("rcept_dt") or "")
        if dt >= summ_dt.get(key, ""):
            summ_dt[key] = dt
            summary_by_corp[d["corp_code"]][(rt, d.get("rag_doc_type"))] = d.get("text")
    # 4) related_corp_codes (entity_relation 해석분).
    related_by_corp: dict[str, list[str]] = defaultdict(list)
    for e in _read_projected(storage, root + "rag/entity_relation/", ["source_corp_code", "mentioned_corp_code"]):
        if e.get("mentioned_corp_code"):
            related_by_corp[e["source_corp_code"]].append(e["mentioned_corp_code"])
    # 5) market_type.
    idx = EntityIndex.from_engine(engine)
    market_by_corp = {corp: (idx.get(corp).market_type if idx.get(corp) else None)
                      for corp in {r["corp_code"] for r in index_rows}}

    rows = build_context_rows(index_rows=index_rows, induty_by_corp=induty_by_corp,
                              summary_by_corp=summary_by_corp, related_by_corp=related_by_corp,
                              market_by_corp=market_by_corp, snapshot_date=snapshot_date)
    storage.write_bytes(GoldPaths.latest_company_context(snapshot_date, run_id),
                        _to_parquet_bytes(rows), content_type="application/octet-stream")
    log.info("latest_company_context: %s corps", len(rows))
    return {"corps": len(rows), "snapshot_date": snapshot_date}
