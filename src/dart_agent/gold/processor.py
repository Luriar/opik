"""GoldProcessor — Silver report.json 1건 → 목적별 Gold row 집합.

핵심 설계:
  - "보고서 파일 단위"가 아니라 "공통 메타 + 유형별 fact + RAG + 관계"로 row grain을 분해한다.
  - 외부 _meta.rcept_no(보고서 접수번호)와 내부 row의 rcept_no(지분공시 등)는 분리한다.
    ownership.majorstock/elestock는 내부 row가 각자 rcept_no/rcept_dt를 가지므로 그 기준으로 fact를 만든다.
  - 숫자/표는 facts(Parquet)로만 보존하고, 벡터 입력(rag_document)에는 narrative만 넣는다.
  - 식별자는 결정적(rcept_no/자연키 기반) → 재처리 멱등, upsert로 중복 수렴.

순수 변환: DB/스토리지 부작용 없음. 이름→corp_code 재연결은 주입된 NameResolver가 담당(테스트 가능).
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from dart_agent.gold import field_roles as fr
from dart_agent.gold import normalize as nz
from dart_agent.gold.chunking import approx_tokens, make_chunk_id, make_chunk_id_keyed, split_text
from dart_agent.gold.entities import NameResolver, NullResolver
from dart_agent.ownership_latest import latest_ownership_rows

GOLD_VERSION = "v3"
CHUNK_SCHEMA_VERSION = "rag_chunk.v3"
CHUNKING_RULE_VERSION = "e5-small-token-budget.v1"
SUMMARY_VERSION = "summary_card.v1"

# 모든 row에서 fact 본문으로 보지 않는 메타 키(정규화 텍스트 생성 시 제외).
_META_KEYS = {"corp_cls", "corp_code", "corp_name", "rcept_no", "stlm_dt"}
# entity_relation을 뽑을 필드 정의: (table/event, 필드, relation_type, source_section).
_OWNERSHIP_RELATIONS = {
    "majorstock": ("repror", "MAJOR_SHAREHOLDER"),
    "elestock": ("repror", "INSIDER"),
}
_FACT_ID_KEYS = ("fact_id", "event_id", "ownership_fact_id", "securities_fact_id")
_RAW_BOILERPLATE_MARKERS = (
    "본 자료는 금융감독원 전자공시시스템",
    "전자공시시스템 dart",
    "첨부서류",
    "정정신고",
    "【 대표이사 등의 확인 】",
)


@dataclass
class GoldRowSet:
    """한 보고서가 만들어낸 Gold 데이터셋별 row 묶음."""

    report_registry: list[dict] = field(default_factory=list)
    company_snapshot: list[dict] = field(default_factory=list)
    document_text: list[dict] = field(default_factory=list)
    financial_statement: list[dict] = field(default_factory=list)
    regular_structured: list[dict] = field(default_factory=list)
    material_event: list[dict] = field(default_factory=list)
    ownership: list[dict] = field(default_factory=list)
    securities: list[dict] = field(default_factory=list)
    rag_document: list[dict] = field(default_factory=list)
    rag_chunk: list[dict] = field(default_factory=list)
    entity_relation: list[dict] = field(default_factory=list)

    def datasets(self) -> dict[str, list[dict]]:
        return {
            "report_registry": self.report_registry,
            "company_snapshot": self.company_snapshot,
            "document_text": self.document_text,
            "financial_statement": self.financial_statement,
            "regular_structured": self.regular_structured,
            "material_event": self.material_event,
            "ownership": self.ownership,
            "securities": self.securities,
            "rag_document": self.rag_document,
            "rag_chunk": self.rag_chunk,
            "entity_relation": self.entity_relation,
        }

    def counts(self) -> dict[str, int]:
        return {k: len(v) for k, v in self.datasets().items() if v}


def _ymd(raw: Any) -> tuple[str, str, str | None]:
    """'20250814' 또는 '2025-08-14' → ('2025','08','2025-08-14'). 파싱 실패 시 ('','', None)."""
    s = str(raw or "").strip().replace("-", "")
    if len(s) >= 8 and s[:8].isdigit():
        return s[:4], s[4:6], f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return "", "", None


def _hash(obj: Any) -> str:
    return hashlib.sha256(
        json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _dart_view_url(rcept_no: Any) -> str | None:
    """DART 원문 뷰어 URL. Silver 링크가 없을 때도 rcept_no 기반으로 복원 가능해야 한다."""
    s = str(rcept_no or "").strip()
    if not s:
        return None
    return f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={s}"


def _row_text(row: dict) -> str:
    """fact row의 검색·디버그용 정규화 텍스트(비메타 문자열 필드만)."""
    vals = []
    for k, v in row.items():
        if k in _META_KEYS or not isinstance(v, str):
            continue
        v = v.strip()
        if v and v != "-":
            vals.append(v.replace("\n", " "))
    return " · ".join(vals)


def _source_fact_ids(rs: GoldRowSet, datasets: tuple[str, ...] | None = None) -> list[str]:
    out: list[str] = []
    selected = datasets or ("financial_statement", "regular_structured", "material_event", "ownership", "securities")
    for dataset in selected:
        for row in getattr(rs, dataset, []):
            for key in _FACT_ID_KEYS:
                if row.get(key):
                    out.append(str(row[key]))
                    break
    return out


def _clean_raw_disclosure_text(text: str) -> str:
    """원문 본문이 들어온 경우에만 최소 정제한다. 현재 Silver는 원문 ZIP 링크만 제공한다."""
    seen: set[str] = set()
    lines: list[str] = []
    for raw in (text or "").splitlines():
        line = " ".join(str(raw).split())
        if len(line) < 2:
            continue
        low = line.lower()
        if any(marker.lower() in low for marker in _RAW_BOILERPLATE_MARKERS):
            continue
        if line in seen:
            continue
        seen.add(line)
        lines.append(line)
    return "\n".join(lines).strip()


def _record_chunk_text(base: dict, section_title: str, body: str) -> str:
    """record 청크를 self-contained하게 만들되 내부 식별자/날짜/코드는 넣지 않는다."""
    header = []
    if base.get("corp_name"):
        header.append(f"회사: {base['corp_name']}")
    if section_title:
        header.append(f"섹션: {section_title}")
    return "\n".join(header + [body])


def _record_source_fact_id(base: dict, api: str, rec_idx: int, record: dict, source_kind: str) -> str | None:
    if source_kind == "structured":
        return f"{base['rcept_no']}:{api}:{rec_idx}"
    if source_kind == "event":
        return f"{base['rcept_no']}:{api}:{rec_idx}"
    if source_kind == "ownership":
        inner_rcept = str(record.get("rcept_no") or base["rcept_no"])
        nat = f"{api}|{inner_rcept}|{record.get('repror','')}|{record.get('stkqy','')}|{record.get('sp_stock_lmp_cnt','')}"
        return f"{api}:{inner_rcept}:{hashlib.sha1(nat.encode()).hexdigest()[:10]}"  # noqa: S324
    return None


class GoldProcessor:
    def __init__(self, resolver: NameResolver | None = None, gold_version: str = GOLD_VERSION):
        self.resolver = resolver or NullResolver()
        self.gold_version = gold_version

    def process(self, report: dict, *, source_uri: str, generated_at: str) -> GoldRowSet:
        meta = report.get("_meta") or {}
        doc = report.get("document") or {}
        corp_code = str(meta.get("corp_code") or "")
        rcept_no = str(meta.get("rcept_no") or "")
        report_type = str(meta.get("report_type") or "DISCLOSURE")
        base_report_type = str(meta.get("base_report_type") or fr.canonical_report_type(report_type))
        stock_code = meta.get("stock_code")
        corp_name = meta.get("corp_name") or ""
        ry, rm, rcept_date = _ymd(meta.get("rcept_dt"))
        doc_id = f"dart:{corp_code}:{rcept_no}:{report_type}"
        is_latest = bool(meta.get("is_latest", True))
        gen_date = generated_at[:10]

        base = {
            "doc_id": doc_id,
            "corp_code": corp_code,
            "stock_code": stock_code,
            "corp_name": corp_name,
            "rcept_no": rcept_no,
            "rcept_dt": rcept_date,
            "rcept_year": ry,
            "rcept_month": rm,
            "report_type": report_type,
            "base_report_type": base_report_type,
            "is_latest": is_latest,
            "group_id": meta.get("group_id"),
            "original_rcept_no": meta.get("original_rcept_no"),
            "latest_rcept_no": meta.get("latest_rcept_no"),
            "is_amended": meta.get("is_amended"),
            "amendment_seq": meta.get("amendment_seq"),
            "source_uri": source_uri,
            "source_system": "DART",
            "source_silver_dataset": source_uri,
            "dart_view_url": doc.get("dart_view_url") or _dart_view_url(rcept_no),
            "gold_version": self.gold_version,
            "created_at": generated_at,
        }
        rs = GoldRowSet()
        derived_lines: list[str] = []  # 요약 카드 본문에 합칠 핵심 한 줄들.

        self._registry(rs, report, meta, base, generated_at)
        self._company_snapshot(rs, report, base, gen_date)
        self._document_text(rs, report, base)
        self._financials(rs, report, base)
        self._regular_structured(rs, report, base, derived_lines)
        self._material_event(rs, report, base, derived_lines)
        self._ownership(rs, report, base, derived_lines)
        self._securities(rs, report, base)
        self._rag(rs, report, meta, base, derived_lines)
        self._rag_records(rs, report, meta, base)
        self._entities(rs, report, base)
        return rs

    # ── report_registry ────────────────────────────────────────────────
    def _registry(self, rs, report, meta, base, generated_at) -> None:
        doc = report.get("document") or {}
        row = {
            **{k: base[k] for k in ("doc_id", "corp_code", "stock_code", "corp_name", "rcept_no",
                                    "rcept_dt", "rcept_year", "rcept_month", "report_type",
                                    "is_latest", "gold_version")},
            "base_report_type": base.get("base_report_type"),
            "group_id": base.get("group_id"),
            "original_rcept_no": base.get("original_rcept_no"),
            "latest_rcept_no": base.get("latest_rcept_no"),
            "is_amended": base.get("is_amended"),
            "amendment_seq": base.get("amendment_seq"),
            "source_silver_uri": base["source_uri"],
            "source_system": base["source_system"],
            "report_nm": (meta.get("report_nm") or "").strip(),
            "silver_version": meta.get("silver_version"),
            "silver_generated_at": meta.get("silver_generated_at"),
            "gold_generated_at": generated_at,
            "bronze_zip_path": doc.get("bronze_zip_path"),
            "dart_view_url": base.get("dart_view_url"),
            "document_available": bool(doc.get("available")),
            "has_text": bool(report.get("text")),
            "has_financials": bool(report.get("financials")),
            "has_structured": bool(report.get("structured")),
            "has_event_reports": any((report.get("event_reports") or {}).values()),
            "has_ownership": any((report.get("ownership") or {}).values()),
            "has_securities": any((report.get("securities") or {}).values()),
        }
        row["content_hash"] = _hash({k: row.get(k) for k in (
            "report_nm", "is_latest", "has_text", "has_financials", "has_structured",
            "has_event_reports", "has_ownership", "has_securities")})
        rs.report_registry.append(row)

    # ── company_snapshot ───────────────────────────────────────────────
    def _company_snapshot(self, rs, report, base, gen_date) -> None:
        ov = report.get("company_overview") or {}
        if not ov:
            return
        rs.company_snapshot.append({
            "corp_code": base["corp_code"],
            "stock_code": ov.get("stock_code") or base["stock_code"],
            "stock_name": ov.get("stock_name"),
            "corp_name": ov.get("corp_name") or base["corp_name"],
            "corp_name_eng": ov.get("corp_name_eng"),
            "corp_cls": ov.get("corp_cls"),
            "ceo_nm": ov.get("ceo_nm"),
            "bizr_no": ov.get("bizr_no"),
            "jurir_no": ov.get("jurir_no"),
            "adres": ov.get("adres"),
            "induty_code": ov.get("induty_code"),
            "est_dt": ov.get("est_dt"),
            "acc_mt": ov.get("acc_mt"),
            "hm_url": ov.get("hm_url"),
            "ir_url": ov.get("ir_url"),
            "source_rcept_no": base["rcept_no"],
            "snapshot_date": gen_date,
            "content_hash": _hash(ov),
        })

    # ── document_text (원문 텍스트가 있을 때만) ─────────────────────────
    def _document_text(self, rs, report, base) -> None:
        text = report.get("text")
        if not text:
            return
        doc = report.get("document") or {}
        rs.document_text.append({
            **{k: base[k] for k in ("doc_id", "corp_code", "stock_code", "corp_name", "rcept_no",
                                    "rcept_dt", "rcept_year", "rcept_month", "report_type")},
            "report_nm": report.get("title") or "",
            "section_no": 0,
            "section_title": None,
            "text": text,
            "text_len": len(text),
            "source_type": "silver_text",
            "source_silver_uri": base["source_uri"],
            "dart_view_url": base.get("dart_view_url"),
            "bronze_zip_path": doc.get("bronze_zip_path"),
            "content_hash": _hash(text),
        })

    # ── facts.financial_statement ──────────────────────────────────────
    def _financials(self, rs, report, base) -> None:
        fin = report.get("financials") or {}
        for fs_div in ("CFS", "OFS"):
            for i, r in enumerate(fin.get(fs_div) or []):
                ord_ = r.get("ord")
                acc = r.get("account_id") or "-"
                rs.financial_statement.append({
                    "fact_id": f"{base['rcept_no']}:{fs_div}:{r.get('sj_div')}:{ord_}:{acc}",
                    "corp_code": base["corp_code"], "stock_code": base["stock_code"],
                    "corp_name": base["corp_name"], "rcept_no": base["rcept_no"],
                    "rcept_dt": base["rcept_dt"], "report_type": base["report_type"],
                    "bsns_year": r.get("bsns_year") or "", "reprt_code": r.get("reprt_code") or "",
                    "fs_div": fs_div, "sj_div": r.get("sj_div"), "sj_nm": r.get("sj_nm"),
                    "account_id": acc, "account_nm": r.get("account_nm"),
                    "account_detail": r.get("account_detail"), "currency": r.get("currency"),
                    "thstrm_nm": r.get("thstrm_nm"), "thstrm_amount": r.get("thstrm_amount"),
                    "thstrm_add_amount": r.get("thstrm_add_amount"),
                    "frmtrm_nm": r.get("frmtrm_nm"), "frmtrm_amount": r.get("frmtrm_amount"),
                    "frmtrm_q_amount": r.get("frmtrm_q_amount"), "ord": ord_,
                    "is_latest": base["is_latest"], "source_silver_uri": base["source_uri"],
                    "dart_view_url": base.get("dart_view_url"),
                    "gold_version": base["gold_version"],
                    "rcept_year": base["rcept_year"], "rcept_month": base["rcept_month"],
                })

    # ── facts.regular_structured ───────────────────────────────────────
    def _regular_structured(self, rs, report, base, derived_lines) -> None:
        structured = report.get("structured") or {}
        for table_name, rows in structured.items():
            if not rows:
                continue
            for i, r in enumerate(rows):
                rs.regular_structured.append({
                    "fact_id": f"{base['rcept_no']}:{table_name}:{i}",
                    "table_name": table_name,
                    "corp_code": base["corp_code"], "stock_code": base["stock_code"],
                    "corp_name": base["corp_name"], "rcept_no": base["rcept_no"],
                    "rcept_dt": base["rcept_dt"], "report_type": base["report_type"],
                    "stlm_dt": r.get("stlm_dt"),
                    "payload_json": json.dumps(r, ensure_ascii=False, sort_keys=True),
                    "normalized_text": _row_text(r),
                    "is_latest": base["is_latest"], "source_silver_uri": base["source_uri"],
                    "dart_view_url": base.get("dart_view_url"),
                    "rcept_year": base["rcept_year"], "rcept_month": base["rcept_month"],
                })
            summary = nz.regular_structured_summary(table_name, rows, base["corp_name"])
            if summary:
                derived_lines.append(summary)

    # ── facts.material_event ───────────────────────────────────────────
    def _material_event(self, rs, report, base, derived_lines) -> None:
        events = report.get("event_reports") or {}
        for event_type, rows in events.items():
            if not rows:
                continue
            for i, r in enumerate(rows):
                rs.material_event.append({
                    "event_id": f"{base['rcept_no']}:{event_type}:{i}",
                    "event_type": event_type,
                    "corp_code": base["corp_code"], "stock_code": base["stock_code"],
                    "corp_name": base["corp_name"], "rcept_no": base["rcept_no"],
                    "rcept_dt": base["rcept_dt"], "report_type": base["report_type"],
                    "base_report_type": base.get("base_report_type"),
                    "report_nm": (report.get("_meta") or {}).get("report_nm"),
                    "event_date": nz.iso_date(r.get("bddd") or r.get("pymd")),
                    "amount": nz.to_int(r.get("bd_fta") or r.get("fdpp_fclt")),
                    "counterparty": r.get("extg"),
                    "security_type": r.get("bd_knd"),
                    "maturity_date": nz.iso_date(r.get("bd_mtd")),
                    "payload_json": json.dumps(r, ensure_ascii=False, sort_keys=True),
                    "normalized_text": nz.material_event_text(event_type, r, base["corp_name"]),
                    "is_latest": base["is_latest"], "source_silver_uri": base["source_uri"],
                    "source_system": base["source_system"],
                    "source_record_key": f"{base['rcept_no']}:{event_type}:{i}",
                    "dart_view_url": base.get("dart_view_url"),
                    "rcept_year": base["rcept_year"], "rcept_month": base["rcept_month"],
                })
                derived_lines.append(nz.material_event_text(event_type, r, base["corp_name"]))

    # ── facts.ownership (내부 rcept_no 기준) ────────────────────────────
    def _ownership(self, rs, report, base, derived_lines) -> None:
        ownership = report.get("ownership") or {}
        for otype, rows in ownership.items():
            rows = latest_ownership_rows(otype, rows or [])
            if not rows:
                continue
            for r in rows:
                inner_rcept = str(r.get("rcept_no") or base["rcept_no"])
                iy, im, idt = _ymd(r.get("rcept_dt"))
                # 자연키: 같은 내부 row가 여러 외부 문서에 중복돼도 fact_id로 수렴(upsert).
                nat = f"{otype}|{inner_rcept}|{r.get('repror','')}|{r.get('stkqy','')}|{r.get('sp_stock_lmp_cnt','')}"
                fact_id = f"{otype}:{inner_rcept}:{hashlib.sha1(nat.encode()).hexdigest()[:10]}"  # noqa: S324
                rs.ownership.append({
                    "ownership_fact_id": fact_id,
                    "ownership_type": otype,
                    "corp_code": r.get("corp_code") or base["corp_code"],
                    "stock_code": base["stock_code"], "corp_name": r.get("corp_name") or base["corp_name"],
                    "rcept_no": inner_rcept, "rcept_dt": idt or base["rcept_dt"],
                    "outer_rcept_no": base["rcept_no"],
                    "outer_rcept_dt": base["rcept_dt"],
                    "report_type": base["report_type"],
                    "base_report_type": base.get("base_report_type"),
                    "repror": r.get("repror"), "report_tp": r.get("report_tp"),
                    "report_resn": r.get("report_resn"),
                    "stkqy": r.get("stkqy"), "stkrt": r.get("stkrt"),
                    "stkqy_irds": r.get("stkqy_irds"), "stkrt_irds": r.get("stkrt_irds"),
                    "sp_stock_lmp_cnt": r.get("sp_stock_lmp_cnt"), "sp_stock_lmp_rate": r.get("sp_stock_lmp_rate"),
                    "sp_stock_lmp_irds_cnt": r.get("sp_stock_lmp_irds_cnt"),
                    "sp_stock_lmp_irds_rate": r.get("sp_stock_lmp_irds_rate"),
                    "source_outer_doc_id": base["doc_id"], "source_silver_uri": base["source_uri"],
                    "source_system": base["source_system"],
                    "source_record_key": fact_id,
                    "outer_dart_view_url": base.get("dart_view_url"),
                    "dart_view_url": _dart_view_url(inner_rcept) or base.get("dart_view_url"),
                    "payload_json": json.dumps(r, ensure_ascii=False, sort_keys=True),
                    "normalized_text": _row_text(r),
                    # 지분 fact는 시계열 이력 — 행마다 별개 filing이라 is_latest=True로 보존(정정은 새 rcept_no).
                    "is_latest": True,
                    "rcept_year": iy or base["rcept_year"], "rcept_month": im or base["rcept_month"],
                })
            summary = nz.ownership_summary_text(otype, rows, base["corp_name"])
            if summary:
                derived_lines.append(summary)

    # ── facts.securities (빈 배열은 row 미생성) ─────────────────────────
    def _securities(self, rs, report, base) -> None:
        securities = report.get("securities") or {}
        for stype, rows in securities.items():
            if not rows:
                continue
            for i, r in enumerate(rows):
                rs.securities.append({
                    "securities_fact_id": f"{base['rcept_no']}:{stype}:{i}",
                    "securities_type": stype,
                    "corp_code": base["corp_code"], "stock_code": base["stock_code"],
                    "corp_name": base["corp_name"], "rcept_no": base["rcept_no"],
                    "rcept_dt": base["rcept_dt"], "report_type": base["report_type"],
                    "payload_json": json.dumps(r, ensure_ascii=False, sort_keys=True),
                    "normalized_text": _row_text(r),
                    "is_latest": base["is_latest"], "source_silver_uri": base["source_uri"],
                    "dart_view_url": base.get("dart_view_url"),
                    "rcept_year": base["rcept_year"], "rcept_month": base["rcept_month"],
                })

    # ── rag_chunk row 빌더(공통) ───────────────────────────────────────
    def _chunk_row(self, base, meta, *, chunk_id, rag_doc_id, rag_doc_type, section_title,
                   api, chunk_index, chunk_text, importance, keywords,
                   source_fact_ids: list[str] | None = None,
                   source_chunk_ids: list[str] | None = None,
                   generated_from_hash: str | None = None,
                   summary_version: str | None = None,
                   dart_view_url: str | None = None,
                   row_identity: dict | None = None,
                   source_record_key: str | None = None) -> dict:
        ident = row_identity or {}
        return {
            "chunk_id": chunk_id, "rag_doc_id": rag_doc_id, "source_doc_id": base["doc_id"],
            "corp_code": base["corp_code"], "stock_code": base["stock_code"],
            "corp_name": base["corp_name"], "rcept_no": ident.get("rcept_no", base["rcept_no"]),
            "rcept_dt": ident.get("rcept_dt", base["rcept_dt"]), "report_type": base["report_type"],
            "base_report_type": base.get("base_report_type"),
            "group_id": base.get("group_id"), "original_rcept_no": base.get("original_rcept_no"),
            "latest_rcept_no": base.get("latest_rcept_no"), "is_amended": base.get("is_amended"),
            "amendment_seq": base.get("amendment_seq"),
            "rag_doc_type": rag_doc_type, "chunk_type": rag_doc_type, "section_title": section_title,
            "api_name": api, "api_group": fr.api_group_of(api),
            "chunk_index": chunk_index, "chunk_text": chunk_text, "token_count": approx_tokens(chunk_text),
            "content_hash": _hash(chunk_text), "is_latest": base["is_latest"],
            "embeddable": True, "source_uri": base["source_uri"],
            "source_system": base["source_system"],
            "source_silver_dataset": base["source_silver_dataset"],
            "source_record_key": source_record_key,
            "source_outer_rcept_no": ident.get("outer_rcept_no"),
            "source_outer_dart_view_url": ident.get("outer_dart_view_url"),
            "dart_view_url": dart_view_url or base.get("dart_view_url"),
            "report_nm": (meta.get("report_nm") or "").strip(),
            "importance": importance, "keywords": keywords,
            "source_fact_ids": list(source_fact_ids or []),
            "source_chunk_ids": list(source_chunk_ids or []),
            "generated_from_hash": generated_from_hash,
            "summary_version": summary_version,
            "chunk_schema_version": CHUNK_SCHEMA_VERSION,
            "chunking_rule_version": CHUNKING_RULE_VERSION,
            "field_mapping_version": fr.FIELD_MAPPING_VERSION,
            "created_at": base["created_at"],
            "rcept_year": ident.get("rcept_year", base["rcept_year"]),
            "rcept_month": ident.get("rcept_month", base["rcept_month"]),
        }

    # ── rag_document + 문서수준 rag_chunk(요약/원문/재무요약) ────────────
    def _rag(self, rs, report, meta, base, derived_lines) -> None:
        rt = base["report_type"]
        doc_importance = fr.importance_of(rt)
        doc_keywords = fr.keywords_of(rt, None)
        docs: list[dict[str, Any]] = []
        # 1) 요약 카드 — 항상. 숫자만 있는 보고서도 이걸로 검색·라우팅.
        summary_source_fact_ids = _source_fact_ids(rs)
        summary_text = nz.summary_card_text(meta, derived_lines)
        docs.append({
            "rag_doc_type": "summary_card",
            "title": "요약",
            "text": summary_text,
            "source_fact_ids": summary_source_fact_ids,
            "source_chunk_ids": [],
            "generated_from_hash": _hash({"derived_lines": derived_lines, "source_fact_ids": summary_source_fact_ids}),
            "summary_version": SUMMARY_VERSION,
        })
        # 2) 원문 본문(공시 text).
        if report.get("text"):
            raw_text = _clean_raw_disclosure_text(report["text"])
            if raw_text:
                docs.append({
                    "rag_doc_type": "raw_disclosure_text",
                    "title": report.get("title") or "",
                    "text": raw_text,
                    "source_fact_ids": [],
                    "source_chunk_ids": [],
                    "generated_from_hash": _hash(raw_text),
                    "summary_version": None,
                })
        # 3) 재무 요약(정기보고서).
        fin_txt = nz.financial_summary_text(meta, report.get("financials") or {})
        if fin_txt:
            docs.append({
                "rag_doc_type": "financial_summary_text",
                "title": "재무요약",
                "text": fin_txt,
                "source_fact_ids": _source_fact_ids(rs, ("financial_statement",)),
                "source_chunk_ids": [],
                "generated_from_hash": _hash(fin_txt),
                "summary_version": None,
            })

        for doc in docs:
            rag_doc_type = doc["rag_doc_type"]
            title = doc["title"]
            text = doc["text"]
            text = (text or "").strip()
            if not text:
                continue
            rag_doc_id = f"{base['doc_id']}:{rag_doc_type}"
            rs.rag_document.append({
                "rag_doc_id": rag_doc_id, "source_doc_id": base["doc_id"], "source_fact_id": None,
                "corp_code": base["corp_code"], "stock_code": base["stock_code"],
                "corp_name": base["corp_name"], "rcept_no": base["rcept_no"],
                "rcept_dt": base["rcept_dt"], "report_type": base["report_type"],
                "group_id": base.get("group_id"), "original_rcept_no": base.get("original_rcept_no"),
                "latest_rcept_no": base.get("latest_rcept_no"), "is_amended": base.get("is_amended"),
                "amendment_seq": base.get("amendment_seq"),
                "rag_doc_type": rag_doc_type, "title": title, "text": text, "text_len": len(text),
                "source_table": None, "source_uri": base["source_uri"],
                "dart_view_url": base.get("dart_view_url"),
                "content_hash": _hash(text), "is_latest": base["is_latest"],
                "source_fact_ids": list(doc.get("source_fact_ids") or []),
                "source_chunk_ids": list(doc.get("source_chunk_ids") or []),
                "generated_from_hash": doc.get("generated_from_hash"),
                "summary_version": doc.get("summary_version"),
                "rcept_year": base["rcept_year"], "rcept_month": base["rcept_month"],
            })
            for idx, chunk in enumerate(split_text(text)):
                rs.rag_chunk.append(self._chunk_row(
                    base, meta,
                    chunk_id=make_chunk_id(base["corp_code"], base["rcept_no"], rag_doc_type, idx),
                    rag_doc_id=rag_doc_id, rag_doc_type=rag_doc_type, section_title=title,
                    api=None, chunk_index=idx, chunk_text=chunk,
                    importance=doc_importance, keywords=doc_keywords,
                    source_fact_ids=list(doc.get("source_fact_ids") or []),
                    source_chunk_ids=list(doc.get("source_chunk_ids") or []),
                    generated_from_hash=doc.get("generated_from_hash"),
                    summary_version=doc.get("summary_version")))

    # ── per-record rag_chunk (서술 필드만 임베딩) + 기업개요 프로필 청크 ──
    def _rag_records(self, rs, report, meta, base) -> None:
        """구조화/이벤트/지분 record를 'embed-role 서술 필드'만으로 청크화(숫자/날짜 제외).

        summary_card(라우팅)와 병행하는 정밀 검색용 청크. 빈 서술이면 청크 미생성.
        company_overview는 corp 단위 1프로필 청크(정기보고서에서만, 중복 최소화).
        """
        rt = base["report_type"]
        # 1) 기업개요 프로필(정기보고서에서만 — corp 단위 dedup, 보고서마다 중복 방지).
        ov = report.get("company_overview") or {}
        if ov and rt == "REGULAR":
            text = fr.render_record_text("company_overview", ov)
            if text:
                text = _record_chunk_text(base, "기업개요", text)
            for split_idx, chunk in enumerate(split_text(text)):
                rs.rag_chunk.append(self._chunk_row(
                    base, meta,
                    chunk_id=f"dart:{base['corp_code']}:company_overview:{split_idx:03d}",
                    rag_doc_id=f"dart:{base['corp_code']}:company_overview",
                    rag_doc_type="record", section_title="기업개요", api="company_overview",
                    chunk_index=split_idx, chunk_text=chunk,
                    importance=fr.importance_of(rt, "company_overview"),
                    keywords=fr.keywords_of(rt, "company_overview")))

        # 2) DS002 정기 구조화 / DS005 주요사항 / DS004 지분 — record당 서술 청크.
        sources = []
        sources.extend(("structured", api, rows) for api, rows in (report.get("structured") or {}).items())
        sources.extend(("event", api, rows) for api, rows in (report.get("event_reports") or {}).items())
        sources.extend(
            ("ownership", api, latest_ownership_rows(api, rows or []))
            for api, rows in (report.get("ownership") or {}).items()
        )
        for source_kind, api, rows in sources:
            if not rows or not fr.is_per_record_api(api):  # 서술 풍부 record만(피벗 metric 표 제외).
                continue
            importance = fr.importance_of(rt, api)
            keywords = fr.keywords_of(rt, api)
            section = fr.section_title_of(api) or api
            for rec_idx, r in enumerate(rows):
                text = fr.render_record_text(api, r)
                if not text:  # 서술 필드 전무 → 청크 미생성(숫자-only record는 facts로만).
                    continue
                text = _record_chunk_text(base, section, text)
                source_fact_id = _record_source_fact_id(base, api, rec_idx, r, source_kind)
                row_identity = None
                row_dart_url = None
                chunk_rcept = base["rcept_no"]
                chunk_key = f"rec_{api}_{rec_idx}"
                if source_kind == "ownership":
                    inner_rcept = str(r.get("rcept_no") or base["rcept_no"])
                    iy, im, idt = _ymd(r.get("rcept_dt"))
                    row_dart_url = _dart_view_url(inner_rcept) or base.get("dart_view_url")
                    row_identity = {
                        "rcept_no": inner_rcept,
                        "rcept_dt": idt or base["rcept_dt"],
                        "rcept_year": iy or base["rcept_year"],
                        "rcept_month": im or base["rcept_month"],
                        "outer_rcept_no": base["rcept_no"],
                        "outer_dart_view_url": base.get("dart_view_url"),
                    }
                    chunk_rcept = inner_rcept
                for split_idx, chunk in enumerate(split_text(text)):
                    if source_kind == "ownership" and source_fact_id:
                        chunk_id = make_chunk_id_keyed(base["corp_code"], chunk_rcept, "record", source_fact_id, split_idx)
                    else:
                        chunk_id = make_chunk_id(base["corp_code"], base["rcept_no"], chunk_key, split_idx)
                    rs.rag_chunk.append(self._chunk_row(
                        base, meta,
                        chunk_id=chunk_id,
                        rag_doc_id=f"{base['doc_id']}:rec:{api}", rag_doc_type="record",
                        section_title=section, api=api, chunk_index=split_idx, chunk_text=chunk,
                        importance=importance, keywords=keywords,
                        source_fact_ids=[source_fact_id] if source_fact_id else [],
                        dart_view_url=row_dart_url,
                        row_identity=row_identity,
                        source_record_key=source_fact_id))

    # ── entity_relation (사전 재연결) ──────────────────────────────────
    def _entities(self, rs, report, base) -> None:
        seen: set[tuple[str, str]] = set()

        def emit(name, relation_type, evidence, method="dictionary"):
            name = (name or "").strip()
            if not name or len(name) < 2:
                return
            key = (relation_type, name)
            if key in seen:
                return
            seen.add(key)
            resolved = self.resolver.resolve(name)
            rs.entity_relation.append({
                "relation_id": hashlib.sha1(f"{base['doc_id']}|{relation_type}|{name}".encode()).hexdigest()[:16],  # noqa: S324
                "source_doc_id": base["doc_id"], "source_fact_id": None,
                "source_corp_code": base["corp_code"], "source_stock_code": base["stock_code"],
                "mentioned_name": name,
                "mentioned_corp_code": resolved.corp_code if resolved else None,
                "mentioned_stock_code": resolved.stock_code if resolved else None,
                "mentioned_market_type": resolved.market_type if resolved else None,
                "mentioned_type": "COMPANY" if resolved else "UNKNOWN",
                "relation_type": relation_type,
                "evidence_text": (evidence or "")[:500],
                "confidence": 0.9 if resolved else 0.4,
                "extraction_method": method,
                "rcept_dt": base["rcept_dt"], "report_type": base["report_type"],
                "rcept_year": base["rcept_year"], "rcept_month": base["rcept_month"],
            })

        ownership = report.get("ownership") or {}
        for otype, (field_name, rel) in _OWNERSHIP_RELATIONS.items():
            for r in latest_ownership_rows(otype, ownership.get(otype) or []):
                emit(r.get(field_name), rel, r.get("report_resn") or r.get("repror"))
        structured = report.get("structured") or {}
        for r in structured.get("hyslrSttus") or []:
            emit(r.get("nm"), "MAX_SHAREHOLDER", r.get("relate"))
        for r in structured.get("hyslrChgHist") or []:
            emit(r.get("mxmm_shrholdr_nm"), "MAX_SHAREHOLDER", r.get("change_cause"))
        events = report.get("event_reports") or {}
        for rows in events.values():
            for r in rows or []:
                emit(r.get("extg"), "COUNTERPARTY", r.get("bd_knd"))
