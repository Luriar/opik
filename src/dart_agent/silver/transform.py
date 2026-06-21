"""Silver 계층 변환 — Bronze complete 마커 → 보고서 단위 JSON.

설계 원칙:
  - 입력은 Bronze complete 마커(self-contained). 마커의 artifacts[].path로 Bronze를 읽고,
    공시 메타는 마커에서 가져온다. → Service DB(collect_job) 조회 없이 동작(환경 독립).
  - rcept_no 하나 → report.json 하나 (멱등: 재실행 시 덮어씀).
  - Bronze 파일이 없는 섹션은 null 또는 [] 로 채운다. 013(nodata) 마커는 list=[]라 빈 처리.
  - document(원문)는 해제·인라인하지 않는다. Bronze 원문 ZIP 위치와 DART 뷰어 URL을
    report.json의 document 컬럼으로 '링크만' 제공한다(원문 본문은 Bronze가 단일 출처).
  - 처리 완료 시 silver _done 마커를 남겨 Gold 증분/재처리 기준으로 쓴다.
  - 경로는 SilverPaths / BronzePaths 사용. 직접 조립 금지.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from dart_agent.ownership_latest import latest_ownership_rows
from dart_agent.opendart.report_apis import (
    OWNERSHIP_REPORT_API_NAMES,
    REPORT_APIS,
    material_event_report_type,
)
from dart_agent.storage.paths import BronzePaths, SilverPaths

log = logging.getLogger(__name__)

# Silver 변환 로직 버전. 변환 규칙이 바뀌면 올려서 _done 마커 version과 비교해 재처리할 수 있다.
SILVER_VERSION = "v5"


def _dart_view_url(rcept_no: str) -> str:
    """DART 공시 원문 뷰어 URL. rcept_no(접수번호) 기반 표준 링크."""
    return f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"


def _event_rows_for_report(rows: list[Any], rcept_no: str) -> list[Any]:
    """Return only DS005 rows that belong to the target disclosure receipt."""
    return [
        row for row in rows
        if isinstance(row, dict) and str(row.get("rcept_no") or "") == rcept_no
    ]


def _silver_report_type(base_report_type: str, material_event_apis: list[str]) -> str:
    if base_report_type != "MATERIAL_EVENT":
        return base_report_type
    if len(material_event_apis) == 1:
        return material_event_report_type(material_event_apis[0])
    if len(material_event_apis) > 1:
        return "005multi_event"
    return "005unclassified"


def build_report_from_marker(storage, marker: dict[str, Any]) -> str:
    """Bronze complete 마커 1건으로 Silver report.json을 생성·저장한다.

    마커의 artifacts(kind, path)를 읽어 섹션별로 분류한다:
      financial:CFS|OFS → financials
      structured:<api> → structured(DS002) 또는 ownership(DS004)
      event:<api> → event_reports + event_report_groups/event_report_index
      securities:<api> → securities (DS006 증권신고서)
      document → 원문 ZIP 위치(bronze_zip_path) + DART 뷰어 URL '링크'만 (해제/인라인 안 함)
    company_overview는 회사 단위라 마커에 없고, BronzePaths로 직접 읽는다.
    반환: 저장한 silver report object_path.
    """
    corp_code = str(marker["corp_code"])
    rcept_no = str(marker["rcept_no"])
    rcept_dt = str(marker["rcept_dt"])
    report_type = str(marker.get("report_type") or "DISCLOSURE")

    financials: dict[str, Any] = {}
    structured: dict[str, Any] = {}
    event_reports: dict[str, Any] = {}
    event_report_groups: dict[str, dict[str, Any]] = {}
    event_report_index: dict[str, Any] = {}
    material_event_apis: list[str] = []
    material_event_groups: list[str] = []
    ownership: dict[str, Any] = {}
    securities: dict[str, Any] = {}
    document: dict[str, Any] | None = None

    for art in marker.get("artifacts", []):
        kind = str(art.get("kind", ""))
        if kind == "document":
            # 원문은 해제하지 않고 Bronze ZIP 위치와 DART 뷰어 URL을 링크로만 연결한다.
            path = art.get("path")
            status = str(art.get("status") or "")
            document = {
                "bronze_zip_path": path,
                "available": bool(path) and status == "present",
                "dart_view_url": _dart_view_url(rcept_no),
            }
            continue
        data = _read_json(storage, art["path"], check_exists=False)
        rows = (data.get("list") or []) if data else []
        if not isinstance(rows, list):
            rows = []
        if kind.startswith("financial:"):
            financials[kind.split(":", 1)[1]] = rows
        elif kind.startswith("structured:"):
            api = kind.split(":", 1)[1]
            if api in OWNERSHIP_REPORT_API_NAMES:
                ownership[api] = latest_ownership_rows(api, rows)
            else:
                structured[api] = rows
        elif kind.startswith("event:"):
            api = kind.split(":", 1)[1]
            rows = _event_rows_for_report(rows, rcept_no)
            event_reports[api] = rows
            spec = REPORT_APIS.get(api)
            schema_group = (spec.schema_group if spec else None) or "unknown"
            event_report_groups.setdefault(schema_group, {})[api] = rows
            if rows:
                if api not in material_event_apis:
                    material_event_apis.append(api)
                if schema_group not in material_event_groups:
                    material_event_groups.append(schema_group)
            event_report_index[api] = {
                "api_group": spec.api_group if spec else "DS005",
                "api_name": api,
                "api_id": spec.api_id if spec else None,
                "endpoint": spec.endpoint if spec else None,
                "title": spec.title if spec else api,
                "schema_group": schema_group,
            }
        elif kind.startswith("securities:"):
            securities[kind.split(":", 1)[1]] = rows

    # document artifact가 없는 수집 모드(structured 등)에서도 DART 원문은 항상 존재하므로
    # 뷰어 URL은 제공한다. ZIP은 수집되지 않았으므로 bronze_zip_path=None, available=False.
    if document is None:
        document = {
            "bronze_zip_path": None,
            "available": False,
            "dart_view_url": _dart_view_url(rcept_no),
        }

    overview = _read_company_overview(storage, corp_code)
    material_event_type = None
    report_subtype = None
    silver_report_type = _silver_report_type(report_type, material_event_apis)
    if report_type == "MATERIAL_EVENT":
        if len(material_event_groups) == 1:
            material_event_type = material_event_groups[0]
            report_subtype = silver_report_type
        elif len(material_event_groups) > 1:
            material_event_type = "multi"
            report_subtype = silver_report_type
        else:
            report_subtype = silver_report_type

    report = {
        "_meta": {
            "rcept_no": rcept_no,
            "corp_code": corp_code,
            "stock_code": marker.get("stock_code"),
            "corp_name": marker.get("corp_name"),
            "rcept_dt": rcept_dt,
            "report_nm": marker.get("report_nm"),
            "report_type": silver_report_type,
            "base_report_type": marker.get("report_type"),
            "report_subtype": report_subtype,
            "material_event_type": material_event_type,
            "material_event_apis": material_event_apis,
            "pblntf_ty": marker.get("pblntf_ty"),
            "pblntf_detail_ty": marker.get("pblntf_detail_ty"),
            "group_id": marker.get("group_id"),
            "original_rcept_no": marker.get("original_rcept_no"),
            "latest_rcept_no": marker.get("latest_rcept_no"),
            "is_amended": bool(marker.get("is_amended", False)),
            "amendment_seq": int(marker.get("amendment_seq") or 0),
            "is_latest": bool(marker.get("is_latest", True)),
            "ingest": marker.get("ingest"),
            "silver_version": SILVER_VERSION,
            "silver_generated_at": datetime.now(timezone.utc).isoformat(),
        },
        "company_overview": overview,
        "financials": financials,
        "structured": structured,
        "event_reports": event_reports,
        "event_report_groups": event_report_groups,
        "event_report_index": event_report_index,
        "ownership": ownership,
        "securities": securities,
        "document": document,
    }

    silver_path = SilverPaths.report(corp_code, silver_report_type, rcept_no)
    storage.write_bytes(
        silver_path,
        json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8"),
        content_type="application/json",
    )
    return silver_path


def write_silver_done_marker(storage, marker: dict[str, Any], silver_path: str | None = None) -> str:
    """Silver 처리 완료 마커를 남긴다(Gold 증분 + Silver 재처리 기준)."""
    corp_code = str(marker["corp_code"])
    rcept_no = str(marker["rcept_no"])
    rcept_dt = str(marker["rcept_dt"])
    report_type = str(marker.get("report_type") or "DISCLOSURE")
    if silver_path is None:
        silver_path = SilverPaths.report(corp_code, report_type, rcept_no)
    done = {
        "rcept_no": rcept_no,
        "corp_code": corp_code,
        "rcept_dt": rcept_dt,
        "silver_path": silver_path,
        "silver_version": SILVER_VERSION,
        "source_complete_marker": BronzePaths.complete_marker(corp_code, rcept_no),
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }
    path = SilverPaths.done_marker_for_version(SILVER_VERSION, corp_code, rcept_no)
    storage.write_bytes(
        path,
        json.dumps(done, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        content_type="application/json",
    )
    return path


# ─────────────────────────────────────────────────────────────────────────────
# 내부 — Bronze 읽기
# ─────────────────────────────────────────────────────────────────────────────

def _read_json(
    storage,
    path: str,
    *,
    check_exists: bool = True,
    missing_ok: bool = False,
) -> dict | None:
    if check_exists and not storage.exists(path):
        return None
    try:
        return json.loads(storage.read_bytes(path).decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        if not missing_ok:
            log.warning("Bronze read failed [%s]: %s", path, exc)
        return None


def _read_company_overview(storage, corp_code: str) -> dict | None:
    return _read_json(
        storage,
        BronzePaths.company_overview(corp_code),
        check_exists=False,
        missing_ok=True,
    )
