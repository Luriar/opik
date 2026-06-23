"""Bronze 수집 완결성 판정 + complete 마커 작성.

expected(코드로 결정론적 계산) vs S3 존재로 공시 완결성을 판정한다.
collect_job 상태(DB)에 의존하지 않고, Bronze 파일 존재만으로 판단한다.
013(NoData)은 nodata 마커 파일이 있으므로 'present'로 친다.

완료(COMPLETE)인 공시에만 complete 마커를 쓴다. Silver는 이 마커를 증분 기준으로 쓴다.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from dart_agent.services.disclosure import bronze_artifacts_for_disclosure
from dart_agent.storage.paths import BronzePaths

log = logging.getLogger(__name__)

MARKER_VERSION = "v2"


def job_done_key(
    job_type: str,
    *,
    corp_code: Any = None,
    api_name: Any = None,
    bsns_year: Any = None,
    reprt_code: Any = None,
    bgn_de: Any = None,
    end_de: Any = None,
    rcept_no: Any = None,
) -> tuple:
    """collect_job DONE 식별 키를 job_type별 의미 컬럼으로 정규화한다.

    과거 completion은 이 키로 DONE fallback을 했지만, 현재 완결성 판정은 실제 Bronze object
    존재만 사용한다. 이 함수는 기존 테스트/레거시 호출 호환용으로만 남긴다.
    """
    if job_type == "STRUCTURED_REPORT":
        return (job_type, corp_code, api_name, bsns_year, reprt_code)
    if job_type in ("EVENT_REPORT", "SECURITIES_REPORT"):
        return (job_type, corp_code, api_name, bgn_de, end_de)
    if job_type in ("FINANCIAL_STATEMENT_ALL", "DISCLOSURE_DOCUMENT"):
        return (job_type, rcept_no)
    return (job_type,)


def evaluate_completion(
    storage, disclosure_row: dict[str, Any], collect_mode: str, engine=None, done_keys: set | None = None
) -> dict[str, Any]:
    """공시의 expected Bronze 산출물이 모두 확보됐는지 평가한다.

    present 판정:
      ① 파일 존재(정상 응답 또는 013/014 nodata 마커)
      ② 파일이 없으면 missing. collect_job DONE은 Bronze 존재 근거로 인정하지 않는다.
         engine/done_keys 인자는 레거시 호출 호환용이며 현재 판정에는 쓰지 않는다.
    반환: {status: COMPLETE|PARTIAL, artifacts: [{kind, path, status}], missing, expected_count}
    """
    report_nm = disclosure_row.get("report_nm", "") or ""
    rcept_dt = str(disclosure_row["rcept_dt"]).replace("-", "")[:8]
    corp_code = str(disclosure_row["corp_code"])
    rcept_no = str(disclosure_row["rcept_no"])

    expected = bronze_artifacts_for_disclosure(
        report_nm,
        rcept_dt,
        corp_code,
        rcept_no,
        pblntf_ty=disclosure_row.get("pblntf_ty"),
        pblntf_detail_ty=disclosure_row.get("pblntf_detail_ty"),
        collect_mode=collect_mode,
    )
    if not expected:
        # 상세 수집 대상이 없는 공시(DISCLOSURE/SECURITIES 등) — Silver 대상에서 제외한다.
        return {"status": "SKIP_NO_DETAIL", "artifacts": [], "missing": 0, "expected_count": 0}
    artifacts: list[dict[str, str]] = []
    missing = 0
    for kind, path, _match in expected:
        present = storage.exists(path)
        artifacts.append({"kind": kind, "path": path, "status": "present" if present else "missing"})
        if not present:
            missing += 1
    return {
        "status": "COMPLETE" if missing == 0 else "PARTIAL",
        "artifacts": artifacts,
        "missing": missing,
        "expected_count": len(expected),
    }


def _job_is_done(engine, match: dict) -> bool:
    """레거시 helper. 현재 evaluate_completion은 collect_job DONE fallback을 사용하지 않는다.

    match 키는 코드 고정 컬럼명(job_type/corp_code/api_name/bsns_year/reprt_code/bgn_de/end_de/rcept_no)이라
    SQL injection 위험이 없다. None 값은 IS NULL로 처리한다(연·분기 없는 DS004 등).
    """
    conds = ["status = 'DONE'"]
    params: dict = {}
    for key, value in match.items():
        if value is None:
            conds.append(f"{key} IS NULL")
        else:
            conds.append(f"{key} = :{key}")
            params[key] = value
    sql = "SELECT 1 FROM collect_job WHERE " + " AND ".join(conds) + " LIMIT 1"
    with engine.connect() as conn:
        return conn.execute(text(sql), params).first() is not None


def write_complete_marker(
    storage,
    disclosure_row: dict[str, Any],
    evaluation: dict[str, Any],
    ingest_mode: str | None = None,
) -> str:
    """완료된 공시의 complete 마커를 작성한다(self-contained: 경로 + 공시메타 + ingest).

    Silver는 이 마커만으로 report.json을 만들 수 있어야 하므로, Bronze 경로(artifacts)와
    report.json _meta에 들어갈 공시 메타를 모두 담는다.
    """
    rcept_dt = str(disclosure_row["rcept_dt"]).replace("-", "")[:8]
    corp_code = str(disclosure_row["corp_code"])
    rcept_no = str(disclosure_row["rcept_no"])
    marker = {
        "rcept_no": rcept_no,
        "corp_code": corp_code,
        "stock_code": disclosure_row.get("stock_code"),
        "corp_name": disclosure_row.get("corp_name"),
        "report_nm": disclosure_row.get("report_nm"),
        "report_type": disclosure_row.get("report_type") or "DISCLOSURE",
        "pblntf_ty": disclosure_row.get("pblntf_ty"),
        "pblntf_detail_ty": disclosure_row.get("pblntf_detail_ty"),
        "rcept_dt": rcept_dt,
        "group_id": disclosure_row.get("group_id"),
        "original_rcept_no": disclosure_row.get("original_rcept_no"),
        "latest_rcept_no": disclosure_row.get("latest_rcept_no"),
        "is_amended": bool(disclosure_row.get("is_amended", False)),
        "amendment_seq": int(disclosure_row.get("amendment_seq") or 0),
        "is_latest": bool(disclosure_row.get("is_latest", True)),
        "artifacts": evaluation["artifacts"],
        "completion_basis": "all expected bronze artifacts present (013 counted via nodata marker)",
        "ingest": {
            # backfill/incremental 구분은 분리가 아니라 메타로만 기록한다(없으면 null).
            "mode": ingest_mode,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        },
        "marker_version": MARKER_VERSION,
    }
    path = BronzePaths.complete_marker(corp_code, rcept_no)
    storage.write_bytes(
        path,
        json.dumps(marker, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        content_type="application/json",
    )
    return path
