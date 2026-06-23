from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from dart_agent.services.disclosure import is_correction_report, normalize_report_name, report_type_for


def upsert_disclosure(conn: Connection, item: dict[str, Any], raw_ref_id: int | None = None) -> str:
    report_nm = str(item["report_nm"])
    report_nm_norm = normalize_report_name(report_nm)
    report_type = report_type_for(report_nm, item.get("pblntf_ty"), item.get("pblntf_detail_ty"))
    rcept_no = str(item["rcept_no"])
    rcept_dt = _parse_yyyymmdd(str(item["rcept_dt"]))
    corp_code = str(item["corp_code"])
    group_id = _ensure_disclosure_group(
        conn=conn,
        corp_code=corp_code,
        report_nm_norm=report_nm_norm,
        report_type=report_type,
        bsns_year="",
        reprt_code="",
    )

    conn.execute(
        text(
            """
            INSERT INTO disclosure (
                rcept_no,
                corp_cls,
                corp_name,
                corp_code,
                stock_code,
                report_nm,
                report_nm_norm,
                report_type,
                pblntf_ty,
                pblntf_detail_ty,
                rcept_dt,
                flr_nm,
                rm,
                group_id,
                is_correction,
                raw_ref_id
            )
            VALUES (
                :rcept_no,
                :corp_cls,
                :corp_name,
                :corp_code,
                :stock_code,
                :report_nm,
                :report_nm_norm,
                :report_type,
                :pblntf_ty,
                :pblntf_detail_ty,
                :rcept_dt,
                :flr_nm,
                :rm,
                :group_id,
                :is_correction,
                :raw_ref_id
            )
            ON CONFLICT (rcept_no)
            DO UPDATE SET
                last_seen_at = CURRENT_TIMESTAMP,
                report_type = EXCLUDED.report_type,
                group_id = EXCLUDED.group_id,
                pblntf_ty = COALESCE(EXCLUDED.pblntf_ty, disclosure.pblntf_ty),
                pblntf_detail_ty = COALESCE(EXCLUDED.pblntf_detail_ty, disclosure.pblntf_detail_ty),
                raw_ref_id = COALESCE(EXCLUDED.raw_ref_id, disclosure.raw_ref_id)
            """
        ),
        {
            "rcept_no": rcept_no,
            "corp_cls": item.get("corp_cls"),
            "corp_name": item.get("corp_name") or item.get("corp_name_eng") or "",
            "corp_code": corp_code,
            "stock_code": item.get("stock_code") or None,
            "report_nm": report_nm,
            "report_nm_norm": report_nm_norm,
            "report_type": report_type,
            "pblntf_ty": item.get("pblntf_ty") or None,
            "pblntf_detail_ty": item.get("pblntf_detail_ty") or None,
            "rcept_dt": rcept_dt,
            "flr_nm": item.get("flr_nm"),
            "rm": item.get("rm"),
            "group_id": group_id,
            "is_correction": is_correction_report(report_nm),
            "raw_ref_id": raw_ref_id,
        },
    )
    _refresh_group_latest(conn, group_id)
    return rcept_no


def _ensure_disclosure_group(
    conn: Connection,
    corp_code: str,
    report_nm_norm: str,
    report_type: str,
    bsns_year: str,
    reprt_code: str,
) -> int:
    # INSERT ... DO NOTHING 후 SELECT로 분리해 데드락을 방지한다.
    # DO UPDATE SET 패턴은 충돌 행에도 배타 락을 걸어 동시 upsert 시 데드락을 유발한다.
    conn.execute(
        text(
            """
            INSERT INTO disclosure_group (
                corp_code,
                report_nm_norm,
                report_type,
                bsns_year,
                reprt_code
            )
            VALUES (
                :corp_code,
                :report_nm_norm,
                :report_type,
                :bsns_year,
                :reprt_code
            )
            ON CONFLICT (corp_code, report_nm_norm, report_type, bsns_year, reprt_code)
            DO NOTHING
            """
        ),
        {
            "corp_code": corp_code,
            "report_nm_norm": report_nm_norm,
            "report_type": report_type,
            "bsns_year": bsns_year,
            "reprt_code": reprt_code,
        },
    )
    row = conn.execute(
        text(
            """
            SELECT id FROM disclosure_group
            WHERE corp_code = :corp_code
              AND report_nm_norm = :report_nm_norm
              AND report_type = :report_type
              AND bsns_year = :bsns_year
              AND reprt_code = :reprt_code
            """
        ),
        {
            "corp_code": corp_code,
            "report_nm_norm": report_nm_norm,
            "report_type": report_type,
            "bsns_year": bsns_year,
            "reprt_code": reprt_code,
        },
    ).one()
    return int(row.id)


def _refresh_group_latest(conn: Connection, group_id: int) -> None:
    latest = conn.execute(
        text(
            """
            SELECT rcept_no
            FROM disclosure
            WHERE group_id = :group_id
            ORDER BY rcept_dt DESC, rcept_no DESC
            LIMIT 1
            """
        ),
        {"group_id": group_id},
    ).scalar_one_or_none()
    if latest is None:
        # 그룹에 disclosure가 아직 없으면(재분류로 group_id가 옮겨간 빈 그룹 등) 갱신할 latest가
        # 없다. version_count만 0으로 맞추고 종료한다(scalar_one() NoResultFound 크래시 방지).
        conn.execute(
            text(
                "UPDATE disclosure_group SET version_count = 0, updated_at = CURRENT_TIMESTAMP "
                "WHERE id = :group_id"
            ),
            {"group_id": group_id},
        )
        return
    conn.execute(
        text("UPDATE disclosure SET is_latest = (rcept_no = :latest) WHERE group_id = :group_id"),
        {"latest": latest, "group_id": group_id},
    )
    conn.execute(
        text(
            """
            UPDATE disclosure_group
            SET latest_rcept_no = :latest,
                version_count = (
                    SELECT COUNT(*)
                    FROM disclosure
                    WHERE group_id = :group_id
                ),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :group_id
            """
        ),
        {"latest": latest, "group_id": group_id},
    )


def _parse_yyyymmdd(value: str) -> date:
    return date(int(value[0:4]), int(value[4:6]), int(value[6:8]))
