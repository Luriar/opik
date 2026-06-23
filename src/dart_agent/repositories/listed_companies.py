from __future__ import annotations

import json
from collections.abc import Iterable

from sqlalchemy import text
from sqlalchemy.engine import Connection

from dart_agent.services.listed_company import (
    ExistingCompany,
    ListedCompany,
    RosterEvent,
    market_for,
)


def load_listed_companies(conn: Connection) -> dict[str, ExistingCompany]:
    """현재 listed_company 전체를 종목코드 기준 dict로 읽는다.

    재상장(과거 DELISTED) 판정이 필요하므로 status에 관계없이 모두 읽는다.
    """
    rows = conn.execute(
        text(
            """
            SELECT stock_code, corp_code, corp_name, corp_eng_name, status
            FROM listed_company
            """
        )
    ).mappings().all()
    return {
        row["stock_code"]: ExistingCompany(
            stock_code=row["stock_code"],
            corp_code=row["corp_code"],
            corp_name=row["corp_name"],
            corp_eng_name=row["corp_eng_name"],
            status=row["status"],
        )
        for row in rows
    }


def corp_cls_lookup(conn: Connection, corp_codes: Iterable[str]) -> dict[str, str]:
    """disclosure(list.json 기준)에서 corp_code별 최신 corp_cls를 읽는다.

    corpCode.xml에는 시장구분이 없으므로 시장구분은 공시 데이터로만 확정한다
    (corpCode.xml만으로 시장구분 확정 금지). 공시가 아직 없으면 매핑에서 빠진다.
    """
    codes = [code for code in {c for c in corp_codes} if code]
    if not codes:
        return {}
    rows = conn.execute(
        text(
            """
            SELECT DISTINCT ON (corp_code) corp_code, corp_cls
            FROM disclosure
            WHERE corp_code = ANY(:codes)
              AND corp_cls IS NOT NULL
            ORDER BY corp_code, rcept_dt DESC, rcept_no DESC
            """
        ),
        {"codes": codes},
    ).mappings().all()
    return {row["corp_code"]: row["corp_cls"] for row in rows}


def upsert_listed_company(
    conn: Connection,
    company: ListedCompany,
    *,
    status: str,
    corp_cls: str | None,
    reason: str,
    observed_date: str,
    source: str = "DART_CORPCODE",
) -> None:
    conn.execute(
        text(
            """
            INSERT INTO listed_company (
                stock_code,
                corp_code,
                corp_name,
                corp_eng_name,
                market_type,
                corp_cls,
                status,
                listing_status_reason,
                source,
                observed_date,
                delisted_at
            )
            VALUES (
                :stock_code,
                :corp_code,
                :corp_name,
                :corp_eng_name,
                :market_type,
                :corp_cls,
                :status,
                :reason,
                :source,
                :observed_date,
                NULL
            )
            ON CONFLICT (stock_code)
            DO UPDATE SET
                corp_code = EXCLUDED.corp_code,
                corp_name = EXCLUDED.corp_name,
                corp_eng_name = EXCLUDED.corp_eng_name,
                market_type = EXCLUDED.market_type,
                corp_cls = EXCLUDED.corp_cls,
                status = EXCLUDED.status,
                listing_status_reason = EXCLUDED.listing_status_reason,
                source = EXCLUDED.source,
                observed_date = EXCLUDED.observed_date,
                delisted_at = NULL,
                last_seen_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            """
        ),
        {
            "stock_code": company.stock_code,
            "corp_code": company.corp_code,
            "corp_name": company.corp_name,
            "corp_eng_name": company.corp_eng_name,
            "market_type": market_for(corp_cls),
            "corp_cls": corp_cls,
            "status": status,
            "reason": reason,
            "source": source,
            "observed_date": observed_date,
        },
    )


def mark_listed_company_delisted(
    conn: Connection,
    stock_code: str,
    *,
    reason: str,
    observed_date: str,
) -> None:
    """상장폐지로 추정된 회사는 삭제하지 않고 status=DELISTED로 표시한다."""
    conn.execute(
        text(
            """
            UPDATE listed_company
            SET status = 'DELISTED',
                listing_status_reason = :reason,
                observed_date = :observed_date,
                delisted_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE stock_code = :stock_code
            """
        ),
        {"stock_code": stock_code, "reason": reason, "observed_date": observed_date},
    )


def insert_listed_company_event(
    conn: Connection,
    event: RosterEvent,
    *,
    observed_date: str,
    corp_cls: str | None,
    source_raw_ref_id: int | None,
) -> int:
    """변동 이력 1건을 저장한다. 같은 종목코드의 이전 이력은 is_latest=false로 내린다.

    RAG 검색 기본 조건(is_latest=true)에서 회사별 최신 상태가 먼저 노출되게 한다.
    """
    conn.execute(
        text(
            """
            UPDATE listed_company_event
            SET is_latest = FALSE
            WHERE stock_code = :stock_code AND is_latest = TRUE
            """
        ),
        {"stock_code": event.stock_code},
    )
    row = conn.execute(
        text(
            """
            INSERT INTO listed_company_event (
                stock_code,
                corp_code,
                corp_name,
                corp_eng_name,
                event_type,
                market_type,
                corp_cls,
                change_reason,
                change_detail,
                searchable_text,
                observed_date,
                source_raw_ref_id,
                is_latest
            )
            VALUES (
                :stock_code,
                :corp_code,
                :corp_name,
                :corp_eng_name,
                :event_type,
                :market_type,
                :corp_cls,
                :change_reason,
                CAST(:change_detail AS JSONB),
                :searchable_text,
                :observed_date,
                :source_raw_ref_id,
                TRUE
            )
            RETURNING id
            """
        ),
        {
            "stock_code": event.stock_code,
            "corp_code": event.corp_code,
            "corp_name": event.corp_name,
            "corp_eng_name": event.corp_eng_name,
            "event_type": event.event_type,
            "market_type": market_for(corp_cls),
            "corp_cls": corp_cls,
            "change_reason": event.change_reason,
            "change_detail": json.dumps(event.change_detail, ensure_ascii=False, sort_keys=True),
            "searchable_text": event.searchable_text,
            "observed_date": observed_date,
            "source_raw_ref_id": source_raw_ref_id,
        },
    ).one()
    return int(row.id)
