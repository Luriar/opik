from __future__ import annotations

from datetime import date
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.engine import Connection

from dart_agent.collectors.corp_code import CorpCodeRecord


def upsert_corp_codes(conn: Connection, rows: Iterable[CorpCodeRecord]) -> int:
    count = 0
    for row in rows:
        conn.execute(
            text(
                """
                INSERT INTO dart_corp_code (
                    corp_code,
                    corp_name,
                    corp_eng_name,
                    stock_code,
                    modify_date
                )
                VALUES (
                    :corp_code,
                    :corp_name,
                    :corp_eng_name,
                    :stock_code,
                    :modify_date
                )
                ON CONFLICT (corp_code)
                DO UPDATE SET
                    corp_name = EXCLUDED.corp_name,
                    corp_eng_name = EXCLUDED.corp_eng_name,
                    stock_code = EXCLUDED.stock_code,
                    modify_date = EXCLUDED.modify_date,
                    observed_at = CURRENT_TIMESTAMP
                """
            ),
            {
                "corp_code": row.corp_code,
                "corp_name": row.corp_name,
                "corp_eng_name": row.corp_eng_name,
                "stock_code": row.stock_code,
                "modify_date": _parse_yyyymmdd(row.modify_date),
            },
        )
        count += 1
    return count


def _parse_yyyymmdd(value: str | None) -> date | None:
    if not value:
        return None
    return date(int(value[0:4]), int(value[4:6]), int(value[6:8]))

