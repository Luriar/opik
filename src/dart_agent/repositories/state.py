from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection


def get_state(conn: Connection, key: str) -> str | None:
    row = conn.execute(
        text("SELECT state_value FROM system_state WHERE state_key = :key"),
        {"key": key},
    ).first()
    return None if row is None else str(row.state_value)


def set_state(conn: Connection, key: str, value: str) -> None:
    conn.execute(
        text(
            """
            INSERT INTO system_state (state_key, state_value)
            VALUES (:key, :value)
            ON CONFLICT (state_key)
            DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = CURRENT_TIMESTAMP
            """
        ),
        {"key": key, "value": value},
    )

