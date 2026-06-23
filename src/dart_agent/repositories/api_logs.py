from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection


def insert_api_request_log(
    conn: Connection,
    *,
    api_group: str | None,
    api_name: str,
    quota_type: str,
    quota_counted: bool,
    request_params: dict[str, Any],
    request_hash: str,
    status_code: str | None,
    http_status: int | None,
    response_ms: int | None,
    error_message: str | None = None,
) -> None:
    conn.execute(
        text(
            """
            INSERT INTO api_request_log (
                api_group,
                api_name,
                quota_type,
                quota_counted,
                request_params,
                request_hash,
                status_code,
                http_status,
                response_ms,
                error_message
            )
            VALUES (
                :api_group,
                :api_name,
                :quota_type,
                :quota_counted,
                CAST(:request_params AS JSONB),
                :request_hash,
                :status_code,
                :http_status,
                :response_ms,
                :error_message
            )
            ON CONFLICT (request_hash)
            DO UPDATE SET
                status_code = EXCLUDED.status_code,
                http_status = EXCLUDED.http_status,
                response_ms = EXCLUDED.response_ms,
                error_message = EXCLUDED.error_message,
                quota_counted = EXCLUDED.quota_counted,
                request_params = EXCLUDED.request_params,
                requested_at = CURRENT_TIMESTAMP
            """
        ),
        {
            "api_group": api_group,
            "api_name": api_name,
            "quota_type": quota_type,
            "quota_counted": quota_counted,
            "request_params": json.dumps(request_params, ensure_ascii=False, sort_keys=True),
            "request_hash": request_hash,
            "status_code": status_code,
            "http_status": http_status,
            "response_ms": response_ms,
            "error_message": error_message,
        },
    )
