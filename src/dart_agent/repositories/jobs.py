from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Connection

from dart_agent.hashing import request_hash


@dataclass(frozen=True)
class CollectJob:
    id: int
    priority: int
    job_type: str
    api_group: str | None
    api_name: str | None
    rcept_no: str | None
    corp_code: str | None
    stock_code: str | None
    bsns_year: str | None
    reprt_code: str | None
    bgn_de: str | None = None
    end_de: str | None = None
    ingest_mode: str | None = None
    retry_count: int = 0


def enqueue_collect_job(
    conn: Connection,
    *,
    priority: int,
    job_type: str,
    api_group: str | None,
    api_name: str | None,
    rcept_no: str | None = None,
    corp_code: str | None = None,
    stock_code: str | None = None,
    bsns_year: str | None = None,
    reprt_code: str | None = None,
    bgn_de: str | None = None,
    end_de: str | None = None,
    ingest_mode: str | None = None,
) -> int | None:
    params: dict[str, Any] = {
        "job_type": job_type,
        "api_group": api_group,
        "api_name": api_name,
        "rcept_no": rcept_no,
        "corp_code": corp_code,
        "stock_code": stock_code,
        "bsns_year": bsns_year,
        "reprt_code": reprt_code,
        "bgn_de": bgn_de,
        "end_de": end_de,
    }
    hashed = request_hash("DART", job_type, params)
    row = conn.execute(
        text(
            """
            INSERT INTO collect_job (
                priority,
                job_type,
                api_group,
                api_name,
                rcept_no,
                corp_code,
                stock_code,
                bsns_year,
                reprt_code,
                bgn_de,
                end_de,
                ingest_mode,
                request_hash
            )
            VALUES (
                :priority,
                :job_type,
                :api_group,
                :api_name,
                :rcept_no,
                :corp_code,
                :stock_code,
                :bsns_year,
                :reprt_code,
                :bgn_de,
                :end_de,
                :ingest_mode,
                :request_hash
            )
            ON CONFLICT (request_hash) DO NOTHING
            RETURNING id
            """
        ),
        {
            **params,
            "priority": priority,
            "ingest_mode": ingest_mode,
            "request_hash": hashed,
        },
    ).first()
    return None if row is None else int(row.id)


def claim_pending_jobs(conn: Connection, batch_size: int) -> list[CollectJob]:
    rows = conn.execute(
        text(
            """
            SELECT *
            FROM collect_job
            WHERE status = 'PENDING'
              AND scheduled_at <= NOW()
            ORDER BY priority ASC, scheduled_at ASC
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
            """
        ),
        {"batch_size": batch_size},
    ).mappings().all()
    for row in rows:
        conn.execute(
            text(
                """
                UPDATE collect_job
                SET status = 'RUNNING', started_at = CURRENT_TIMESTAMP
                WHERE id = :id
                """
            ),
            {"id": row["id"]},
        )
    return _collect_jobs_from_rows(rows)


def claim_pending_jobs_for_types(
    conn: Connection,
    *,
    batch_size: int,
    job_types: tuple[str, ...],
) -> list[CollectJob]:
    rows = conn.execute(
        text(
            """
            SELECT *
            FROM collect_job
            WHERE status = 'PENDING'
              AND scheduled_at <= NOW()
              AND job_type IN :job_types
            ORDER BY priority ASC, scheduled_at ASC
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
            """
        ).bindparams(bindparam("job_types", expanding=True)),
        {"batch_size": batch_size, "job_types": job_types},
    ).mappings().all()
    for row in rows:
        conn.execute(
            text(
                """
                UPDATE collect_job
                SET status = 'RUNNING', started_at = CURRENT_TIMESTAMP
                WHERE id = :id
                """
            ),
            {"id": row["id"]},
        )
    return _collect_jobs_from_rows(rows)


def _collect_jobs_from_rows(rows) -> list[CollectJob]:
    return [
        CollectJob(
            id=int(row["id"]),
            priority=int(row["priority"]),
            job_type=row["job_type"],
            api_group=row["api_group"],
            api_name=row["api_name"],
            rcept_no=row["rcept_no"],
            corp_code=row["corp_code"],
            stock_code=row["stock_code"],
            bsns_year=row["bsns_year"],
            reprt_code=row["reprt_code"],
            bgn_de=row["bgn_de"],
            end_de=row["end_de"],
            ingest_mode=row["ingest_mode"],
            retry_count=int(row["retry_count"]) if row["retry_count"] is not None else 0,
        )
        for row in rows
    ]


def mark_job_done(conn: Connection, job_id: int) -> None:
    conn.execute(
        text(
            """
            UPDATE collect_job
            SET status = 'DONE',
                finished_at = CURRENT_TIMESTAMP,
                error_code = NULL,
                error_message = NULL
            WHERE id = :job_id
            """
        ),
        {"job_id": job_id},
    )


def mark_job_failed(conn: Connection, job_id: int, error_code: str, error_message: str) -> None:
    conn.execute(
        text(
            """
            UPDATE collect_job
            SET status = 'FAILED',
                retry_count = retry_count + 1,
                finished_at = CURRENT_TIMESTAMP,
                error_code = :error_code,
                error_message = :error_message
            WHERE id = :job_id
            """
        ),
        {
            "job_id": job_id,
            "error_code": error_code,
            "error_message": error_message[:4000],
        },
    )


def reset_job_to_pending(conn: Connection, job_id: int) -> None:
    """RUNNING 상태인 job을 PENDING으로 복구한다. RateLimitExceeded 등 일시적 자원 고갈 시 사용."""
    conn.execute(
        text(
            """
            UPDATE collect_job
            SET status = 'PENDING',
                started_at = NULL
            WHERE id = :job_id
              AND status = 'RUNNING'
            """
        ),
        {"job_id": job_id},
    )


def reschedule_job_to_pending(
    conn: Connection,
    job_id: int,
    *,
    delay_seconds: int,
    error_code: str,
    error_message: str,
) -> None:
    """RUNNING job을 지연 후 재시도하도록 PENDING으로 되돌린다.

    TRANSPORT(네트워크) 실패처럼 일시적이고 job 잘못이 아닌 경우에 쓴다. in-thread sleep으로
    worker를 점유하는 대신 scheduled_at을 미래로 밀어 다음 collector tick이 재시도하게 한다.
    retry_count를 올려 호출부가 영구 FAILED 전환 시점을 판단할 수 있게 한다.
    """
    conn.execute(
        text(
            """
            UPDATE collect_job
            SET status = 'PENDING',
                started_at = NULL,
                scheduled_at = NOW() + make_interval(secs => :delay_seconds),
                retry_count = retry_count + 1,
                error_code = :error_code,
                error_message = :error_message
            WHERE id = :job_id
              AND status = 'RUNNING'
            """
        ),
        {
            "job_id": job_id,
            "delay_seconds": delay_seconds,
            "error_code": error_code,
            "error_message": error_message[:4000],
        },
    )
