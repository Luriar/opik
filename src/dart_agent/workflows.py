from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
import secrets
from time import monotonic
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, text

from dart_agent.collectors.corp_code import (
    extract_corp_code_xml,
    listed_company_records,
    parse_corp_code_xml,
    parse_corp_code_zip,
)
from dart_agent.config import ConfigApprovalState, Settings, get_settings, parse_optional_date
from dart_agent.dates import DateWindow, backfill_windows, date_range_windows, incremental_window
from dart_agent.db import engine_from_url
from dart_agent.hashing import request_hash
from dart_agent.ownership_latest import filter_ownership_payload
from dart_agent.dart_keys import DartApiKey
from dart_agent.opendart import OpenDartError, OpenDartNoData
from dart_agent.opendart.key_pool import OpenDartClientPool
from dart_agent.opendart.key_provider import load_keys_from_env_file
from dart_agent.opendart.report_apis import (
    MATERIAL_EVENT_API_NAMES,
    REPORT_APIS,
    REGULAR_REPORT_API_NAMES,
    SECURITIES_REPORT_API_NAMES,
    build_report_params,
)
from dart_agent.rate_limiter import DbRateLimiter, QuotaLimits, RateLimitExceeded
from dart_agent.repositories.api_logs import insert_api_request_log
from dart_agent.repositories.corp_codes import upsert_corp_codes
from dart_agent.repositories.disclosures import upsert_disclosure
from dart_agent.repositories.jobs import (
    CollectJob,
    claim_pending_jobs,
    claim_pending_jobs_for_types,
    enqueue_collect_job,
    mark_job_done,
    mark_job_failed,
    reschedule_job_to_pending,
    reset_job_to_pending,
)
from dart_agent.repositories.listed_companies import (
    corp_cls_lookup,
    insert_listed_company_event,
    load_listed_companies,
    mark_listed_company_delisted,
    upsert_listed_company,
)
from dart_agent.repositories.raw_files import upsert_raw_file_reference
from dart_agent.repositories.state import get_state, set_state
from dart_agent.services.disclosure import (
    DetailJobSpec,
    bronze_artifacts_for_disclosure,
    detail_jobs_for_disclosure,
    report_type_for,
)
from dart_agent.services.listed_company import (
    ListedCompany,
    RosterDiff,
    compute_roster_diff,
)
from dart_agent.storage import BronzePaths, SilverPaths, build_storage


@dataclass(frozen=True)
class AppContext:
    """workflow 실행에 필요한 공용 의존성 묶음(설정/클라이언트/rate limiter)."""

    settings: Settings
    api_keys: tuple[DartApiKey, ...]
    client: OpenDartClientPool
    rate_limiter: DbRateLimiter

    @property
    def service_engine(self):
        """Service DB(SERVICE_DB_URL) SQLAlchemy engine. 적재/상태/큐 접근에 쓴다."""
        return engine_from_url(self.settings.service_db_url)


@dataclass(frozen=True)
class IngestRun:
    mode: str
    collected_date: str
    run_id: str


KST = ZoneInfo("Asia/Seoul")


def build_context() -> AppContext:
    """환경설정을 읽어 DB rate limiter와 key pool 클라이언트를 구성한 AppContext를 만든다.

    모든 run_* 진입점이 시작 시 호출하는 공용 팩토리다.
    """
    settings = get_settings()
    engine = engine_from_url(settings.service_db_url)
    api_keys = _resolve_api_keys(settings)
    rate_limiter = DbRateLimiter(
        engine=engine,
        api_key_identifier=settings.dart_api_key_identifier,
        limits=QuotaLimits(
            daily_limit=settings.dart_daily_limit,
            daily_safe_limit=settings.dart_daily_safe_limit,
            daily_emergency_limit=settings.dart_daily_emergency_limit,
            minute_limit=settings.dart_minute_limit,
            minute_safe_limit=settings.dart_minute_safe_limit,
            minute_global_limit=settings.dart_minute_global_limit,
            minute_global_safe_limit=settings.dart_minute_global_safe_limit,
        ),
    )
    return AppContext(
        settings=settings,
        api_keys=api_keys,
        rate_limiter=rate_limiter,
        client=OpenDartClientPool(
            api_keys=api_keys,
            rate_limiter=rate_limiter,
        ),
    )


def _resolve_api_keys(settings: Settings) -> tuple[DartApiKey, ...]:
    """mount된 .env가 있으면 거기서 키를 읽고, 없으면 환경변수(DART_API_KEYS)로 fallback한다(무중단 키 관리).

    .env 우선 → .env가 비었거나 깨졌으면 환경변수 fallback. 둘 다 없으면 get_settings에서 이미 차단된다.
    """
    if settings.dart_env_file:
        env_file_keys = load_keys_from_env_file(settings.dart_env_file)
        if env_file_keys:
            return env_file_keys
    return settings.dart_api_keys


def _today_kst() -> date:
    return datetime.now(KST).date()


def _new_ingest_run(mode: str) -> IngestRun:
    now = datetime.now(KST)
    return IngestRun(
        mode=mode,
        collected_date=now.date().isoformat(),
        run_id=f"{now.strftime('%Y%m%dT%H%M%SKST')}_{secrets.token_hex(2)}",
    )


def _backfill_windows(settings: Settings) -> list[DateWindow]:
    if (
        settings.dart_backfill_start_date is not None
        and settings.dart_backfill_end_date is not None
    ):
        return date_range_windows(
            settings.dart_backfill_start_date,
            settings.dart_backfill_end_date,
        )
    return backfill_windows(_today_kst(), settings.dart_backfill_months.value)


def run_company_master() -> dict[str, Any]:
    """dag_dart_company_master 진입점.

    역할: OpenDART corpCode.xml(전체 고유번호 마스터)을 받아 Bronze에 저장하고 dart_corp_code에 upsert한다.
    기준: 게이트 없이 매 실행 전체 갱신. corpCode 호출은 daily+minute quota로 계수한다.
    반환: 저장된 raw_file_reference id와 upsert한 corp_code 건수.
    """
    context = build_context()
    storage = build_storage(context.settings)
    observed_date = date.today().isoformat()
    request_params = {"observed_date": observed_date}

    try:
        data, http_status, elapsed_ms = context.client.corp_code_zip()
    except Exception as exc:
        with context.service_engine.begin() as conn:
            _log_api_request(
                context,
                conn,
                api_group="DS001",
                api_name="corpCode",
                quota_type="DAILY_AND_MINUTE",
                request_params=request_params,
                status_code=None,
                http_status=None,
                response_ms=None,
                error_message=str(exc),
            )
        raise
    storage_path = BronzePaths.corp_code(observed_date)
    stored = storage.write_bytes(storage_path, data, content_type="application/zip")
    rows = parse_corp_code_zip(data)

    with context.service_engine.begin() as conn:
        raw_ref_id = upsert_raw_file_reference(conn, stored)
        count = upsert_corp_codes(conn, rows)
        _log_api_request(
            context,
            conn,
            api_group="DS001",
            api_name="corpCode",
            quota_type="DAILY_AND_MINUTE",
            request_params=request_params,
            status_code="000",
            http_status=http_status,
            response_ms=elapsed_ms,
        )
        set_state(conn, "company_master_last_observed_date", observed_date)

    return {"raw_ref_id": raw_ref_id, "corp_code_count": count}


def run_listed_company_sync() -> dict[str, Any]:
    """현재 상장사 명단을 corpCode.xml(ZIP)에서 받아 기존 명단과 비교한다.

    명단이 바뀐 경우(신규상장/상장폐지/재상장/정보변경)에만 listed_company를 갱신하고,
    변동 이력(listed_company_event)과 조회에 사용한 원본 XML을 Bronze에 보존한다.
    명단이 그대로면 어떤 정보도 바꾸지 않고 관측 일자만 기록한다.

    상장 시각이 보통 09:00이라 DAG는 09:01(Asia/Seoul)에 도는 것을 기준으로 한다.
    스케줄을 바꾸려면 dags/dag_dart_listed_company.py의 schedule을 수정한다.
    """
    context = build_context()
    storage = build_storage(context.settings)
    observed_date = date.today().isoformat()
    request_params = {"observed_date": observed_date}

    try:
        data, http_status, elapsed_ms = context.client.corp_code_zip()
    except Exception as exc:
        with context.service_engine.begin() as conn:
            _log_api_request(
                context,
                conn,
                api_group="DS001",
                api_name="corpCode",
                quota_type="DAILY_AND_MINUTE",
                request_params=request_params,
                status_code=None,
                http_status=None,
                response_ms=None,
                error_message=str(exc),
            )
        raise

    xml_bytes = extract_corp_code_xml(data)
    records = parse_corp_code_xml(xml_bytes)
    roster = {
        record.stock_code: ListedCompany.from_record(record)
        for record in listed_company_records(records)
    }

    with context.service_engine.begin() as conn:
        existing = load_listed_companies(conn)
        is_initial = not existing
        # 1차 구조 비교(시장구분 없이): 어떤 종목이 바뀌었는지만 본다.
        structural_diff = compute_roster_diff(roster, existing, observed_date=observed_date)

        _log_api_request(
            context,
            conn,
            api_group="DS001",
            api_name="corpCode",
            quota_type="DAILY_AND_MINUTE",
            request_params=request_params,
            status_code="000",
            http_status=http_status,
            response_ms=elapsed_ms,
        )

        if not structural_diff.has_changes and not is_initial:
            # 명단 불변: 정보를 바꾸지 않고 관측 일자만 남긴다.
            set_state(conn, "listed_company_last_observed_date", observed_date)
            return {
                "status": "NO_CHANGE",
                "observed_date": observed_date,
                "listed_total": len(roster),
            }

        # 변동분 corp_code의 시장구분(corp_cls)을 공시에서 enrich한 뒤 이력 텍스트를 재생성한다.
        affected_codes = {event.corp_code for event in structural_diff.events if event.corp_code}
        cls_lookup = corp_cls_lookup(conn, affected_codes)
        diff = compute_roster_diff(
            roster,
            existing,
            observed_date=observed_date,
            corp_cls_lookup=cls_lookup,
        )

        # 조회에 사용한 원본(압축해제 후 XML)을 변동/초기화 시점에 보존한다.
        storage_path = BronzePaths.listed_company(observed_date)
        stored = storage.write_bytes(storage_path, xml_bytes, content_type="application/xml")
        raw_ref_id = upsert_raw_file_reference(conn, stored)

        _persist_roster_diff(conn, diff, cls_lookup, observed_date, raw_ref_id)

        set_state(conn, "listed_company_last_observed_date", observed_date)
        set_state(conn, "listed_company_last_changed_date", observed_date)

    return {
        "status": "INITIALIZED" if is_initial else "CHANGED",
        "observed_date": observed_date,
        "listed_total": len(roster),
        "raw_ref_id": raw_ref_id,
        **diff.summary(),
    }


def _persist_roster_diff(
    conn,
    diff: RosterDiff,
    cls_lookup: dict[str, str],
    observed_date: str,
    raw_ref_id: int,
) -> None:
    """명단 diff를 한 트랜잭션에서 DB에 반영한다.

    역할: 신규상장/재상장/정보변경은 listed_company를 ACTIVE로 upsert,
          상장폐지는 삭제 없이 status=DELISTED로 표시하고, 모든 변동은
          listed_company_event 이력으로 남긴다(원본 raw_ref_id 연결).
    기준: corp_cls는 cls_lookup(공시 enrich)으로 채우고, 없으면 UNKNOWN으로 둔다.
    """
    for event in (*diff.listed, *diff.relisted, *diff.info_changed):
        corp_cls = cls_lookup.get(event.corp_code or "")
        upsert_listed_company(
            conn,
            ListedCompany(
                stock_code=event.stock_code,
                corp_code=event.corp_code or "",
                corp_name=event.corp_name,
                corp_eng_name=event.corp_eng_name,
            ),
            status="ACTIVE",
            corp_cls=corp_cls,
            reason=event.change_reason,
            observed_date=observed_date,
        )
        insert_listed_company_event(
            conn,
            event,
            observed_date=observed_date,
            corp_cls=corp_cls,
            source_raw_ref_id=raw_ref_id,
        )

    for event in diff.delisted:
        corp_cls = cls_lookup.get(event.corp_code or "")
        mark_listed_company_delisted(
            conn,
            event.stock_code,
            reason=event.change_reason,
            observed_date=observed_date,
        )
        insert_listed_company_event(
            conn,
            event,
            observed_date=observed_date,
            corp_cls=corp_cls,
            source_raw_ref_id=raw_ref_id,
        )


def run_backfill_discovery(
    rcept_dt_from: str | None = None,
    rcept_dt_to: str | None = None,
) -> dict[str, Any]:
    """dag_dart_backfill_discovery 진입점.

    역할: rcept_dt_from/rcept_dt_to override가 있으면 그 접수일자 범위를 우선 사용한다.
          override가 없고 명시 백필 시작/종료일이 있으면 그 범위, 없으면 최근
          DART_BACKFILL_MONTHS개월을 1개월 window로 나눠 KOSPI/KOSDAQ 공시검색을 돌리고,
          disclosure upsert + 상세 collect_job 생성을 한다.
    기준: months fallback에서 months>36이면 MANUAL_APPROVAL_REQUIRED로 즉시 중단.
          정상 완료 시 backfill_status=COMPLETED, incremental_enabled=true로 증분 게이트를 연다.
    """
    context = build_context()
    config = context.settings.dart_backfill_months
    override_start = parse_optional_date("rcept_dt_from", rcept_dt_from)
    override_end = parse_optional_date("rcept_dt_to", rcept_dt_to)
    if (override_start is None) != (override_end is None):
        raise ValueError("rcept_dt_from and rcept_dt_to must be set together")
    has_override_range = override_start is not None and override_end is not None
    if has_override_range and override_start > override_end:
        raise ValueError("rcept_dt_from must be <= rcept_dt_to")

    has_explicit_range = has_override_range or context.settings.dart_backfill_start_date is not None
    if (
        not has_explicit_range
        and config.approval_state == ConfigApprovalState.MANUAL_APPROVAL_REQUIRED
    ):
        with context.service_engine.begin() as conn:
            set_state(conn, "backfill_status", "MANUAL_APPROVAL_REQUIRED")
        return {"status": "MANUAL_APPROVAL_REQUIRED", "months": config.value}

    ingest_run = _new_ingest_run("backfill")
    total_disclosures = 0
    total_jobs = 0
    if has_override_range:
        windows = list(reversed(date_range_windows(override_start, override_end)))  # 최신→과거 순
    else:
        windows = list(reversed(_backfill_windows(context.settings)))  # 최신→과거 순
    for window in windows:
        result = _discover_window(context, window, sort_mth="desc", ingest_run=ingest_run)
        total_disclosures += result["disclosures"]
        total_jobs += result["jobs"]

    with context.service_engine.begin() as conn:
        set_state(conn, "backfill_status", "COMPLETED")
        set_state(conn, "backfill_completed_at", datetime.now(timezone.utc).isoformat())
        set_state(conn, "incremental_enabled", "true")

    return {
        "status": "COMPLETED",
        "disclosures": total_disclosures,
        "jobs": total_jobs,
        "run_id": ingest_run.run_id,
        "windows": len(windows),
    }


def run_nightly_backfill() -> dict[str, Any]:
    """dag_dart_nightly_backfill 진입점.

    역할: 매일 20:00 KST 이후 남은 quota로 과거 구간을 최신→과거 순으로 1개월씩 채운다.
    중단 조건:
        - KST 23:50 초과
        - 당일 남은 quota < DART_MINUTE_SAFE_LIMIT × 2 (안전 여유)
    진행 위치: backfill_nightly_frontier (DB state) — 마지막으로 처리한 window의 start date.
               다음 실행 시 그 이전 구간부터 재개한다.
    """
    KST = ZoneInfo("Asia/Seoul")
    DEADLINE_HOUR, DEADLINE_MIN = 23, 50

    context = build_context()
    settings = context.settings
    engine = context.service_engine

    # backfill_discovery 완료 게이트 — 초기 백필이 끝나기 전에는 실행하지 않는다
    with engine.connect() as conn:
        backfill_status = get_state(conn, "backfill_status")
    if backfill_status != "COMPLETED":
        return {"status": "SKIPPED", "reason": "backfill_discovery not completed yet"}

    # 데드라인 계산
    now_kst = datetime.now(KST)
    deadline = now_kst.replace(hour=DEADLINE_HOUR, minute=DEADLINE_MIN, second=0, microsecond=0)
    if now_kst >= deadline:
        return {"status": "SKIPPED", "reason": "already past deadline"}

    # 전체 window 목록 (최신→과거)
    all_windows = list(reversed(_backfill_windows(settings)))

    # frontier: 마지막 실행에서 처리한 가장 오래된 window start date
    with engine.connect() as conn:
        frontier_str = get_state(conn, "backfill_nightly_frontier")

    if frontier_str:
        frontier_date = date.fromisoformat(frontier_str)
        # frontier 이전 구간만 처리 (이미 처리한 구간 skip)
        pending_windows = [w for w in all_windows if w.end < frontier_date]
    else:
        pending_windows = all_windows

    if not pending_windows:
        return {"status": "COMPLETED", "reason": "all windows processed"}

    ingest_run = _new_ingest_run("nightly_backfill")
    total_disclosures = 0
    total_jobs = 0
    processed_windows = 0
    last_window_start: date | None = None

    for window in pending_windows:
        # 데드라인 초과 시 중단
        if datetime.now(KST) >= deadline:
            break

        result = _discover_window(context, window, sort_mth="desc", ingest_run=ingest_run)
        total_disclosures += result["disclosures"]
        total_jobs += result["jobs"]
        processed_windows += 1
        last_window_start = window.start

        # 처리한 frontier 저장 (중단돼도 다음 실행에서 재개 가능)
        with engine.begin() as conn:
            set_state(conn, "backfill_nightly_frontier", last_window_start.isoformat())

    with engine.begin() as conn:
        set_state(conn, "nightly_backfill_last_run_at", datetime.now(timezone.utc).isoformat())

    return {
        "status": "PARTIAL" if pending_windows[processed_windows:] else "COMPLETED",
        "processed_windows": processed_windows,
        "remaining_windows": len(pending_windows) - processed_windows,
        "disclosures": total_disclosures,
        "jobs": total_jobs,
        "frontier": last_window_start.isoformat() if last_window_start else None,
        "run_id": ingest_run.run_id,
    }


def run_incremental_discovery() -> dict[str, Any]:
    """dag_dart_incremental_discovery 진입점.

    역할: 최근 DART_INCREMENTAL_DAYS일 KOSPI/KOSDAQ 공시를 증분 수집한다.
    기준: backfill_status==COMPLETED 가 아니면 SKIP(백필 완료 게이트).
          days>30이면 MANUAL_APPROVAL_REQUIRED. 정상 시 incremental_last_completed_at 기록.
    """
    context = build_context()
    with context.service_engine.begin() as conn:
        backfill_status = get_state(conn, "backfill_status")
    if backfill_status != "COMPLETED":
        return {"status": "SKIPPED", "reason": "backfill is not completed"}

    config = context.settings.dart_incremental_days
    if config.approval_state == ConfigApprovalState.MANUAL_APPROVAL_REQUIRED:
        with context.service_engine.begin() as conn:
            set_state(conn, "incremental_status", "MANUAL_APPROVAL_REQUIRED")
        return {"status": "MANUAL_APPROVAL_REQUIRED", "days": config.value}

    ingest_run = _new_ingest_run("incremental")
    result = _discover_window(
        context,
        incremental_window(_today_kst(), config.value),
        sort_mth="desc",
        ingest_run=ingest_run,
    )
    with context.service_engine.begin() as conn:
        set_state(conn, "incremental_last_completed_at", datetime.now(timezone.utc).isoformat())
    return {"status": "COMPLETED", "run_id": ingest_run.run_id, **result}


def run_detail_collector(
    batch_size: int = 50,
    concurrency: int = 1,
    job_types: list[str] | tuple[str, ...] | None = None,
    max_runtime_seconds: int | None = None,
) -> dict[str, Any]:
    """dag_dart_detail_collector 진입점.

    역할: collect_job에서 PENDING job을 batch_size만큼 claim해 job_type별 collector를 실행한다.
    기준: 각 job은 독립 트랜잭션·독립 HTTP 호출로 처리하며, 성공은 DONE, 예외는 FAILED(+retry_count)로 남겨
          개별 실패가 batch 전체를 막지 않게 한다. 동시 worker 중복은 SKIP LOCKED claim으로 방지.
          concurrency>1이면 claim한 batch를 ThreadPoolExecutor로 동시에 처리한다. quota는
          DbRateLimiter가 DB에서 원자적으로 차감하므로 동시 호출에도 분당/일일 한도를 넘기지 않는다.
    max_runtime_seconds: 지정 시 그 시간까지 claim→처리를 반복하고, 시간을 넘기면 아직 시작 안 한
          job을 PENDING으로 되돌리고 run을 종료한다. batch_size는 "run당 총량"이 아니라 "1회 claim
          단위"다 — 한 run이 batch_size 한 묶음만 처리하고 노는 것을 막아, 한 run이 시간 예산을
          꽉 채워 일하게 한다(throughput 상한을 batch가 아니라 시간으로 제어). 미지정(None)이면
          기존처럼 한 batch만 처리한다(다른 호출자 하위호환).
          또한 DART 지연/장애로 run이 길어져 max_active_runs=1 DAG의 다음 스케줄을 막는 것도 막는다.
    반환: claim/done/failed/rate_limited/rescheduled/deadline_skipped/batches 건수 요약.
    """
    context = build_context()
    # 모든 키의 일일 quota가 소진됐으면 claim+reset 헛돌이 없이 즉시 종료(자정에 자연 회복).
    if all(
        context.rate_limiter.daily_quota_remaining(api_key_identifier=key.identifier) <= 0
        for key in context.api_keys
    ):
        return {"claimed": 0, "done": 0, "failed": 0, "rate_limited": 0, "skipped": "daily_quota_exhausted"}
    storage = build_storage(context.settings)  # job마다 재생성하지 않고 run 전체에서 1개 재사용
    engine = context.service_engine
    deadline = (
        monotonic() + max_runtime_seconds
        if max_runtime_seconds and max_runtime_seconds > 0
        else None
    )

    totals = {"claimed": 0, "done": 0, "failed": 0, "rate_limited": 0, "rescheduled": 0, "deadline_skipped": 0}
    batches = 0
    while deadline is None or monotonic() < deadline:
        with engine.begin() as conn:
            if job_types:
                jobs = claim_pending_jobs_for_types(
                    conn,
                    batch_size=batch_size,
                    job_types=tuple(job_types),
                )
            else:
                jobs = claim_pending_jobs(conn, batch_size=batch_size)
        if not jobs:
            break  # 큐가 비었다 — 더 돌 이유 없음
        result = _process_claimed_jobs(context, storage, jobs, max(1, concurrency), deadline=deadline)
        batches += 1
        for key in totals:
            totals[key] += int(result.get(key, 0))
        # max_runtime_seconds 미지정 시에는 한 batch만 처리(기존 동작 유지).
        if deadline is None:
            break
        if result.get("quota_exhausted"):
            qtype = result.get("quota_exhausted_type") or ""
            qkey = result.get("quota_exhausted_key") or ""
            # minute throttle은 보통 OpenDartClientPool 내부에서 처리된다. 여기까지 새면
            # 같은 retry-after 계산으로 deadline 안에서만 대기 후 재개한다.
            if qtype.startswith("minute") and _wait_for_quota_capacity(context.rate_limiter, qtype, qkey, deadline):
                continue
            # 일일/외부 한도(020)거나 deadline까지 못 기다리면 더 claim해봐야 전량 reset이므로 종료.
            break

    totals["batches"] = batches
    return totals


# TRANSPORT(네트워크) 실패 job은 영구 FAILED 대신 지연 후 재시도한다. 이 횟수를 넘으면 FAILED로 확정한다.
_MAX_TRANSPORT_RETRY = 5


def _transport_retry_delay(retry_count: int) -> int:
    """TRANSPORT 재시도 backoff(초). retry_count에 비례해 늘리되 상한을 둔다(다음 tick 이후로 분산)."""
    return min(600, 60 * (retry_count + 1))


def _wait_for_quota_capacity(
    rate_limiter: DbRateLimiter,
    quota_type: str,
    quota_key: str,
    deadline: float | None,
) -> bool:
    """deadline 안에서 minute-level quota 여유가 생길 때까지만 대기한다.

    deadline 전에 대기를 마칠 수 없으면 자지 않고 False를 반환한다(호출부가 run을 종료).
    대기했으면 True.
    """
    import time as _time

    exc = RateLimitExceeded("quota capacity wait", quota_type=quota_type, quota_key=quota_key)
    wait = rate_limiter.retry_after_seconds(exc)
    if deadline is not None and monotonic() + wait >= deadline:
        return False
    _time.sleep(wait)
    return True


def _process_claimed_jobs(
    context: AppContext,
    storage,
    jobs: list[CollectJob],
    concurrency: int,
    deadline: float | None = None,
) -> dict[str, Any]:
    """claim된 job들을 동시(또는 순차)로 처리하고 결과를 집계한다.

    quota 고갈(RateLimitExceeded) 발생 시: 해당 job은 reset_to_pending(FAILED 아님)으로 되돌리고
    quota_exhausted 신호를 세워, 아직 시작하지 않은 job은 reserve 헛호출 없이 즉시 reset한다.
    이미 실행 중인 job은 안전하게 마무리한다(claim 후 RUNNING 상태가 유실되지 않게 함).

    TRANSPORT(네트워크) 실패: job 잘못이 아닌 일시 장애이므로 영구 FAILED가 아니라 지연 후 재시도
    (reschedule)로 처리한다. _MAX_TRANSPORT_RETRY를 넘기면 FAILED로 확정한다.

    deadline(monotonic): 지정 시 그 시각을 넘기면 아직 시작 안 한 job을 reset_to_pending으로 되돌리고
    더 처리하지 않는다. DART 지연/장애로 한 run이 길어져 max_active_runs=1 DAG의 다음 스케줄을
    막는 것을 끊는다(이미 실행 중인 job은 안전하게 마무리).
    """
    engine = context.service_engine
    quota_exhausted = threading.Event()
    lock = threading.Lock()
    counts = {"done": 0, "failed": 0, "rate_limited": 0, "rescheduled": 0, "deadline_skipped": 0}
    # 어떤 한도로 고갈됐는지 기록한다. minute_*는 일반적으로 pool 내부에서 대기하지만,
    # 누수되면 호출부가 retry-after 기준으로 대기한다. daily/external은 run 종료 대상이다.
    exhausted_type: dict[str, str] = {}

    def _bump(key: str) -> None:
        with lock:
            counts[key] += 1

    def _mark_exhausted(quota_type: str, quota_key: str = "") -> None:
        with lock:
            exhausted_type.setdefault("type", quota_type)
            exhausted_type.setdefault("key", quota_key)
        quota_exhausted.set()

    def _past_deadline() -> bool:
        return deadline is not None and monotonic() >= deadline

    def process(job: CollectJob) -> None:
        if quota_exhausted.is_set() or _past_deadline():
            # quota 고갈이면 rate_limited, 아니면 deadline 초과로 분류한다.
            skipped_for = "rate_limited" if quota_exhausted.is_set() else "deadline_skipped"
            with engine.begin() as conn:
                reset_job_to_pending(conn, job.id)
            _bump(skipped_for)
            return
        try:
            _run_collect_job(context, job, storage)
        except RateLimitExceeded as exc:
            _mark_exhausted(exc.quota_type, exc.quota_key)
            with engine.begin() as conn:
                reset_job_to_pending(conn, job.id)
            _bump("rate_limited")
        except OpenDartError as exc:
            if exc.status == "020":
                _mark_exhausted("daily_external")
                with engine.begin() as conn:
                    reset_job_to_pending(conn, job.id)
                _bump("rate_limited")
                return
            if exc.status == "TRANSPORT" and job.retry_count < _MAX_TRANSPORT_RETRY:
                with engine.begin() as conn:
                    reschedule_job_to_pending(
                        conn,
                        job.id,
                        delay_seconds=_transport_retry_delay(job.retry_count),
                        error_code=exc.status,
                        error_message=str(exc),
                    )
                _bump("rescheduled")
                return
            with engine.begin() as conn:
                mark_job_failed(conn, job.id, exc.__class__.__name__, str(exc))
            _bump("failed")
        except Exception as exc:  # noqa: BLE001 - failures are persisted for retries.
            with engine.begin() as conn:
                mark_job_failed(conn, job.id, exc.__class__.__name__, str(exc))
            _bump("failed")
        else:
            with engine.begin() as conn:
                mark_job_done(conn, job.id)
            _bump("done")

    if concurrency <= 1:
        for job in jobs:
            process(job)
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            list(pool.map(process, jobs))

    # quota_exhausted: 호출부(run_detail_collector)가 같은 run에서 더 claim할지 판단하는 신호.
    # quota_exhausted_type/key: 호출부가 minute 누수와 daily/external 종료를 구분하는 신호.
    return {
        "claimed": len(jobs),
        "quota_exhausted": quota_exhausted.is_set(),
        "quota_exhausted_type": exhausted_type.get("type"),
        "quota_exhausted_key": exhausted_type.get("key"),
        **counts,
    }


def run_raw_to_silver(concurrency: int = 24) -> dict[str, Any]:
    """dag_dart_bronze_to_silver 진입점.

    역할: Bronze complete 마커별로 Bronze 파일들을 취합해 Silver JSON(report.json)을 생성한다.

    변환 원칙:
        - Silver JSON = Bronze 파일 직접 파싱 (DB 스테이징 미사용)
        - rcept_no 하나당 report.json 하나 (멱등 — 재실행 시 덮어씀)
        - 부분 실패는 로그로 기록하되 전체를 중단하지 않음

    성능: todo 계산은 complete/done 마커를 list_keys로 각 1회만 받아 차집합으로 구한다(공시별 exists
          제거). 단 SILVER_VERSION이 바뀐 기존 done은 재처리해야 하므로, done 마커에 한해 version을
          확인한다(done은 보통 todo보다 적다). build는 공시당 Bronze read가 여러 번이라 ThreadPoolExecutor로
          병렬 처리한다(원격 I/O).
    """
    import logging

    from dart_agent.silver.transform import SILVER_VERSION, build_report_from_marker, write_silver_done_marker

    log = logging.getLogger(__name__)
    context = build_context()
    storage = build_storage(context.settings)

    # 증분: {complete marker} - {current-version done marker}. Done identity is encoded in the path.
    log.info("raw_to_silver scan start: silver_version=%s concurrency=%d", SILVER_VERSION, concurrency)
    scan_start = monotonic()
    complete = _markers_index(storage, BronzePaths.complete_prefix())
    complete_scan_elapsed = monotonic() - scan_start
    done_scan_start = monotonic()
    done_current = _silver_done_current_index(storage, SILVER_VERSION)
    done_scan_elapsed = monotonic() - done_scan_start
    todo = sorted(complete - done_current)

    total = len(todo)
    start = monotonic()
    lock = threading.Lock()
    counts = {"built": 0, "failed": 0}

    def _bump(key: str) -> None:
        with lock:
            counts[key] += 1

    log.info(
        (
            "raw_to_silver start: complete=%d done_current=%d todo=%d "
            "complete_scan=%.1fs done_scan=%.1fs silver_version=%s concurrency=%d"
        ),
        len(complete), len(done_current), total,
        complete_scan_elapsed, done_scan_elapsed, SILVER_VERSION, concurrency,
    )

    def process(cr) -> None:
        corp_code, rcept_no = cr
        try:
            raw = storage.read_bytes(BronzePaths.complete_marker(corp_code, rcept_no))
            marker = json.loads(raw.decode("utf-8"))
            silver_path = build_report_from_marker(storage, marker)
            write_silver_done_marker(storage, marker, silver_path)
            _bump("built")
        except Exception as exc:  # noqa: BLE001
            log.error("silver build failed for %s/%s: %s", corp_code, rcept_no, exc)
            _bump("failed")

    if concurrency <= 1:
        for cr in todo:
            process(cr)
    else:
        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
            list(pool.map(process, todo))

    with context.service_engine.begin() as conn:
        set_state(conn, "bronze_to_silver_last_run_at", datetime.now(timezone.utc).isoformat())

    log.info(
        "raw_to_silver finished: todo=%d built=%d failed=%d elapsed=%.0fs",
        total, counts["built"], counts["failed"], monotonic() - start,
    )
    return {"todo": total, "built": counts["built"], "failed": counts["failed"]}


def _markers_index(storage, prefix: str) -> set[tuple[str, str]]:
    """마커 prefix를 list해 (corp_code, rcept_no) 집합으로 만든다.

    경로 패턴: .../corp_code={cc}/rcept_no={rn}.json. set-difference 증분에 쓴다.
    """
    index: set[tuple[str, str]] = set()
    for key in storage.list_keys(prefix):
        corp_code = rcept_no = None
        for part in key.split("/"):
            if part.startswith("corp_code="):
                corp_code = part[len("corp_code="):]
            elif part.startswith("rcept_no=") and part.endswith(".json"):
                rcept_no = part[len("rcept_no="):-len(".json")]
        if corp_code and rcept_no:
            index.add((corp_code, rcept_no))
    return index


LEGACY_SILVER_DONE_VERSION = "v2"


def _legacy_silver_done_index(storage) -> set[tuple[str, str]]:
    index: set[tuple[str, str]] = set()
    for key in storage.list_keys(SilverPaths.done_prefix()):
        if "/sv=" in key:
            continue
        corp_code = rcept_no = None
        for part in key.split("/"):
            if part.startswith("corp_code="):
                corp_code = part[len("corp_code="):]
            elif part.startswith("rcept_no=") and part.endswith(".json"):
                rcept_no = part[len("rcept_no="):-len(".json")]
        if corp_code and rcept_no:
            index.add((corp_code, rcept_no))
    return index


def _silver_done_current_index(storage, silver_version: str) -> set[tuple[str, str]]:
    """Build the current Silver done set without reading marker bodies."""
    current = _markers_index(storage, SilverPaths.done_prefix_for_version(silver_version))
    if silver_version == LEGACY_SILVER_DONE_VERSION:
        current.update(_legacy_silver_done_index(storage))
    return current


def _silver_done_current(storage, corp_code: str, rcept_no: str, silver_version: str) -> bool:
    versioned_path = SilverPaths.done_marker_for_version(silver_version, corp_code, rcept_no)
    if storage.exists(versioned_path):
        return True
    legacy_path = SilverPaths.done_marker(corp_code, rcept_no)
    if silver_version == LEGACY_SILVER_DONE_VERSION and storage.exists(legacy_path):
        return True
    if not storage.exists(legacy_path):
        return False
    try:
        marker = json.loads(storage.read_bytes(legacy_path).decode("utf-8"))
    except Exception:  # noqa: BLE001 - unreadable marker should be rebuilt.
        return False
    return marker.get("silver_version") == silver_version


def _complete_marker_current(marker: dict[str, Any], disclosure_row: dict[str, Any], evaluation: dict[str, Any]) -> bool:
    from dart_agent.silver.completion import MARKER_VERSION

    if evaluation.get("status") != "COMPLETE":
        return False
    expected = {
        (str(art.get("kind")), str(art.get("path")), str(art.get("status")))
        for art in evaluation.get("artifacts", [])
    }
    current = {
        (str(art.get("kind")), str(art.get("path")), str(art.get("status")))
        for art in marker.get("artifacts", [])
    }
    return (
        str(marker.get("marker_version") or "") == MARKER_VERSION
        and str(marker.get("report_type") or "") == str(disclosure_row.get("report_type") or "DISCLOSURE")
        and marker.get("pblntf_ty") == disclosure_row.get("pblntf_ty")
        and marker.get("pblntf_detail_ty") == disclosure_row.get("pblntf_detail_ty")
        and str(marker.get("group_id") or "") == str(disclosure_row.get("group_id") or "")
        and bool(marker.get("is_latest", True)) == bool(disclosure_row.get("is_latest", True))
        and str(marker.get("original_rcept_no") or "") == str(disclosure_row.get("original_rcept_no") or "")
        and str(marker.get("latest_rcept_no") or "") == str(disclosure_row.get("latest_rcept_no") or "")
        and bool(marker.get("is_amended", False)) == bool(disclosure_row.get("is_amended", False))
        and int(marker.get("amendment_seq") or 0) == int(disclosure_row.get("amendment_seq") or 0)
        and current == expected
    )


def _completion_row_changed_since(disclosure_row: dict[str, Any], last_run_at: str | None) -> bool:
    """Return True when DB metadata changed after the last full completion pass."""
    if not last_run_at:
        return True
    try:
        cutoff = datetime.fromisoformat(last_run_at.replace("Z", "+00:00"))
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)
    except Exception:  # noqa: BLE001 - unknown state should fall back to verification.
        return True
    for key in ("last_seen_at", "group_updated_at"):
        value = disclosure_row.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except Exception:  # noqa: BLE001
                return True
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            if value > cutoff:
                return True
    return False


def _delete_stale_silver_outputs(storage, corp_code: str, rcept_no: str, marker: dict[str, Any] | None = None) -> None:
    storage.delete(BronzePaths.complete_marker(corp_code, rcept_no))
    storage.delete(SilverPaths.done_marker(corp_code, rcept_no))
    try:
        from dart_agent.silver.transform import SILVER_VERSION

        storage.delete(SilverPaths.done_marker_for_version(SILVER_VERSION, corp_code, rcept_no))
    except Exception:  # noqa: BLE001 - best-effort cleanup; report deletion below is authoritative.
        pass
    if marker:
        old_report_type = str(marker.get("report_type") or "DISCLOSURE")
        storage.delete(SilverPaths.report(corp_code, old_report_type, rcept_no))


class _PrefetchedStorage:
    """exists를 미리 적재한 키 집합으로 메모리 조회하는 storage 래퍼.

    completion은 공시마다 여러 번 S3 exists(원격 ~100ms)를 호출하는데, 13만 건이면 수십만 HEAD가
    되어 수 시간이 걸린다. Bronze 전체 키를 list_keys로 한 번 적재(prefetch)해 exists를 메모리
    조회(O(1))로 바꾼다(HEAD 수십만 → LIST 수백). read/write/delete는 실제 storage에 위임하고,
    write/delete는 같은 run 내 일관성을 위해 키 집합도 갱신한다.
    """

    def __init__(self, storage, keys: set[str], s3_fallback: bool = False) -> None:
        self._storage = storage
        self._keys = keys
        # s3_fallback: DB(raw_file_reference) 캐시에 없을 때 실제 S3 존재까지 확인할지.
        # 기본 False(수집처: DB가 권위 → 빠름). True면 DB가 비어도 S3 파일 존재로 판정(환경독립).
        self._s3_fallback = s3_fallback

    def exists(self, path: str) -> bool:
        if path in self._keys:
            return True
        if self._s3_fallback:
            return self._storage.exists(path)
        return False

    def read_bytes(self, path: str) -> bytes:
        return self._storage.read_bytes(path)

    def write_bytes(self, path: str, data: bytes, content_type: str | None = None):
        self._keys.add(path)
        return self._storage.write_bytes(path, data, content_type=content_type)

    def delete(self, path: str) -> None:
        self._keys.discard(path)
        self._storage.delete(path)

    def list_keys(self, prefix: str):
        return self._storage.list_keys(prefix)


def run_bronze_completion(
    chunk_size: int = 10_000,
    concurrency: int = 24,
    s3_fallback: bool = True,
    verify_existing_markers: bool = False,
) -> dict[str, Any]:
    """Bronze 수집 완결성 판정 → complete 마커 작성 진입점.

    disclosure를 순회하며 expected(코드 계산) vs Bronze 존재를 평가하고, COMPLETE면 마커를 쓴다.

    성능/메모리: 공시당 원격 exists를 여러 번 하면 13만 건이 수 시간이 걸린다. 그렇다고 Bronze 전체
    키를 메모리에 한 번에 올리면 키 수에 비례해 메모리가 커져 작은 스펙에서 위험하다. 그래서
    disclosure를 chunk_size씩 끊고, 그 chunk의 expected path만 raw_file_reference(Bronze write를
    추적하는 DB)로 batch 조회해 exists를 메모리 조회로 바꾼다 — 메모리는 항상 chunk 분량으로 제한되어
    하드웨어 스펙에 맞춰 chunk_size로 조절할 수 있다(원격 HEAD 0, DB 인덱스 조회).
    nodata(013/014)는 collector가 같은 expected 경로에 marker object를 쓰므로 실제 Bronze object로
    판정한다. collect_job DONE은 Bronze 존재 근거로 쓰지 않는다. complete 마커는 완결분만 있어
    list_keys(complete_prefix) 1회로 싸게 전체 집합을 만든다.
    """
    import logging

    from dart_agent.services.disclosure import bronze_artifacts_for_disclosure
    from dart_agent.silver.completion import MARKER_VERSION, evaluate_completion, write_complete_marker

    context = build_context()
    base_storage = build_storage(context.settings)
    engine = context.service_engine
    collect_mode = context.settings.dart_collect_mode
    log = logging.getLogger(__name__)

    with engine.connect() as conn:
        last_run_at = get_state(conn, "bronze_completion_last_run_at")
        last_marker_version = get_state(conn, "bronze_completion_marker_version")
        rows = conn.execute(
            text(
                """
                WITH disclosure_with_amendment AS (
                    SELECT d.rcept_no, d.corp_code, d.stock_code, d.corp_name, d.rcept_dt, d.report_nm,
                           d.report_type, d.pblntf_ty, d.pblntf_detail_ty, d.is_latest,
                           d.last_seen_at, COALESCE(g.updated_at, d.last_seen_at) AS group_updated_at,
                           d.group_id, d.is_correction AS is_amended,
                           COALESCE(g.latest_rcept_no, d.rcept_no) AS latest_rcept_no,
                           FIRST_VALUE(d.rcept_no) OVER (
                               PARTITION BY COALESCE(d.group_id::text, d.rcept_no)
                               ORDER BY d.rcept_dt ASC, d.rcept_no ASC
                           ) AS original_rcept_no,
                           ROW_NUMBER() OVER (
                               PARTITION BY COALESCE(d.group_id::text, d.rcept_no)
                               ORDER BY d.rcept_dt ASC, d.rcept_no ASC
                           ) - 1 AS amendment_seq
                    FROM disclosure d
                    LEFT JOIN disclosure_group g ON d.group_id = g.id
                )
                SELECT rcept_no, corp_code, stock_code, corp_name, rcept_dt, report_nm,
                       report_type, pblntf_ty, pblntf_detail_ty, is_latest,
                       last_seen_at, group_updated_at,
                       group_id, original_rcept_no, latest_rcept_no, is_amended, amendment_seq
                FROM disclosure_with_amendment
                ORDER BY rcept_dt ASC, rcept_no ASC
                """
            )
        ).mappings().all()

    # 완결 marker는 완결분만 존재(보통 적음) → 1회 list로 전체 집합 확보. exists는 이 집합으로 판정.
    marker_set = set(base_storage.list_keys(BronzePaths.complete_prefix()))
    force_material_event_recheck = last_marker_version != MARKER_VERSION
    log.info(
        "bronze_completion: %d disclosures, %d complete markers, chunk_size=%d, marker_version=%s, previous_marker_version=%s",
        len(rows), len(marker_set), chunk_size, MARKER_VERSION, last_marker_version,
    )

    lock = threading.Lock()
    counts = {"complete": 0, "partial": 0, "skipped": 0, "stale": 0, "failed": 0, "no_detail": 0}

    def _bump(key: str) -> None:
        with lock:
            counts[key] += 1

    for start in range(0, len(rows), max(1, chunk_size)):
        chunk = rows[start:start + max(1, chunk_size)]

        # chunk의 expected artifact path를 모아 raw_file_reference에서 한 번에 조회(메모리=chunk 분량).
        chunk_paths: set[str] = set()
        for row in chunk:
            d = dict(row)
            marker_path = BronzePaths.complete_marker(str(d["corp_code"]), str(d["rcept_no"]))
            if (
                not verify_existing_markers
                and marker_path in marker_set
                and not _completion_row_changed_since(d, last_run_at)
                and not (force_material_event_recheck and str(d.get("report_type")) == "MATERIAL_EVENT")
            ):
                continue
            for _kind, path, _match in bronze_artifacts_for_disclosure(
                d.get("report_nm", "") or "",
                str(d["rcept_dt"]).replace("-", "")[:8],
                str(d["corp_code"]),
                str(d["rcept_no"]),
                pblntf_ty=d.get("pblntf_ty"),
                pblntf_detail_ty=d.get("pblntf_detail_ty"),
                collect_mode=collect_mode,
            ):
                chunk_paths.add(path)

        present: set[str] = set(marker_set)
        with engine.connect() as conn:
            if chunk_paths:
                res = conn.execute(
                    text(
                        "SELECT object_path FROM raw_file_reference "
                        "WHERE storage_backend = :backend AND object_path = ANY(:paths)"
                    ),
                    {"backend": getattr(base_storage, "backend", "s3"), "paths": list(chunk_paths)},
                )
                present.update(r[0] for r in res)
        storage = _PrefetchedStorage(base_storage, present, s3_fallback=s3_fallback)

        def process(row, _storage=storage) -> None:
            # evaluate는 prefetched path/S3 fallback으로 exists를 줄이고, complete 마커 read/write는 원격 I/O이므로
            # chunk 안에서 ThreadPoolExecutor로 병렬 처리한다.
            d = dict(row)
            try:
                marker_path = BronzePaths.complete_marker(str(d["corp_code"]), str(d["rcept_no"]))
                if (
                    not verify_existing_markers
                    and marker_path in marker_set
                    and not _completion_row_changed_since(d, last_run_at)
                    and not (force_material_event_recheck and str(d.get("report_type")) == "MATERIAL_EVENT")
                ):
                    _bump("skipped")
                    return
                evaluation = evaluate_completion(_storage, d, collect_mode, engine=engine)
                status = evaluation["status"]
                existing_marker: dict[str, Any] | None = None
                if _storage.exists(marker_path):
                    try:
                        existing_marker = json.loads(base_storage.read_bytes(marker_path).decode("utf-8"))
                    except Exception:  # noqa: BLE001 - unreadable marker should be rebuilt.
                        existing_marker = None
                    if existing_marker and _complete_marker_current(existing_marker, d, evaluation):
                        _bump("skipped")
                        return
                    _bump("stale")
                if status == "COMPLETE":
                    write_complete_marker(_storage, d, evaluation, _lookup_ingest_mode(engine, str(d["rcept_no"])))
                    _bump("complete")
                elif status == "SKIP_NO_DETAIL":
                    if existing_marker:
                        _delete_stale_silver_outputs(base_storage, str(d["corp_code"]), str(d["rcept_no"]), existing_marker)
                    _bump("no_detail")  # 상세 없는 공시(DISCLOSURE 등) — 마커 안 만듦
                else:
                    if existing_marker:
                        _delete_stale_silver_outputs(base_storage, str(d["corp_code"]), str(d["rcept_no"]), existing_marker)
                    _bump("partial")
            except Exception as exc:  # noqa: BLE001 - 개별 공시 실패가 전체 run을 죽이지 않게 격리.
                log.error("bronze completion failed for %s/%s: %s", d.get("corp_code"), d.get("rcept_no"), exc)
                _bump("failed")

        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
            list(pool.map(process, chunk))

    with engine.begin() as conn:
        set_state(conn, "bronze_completion_last_run_at", datetime.now(timezone.utc).isoformat())
        set_state(conn, "bronze_completion_marker_version", MARKER_VERSION)

    return {
        "total": len(rows),
        "complete": counts["complete"],
        "partial": counts["partial"],
        "no_detail": counts["no_detail"],
        "failed": counts["failed"],
        "stale_markers": counts["stale"],
        "skipped_already_marked": counts["skipped"],
    }


def run_silver_incremental(
    max_runtime_seconds: int = 240,
    bootstrap_lookback_days: int = 0,
    corp_recent_days: int = 30,
) -> dict[str, Any]:
    """dag_dart_silver_incremental 진입점 — 근실시간 증분(신규 변경분만 completion → Silver).

    역할: 직전 실행 이후 collect_job이 새로 DONE된(=Bronze가 새로 쌓인) 공시만 평가해, expected가
          모두 모였으면 complete 마커 → 곧바로 Silver report.json을 작성한다. detail_collector의
          Bronze 수집 주기를 따라가 "공시가 done되는 대로 바로 Silver"를 만든다.
    증분 방식: watermark(silver_completion_watermark) 이후 finished_at이 갱신된 DONE job의
          corp_code/rcept_no에 연관된 disclosure만 본다. structured/event/securities는 rcept_no가
          없어 corp_code 단위로, document/financial은 rcept_no로 연관한다. 마커 있으면 skip이라
          전체/윈도우 재평가 없이 변경분만 처리한다(아직 미완결 공시는 다음에 그 job이 또 DONE될 때 재평가).
    멱등: 마커+Silver _done이 둘 다 있으면 skip. 마커만 있고 Silver가 없으면 Silver만 보정.
    watermark 전진: 시간 예산(max_runtime_seconds) 안에 변경분을 다 처리했을 때만 watermark를 run
          시작 시각으로 전진한다. 중간에 끊기면(stopped_early) 유지해 다음 run이 이어서 본다.
    corp_recent_days: structured/event/securities는 rcept_no가 없어 corp_code로만 연관되는데, 그
          회사의 과거 공시 전체를 끌어오면 범위가 폭발한다. corp_code 연관은 최근 corp_recent_days일
          공시로 제한한다(과거 공시는 dag_dart_raw_to_silver 전체 보정이 담당). rcept_no 직접 연관
          (document/financial)은 정확하므로 날짜 제한이 없다.
    bootstrap: watermark가 없으면(첫 가동) 과거 전체는 dag_dart_raw_to_silver(전체)에 맡기고,
          watermark를 now-bootstrap_lookback_days로 시작한다(기본 0 = 지금 이후 변경분만).
    """
    import logging

    from dart_agent.silver.completion import evaluate_completion, write_complete_marker
    from dart_agent.silver.transform import SILVER_VERSION, build_report_from_marker, write_silver_done_marker

    log = logging.getLogger(__name__)
    context = build_context()
    storage = build_storage(context.settings)
    engine = context.service_engine
    collect_mode = context.settings.dart_collect_mode
    deadline = monotonic() + max_runtime_seconds if max_runtime_seconds and max_runtime_seconds > 0 else None
    run_start = datetime.now(timezone.utc)

    with engine.begin() as conn:
        watermark = get_state(conn, "silver_completion_watermark")
        if watermark is None:
            watermark = (run_start - timedelta(days=max(0, bootstrap_lookback_days))).isoformat()
            set_state(conn, "silver_completion_watermark", watermark)

    # 직전 watermark 이후 새로 DONE된 job의 연관 키(변경분)만 추린다.
    with engine.connect() as conn:
        changed = conn.execute(
            text(
                """
                SELECT DISTINCT corp_code, rcept_no
                FROM collect_job
                WHERE status = 'DONE' AND finished_at > CAST(:wm AS timestamptz)
                """
            ),
            {"wm": watermark},
        ).all()
    corp_codes = sorted({r.corp_code for r in changed if r.corp_code})
    rcept_nos = sorted({r.rcept_no for r in changed if r.rcept_no})

    if not corp_codes and not rcept_nos:
        with engine.begin() as conn:
            set_state(conn, "silver_completion_watermark", run_start.isoformat())
        return {"changed_jobs": 0, "scanned": 0, "built": 0, "silver_only": 0,
                "partial": 0, "no_detail": 0, "skipped": 0, "failed": 0, "stopped_early": False}

    corp_cutoff = run_start.date() - timedelta(days=max(0, corp_recent_days))
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                WITH disclosure_with_amendment AS (
                    SELECT d.rcept_no, d.corp_code, d.stock_code, d.corp_name, d.rcept_dt, d.report_nm,
                           d.report_type, d.pblntf_ty, d.pblntf_detail_ty, d.is_latest,
                           d.group_id, d.is_correction AS is_amended,
                           COALESCE(g.latest_rcept_no, d.rcept_no) AS latest_rcept_no,
                           FIRST_VALUE(d.rcept_no) OVER (
                               PARTITION BY COALESCE(d.group_id::text, d.rcept_no)
                               ORDER BY d.rcept_dt ASC, d.rcept_no ASC
                           ) AS original_rcept_no,
                           ROW_NUMBER() OVER (
                               PARTITION BY COALESCE(d.group_id::text, d.rcept_no)
                               ORDER BY d.rcept_dt ASC, d.rcept_no ASC
                           ) - 1 AS amendment_seq
                    FROM disclosure d
                    LEFT JOIN disclosure_group g ON d.group_id = g.id
                )
                SELECT rcept_no, corp_code, stock_code, corp_name, rcept_dt, report_nm,
                       report_type, pblntf_ty, pblntf_detail_ty, is_latest,
                       group_id, original_rcept_no, latest_rcept_no, is_amended, amendment_seq
                FROM disclosure_with_amendment
                WHERE rcept_no = ANY(:rcepts)
                   OR (corp_code = ANY(:corps) AND rcept_dt >= :corp_cutoff)
                ORDER BY rcept_dt ASC, rcept_no ASC
                """
            ),
            {"corps": corp_codes, "rcepts": rcept_nos, "corp_cutoff": corp_cutoff},
        ).mappings().all()

    built = silver_only = partial = no_detail = skipped = failed = 0
    stopped_early = False
    for row in rows:
        if deadline is not None and monotonic() >= deadline:
            # 시간 예산 초과 — 남은 공시는 다음 run이 이어서 처리(마커/skip 기반 멱등).
            stopped_early = True
            break
        d = dict(row)
        corp_code = str(d["corp_code"])
        rcept_no = str(d["rcept_no"])
        try:
            marker_path = BronzePaths.complete_marker(corp_code, rcept_no)
            evaluation = evaluate_completion(storage, d, collect_mode, engine=engine)
            if storage.exists(marker_path):
                marker = json.loads(storage.read_bytes(marker_path).decode("utf-8"))
                if _complete_marker_current(marker, d, evaluation) and _silver_done_current(storage, corp_code, rcept_no, SILVER_VERSION):
                    skipped += 1
                    continue
                if _complete_marker_current(marker, d, evaluation):
                    # 마커는 있으나 Silver 누락 또는 구버전 → Silver만 보정.
                    silver_path = build_report_from_marker(storage, marker)
                    write_silver_done_marker(storage, marker, silver_path)
                    silver_only += 1
                    continue
                if evaluation["status"] != "COMPLETE":
                    _delete_stale_silver_outputs(storage, corp_code, rcept_no, marker)
            status = evaluation["status"]
            if status == "COMPLETE":
                write_complete_marker(storage, d, evaluation, _lookup_ingest_mode(engine, rcept_no))
                marker = json.loads(storage.read_bytes(marker_path).decode("utf-8"))
                silver_path = build_report_from_marker(storage, marker)
                write_silver_done_marker(storage, marker, silver_path)
                built += 1
            elif status == "SKIP_NO_DETAIL":
                no_detail += 1
            else:
                partial += 1
        except Exception as exc:  # noqa: BLE001 - 개별 공시 실패가 전체 run을 죽이지 않게 격리.
            log.error("silver_incremental failed for %s/%s: %s", corp_code, rcept_no, exc)
            failed += 1

    # 변경분을 끝까지 처리했을 때만 watermark 전진(중간에 끊기면 다음 run이 같은 범위 재시도).
    if not stopped_early:
        with engine.begin() as conn:
            set_state(conn, "silver_completion_watermark", run_start.isoformat())
    with engine.begin() as conn:
        set_state(conn, "silver_incremental_last_run_at", datetime.now(timezone.utc).isoformat())

    result = {
        "changed_jobs": len(changed),
        "scanned": len(rows),
        "built": built,
        "silver_only": silver_only,
        "partial": partial,
        "no_detail": no_detail,
        "skipped": skipped,
        "failed": failed,
        "stopped_early": stopped_early,
    }
    log.info("silver_incremental: %s", result)
    return result


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def _load_disclosure_targets(
    engine,
    *,
    rcept_nos: list[str] | None = None,
    corp_code: str | None = None,
    rcept_dt_from: str | None = None,
    rcept_dt_to: str | None = None,
    report_type: str | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    """운영 보정용 disclosure target 조회.

    rcept_no/corp/date/report_type 중 하나 이상의 selector가 있어야 한다. 실수로 전체 공시를
    재처리하지 않도록 selector 없는 호출은 빈 목록을 반환한다.
    """
    selectors = [bool(rcept_nos), bool(corp_code), bool(rcept_dt_from), bool(rcept_dt_to), bool(report_type)]
    if not any(selectors):
        return []

    conds = ["1=1"]
    params: dict[str, Any] = {"limit": max(1, int(limit))}
    sql = """
        WITH disclosure_with_amendment AS (
            SELECT d.rcept_no, d.corp_code, d.stock_code, d.corp_name, d.rcept_dt, d.report_nm,
                   d.report_type, d.pblntf_ty, d.pblntf_detail_ty, d.is_latest,
                   d.group_id, d.is_correction AS is_amended,
                   COALESCE(g.latest_rcept_no, d.rcept_no) AS latest_rcept_no,
                   FIRST_VALUE(d.rcept_no) OVER (
                       PARTITION BY COALESCE(d.group_id::text, d.rcept_no)
                       ORDER BY d.rcept_dt ASC, d.rcept_no ASC
                   ) AS original_rcept_no,
                   ROW_NUMBER() OVER (
                       PARTITION BY COALESCE(d.group_id::text, d.rcept_no)
                       ORDER BY d.rcept_dt ASC, d.rcept_no ASC
                   ) - 1 AS amendment_seq
            FROM disclosure d
            LEFT JOIN disclosure_group g ON d.group_id = g.id
        )
        SELECT d.rcept_no, d.corp_code, d.stock_code, d.corp_name, d.rcept_dt, d.report_nm,
               d.report_type, d.pblntf_ty, d.pblntf_detail_ty, d.is_latest,
               d.group_id, d.original_rcept_no, d.latest_rcept_no, d.is_amended, d.amendment_seq
        FROM disclosure_with_amendment d
        WHERE {where}
        ORDER BY d.rcept_dt ASC, d.rcept_no ASC
        LIMIT :limit
        """

    bindparams = []
    if rcept_nos:
        conds.append("d.rcept_no IN :rcept_nos")
        params["rcept_nos"] = rcept_nos
        bindparams.append(bindparam("rcept_nos", expanding=True))
    if corp_code:
        conds.append("d.corp_code = :corp_code")
        params["corp_code"] = corp_code
    if rcept_dt_from:
        conds.append("d.rcept_dt >= CAST(:rcept_dt_from AS date)")
        params["rcept_dt_from"] = rcept_dt_from
    if rcept_dt_to:
        conds.append("d.rcept_dt <= CAST(:rcept_dt_to AS date)")
        params["rcept_dt_to"] = rcept_dt_to
    if report_type:
        conds.append("d.report_type = :report_type")
        params["report_type"] = report_type

    stmt = text(sql.format(where=" AND ".join(conds)))
    if bindparams:
        stmt = stmt.bindparams(*bindparams)

    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(stmt, params).mappings().all()]


def _delete_silver_outputs(storage, corp_code: str, rcept_no: str, report_type: str | None) -> None:
    storage.delete(SilverPaths.done_marker(corp_code, rcept_no))
    try:
        from dart_agent.silver.transform import SILVER_VERSION

        storage.delete(SilverPaths.done_marker_for_version(SILVER_VERSION, corp_code, rcept_no))
    except Exception:  # noqa: BLE001 - best-effort cleanup.
        pass
    if report_type:
        storage.delete(SilverPaths.report(corp_code, report_type, rcept_no))


def run_silver_rebuild_targets(
    *,
    rcept_nos: list[str] | str | None = None,
    corp_code: str | None = None,
    rcept_dt_from: str | None = None,
    rcept_dt_to: str | None = None,
    report_type: str | None = None,
    rebuild_completion: bool = False,
    limit: int = 1000,
    dry_run: bool = False,
) -> dict[str, Any]:
    """수동 운영 DAG용: 대상 Silver report.json/_done을 강제 재생성한다.

    사용 시나리오:
      - Silver parser/정제 로직이 잘못되어 같은 Bronze로 Silver만 다시 만들 때
      - report_type/stale marker 보정 후 특정 rcept_no만 즉시 재검증할 때

    selector 없이 전체 재생성하지 않는다. 전체 재처리는 SILVER_VERSION bump + dag_dart_raw_to_silver를
    사용한다.
    """
    from dart_agent.silver.completion import evaluate_completion, write_complete_marker
    from dart_agent.silver.transform import build_report_from_marker, write_silver_done_marker

    context = build_context()
    storage = build_storage(context.settings)
    engine = context.service_engine
    collect_mode = context.settings.dart_collect_mode

    targets = _load_disclosure_targets(
        engine,
        rcept_nos=_as_list(rcept_nos),
        corp_code=corp_code,
        rcept_dt_from=rcept_dt_from,
        rcept_dt_to=rcept_dt_to,
        report_type=report_type,
        limit=limit,
    )
    if not targets:
        return {"status": "SKIPPED_NO_TARGETS", "targets": 0, "rebuilt": 0}

    rebuilt = 0
    completion_rebuilt = 0
    partial = 0
    missing_complete = 0
    failed = 0
    for d in targets:
        corp = str(d["corp_code"])
        rcept = str(d["rcept_no"])
        try:
            marker_path = BronzePaths.complete_marker(corp, rcept)
            if rebuild_completion:
                evaluation = evaluate_completion(storage, d, collect_mode, engine=engine)
                if evaluation["status"] == "COMPLETE":
                    if not dry_run:
                        write_complete_marker(storage, d, evaluation, _lookup_ingest_mode(engine, rcept))
                    completion_rebuilt += 1
                else:
                    if not dry_run:
                        _delete_stale_silver_outputs(storage, corp, rcept, None)
                        _delete_silver_outputs(storage, corp, rcept, d.get("report_type") or "DISCLOSURE")
                    partial += 1
                    continue

            if not storage.exists(marker_path):
                missing_complete += 1
                continue

            marker = json.loads(storage.read_bytes(marker_path).decode("utf-8"))
            marker_report_type = str(marker.get("report_type") or d.get("report_type") or "DISCLOSURE")
            if not dry_run:
                _delete_silver_outputs(storage, corp, rcept, marker_report_type)
                silver_path = build_report_from_marker(storage, marker)
                write_silver_done_marker(storage, marker, silver_path)
            rebuilt += 1
        except Exception:  # noqa: BLE001 - target 단위 실패 격리
            failed += 1

    return {
        "status": "DONE",
        "targets": len(targets),
        "rebuilt": rebuilt,
        "completion_rebuilt": completion_rebuilt,
        "partial": partial,
        "missing_complete": missing_complete,
        "failed": failed,
        "dry_run": dry_run,
    }


def _job_params_from_spec(row: dict[str, Any], spec) -> dict[str, Any]:
    return {
        "job_type": spec.job_type,
        "api_group": spec.api_group,
        "api_name": spec.api_name,
        "rcept_no": str(row["rcept_no"]) if spec.rcept_scoped else None,
        "corp_code": str(row["corp_code"]),
        "stock_code": row.get("stock_code") or None,
        "bsns_year": spec.bsns_year,
        "reprt_code": spec.reprt_code,
        "bgn_de": spec.bgn_de,
        "end_de": spec.end_de,
    }


def _reset_or_enqueue_job(conn, row: dict[str, Any], spec, *, reset_running: bool) -> str:
    params = _job_params_from_spec(row, spec)
    hashed = request_hash("DART", spec.job_type, params)
    existing = conn.execute(
        text("SELECT id, status FROM collect_job WHERE request_hash = :request_hash"),
        {"request_hash": hashed},
    ).mappings().first()
    if existing:
        if existing["status"] == "RUNNING" and not reset_running:
            return "skipped_running"
        conn.execute(
            text(
                """
                UPDATE collect_job
                SET status = 'PENDING',
                    started_at = NULL,
                    finished_at = NULL,
                    error_code = NULL,
                    error_message = NULL,
                    scheduled_at = CURRENT_TIMESTAMP,
                    ingest_mode = 'bronze_recollect'
                WHERE id = :id
                """
            ),
            {"id": existing["id"]},
        )
        return "reset"

    job_id = enqueue_collect_job(
        conn,
        priority=spec.priority,
        job_type=spec.job_type,
        api_group=spec.api_group,
        api_name=spec.api_name,
        rcept_no=params["rcept_no"],
        corp_code=params["corp_code"],
        stock_code=params["stock_code"],
        bsns_year=params["bsns_year"],
        reprt_code=params["reprt_code"],
        bgn_de=params["bgn_de"],
        end_de=params["end_de"],
        ingest_mode="bronze_recollect",
    )
    return "enqueued" if job_id is not None else "skipped_conflict"


def _quarantine_and_delete(storage, path: str, repair_run_id: str) -> bool:
    if not storage.exists(path):
        return False
    payload = storage.read_bytes(path)
    bronze_root = BronzePaths.complete_prefix().split("/complete/", 1)[0]
    quarantine_path = f"{bronze_root}/_quarantine/repair_run={repair_run_id}/{path}"
    storage.write_bytes(quarantine_path, payload)
    storage.delete(path)
    return True


def run_bronze_recollect_targets(
    *,
    rcept_nos: list[str] | str | None = None,
    corp_code: str | None = None,
    rcept_dt_from: str | None = None,
    rcept_dt_to: str | None = None,
    report_type: str | None = None,
    job_types: list[str] | str | None = None,
    api_names: list[str] | str | None = None,
    limit: int = 1000,
    quarantine_existing: bool = True,
    reset_running: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """수동 운영 DAG용: 잘못 수집된 Bronze 일부를 무효화하고 해당 job만 재수집 대기화한다.

    기존 Bronze는 기본적으로 `_quarantine` prefix에 복사한 뒤 원 경로를 삭제한다. 원 경로를
    삭제해야 completion이 기존 잘못된 파일을 다시 COMPLETE로 판정하지 않는다.
    실제 API 재호출은 이 함수가 하지 않고, 기존 backfill/detail collector가 PENDING job을 처리한다.
    """
    context = build_context()
    storage = build_storage(context.settings)
    engine = context.service_engine
    collect_mode = context.settings.dart_collect_mode
    selected_job_types = set(_as_list(job_types))
    selected_api_names = set(_as_list(api_names))
    repair_run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    targets = _load_disclosure_targets(
        engine,
        rcept_nos=_as_list(rcept_nos),
        corp_code=corp_code,
        rcept_dt_from=rcept_dt_from,
        rcept_dt_to=rcept_dt_to,
        report_type=report_type,
        limit=limit,
    )
    if not targets:
        return {"status": "SKIPPED_NO_TARGETS", "targets": 0, "jobs_reset": 0, "jobs_enqueued": 0}

    invalidated_targets = 0
    quarantined = 0
    missing_artifacts = 0
    jobs_reset = 0
    jobs_enqueued = 0
    skipped_running = 0
    skipped_specs = 0

    for d in targets:
        rcept_dt = str(d["rcept_dt"]).replace("-", "")[:8]
        specs = detail_jobs_for_disclosure(
            str(d["report_nm"] or ""),
            rcept_dt,
            pblntf_ty=d.get("pblntf_ty"),
            pblntf_detail_ty=d.get("pblntf_detail_ty"),
            collect_mode=collect_mode,
        )
        specs = [
            spec for spec in specs
            if (not selected_job_types or spec.job_type in selected_job_types)
            and (not selected_api_names or spec.api_name in selected_api_names)
        ]
        if not specs:
            skipped_specs += 1
            continue

        corp = str(d["corp_code"])
        rcept = str(d["rcept_no"])
        if not dry_run:
            marker: dict[str, Any] | None = None
            marker_path = BronzePaths.complete_marker(corp, rcept)
            if storage.exists(marker_path):
                try:
                    marker = json.loads(storage.read_bytes(marker_path).decode("utf-8"))
                except Exception:  # noqa: BLE001
                    marker = None
            _delete_stale_silver_outputs(storage, corp, rcept, marker)
            if marker is None:
                _delete_silver_outputs(storage, corp, rcept, d.get("report_type") or "DISCLOSURE")
        invalidated_targets += 1

        artifacts = bronze_artifacts_for_disclosure(
            str(d["report_nm"] or ""),
            rcept_dt,
            corp,
            rcept,
            pblntf_ty=d.get("pblntf_ty"),
            pblntf_detail_ty=d.get("pblntf_detail_ty"),
            collect_mode=collect_mode,
        )
        for _, path, match in artifacts:
            if selected_job_types and match.get("job_type") not in selected_job_types:
                continue
            if selected_api_names and match.get("api_name") not in selected_api_names:
                continue
            if dry_run:
                if storage.exists(path):
                    quarantined += 1
                else:
                    missing_artifacts += 1
            elif quarantine_existing:
                if _quarantine_and_delete(storage, path, repair_run_id):
                    quarantined += 1
                else:
                    missing_artifacts += 1
            else:
                storage.delete(path)

        if dry_run:
            continue
        with engine.begin() as conn:
            for spec in specs:
                result = _reset_or_enqueue_job(conn, d, spec, reset_running=reset_running)
                if result == "reset":
                    jobs_reset += 1
                elif result == "enqueued":
                    jobs_enqueued += 1
                elif result == "skipped_running":
                    skipped_running += 1

    return {
        "status": "DONE",
        "targets": len(targets),
        "invalidated_targets": invalidated_targets,
        "quarantined_artifacts": quarantined,
        "missing_artifacts": missing_artifacts,
        "jobs_reset": jobs_reset,
        "jobs_enqueued": jobs_enqueued,
        "skipped_running": skipped_running,
        "skipped_no_matching_specs": skipped_specs,
        "repair_run_id": repair_run_id,
        "dry_run": dry_run,
    }


def _lookup_ingest_mode(engine, rcept_no: str) -> str | None:
    """complete 마커에 기록할 ingest_mode를 best-effort로 조회한다(collect_job, 없으면 None)."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT ingest_mode FROM collect_job"
                " WHERE rcept_no = :rcept_no AND ingest_mode IS NOT NULL LIMIT 1"
            ),
            {"rcept_no": rcept_no},
        ).first()
    return row.ingest_mode if row else None


def run_detail_backfill(batch_size: int = 1000) -> dict[str, Any]:
    """기존 disclosure 전체에 대해 detail_jobs_for_disclosure가 정한 모든 상세 job을 소급 enqueue한다.

    collect_mode=both 전환(원문 추가)·EVENT bgn_de/end_de 누락 버그 수정 후, 빠지거나 잘못 생성된
    job을 채운다. request_hash dedup으로 기존 정상 job과 중복되지 않으며, EVENT(올바른 bgn_de/end_de)·
    document 등 누락분만 새로 들어간다. 대량 트랜잭션을 피하려고 batch_size 단위로 커밋한다.
    """
    context = build_context()
    engine = context.service_engine
    collect_mode = context.settings.dart_collect_mode
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT rcept_no, corp_code, stock_code, report_nm, rcept_dt, pblntf_ty, pblntf_detail_ty "
                "FROM disclosure ORDER BY rcept_dt ASC, rcept_no ASC"
            )
        ).mappings().all()

    enqueued = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        with engine.begin() as conn:
            for row in chunk:
                report_nm = str(row["report_nm"] or "")
                rcept_dt = str(row["rcept_dt"]).replace("-", "")[:8]
                for spec in detail_jobs_for_disclosure(
                    report_nm,
                    rcept_dt,
                    pblntf_ty=row["pblntf_ty"],
                    pblntf_detail_ty=row["pblntf_detail_ty"],
                    collect_mode=collect_mode,
                ):
                    job_id = enqueue_collect_job(
                        conn,
                        priority=spec.priority,
                        job_type=spec.job_type,
                        api_group=spec.api_group,
                        api_name=spec.api_name,
                        rcept_no=str(row["rcept_no"]) if spec.rcept_scoped else None,
                        corp_code=str(row["corp_code"]),
                        stock_code=row["stock_code"] or None,
                        bsns_year=spec.bsns_year,
                        reprt_code=spec.reprt_code,
                        bgn_de=spec.bgn_de,
                        end_de=spec.end_de,
                        ingest_mode="backfill_resync",
                    )
                    if job_id is not None:
                        enqueued += 1
    return {"total": len(rows), "enqueued": enqueued}


def _replay_bronze_list_window(
    context: AppContext,
    storage,
    window: DateWindow,
    *,
    ingest_run: IngestRun,
    batch_size: int = 500,
) -> dict[str, Any]:
    """이미 S3에 있는 Bronze list 파일을 discovery 입력으로 replay한다.

    `backfill_discovery`가 같은 기간을 다시 돌 때, 기존 list/header 파일이 있으면 OpenDART
    list API를 다시 호출하지 않고 이 파일을 authoritative input으로 쓴다. DB는 이 입력에서
    재생성되는 실행 큐일 뿐이다.
    """
    engine = context.service_engine
    mode = context.settings.dart_collect_mode
    counts = {
        "list_files": 0,
        "missing_list_dates": 0,
        "malformed_list_files": 0,
        "disclosures": 0,
        "expected_jobs": 0,
        "already_present": 0,
        "enqueued": 0,
        "reset": 0,
        "existing_active": 0,
        "existing_done_or_failed_without_reset": 0,
        "missing_jobs": 0,
        "invalidated_markers": 0,
    }

    pending_invalidations: set[tuple[str, str, str]] = set()
    seen_rcept_nos: set[str] = set()
    current = window.start
    while current <= window.end:
        rcept_dt = current.strftime("%Y%m%d")
        date_had_keys = False
        for corp_cls in ("Y", "K"):
            prefix = BronzePaths.disclosure_list_prefix(corp_cls=corp_cls, rcept_dt=rcept_dt)
            keys = sorted(storage.list_keys(prefix))
            if not keys:
                continue
            date_had_keys = True
            for key in keys:
                counts["list_files"] += 1
                try:
                    payload = json.loads(storage.read_bytes(key).decode("utf-8"))
                    rows = payload.get("list") or []
                    if not isinstance(rows, list):
                        raise ValueError("payload.list is not a list")
                except Exception:  # noqa: BLE001 - bad list file should not stop the whole repair.
                    counts["malformed_list_files"] += 1
                    continue

                for i in range(0, len(rows), max(1, batch_size)):
                    chunk = rows[i:i + max(1, batch_size)]
                    with engine.begin() as conn:
                        raw_ref_id = _lookup_raw_ref_id(conn, key)
                        for raw_item in chunk:
                            item = dict(raw_item)
                            item.setdefault("corp_cls", corp_cls)
                            if str(item.get("rcept_dt") or "") != rcept_dt:
                                continue
                            rcept_no = str(item["rcept_no"])
                            if rcept_no in seen_rcept_nos:
                                continue
                            seen_rcept_nos.add(rcept_no)
                            upsert_disclosure(conn, item, raw_ref_id=raw_ref_id)
                            counts["disclosures"] += 1
                            report_nm = str(item["report_nm"] or "")
                            corp_code = str(item["corp_code"])
                            report_type = report_type_for(
                                report_nm,
                                item.get("pblntf_ty"),
                                item.get("pblntf_detail_ty"),
                            )
                            touched = _resync_detail_jobs_for_item(
                                conn,
                                storage,
                                item,
                                collect_mode=mode,
                                reset_existing=True,
                                reset_running=False,
                                ingest_mode=ingest_run.mode,
                                counts=counts,
                            )
                            if touched:
                                pending_invalidations.add((corp_code, rcept_no, report_type))
        if not date_had_keys:
            counts["missing_list_dates"] += 1
        current += timedelta(days=1)

    for corp_code, rcept_no, report_type in sorted(pending_invalidations):
        if _invalidate_completion_for_repair(storage, corp_code, rcept_no, report_type):
            counts["invalidated_markers"] += 1

    return {
        "disclosures": counts["disclosures"],
        "jobs": counts["enqueued"] + counts["reset"],
        "replayed_from_bronze_list": counts["list_files"] > 0,
        "rcept_dt_from": window.start.isoformat(),
        "rcept_dt_to": window.end.isoformat(),
        "collect_mode": mode,
        "unique_rcept_nos": len(seen_rcept_nos),
        **counts,
    }


def _lookup_raw_ref_id(conn, object_path: str) -> int | None:
    row = conn.execute(
        text("SELECT id FROM raw_file_reference WHERE object_path = :object_path LIMIT 1"),
        {"object_path": object_path},
    ).first()
    return int(row.id) if row else None


def _detail_specs_for_item(item: dict[str, Any], collect_mode: str) -> list[DetailJobSpec]:
    return detail_jobs_for_disclosure(
        str(item["report_nm"] or ""),
        str(item["rcept_dt"]),
        pblntf_ty=item.get("pblntf_ty"),
        pblntf_detail_ty=item.get("pblntf_detail_ty"),
        collect_mode=collect_mode,
    )


def _resync_detail_jobs_for_item(
    conn,
    storage,
    item: dict[str, Any],
    *,
    collect_mode: str,
    reset_existing: bool,
    reset_running: bool,
    ingest_mode: str,
    counts: dict[str, int],
) -> bool:
    touched = False
    for spec in _detail_specs_for_item(item, collect_mode):
        counts["expected_jobs"] += 1
        if _spec_artifacts_present(storage, item, spec):
            counts["already_present"] += 1
            continue

        counts["missing_jobs"] += 1
        job_kwargs = _job_kwargs_for_spec(item, spec, ingest_mode=ingest_mode)
        job_id = enqueue_collect_job(conn, **job_kwargs)
        if job_id is not None:
            counts["enqueued"] += 1
            touched = True
            continue

        existing = _lookup_collect_job_by_request_hash(conn, _collect_job_hash(job_kwargs))
        if existing is None:
            continue
        status = str(existing["status"])
        if status == "PENDING" or (status == "RUNNING" and not reset_running):
            counts["existing_active"] += 1
            continue
        if not reset_existing and status in {"DONE", "FAILED"}:
            counts["existing_done_or_failed_without_reset"] += 1
            continue
        _reset_existing_collect_job_for_resync(
            conn,
            int(existing["id"]),
            ingest_mode=ingest_mode,
            allow_running=reset_running,
        )
        counts["reset"] += 1
        touched = True
    return touched


def _job_kwargs_for_spec(item: dict[str, Any], spec: DetailJobSpec, *, ingest_mode: str) -> dict[str, Any]:
    return {
        "priority": spec.priority,
        "job_type": spec.job_type,
        "api_group": spec.api_group,
        "api_name": spec.api_name,
        "rcept_no": str(item["rcept_no"]) if spec.rcept_scoped else None,
        "corp_code": str(item["corp_code"]),
        "stock_code": item.get("stock_code") or None,
        "bsns_year": spec.bsns_year,
        "reprt_code": spec.reprt_code,
        "bgn_de": spec.bgn_de,
        "end_de": spec.end_de,
        "ingest_mode": ingest_mode,
    }


def _collect_job_hash(job_kwargs: dict[str, Any]) -> str:
    params = {
        "job_type": job_kwargs["job_type"],
        "api_group": job_kwargs["api_group"],
        "api_name": job_kwargs["api_name"],
        "rcept_no": job_kwargs["rcept_no"],
        "corp_code": job_kwargs["corp_code"],
        "stock_code": job_kwargs["stock_code"],
        "bsns_year": job_kwargs["bsns_year"],
        "reprt_code": job_kwargs["reprt_code"],
        "bgn_de": job_kwargs["bgn_de"],
        "end_de": job_kwargs["end_de"],
    }
    return request_hash("DART", job_kwargs["job_type"], params)


def _lookup_collect_job_by_request_hash(conn, hashed: str):
    return conn.execute(
        text("SELECT id, status FROM collect_job WHERE request_hash = :request_hash"),
        {"request_hash": hashed},
    ).mappings().first()


def _reset_existing_collect_job_for_resync(
    conn,
    job_id: int,
    *,
    ingest_mode: str,
    allow_running: bool,
) -> None:
    status_filter = "" if allow_running else "AND status <> 'RUNNING'"
    conn.execute(
        text(
            f"""
            UPDATE collect_job
            SET status = 'PENDING',
                ingest_mode = :ingest_mode,
                retry_count = 0,
                scheduled_at = CURRENT_TIMESTAMP,
                started_at = NULL,
                finished_at = NULL,
                error_code = NULL,
                error_message = NULL
            WHERE id = :job_id
              {status_filter}
            """
        ),
        {"job_id": job_id, "ingest_mode": ingest_mode},
    )


def _spec_artifacts_present(storage, item: dict[str, Any], spec: DetailJobSpec) -> bool:
    paths = _artifact_paths_for_spec(item, spec)
    return bool(paths) and all(storage.exists(path) for path in paths)


def _artifact_paths_for_spec(item: dict[str, Any], spec: DetailJobSpec) -> list[str]:
    corp_code = str(item["corp_code"])
    rcept_no = str(item["rcept_no"])
    if spec.job_type == "DISCLOSURE_DOCUMENT":
        return [BronzePaths.document(rcept_no)]
    if spec.job_type == "FINANCIAL_STATEMENT_ALL":
        return [
            BronzePaths.financial_statement(corp_code, spec.bsns_year, spec.reprt_code, fs_div)
            for fs_div in ("CFS", "OFS")
            if spec.bsns_year and spec.reprt_code
        ]
    if spec.job_type == "STRUCTURED_REPORT":
        return [
            BronzePaths.structured_report(
                spec.api_group,
                spec.api_name,
                corp_code,
                bsns_year=spec.bsns_year,
                reprt_code=spec.reprt_code,
            )
        ]
    if spec.job_type == "EVENT_REPORT":
        return [
            BronzePaths.event_report(
                spec.api_group,
                spec.api_name,
                corp_code,
                spec.bgn_de,
                spec.end_de,
            )
        ] if spec.bgn_de and spec.end_de else []
    if spec.job_type == "SECURITIES_REPORT":
        return [
            BronzePaths.securities_report(
                spec.api_group,
                spec.api_name,
                corp_code,
                spec.bgn_de,
                spec.end_de,
            )
        ] if spec.bgn_de and spec.end_de else []
    return []


def _invalidate_completion_for_repair(storage, corp_code: str, rcept_no: str, report_type: str) -> bool:
    from dart_agent.silver.transform import SILVER_VERSION

    marker_path = BronzePaths.complete_marker(corp_code, rcept_no)
    marker = None
    if storage.exists(marker_path):
        try:
            marker = json.loads(storage.read_bytes(marker_path).decode("utf-8"))
        except Exception:  # noqa: BLE001 - unreadable marker is still stale and should be deleted.
            marker = None
    existed = (
        storage.exists(marker_path)
        or storage.exists(SilverPaths.done_marker(corp_code, rcept_no))
        or storage.exists(SilverPaths.done_marker_for_version(SILVER_VERSION, corp_code, rcept_no))
        or storage.exists(SilverPaths.report(corp_code, report_type or "DISCLOSURE", rcept_no))
    )
    _delete_stale_silver_outputs(storage, corp_code, rcept_no, marker)
    storage.delete(SilverPaths.report(corp_code, report_type or "DISCLOSURE", rcept_no))
    return existed


def run_collect_company_overviews() -> dict[str, Any]:
    """dag_dart_company_overview 진입점.

    역할: 상장사 전체에 대해 DS001 /api/company.json 기업개황 수집 job을 enqueue한다.
          COMPANY_OVERVIEW job이 이미 있으면 request_hash dedup으로 중복 생성되지 않는다.
    기준: 전체 listed_company를 순회해 corp_code마다 enqueue. 실제 API 호출은 run_detail_collector에서.
    """
    context = build_context()
    engine = context.service_engine

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT corp_code, stock_code FROM listed_company"
                " WHERE status = 'ACTIVE' AND market_type IN ('KOSPI', 'KOSDAQ')"
            )
        ).mappings().all()

    enqueued = 0
    with engine.begin() as conn:
        for row in rows:
            job_id = enqueue_collect_job(
                conn,
                priority=10,
                job_type="COMPANY_OVERVIEW",
                api_group="DS001",
                api_name="company",
                corp_code=row["corp_code"],
                stock_code=row["stock_code"],
            )
            if job_id is not None:
                enqueued += 1

    return {"total": len(rows), "enqueued": enqueued}


def _dart_date_arg(name: str, value: str | None) -> str:
    parsed = parse_optional_date(name, value)
    if parsed is None:
        raise ValueError(f"{name} is required")
    return parsed.strftime("%Y%m%d")


def run_material_event_report_backfill(
    bgn_de: str | None = None,
    end_de: str | None = None,
    *,
    market_types: tuple[str, ...] | None = None,
    limit_companies: int | None = None,
) -> dict[str, Any]:
    """전체 ACTIVE 상장사에 대해 지정 기간 DS005 EVENT_REPORT job을 enqueue한다.

    OpenDART DS005는 rcept_no가 아니라 corp_code+bgn_de+end_de로 조회한다. 따라서
    listed_company의 회사 고유번호를 기준으로 DS005 36개 API를 기간 단위로 수집하고,
    Silver에서 응답 row의 rcept_no로 공시 단위 report.json에 재분리한다.
    """
    start = _dart_date_arg("bgn_de", bgn_de)
    end = _dart_date_arg("end_de", end_de)
    if start > end:
        raise ValueError("bgn_de must be <= end_de")

    context = build_context()
    params: dict[str, Any] = {}
    market_filter_sql = ""
    if market_types:
        market_filter_sql = " AND market_type IN :market_types"
        params["market_types"] = tuple(market_types)
    limit_sql = ""
    if limit_companies is not None:
        limit_sql = " LIMIT :limit_companies"
        params["limit_companies"] = max(1, int(limit_companies))

    stmt = text(
        """
        SELECT corp_code, stock_code
        FROM listed_company
        WHERE status = 'ACTIVE'
        """ + market_filter_sql + """
        ORDER BY corp_code ASC
        """ + limit_sql
    )
    if market_types:
        stmt = stmt.bindparams(bindparam("market_types", expanding=True))

    with context.service_engine.connect() as conn:
        rows = conn.execute(stmt, params).mappings().all()

    enqueued = 0
    skipped = 0
    with context.service_engine.begin() as conn:
        for row in rows:
            for api_name in MATERIAL_EVENT_API_NAMES:
                spec = REPORT_APIS[api_name]
                job_id = enqueue_collect_job(
                    conn,
                    priority=35,
                    job_type="EVENT_REPORT",
                    api_group=spec.api_group,
                    api_name=spec.api_name,
                    corp_code=str(row["corp_code"]),
                    stock_code=row["stock_code"] or None,
                    bgn_de=start,
                    end_de=end,
                    ingest_mode="material_event_period",
                )
                if job_id is None:
                    skipped += 1
                else:
                    enqueued += 1

    return {
        "status": "DONE",
        "bgn_de": start,
        "end_de": end,
        "market_types": list(market_types) if market_types else ["ALL_ACTIVE"],
        "companies": len(rows),
        "apis": list(MATERIAL_EVENT_API_NAMES),
        "candidate_jobs": len(rows) * len(MATERIAL_EVENT_API_NAMES),
        "enqueued": enqueued,
        "skipped": skipped,
    }


def run_api_gap_filler() -> dict[str, Any]:
    """dag_dart_api_gap_filler 진입점.

    역할: REGULAR_REPORT_API_NAMES에 새로 추가된 DS002 API 항목에 대해,
          기존에 이미 discovery된 REGULAR 공시의 collect_job을 소급 생성한다.
    기준:
      - FINANCIAL_STATEMENT_ALL job 목록에서 unique (corp_code, stock_code, bsns_year, reprt_code) 추출.
      - 현재 REGULAR_REPORT_API_NAMES에 있지만 STRUCTURED_REPORT job에 없는 api_name(gap)을 탐지.
      - gap API × combo 조합마다 enqueue_collect_job 호출(ON CONFLICT DO NOTHING으로 멱등 보장).
    실행: 수동 트리거만. 반복 실행 시 중복 없음(request_hash dedup).
    """
    context = build_context()
    engine = context.service_engine

    # 현재 코드 기준 필요한 DS002 API 집합
    needed: set[str] = set(REGULAR_REPORT_API_NAMES)

    # DB에 이미 enqueue된 DS002 STRUCTURED_REPORT api_name 집합
    with engine.connect() as conn:
        existing_rows = conn.execute(
            text("""
                SELECT DISTINCT api_name
                FROM collect_job
                WHERE job_type = 'STRUCTURED_REPORT' AND api_group = 'DS002'
            """)
        ).fetchall()
    existing: set[str] = {r[0] for r in existing_rows}

    gap_apis: set[str] = needed - existing
    if not gap_apis:
        return {"status": "NO_GAP", "enqueued": 0, "gap_apis": []}

    # FINANCIAL_STATEMENT_ALL job에서 unique (corp_code, stock_code, bsns_year, reprt_code) 추출
    with engine.connect() as conn:
        combo_rows = conn.execute(
            text("""
                SELECT DISTINCT corp_code, stock_code, bsns_year, reprt_code
                FROM collect_job
                WHERE job_type = 'FINANCIAL_STATEMENT_ALL'
                  AND bsns_year IS NOT NULL
                  AND reprt_code IS NOT NULL
            """)
        ).fetchall()

    enqueued = 0
    skipped = 0
    for row in combo_rows:
        for api_name in sorted(gap_apis):
            spec = REPORT_APIS[api_name]
            with engine.begin() as conn:
                result = enqueue_collect_job(
                    conn,
                    priority=30,
                    job_type="STRUCTURED_REPORT",
                    api_group=spec.api_group,
                    api_name=spec.api_name,
                    corp_code=row.corp_code,
                    stock_code=row.stock_code,
                    bsns_year=row.bsns_year,
                    reprt_code=row.reprt_code,
                )
                if result is not None:
                    enqueued += 1
                else:
                    skipped += 1

    return {
        "status": "DONE",
        "gap_apis": sorted(gap_apis),
        "combos": len(combo_rows),
        "enqueued": enqueued,
        "skipped": skipped,
    }


def run_requeue_quota_failed_jobs() -> dict[str, Any]:
    """quota 부족으로 FAILED 처리된 collect_job을 잔여 quota가 있을 때 PENDING으로 복구한다."""
    context = build_context()
    remaining_by_key = {
        key.identifier: context.rate_limiter.daily_quota_remaining(api_key_identifier=key.identifier)
        for key in context.api_keys
    }
    total_remaining = sum(remaining_by_key.values())
    if total_remaining <= 0:
        return {
            "status": "SKIPPED_NO_QUOTA",
            "requeued": 0,
            "remaining_by_key": remaining_by_key,
        }

    with context.service_engine.begin() as conn:
        result = conn.execute(
            text(
                """
                UPDATE collect_job
                SET status = 'PENDING',
                    started_at = NULL,
                    finished_at = NULL,
                    error_code = NULL,
                    error_message = NULL,
                    scheduled_at = CURRENT_TIMESTAMP
                WHERE status = 'FAILED'
                  AND (
                    error_code = 'RateLimitExceeded'
                    OR error_message LIKE '%020 사용한도%'
                    OR error_message ILIKE '%quota%'
                    OR error_message ILIKE '%rate limit%'
                  )
                """
            )
        )
        requeued = int(result.rowcount or 0)

    return {
        "status": "DONE",
        "requeued": requeued,
        "remaining_by_key": remaining_by_key,
    }


def run_securities_report_gap_filler() -> dict[str, Any]:
    """기존 증권신고서 공시에 대해 DS006 SECURITIES_REPORT job을 소급 생성한다."""
    context = build_context()
    rows = []
    with context.service_engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT DISTINCT rcept_no, corp_code, stock_code, rcept_dt
                FROM disclosure
                WHERE report_type = 'SECURITIES_REGISTRATION'
                ORDER BY rcept_dt ASC, rcept_no ASC
                """
            )
        ).mappings().all()

    enqueued = 0
    skipped = 0
    with context.service_engine.begin() as conn:
        for row in rows:
            rcept_dt = str(row["rcept_dt"]).replace("-", "")[:8]
            for api_name in SECURITIES_REPORT_API_NAMES:
                spec = REPORT_APIS[api_name]
                job_id = enqueue_collect_job(
                    conn,
                    priority=33,
                    job_type="SECURITIES_REPORT",
                    api_group=spec.api_group,
                    api_name=spec.api_name,
                    corp_code=str(row["corp_code"]),
                    stock_code=row["stock_code"] or None,
                    bgn_de=rcept_dt,
                    end_de=rcept_dt,
                    ingest_mode="securities_gap",
                )
                if job_id is None:
                    skipped += 1
                else:
                    enqueued += 1

    return {
        "status": "DONE",
        "securities_disclosures": len(rows),
        "apis": list(SECURITIES_REPORT_API_NAMES),
        "enqueued": enqueued,
        "skipped": skipped,
    }


def run_rag_builder() -> dict[str, str]:
    """dag_dart_rag_builder 진입점(placeholder).

    역할: Silver 문서를 RAG document/chunk로 만들고 embedding/vector upsert할 자리. 현재는 점검 시각만 기록.
    기준: Silver 텍스트 준비가 선행 조건이며 로직은 미구현.
    """
    context = build_context()
    with context.service_engine.begin() as conn:
        set_state(conn, "rag_builder_last_checked_at", datetime.now(timezone.utc).isoformat())
    return {"status": "PLACEHOLDER", "reason": "RAG builder depends on Silver document text"}


def run_requeue_transport_failed_jobs() -> dict[str, Any]:
    """TRANSPORT(네트워크)/5xx로 FAILED 처리된 collect_job을 PENDING으로 복구한다.

    quota와 무관한 일시 장애이므로 quota 잔량 게이트 없이 복구한다. job 잘못이 아닌 실패만
    되살리되, retry_count가 _MAX_TRANSPORT_RETRY 이상인 job은 무한 재시도를 막기 위해 제외한다.
    (정상 경로에서는 collector가 reschedule로 처리하므로, 이 함수는 과거에 영구 FAILED로 박힌
    잔여분과 _MAX_TRANSPORT_RETRY 도달 전 실패분의 안전망이다.)
    """
    context = build_context()
    with context.service_engine.begin() as conn:
        result = conn.execute(
            text(
                """
                UPDATE collect_job
                SET status = 'PENDING',
                    started_at = NULL,
                    finished_at = NULL,
                    error_code = NULL,
                    error_message = NULL,
                    scheduled_at = CURRENT_TIMESTAMP
                WHERE status = 'FAILED'
                  AND retry_count < :max_retry
                  AND (
                    error_message ILIKE '%TRANSPORT%'
                    OR error_message ILIKE '%timeout%'
                    OR error_message ILIKE '%HTTP_5%'
                  )
                """
            ),
            {"max_retry": _MAX_TRANSPORT_RETRY},
        )
        requeued = int(result.rowcount or 0)
    return {"status": "DONE", "requeued": requeued}


def run_reconciliation() -> dict[str, Any]:
    """dag_dart_reconciliation 진입점.

    역할: 누락/정정/실패 작업 보정. 현재는 TRANSPORT(네트워크)/5xx로 FAILED된 collect_job을
          PENDING으로 복구해 자동 재수집되게 한다(나머지 보정 로직은 미구현).
    기준: 추가 보정 설계는 docs/operations/hardening-plan.md 참고.
    """
    context = build_context()
    requeue = run_requeue_transport_failed_jobs()
    with context.service_engine.begin() as conn:
        set_state(conn, "reconciliation_last_checked_at", datetime.now(timezone.utc).isoformat())
    return {"status": "DONE", "transport_requeued": requeue["requeued"]}


def _discover_window(
    context: AppContext,
    window: DateWindow,
    sort_mth: str,
    ingest_run: IngestRun,
) -> dict[str, Any]:
    """한 기간(window)의 공시검색을 수행하는 백필/증분 공통 루틴.

    역할: corp_cls Y/K 각각에 대해 list.json을 page_count=100으로 끝까지 페이지네이션하고,
          응답 row를 실제 접수일(rcept_dt)별 Bronze 파일로 저장한 뒤 disclosure upsert,
          DART_COLLECT_MODE 기준 상세 collect_job enqueue.
    기준: last_reprt_at=N(정정 포함). sort_mth는 백필=asc, 증분=desc로 호출자가 정한다.
          구조화 job은 rcept_scoped=False면 rcept_no 없이 corp_code(+기간) 기준으로 dedup된다.
    반환: 처리한 disclosure/job 건수.
    """
    storage = build_storage(context.settings)
    if ingest_run.mode == "backfill":
        replayed = _replay_bronze_list_window(context, storage, window, ingest_run=ingest_run)
        if replayed["list_files"] > 0:
            return replayed

    disclosure_count = 0
    job_count = 0
    for corp_cls in ("Y", "K"):
        daily_page_no_by_date: dict[tuple[str, str], int] = {}
        page_no = 1
        while True:
            params = {
                **window.as_dart_params(),
                "corp_cls": corp_cls,
                "page_no": str(page_no),
                "page_count": "100",
                "sort": "date",
                "sort_mth": sort_mth,
                "last_reprt_at": "N",
            }
            try:
                response = context.client.list_disclosures(
                    bgn_de=params["bgn_de"],
                    end_de=params["end_de"],
                    corp_cls=corp_cls,
                    page_no=page_no,
                    page_count=100,
                    sort="date",
                    sort_mth=sort_mth,
                    last_reprt_at="N",
                )
            except OpenDartNoData:
                break

            payload = response.payload
            rows = payload.get("list") or []
            stored_daily_pages = []
            for rcept_dt, daily_rows in _group_disclosure_rows_by_rcept_dt(rows):
                counter_key = (corp_cls, rcept_dt)
                daily_page_no_by_date[counter_key] = daily_page_no_by_date.get(counter_key, 0) + 1
                rcept_date = _parse_dart_yyyymmdd(rcept_dt)
                daily_payload = _daily_disclosure_payload(
                    payload=payload,
                    rows=daily_rows,
                    source_params=params,
                    source_page_no=page_no,
                    ingest_run=ingest_run,
                )
                storage_path = BronzePaths.disclosure_list(
                    corp_cls,
                    DateWindow(start=rcept_date, end=rcept_date),
                    daily_page_no_by_date[counter_key],
                    ingest_mode=ingest_run.mode,
                    run_id=ingest_run.run_id,
                )
                stored = storage.write_bytes(
                    storage_path,
                    json.dumps(daily_payload, ensure_ascii=False, sort_keys=True).encode("utf-8"),
                    content_type="application/json",
                )
                stored_daily_pages.append((stored, daily_rows))
            with context.service_engine.begin() as conn:
                _log_api_request(
                    context,
                    conn,
                    api_group="DS001",
                    api_name="list",
                    quota_type="MINUTE_ONLY",
                    request_params=params,
                    status_code=response.status,
                    http_status=response.http_status,
                    response_ms=response.elapsed_ms,
                )
                for stored, daily_rows in stored_daily_pages:
                    raw_ref_id = upsert_raw_file_reference(conn, stored)
                    for item in daily_rows:
                        upsert_disclosure(conn, item, raw_ref_id=raw_ref_id)
                        disclosure_count += 1
                        for spec in detail_jobs_for_disclosure(
                            str(item["report_nm"]),
                            str(item["rcept_dt"]),
                            pblntf_ty=item.get("pblntf_ty"),
                            pblntf_detail_ty=item.get("pblntf_detail_ty"),
                            collect_mode=context.settings.dart_collect_mode,
                        ):
                            job_id = enqueue_collect_job(
                                conn,
                                priority=spec.priority,
                                job_type=spec.job_type,
                                api_group=spec.api_group,
                                api_name=spec.api_name,
                                rcept_no=str(item["rcept_no"]) if spec.rcept_scoped else None,
                                corp_code=str(item["corp_code"]),
                                stock_code=item.get("stock_code") or None,
                                bsns_year=spec.bsns_year,
                                reprt_code=spec.reprt_code,
                                bgn_de=spec.bgn_de,
                                end_de=spec.end_de,
                                ingest_mode=ingest_run.mode,
                            )
                            if job_id is not None:
                                job_count += 1
            total_page = int(payload.get("total_page") or 1)
            if page_no >= total_page:
                break
            page_no += 1
    return {"disclosures": disclosure_count, "jobs": job_count}


def _group_disclosure_rows_by_rcept_dt(
    rows: list[dict[str, Any]],
) -> list[tuple[str, list[dict[str, Any]]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in rows:
        rcept_dt = str(item["rcept_dt"])
        grouped.setdefault(rcept_dt, []).append(item)
    return list(grouped.items())


def _daily_disclosure_payload(
    *,
    payload: dict[str, Any],
    rows: list[dict[str, Any]],
    source_params: dict[str, str],
    source_page_no: int,
    ingest_run: IngestRun,
) -> dict[str, Any]:
    daily_payload = dict(payload)
    daily_payload["list"] = rows
    daily_payload["_ingest"] = {
        "ingest_mode": ingest_run.mode,
        "collected_date": ingest_run.collected_date,
        "run_id": ingest_run.run_id,
        "source_page_no": source_page_no,
        "source_bgn_de": source_params["bgn_de"],
        "source_end_de": source_params["end_de"],
    }
    return daily_payload


def _parse_dart_yyyymmdd(value: str) -> date:
    if len(value) != 8 or not value.isdigit():
        raise ValueError(f"invalid DART date: {value}")
    return date(int(value[0:4]), int(value[4:6]), int(value[6:8]))


def _write_nodata_marker(storage, path: str, message: str) -> None:
    """013(NoData)을 Bronze 경로에 마커로 남긴다(파일 존재 = 처리 완료).

    정상 응답과 같은 경로에 두되 _nodata=True, list=[]로 둬서 Silver는 빈 데이터로
    자연 처리하고, completion 단계는 파일 존재만으로 '수집 시도 완료'를 인식한다
    ('미수집'과 '데이터 없음'을 S3에서 구분 가능하게 하는 핵심).
    """
    marker = {
        "status": "013",
        "message": message,
        "list": [],
        "_nodata": True,
        "_collected_at": datetime.now(timezone.utc).isoformat(),
    }
    storage.write_bytes(
        path,
        json.dumps(marker, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        content_type="application/json",
    )


def _run_collect_job(context: AppContext, job: CollectJob, storage) -> None:
    """job_type에 맞는 collector로 분기하는 dispatcher.

    기준: 미지원 job_type은 NotImplementedError를 던져 호출자(run_detail_collector)가 FAILED로 남긴다.
          storage는 batch 단위로 1개를 만들어 받아 쓴다(boto3 client 재생성 방지).
    """
    if job.job_type == "DISCLOSURE_DOCUMENT":
        _collect_document(context, job, storage)
        return
    if job.job_type == "FINANCIAL_STATEMENT_ALL":
        _collect_financial_statement_all(context, job, storage)
        return
    if job.job_type == "STRUCTURED_REPORT":
        _collect_structured_report(context, job, storage)
        return
    if job.job_type == "EVENT_REPORT":
        _collect_event_report(context, job, storage)
        return
    if job.job_type == "SECURITIES_REPORT":
        _collect_securities_report(context, job, storage)
        return
    if job.job_type == "COMPANY_OVERVIEW":
        _collect_company_overview(context, job, storage)
        return
    raise NotImplementedError(f"{job.job_type} collector is not implemented yet")


def _collect_structured_report(context: AppContext, job: CollectJob, storage) -> None:
    """DS002~DS006 구조화 API를 호출해 list row를 structured_report_item에 적재한다."""
    if not job.api_name or job.api_name not in REPORT_APIS:
        raise ValueError(f"unknown structured api_name: {job.api_name}")
    if not job.corp_code:
        raise ValueError("corp_code is required")
    spec = REPORT_APIS[job.api_name]
    if storage.exists(BronzePaths.structured_report(
        spec.api_group, spec.api_name, job.corp_code,
        bsns_year=job.bsns_year, reprt_code=job.reprt_code,
    )):
        return  # 이미 수집(정상/nodata) — S3 멱등으로 API 호출을 건너뛴다(중복 호출 방지).
    params = build_report_params(
        spec,
        corp_code=job.corp_code,
        bsns_year=job.bsns_year,
        reprt_code=job.reprt_code,
    )
    request_params = dict(params)
    try:
        response = context.client.report_json(
            api_name=spec.api_name,
            endpoint=spec.endpoint,
            params=params,
        )
    except OpenDartNoData as exc:
        # 데이터 없음(013)도 처리 완료다 — Bronze 경로에 nodata 마커를 남긴다.
        _write_nodata_marker(
            storage,
            BronzePaths.structured_report(
                spec.api_group, spec.api_name, job.corp_code,
                bsns_year=job.bsns_year, reprt_code=job.reprt_code,
            ),
            exc.message,
        )
        with context.service_engine.begin() as conn:
            _log_api_request(
                context,
                conn,
                api_group=spec.api_group,
                api_name=spec.api_name,
                quota_type="DAILY_AND_MINUTE",
                request_params=request_params,
                status_code=exc.status,
                http_status=None,
                response_ms=None,
                error_message=exc.message,
            )
        return
    except OpenDartError as exc:
        with context.service_engine.begin() as conn:
            _log_api_request(
                context,
                conn,
                api_group=spec.api_group,
                api_name=spec.api_name,
                quota_type="DAILY_AND_MINUTE",
                request_params=request_params,
                status_code=exc.status,
                http_status=None,
                response_ms=None,
                error_message=exc.message,
            )
        raise
    except Exception as exc:
        with context.service_engine.begin() as conn:
            _log_api_request(
                context,
                conn,
                api_group=spec.api_group,
                api_name=spec.api_name,
                quota_type="DAILY_AND_MINUTE",
                request_params=request_params,
                status_code=None,
                http_status=None,
                response_ms=None,
                error_message=str(exc),
            )
        raise

    payload = filter_ownership_payload(spec.api_name, response.payload)
    storage_path = BronzePaths.structured_report(
        spec.api_group,
        spec.api_name,
        job.corp_code,
        bsns_year=job.bsns_year,
        reprt_code=job.reprt_code,
    )
    stored = storage.write_bytes(
        storage_path,
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        content_type="application/json",
    )
    with context.service_engine.begin() as conn:
        upsert_raw_file_reference(conn, stored)
        _log_api_request(
            context,
            conn,
            api_group=spec.api_group,
            api_name=spec.api_name,
            quota_type="DAILY_AND_MINUTE",
            request_params=request_params,
            status_code=response.status,
            http_status=response.http_status,
            response_ms=response.elapsed_ms,
        )


def _collect_event_report(context: AppContext, job: CollectJob, storage) -> None:
    """EVENT_REPORT job 처리: DS005 주요사항보고서 API를 호출해 Bronze에 저장한다."""
    if not job.api_name or job.api_name not in REPORT_APIS:
        raise ValueError(f"unknown event_report api_name: {job.api_name}")
    if not job.corp_code:
        raise ValueError("corp_code is required for EVENT_REPORT")
    if not job.bgn_de or not job.end_de:
        raise ValueError("bgn_de and end_de are required for EVENT_REPORT")
    spec = REPORT_APIS[job.api_name]
    if storage.exists(BronzePaths.event_report(spec.api_group, spec.api_name, job.corp_code, job.bgn_de, job.end_de)):
        return  # 이미 수집(정상/nodata) — S3 멱등으로 API 호출을 건너뛴다(중복 호출 방지).
    params = build_report_params(
        spec,
        corp_code=job.corp_code,
        bgn_de=job.bgn_de,
        end_de=job.end_de,
    )
    request_params = dict(params)
    try:
        response = context.client.report_json(
            api_name=spec.api_name,
            endpoint=spec.endpoint,
            params=params,
        )
    except OpenDartNoData as exc:
        _write_nodata_marker(
            storage,
            BronzePaths.event_report(spec.api_group, spec.api_name, job.corp_code, job.bgn_de, job.end_de),
            exc.message,
        )
        with context.service_engine.begin() as conn:
            _log_api_request(
                context, conn,
                api_group=spec.api_group, api_name=spec.api_name,
                quota_type="DAILY_AND_MINUTE", request_params=request_params,
                status_code=exc.status, http_status=None, response_ms=None,
                error_message=exc.message,
            )
        return
    except OpenDartError as exc:
        with context.service_engine.begin() as conn:
            _log_api_request(
                context, conn,
                api_group=spec.api_group, api_name=spec.api_name,
                quota_type="DAILY_AND_MINUTE", request_params=request_params,
                status_code=exc.status, http_status=None, response_ms=None,
                error_message=str(exc),
            )
        raise

    payload = response.payload
    storage_path = BronzePaths.event_report(
        spec.api_group, spec.api_name, job.corp_code, job.bgn_de, job.end_de,
    )
    stored = storage.write_bytes(
        storage_path,
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        content_type="application/json",
    )
    with context.service_engine.begin() as conn:
        upsert_raw_file_reference(conn, stored)
        _log_api_request(
            context, conn,
            api_group=spec.api_group, api_name=spec.api_name,
            quota_type="DAILY_AND_MINUTE", request_params=request_params,
            status_code=response.status, http_status=response.http_status,
            response_ms=response.elapsed_ms,
        )


def _collect_securities_report(context: AppContext, job: CollectJob, storage) -> None:
    """SECURITIES_REPORT job 처리: DS006 증권신고서 API를 호출해 Bronze에 저장한다."""
    if not job.api_name or job.api_name not in REPORT_APIS:
        raise ValueError(f"unknown securities_report api_name: {job.api_name}")
    if not job.corp_code:
        raise ValueError("corp_code is required for SECURITIES_REPORT")
    if not job.bgn_de or not job.end_de:
        raise ValueError("bgn_de and end_de are required for SECURITIES_REPORT")
    spec = REPORT_APIS[job.api_name]
    params = build_report_params(
        spec,
        corp_code=job.corp_code,
        bgn_de=job.bgn_de,
        end_de=job.end_de,
    )
    request_params = dict(params)
    storage_path = BronzePaths.securities_report(
        spec.api_group, spec.api_name, job.corp_code, job.bgn_de, job.end_de,
    )
    if storage.exists(storage_path):
        return  # 이미 수집(정상/nodata) — S3 멱등으로 API 호출을 건너뛴다(중복 호출 방지).
    try:
        response = context.client.report_json(
            api_name=spec.api_name,
            endpoint=spec.endpoint,
            params=params,
        )
    except OpenDartNoData as exc:
        _write_nodata_marker(storage, storage_path, exc.message)
        with context.service_engine.begin() as conn:
            _log_api_request(
                context, conn,
                api_group=spec.api_group, api_name=spec.api_name,
                quota_type="DAILY_AND_MINUTE", request_params=request_params,
                status_code=exc.status, http_status=None, response_ms=None,
                error_message=exc.message,
            )
        return
    except OpenDartError as exc:
        with context.service_engine.begin() as conn:
            _log_api_request(
                context, conn,
                api_group=spec.api_group, api_name=spec.api_name,
                quota_type="DAILY_AND_MINUTE", request_params=request_params,
                status_code=exc.status, http_status=None, response_ms=None,
                error_message=str(exc),
            )
        raise

    stored = storage.write_bytes(
        storage_path,
        json.dumps(response.payload, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        content_type="application/json",
    )
    with context.service_engine.begin() as conn:
        upsert_raw_file_reference(conn, stored)
        _log_api_request(
            context, conn,
            api_group=spec.api_group, api_name=spec.api_name,
            quota_type="DAILY_AND_MINUTE", request_params=request_params,
            status_code=response.status, http_status=response.http_status,
            response_ms=response.elapsed_ms,
        )


def _collect_company_overview(context: AppContext, job: CollectJob, storage) -> None:
    """COMPANY_OVERVIEW job 처리: DS001 /api/company.json을 받아 Bronze에 저장한다."""
    if not job.corp_code:
        raise ValueError("corp_code is required for COMPANY_OVERVIEW")
    if storage.exists(BronzePaths.company_overview(job.corp_code)):
        return  # 이미 수집(정상/nodata) — S3 멱등으로 API 호출을 건너뛴다(중복 호출 방지).
    request_params = {"corp_code": job.corp_code}
    try:
        response = context.client.company_overview(job.corp_code)
    except OpenDartNoData as exc:
        _write_nodata_marker(storage, BronzePaths.company_overview(job.corp_code), exc.message)
        with context.service_engine.begin() as conn:
            _log_api_request(
                context, conn,
                api_group="DS001", api_name="company",
                quota_type="DAILY_AND_MINUTE", request_params=request_params,
                status_code=exc.status, http_status=None, response_ms=None,
                error_message=exc.message,
            )
        return
    except OpenDartError as exc:
        with context.service_engine.begin() as conn:
            _log_api_request(
                context, conn,
                api_group="DS001", api_name="company",
                quota_type="DAILY_AND_MINUTE", request_params=request_params,
                status_code=exc.status, http_status=None, response_ms=None,
                error_message=str(exc),
            )
        raise

    payload = response.payload
    storage_path = BronzePaths.company_overview(job.corp_code)
    stored = storage.write_bytes(
        storage_path,
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        content_type="application/json",
    )
    with context.service_engine.begin() as conn:
        upsert_raw_file_reference(conn, stored)
        _log_api_request(
            context, conn,
            api_group="DS001", api_name="company",
            quota_type="DAILY_AND_MINUTE", request_params=request_params,
            status_code=response.status, http_status=response.http_status,
            response_ms=response.elapsed_ms,
        )


def _collect_document(context: AppContext, job: CollectJob, storage) -> None:
    """DISCLOSURE_DOCUMENT job 처리: DS001 document.xml 원문 ZIP을 받아 Bronze에 저장한다.

    역할: rcept_no 기준 원문 ZIP 확보(원문/RAG 라인의 원천). 기본 비활성(DART_COLLECT_MODE).
    기준: rcept_no 필수. daily+minute quota로 계수. 실패는 로그 후 예외를 올려 job FAILED 처리.
    """
    if not job.rcept_no:
        raise ValueError("rcept_no is required")
    if storage.exists(BronzePaths.document(job.rcept_no)):
        return  # 이미 수집(원문 ZIP 또는 014 nodata 마커) — S3 멱등으로 API 호출을 건너뛴다.
    request_params = {"rcept_no": job.rcept_no}
    try:
        data, http_status, elapsed_ms = context.client.document_zip(job.rcept_no)
    except OpenDartError as exc:
        if exc.status == "014":
            storage_path = BronzePaths.document(job.rcept_no)
            _write_nodata_marker(storage, storage_path, exc.message)
            with context.service_engine.begin() as conn:
                _log_api_request(
                    context,
                    conn,
                    api_group="DS001",
                    api_name="document",
                    quota_type="DAILY_AND_MINUTE",
                    request_params=request_params,
                    status_code=exc.status,
                    http_status=None,
                    response_ms=None,
                    error_message=exc.message,
                )
            return
        with context.service_engine.begin() as conn:
            _log_api_request(
                context,
                conn,
                api_group="DS001",
                api_name="document",
                quota_type="DAILY_AND_MINUTE",
                request_params=request_params,
                status_code=exc.status,
                http_status=None,
                response_ms=None,
                error_message=exc.message,
            )
        raise
    except Exception as exc:
        with context.service_engine.begin() as conn:
            _log_api_request(
                context,
                conn,
                api_group="DS001",
                api_name="document",
                quota_type="DAILY_AND_MINUTE",
                request_params=request_params,
                status_code=None,
                http_status=None,
                response_ms=None,
                error_message=str(exc),
            )
        raise
    storage_path = BronzePaths.document(job.rcept_no)
    stored = storage.write_bytes(storage_path, data, content_type="application/zip")
    with context.service_engine.begin() as conn:
        upsert_raw_file_reference(conn, stored)
        _log_api_request(
            context,
            conn,
            api_group="DS001",
            api_name="document",
            quota_type="DAILY_AND_MINUTE",
            request_params=request_params,
            status_code="000",
            http_status=http_status,
            response_ms=elapsed_ms,
        )


def _collect_financial_statement_all(context: AppContext, job: CollectJob, storage) -> None:
    """FINANCIAL_STATEMENT_ALL job 처리: DS003 fnlttSinglAcntAll.json을 받아 RDB에 적재한다.

    역할: 연결(CFS)·별도(OFS) 재무제표를 받아 financial_statement/financial_account에 replace 적재.
    기준: corp_code+bsns_year+reprt_code 필수. fs_div별로 1회씩 호출하며, 013(데이터 없음)은 건너뛴다.
          재수집 시 (corp_code,bsns_year,reprt_code,fs_div) 키로 덮어써 중복을 막는다.
    """
    if not job.corp_code or not job.bsns_year or not job.reprt_code:
        raise ValueError("corp_code, bsns_year and reprt_code are required")
    for fs_div in ("CFS", "OFS"):
        if storage.exists(BronzePaths.financial_statement(job.corp_code, job.bsns_year, job.reprt_code, fs_div)):
            continue  # 이 fs_div 이미 수집(정상/nodata) — S3 멱등으로 API 호출을 건너뛴다.
        request_params = {
            "corp_code": job.corp_code,
            "bsns_year": job.bsns_year,
            "reprt_code": job.reprt_code,
            "fs_div": fs_div,
        }
        try:
            response = context.client.financial_statement_all(
                corp_code=job.corp_code,
                bsns_year=job.bsns_year,
                reprt_code=job.reprt_code,
                fs_div=fs_div,
            )
        except OpenDartNoData as exc:
            _write_nodata_marker(
                storage,
                BronzePaths.financial_statement(job.corp_code, job.bsns_year, job.reprt_code, fs_div),
                exc.message,
            )
            with context.service_engine.begin() as conn:
                _log_api_request(
                    context,
                    conn,
                    api_group="DS003",
                    api_name="fnlttSinglAcntAll",
                    quota_type="DAILY_AND_MINUTE",
                    request_params=request_params,
                    status_code=exc.status,
                    http_status=None,
                    response_ms=None,
                    error_message=exc.message,
                )
            continue
        except OpenDartError as exc:
            with context.service_engine.begin() as conn:
                _log_api_request(
                    context,
                    conn,
                    api_group="DS003",
                    api_name="fnlttSinglAcntAll",
                    quota_type="DAILY_AND_MINUTE",
                    request_params=request_params,
                    status_code=exc.status,
                    http_status=None,
                    response_ms=None,
                    error_message=exc.message,
                )
            raise
        except Exception as exc:
            with context.service_engine.begin() as conn:
                _log_api_request(
                    context,
                    conn,
                    api_group="DS003",
                    api_name="fnlttSinglAcntAll",
                    quota_type="DAILY_AND_MINUTE",
                    request_params=request_params,
                    status_code=None,
                    http_status=None,
                    response_ms=None,
                    error_message=str(exc),
                )
            raise

        payload = response.payload
        storage_path = BronzePaths.financial_statement(
            job.corp_code, job.bsns_year, job.reprt_code, fs_div
        )
        stored = storage.write_bytes(
            storage_path,
            json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8"),
            content_type="application/json",
        )
        with context.service_engine.begin() as conn:
            upsert_raw_file_reference(conn, stored)
            _log_api_request(
                context,
                conn,
                api_group="DS003",
                api_name="fnlttSinglAcntAll",
                quota_type="DAILY_AND_MINUTE",
                request_params=request_params,
                status_code=response.status,
                http_status=response.http_status,
                response_ms=response.elapsed_ms,
            )


def _log_api_request(
    context: AppContext,
    conn,
    *,
    api_group: str | None,
    api_name: str,
    quota_type: str,
    request_params: dict[str, Any],
    status_code: str | None,
    http_status: int | None,
    response_ms: int | None,
    error_message: str | None = None,
) -> None:
    """api_request_log에 호출 1건을 기록한다(인증키 제외, request_hash로 멱등 upsert).

    역할: DART 호출 파라미터/상태/응답시간/에러를 감사 로그로 남긴다.
    기준: quota_counted는 quota_type=="DAILY_AND_MINUTE"일 때만 true.
          quota-counted 호출은 DART_QUOTA_REQUEST_LOG_ENABLED=false면 기록을 건너뛴다
          (MINUTE_ONLY는 스위치와 무관하게 기록).
    """
    quota_counted = quota_type == "DAILY_AND_MINUTE"
    if quota_counted and not context.settings.dart_quota_request_log_enabled:
        return
    insert_api_request_log(
        conn,
        api_group=api_group,
        api_name=api_name,
        quota_type=quota_type,
        quota_counted=quota_counted,
        request_params=request_params,
        request_hash=request_hash("DART", api_name, request_params),
        status_code=status_code,
        http_status=http_status,
        response_ms=response_ms,
        error_message=error_message,
    )
