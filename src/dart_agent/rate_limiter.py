from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from dart_agent.opendart.client import QuotaPolicy, quota_policy_for
from dart_agent.quota_keys import (
    daily_all_quota_key,
    daily_attempts_quota_key,
    daily_quota_key,
    minute_global_second_quota_key,
    minute_quota_key,
)

KST = ZoneInfo("Asia/Seoul")

# IP 전역 분당 한도를 임의 60초 구간으로 강제하기 위한 슬라이딩 윈도우 크기(초).
_GLOBAL_WINDOW_SECONDS = 60
# 전역 분당 예약(합산-후-증가)을 모든 worker에 걸쳐 직렬화하기 위한 advisory lock id(고정 상수).
# IP 한도는 본래 단일 공유 자원이라 직렬화 비용이 의미 없을 만큼 작다(<=수십 req/s).
_GLOBAL_MINUTE_LOCK_ID = 73958146


class RateLimitExceeded(RuntimeError):
    def __init__(self, message: str, *, quota_type: str, quota_key: str) -> None:
        super().__init__(message)
        self.quota_type = quota_type
        self.quota_key = quota_key


@dataclass(frozen=True)
class QuotaLimits:
    daily_limit: int
    daily_safe_limit: int
    daily_emergency_limit: int
    minute_limit: int
    minute_safe_limit: int
    # IP(프로세스 전체) 분당 한도. 모든 키의 호출을 합산해 적용한다.
    minute_global_limit: int = 1000
    minute_global_safe_limit: int = 900


@dataclass(frozen=True)
class _CounterSpec:
    quota_key: str
    quota_type: str
    limit_count: int
    safe_limit: int | None
    window_start: datetime
    window_end: datetime


class DbRateLimiter:
    def __init__(self, engine: Engine, limits: QuotaLimits, api_key_identifier: str) -> None:
        self.engine = engine
        self.limits = limits
        self.api_key_identifier = api_key_identifier

    def reserve(
        self,
        api_name: str,
        now: datetime | None = None,
        api_key_identifier: str | None = None,
    ) -> None:
        now = _quota_now(now)
        key_identifier = api_key_identifier or self.api_key_identifier
        policy = quota_policy_for(api_name)

        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        minute_start = now.replace(second=0, microsecond=0)
        counters = [
            _CounterSpec(
                quota_key=daily_all_quota_key(key_identifier, now),
                quota_type="daily_all",
                limit_count=self.limits.daily_limit,
                safe_limit=None,
                window_start=day_start,
                window_end=day_start + timedelta(days=1),
            ),
            _CounterSpec(
                quota_key=minute_quota_key(key_identifier, now),
                quota_type="minute_all",
                limit_count=self.limits.minute_limit,
                safe_limit=self.limits.minute_safe_limit,
                window_start=minute_start,
                window_end=minute_start + timedelta(minutes=1),
            ),
        ]
        if policy == QuotaPolicy.DAILY_AND_MINUTE:
            counters.append(
                _CounterSpec(
                    quota_key=daily_quota_key(key_identifier, now),
                    quota_type="daily_quota_counted",
                    limit_count=self.limits.daily_limit,
                    safe_limit=self.limits.daily_emergency_limit,
                    window_start=day_start,
                    window_end=day_start + timedelta(days=1),
                )
            )

        with self.engine.begin() as conn:
            # 모든 한도를 먼저 확인하고(증가 전), 전부 통과할 때만 증가한다. 한 counter라도
            # 초과면 다른 counter를 증가시키지 않는다(check-before-increment 불변식).
            self._ensure_counter_rows(conn, counters)
            self._check_counters(conn, counters)
            # IP 전역 분당: 고정 분 버킷이 아니라 슬라이딩 60초(1초 sub-bucket 합산)로 막는다.
            self._check_global_minute_sliding(conn, now)
            self._increment_counters(conn, counters)
            self._increment_global_minute_sliding(conn, now)

    def wait_for_capacity(self, exc: RateLimitExceeded) -> None:
        """Wait for minute-level capacity without turning throttle into job failure."""
        time.sleep(self.retry_after_seconds(exc))

    def retry_after_seconds(self, exc: RateLimitExceeded) -> float:
        """Return wait time for minute-level throttles; re-raise non-minute limits."""
        if exc.quota_type == "minute_global":
            return self._global_minute_wait_seconds()
        if exc.quota_type == "minute_all":
            now = _quota_now()
            return max(0.05, 60 - now.second - now.microsecond / 1_000_000 + 0.05)
        raise exc

    def _ensure_counter_rows(self, conn, counters: list[_CounterSpec]) -> None:
        for counter in counters:
            conn.execute(
                text(
                    """
                    INSERT INTO api_quota_counter (
                        quota_key, quota_type, count, limit_count, window_start, window_end
                    )
                    VALUES (:quota_key, :quota_type, 0, :limit_count, :window_start, :window_end)
                    ON CONFLICT (quota_key) DO NOTHING
                    """
                ),
                {
                    "quota_key": counter.quota_key,
                    "quota_type": counter.quota_type,
                    "limit_count": counter.limit_count,
                    "window_start": counter.window_start,
                    "window_end": counter.window_end,
                },
            )

    def _check_counters(self, conn, counters: list[_CounterSpec]) -> None:
        for counter in counters:
            row = conn.execute(
                text("SELECT count FROM api_quota_counter WHERE quota_key = :quota_key FOR UPDATE"),
                {"quota_key": counter.quota_key},
            ).one()
            if counter.safe_limit is not None and int(row.count) >= counter.safe_limit:
                raise RateLimitExceeded(
                    (
                        f"{counter.quota_type} quota blocked at "
                        f"{row.count}/{counter.safe_limit}: {counter.quota_key}"
                    ),
                    quota_type=counter.quota_type,
                    quota_key=counter.quota_key,
                )

    def _increment_counters(self, conn, counters: list[_CounterSpec]) -> None:
        for counter in counters:
            conn.execute(
                text(
                    """
                    UPDATE api_quota_counter
                    SET count = count + 1, updated_at = CURRENT_TIMESTAMP
                    WHERE quota_key = :quota_key
                    """
                ),
                {"quota_key": counter.quota_key},
            )

    def _check_global_minute_sliding(self, conn, now: datetime) -> None:
        """IP 전역 분당 한도를 슬라이딩 60초 윈도우로 확인한다(초과면 RateLimitExceeded).

        직전 60개 1초 bucket의 count 합이 minute_global_safe_limit 이상이면 차단한다.
        advisory lock으로 전역 예약을 직렬화해 합산-후-증가를 원자적으로 만든다.
        """
        conn.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"),
            {"lock_id": _GLOBAL_MINUTE_LOCK_ID},
        )
        sec = now.replace(microsecond=0)
        keys = [
            minute_global_second_quota_key(sec - timedelta(seconds=i))
            for i in range(_GLOBAL_WINDOW_SECONDS)
        ]
        total = (
            conn.execute(
                text(
                    "SELECT COALESCE(SUM(count), 0) AS total "
                    "FROM api_quota_counter WHERE quota_key IN :keys"
                ).bindparams(bindparam("keys", expanding=True)),
                {"keys": keys},
            )
            .one()
            .total
        )
        if int(total) >= self.limits.minute_global_safe_limit:
            raise RateLimitExceeded(
                (
                    f"minute_global sliding-{_GLOBAL_WINDOW_SECONDS}s blocked at "
                    f"{total}/{self.limits.minute_global_safe_limit}"
                ),
                quota_type="minute_global",
                quota_key=keys[0],
            )
        current_second = conn.execute(
            text("SELECT count FROM api_quota_counter WHERE quota_key = :quota_key"),
            {"quota_key": keys[0]},
        ).first()
        if current_second and int(current_second.count) >= self._global_per_second_safe_limit():
            raise RateLimitExceeded(
                (
                    f"minute_global per-second pacing blocked at "
                    f"{current_second.count}/{self._global_per_second_safe_limit()}"
                ),
                quota_type="minute_global",
                quota_key=keys[0],
            )

    def _global_per_second_safe_limit(self) -> int:
        return max(1, self.limits.minute_global_safe_limit // _GLOBAL_WINDOW_SECONDS)

    def _global_minute_wait_seconds(self, now: datetime | None = None) -> float:
        now = _quota_now(now)
        sec = now.replace(microsecond=0)
        oldest_in_window = sec - timedelta(seconds=_GLOBAL_WINDOW_SECONDS - 1)
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT
                        COALESCE(SUM(count), 0) AS total,
                        COALESCE(SUM(CASE WHEN window_start = :sec THEN count ELSE 0 END), 0)
                            AS current_second_count,
                        MIN(CASE WHEN count > 0 THEN window_start ELSE NULL END) AS oldest
                    FROM api_quota_counter
                    WHERE quota_type = 'minute_global_sec'
                      AND window_start >= :oldest_in_window
                      AND window_start <= :sec
                    """
                ),
                {"sec": sec, "oldest_in_window": oldest_in_window},
            ).mappings().one()

        waits: list[float] = []
        if int(row["current_second_count"]) >= self._global_per_second_safe_limit():
            waits.append(((sec + timedelta(seconds=1)) - now).total_seconds() + 0.02)
        if int(row["total"]) >= self.limits.minute_global_safe_limit and row["oldest"] is not None:
            waits.append(
                (row["oldest"] + timedelta(seconds=_GLOBAL_WINDOW_SECONDS) - now).total_seconds()
                + 0.02
            )
        if not waits:
            waits.append(0.05)
        return max(0.05, min(max(waits), _GLOBAL_WINDOW_SECONDS + 1.0))

    def _increment_global_minute_sliding(self, conn, now: datetime) -> None:
        sec = now.replace(microsecond=0)
        conn.execute(
            text(
                """
                INSERT INTO api_quota_counter (
                    quota_key, quota_type, count, limit_count, window_start, window_end
                )
                VALUES (:quota_key, 'minute_global_sec', 1, :limit_count, :window_start, :window_end)
                ON CONFLICT (quota_key) DO UPDATE
                SET count = api_quota_counter.count + 1, updated_at = CURRENT_TIMESTAMP
                """
            ),
            {
                "quota_key": minute_global_second_quota_key(sec),
                "limit_count": self.limits.minute_global_limit,
                "window_start": sec,
                "window_end": sec + timedelta(seconds=1),
            },
        )
        # 실제 HTTP 시도 수(재시도 포함) — never released, 한도 없음. release가 quota를 원복해도
        # 이 counter와 minute_global_sec는 그대로 남아 실제 DART로 나간 호출량을 보여준다.
        day_start = sec.replace(hour=0, minute=0, second=0)
        conn.execute(
            text(
                """
                INSERT INTO api_quota_counter (
                    quota_key, quota_type, count, limit_count, window_start, window_end
                )
                VALUES (:quota_key, 'daily_attempts', 1, 0, :window_start, :window_end)
                ON CONFLICT (quota_key) DO UPDATE
                SET count = api_quota_counter.count + 1, updated_at = CURRENT_TIMESTAMP
                """
            ),
            {
                "quota_key": daily_attempts_quota_key(sec),
                "window_start": day_start,
                "window_end": day_start + timedelta(days=1),
            },
        )
        # 윈도우 밖으로 빠진 1초 bucket을 제거(O(1))하고, 매 분 1회 잔여 bucket을 정리한다.
        conn.execute(
            text("DELETE FROM api_quota_counter WHERE quota_key = :quota_key"),
            {
                "quota_key": minute_global_second_quota_key(
                    sec - timedelta(seconds=_GLOBAL_WINDOW_SECONDS)
                )
            },
        )
        if sec.second == 0:
            conn.execute(
                text(
                    "DELETE FROM api_quota_counter "
                    "WHERE quota_type = 'minute_global_sec' AND window_end < :cutoff"
                ),
                {"cutoff": sec - timedelta(seconds=_GLOBAL_WINDOW_SECONDS * 2)},
            )

    def daily_quota_remaining(
        self,
        now: datetime | None = None,
        api_key_identifier: str | None = None,
    ) -> int:
        """오늘 daily_quota_counted 기준 emergency_limit까지 남은 호출 수(음수는 0으로 클램프).

        detail_collector가 run 진입 전에 호출해, 일일 quota가 소진된 키만 있을 때
        claim+reset 헛돌이(5분마다 batch 전량 reset)를 피하는 판단에 쓴다.
        list(MINUTE_ONLY)는 daily_quota_counted에 계수되지 않으므로 이 값에 영향이 없다.
        """
        now = _quota_now(now)
        key_identifier = api_key_identifier or self.api_key_identifier
        quota_key = daily_quota_key(key_identifier, now)
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT count FROM api_quota_counter WHERE quota_key = :quota_key"),
                {"quota_key": quota_key},
            ).first()
        used = int(row.count) if row else 0
        return max(0, self.limits.daily_emergency_limit - used)

    def release(
        self,
        api_name: str,
        now: datetime | None = None,
        api_key_identifier: str | None = None,
    ) -> None:
        """TRANSPORT 실패 시 quota(과금) counter만 원복한다.

        DART는 전송 실패한 호출을 일일 quota에 과금하지 않으므로 quota counter는 원복해 정당한
        재시도가 quota를 태우지 않게 한다. 그러나 실패·재시도도 DART 엣지에는 닿아 IP 차단의
        원인이 되므로, IP 레이트(minute_global_sec)와 일일 시도 수(daily_attempts)는 원복하지
        않는다. 그 결과 전송 장애가 누적되면 슬라이딩 윈도우가 차서 스스로 throttle(밴된 IP를
        계속 두드리는 것을 막음)된다.
        """
        now = _quota_now(now)
        key_identifier = api_key_identifier or self.api_key_identifier
        policy = quota_policy_for(api_name)

        quota_keys = [
            daily_all_quota_key(key_identifier, now),
            minute_quota_key(key_identifier, now),
        ]
        if policy == QuotaPolicy.DAILY_AND_MINUTE:
            quota_keys.append(daily_quota_key(key_identifier, now))

        with self.engine.begin() as conn:
            for qk in quota_keys:
                conn.execute(
                    text(
                        """
                        UPDATE api_quota_counter
                        SET count = GREATEST(0, count - 1),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE quota_key = :quota_key
                        """
                    ),
                    {"quota_key": qk},
                )

    def mark_external_daily_limit(
        self,
        api_name: str,
        now: datetime | None = None,
        api_key_identifier: str | None = None,
    ) -> None:
        """DART가 020을 반환한 키를 오늘 더 쓰지 않도록 내부 counter를 보정한다."""
        now = _quota_now(now)
        key_identifier = api_key_identifier or self.api_key_identifier
        quota_keys = [daily_all_quota_key(key_identifier, now)]
        if quota_policy_for(api_name) == QuotaPolicy.DAILY_AND_MINUTE:
            quota_keys.append(daily_quota_key(key_identifier, now))
        with self.engine.begin() as conn:
            for quota_key in quota_keys:
                conn.execute(
                    text(
                        """
                        INSERT INTO api_quota_counter (
                            quota_key,
                            quota_type,
                            count,
                            limit_count,
                            window_start,
                            window_end
                        )
                        VALUES (
                            :quota_key,
                            :quota_type,
                            :count,
                            :limit_count,
                            :window_start,
                            :window_end
                        )
                        ON CONFLICT (quota_key) DO UPDATE
                        SET count = GREATEST(api_quota_counter.count, EXCLUDED.count),
                            updated_at = CURRENT_TIMESTAMP
                        """
                    ),
                    {
                        "quota_key": quota_key,
                        "quota_type": "daily_quota_counted"
                        if ":quota_counted:" in quota_key
                        else "daily_all",
                        "count": self.limits.daily_emergency_limit,
                        "limit_count": self.limits.daily_limit,
                        "window_start": now.replace(hour=0, minute=0, second=0, microsecond=0),
                        "window_end": now.replace(hour=0, minute=0, second=0, microsecond=0)
                        + timedelta(days=1),
                    },
                )


def _quota_now(now: datetime | None = None) -> datetime:
    value = now or datetime.now(KST)
    if value.tzinfo is None:
        return value.replace(tzinfo=KST)
    return value.astimezone(KST)
