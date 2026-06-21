from __future__ import annotations

from datetime import datetime
import hashlib


def api_key_id(api_key: str) -> str:
    value = api_key.strip()
    if not value:
        raise ValueError("api_key is required")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def daily_quota_key(api_key_identifier: str, now: datetime) -> str:
    return f"dart:daily:key:{api_key_identifier}:quota_counted:{now.strftime('%Y%m%d')}"


def daily_all_quota_key(api_key_identifier: str, now: datetime) -> str:
    return f"dart:daily:key:{api_key_identifier}:all_api:{now.strftime('%Y%m%d')}"


def daily_attempts_quota_key(now: datetime) -> str:
    """IP(프로세스 전체) 일일 실제 HTTP 시도 수 counter (never released, 한도 없음).

    quota(daily_all/quota_counted)는 TRANSPORT 실패 시 release로 원복되어 '실제로 DART에 나간
    호출 수'를 숨긴다. 이 counter는 reserve마다 1 증가하고 release에서 절대 감소하지 않으므로,
    재시도를 포함한 실제 호출 시도 총량을 그대로 보여준다(가시화 전용).
    """
    return f"dart:daily:global:attempts:{now.strftime('%Y%m%d')}"


def minute_quota_key(api_key_identifier: str, now: datetime) -> str:
    return f"dart:minute:key:{api_key_identifier}:all_api:{now.strftime('%Y%m%d%H%M')}"


def minute_global_quota_key(now: datetime) -> str:
    """key와 무관한 IP(프로세스 전체) 분당 counter (고정 분 버킷 — 레거시/관측용).

    OpenDART는 분당 호출 제한을 IP 기준으로 적용한다(키를 늘려도 같은 IP면 합산).
    실제 한도 강제는 분 경계 버스트를 막기 위해 `minute_global_second_quota_key` 기반
    슬라이딩 60초 윈도우로 한다.
    """
    return f"dart:minute:global:all_api:{now.strftime('%Y%m%d%H%M')}"


def minute_global_second_quota_key(now: datetime) -> str:
    """IP(프로세스 전체) 1초 단위 sub-bucket.

    슬라이딩 60초 윈도우의 합산 단위다. 예약 시 직전 60개(1초×60) bucket의 count를 합산해
    임의 60초 구간이 IP 분당 한도를 넘지 못하게 막는다. 고정 분 버킷은 12:00:59에 900 +
    12:01:00에 900처럼 분 경계에서 1초 안에 최대 2배가 새므로 IP 차단을 못 막는다.
    """
    return f"dart:secwin:global:all_api:{now.strftime('%Y%m%d%H%M%S')}"
