from __future__ import annotations

from dataclasses import dataclass, field

from dart_agent.quota_keys import api_key_id


@dataclass(frozen=True)
class DartApiKey:
    value: str = field(repr=False)
    identifier: str


def parse_dart_api_keys(raw_keys: str | None) -> tuple[DartApiKey, ...]:
    """DART_API_KEYS(콤마/개행 구분)를 파싱한다. 키 1개면 콤마 없이 그 값만 넣으면 된다."""
    if not raw_keys or not raw_keys.strip():
        return ()

    values: list[str] = []
    seen: set[str] = set()
    for raw in raw_keys.replace("\n", ",").split(","):
        value = raw.strip()
        if not value or value in seen:
            continue
        values.append(value)
        seen.add(value)

    return tuple(DartApiKey(value=value, identifier=api_key_id(value)) for value in values)
