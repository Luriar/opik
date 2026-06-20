"""DART API key를 런타임에 .env 파일에서 직접 읽는 공급 계층.

키를 docker compose environment(${DART_API_KEYS})로만 주입하면, 값은 컨테이너
생성 시점에 고정되어 변경 시 컨테이너 재생성이 강제된다. 이 모듈은 volume mount된
.env 파일을 직접 읽어, .env의 DART_API_KEYS만 고치면 컨테이너 재생성 없이 키를
추가/회전할 수 있게 한다.

mtime 캐시: .env가 바뀌면 다음 호출에서 즉시 반영하고, 바뀌지 않았으면 재파싱하지 않는다.
파일이 없거나 읽기에 실패하면 빈 튜플을 돌려, 호출자가 프로세스 환경변수(os.environ)로
fallback하도록 한다(.env가 일시적으로 깨져도 수집이 멈추지 않게 하는 안전장치).

.env에서 DART_API_KEYS 줄만 해석하며, 다른 시크릿은 읽지 않는다.
"""

from __future__ import annotations

import logging
import os
import threading

from dart_agent.dart_keys import DartApiKey, parse_dart_api_keys

log = logging.getLogger(__name__)

_lock = threading.Lock()
_cache: dict[str, object] = {"keys": None, "mtime": None, "path": None}


def load_keys_from_env_file(path: str) -> tuple[DartApiKey, ...]:
    """.env 파일의 DART_API_KEYS를 읽어 키 튜플로 반환한다.

    없거나 실패 시 빈 튜플(→ 환경변수 fallback). 파싱 규칙은 env와 동일하게
    parse_dart_api_keys를 재사용한다(콤마 구분, 중복 제거).
    """
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return ()
    with _lock:
        if _cache["keys"] is not None and _cache["path"] == path and _cache["mtime"] == mtime:
            return _cache["keys"]  # type: ignore[return-value]
        try:
            raw_keys = _read_dart_api_keys(path)
        except OSError as exc:
            log.error("DART .env read failed (%s): %s — falling back to process env", path, exc)
            return ()
        keys = parse_dart_api_keys(raw_keys)
        _cache.update(keys=keys, mtime=mtime, path=path)
        return keys


def _read_dart_api_keys(path: str) -> str | None:
    """.env에서 DART_API_KEYS 값만 추출한다(다른 줄은 무시)."""
    with open(path, encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, _, value = line.partition("=")
            if name.strip().removeprefix("export ").strip() == "DART_API_KEYS":
                return value.strip().strip('"').strip("'")
    return None
