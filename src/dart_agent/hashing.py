from __future__ import annotations

import hashlib
import json
from typing import Any


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def request_hash(provider: str, api_name: str, params: dict[str, Any]) -> str:
    payload = {
        "provider": provider,
        "api_name": api_name,
        "params": params,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

