from __future__ import annotations

from typing import Any


OWNERSHIP_LATEST_VERSION = "ownership_latest.v1"


def latest_ownership_rows(api_name: str, rows: list[Any]) -> list[dict[str, Any]]:
    """Return the latest DS004 row per holder identity.

    OpenDART DS004 majorstock/elestock accepts only corp_code, so latest-state
    filtering is a project-side rule. We keep the newest row per
    corp_code+repror, ordered by rcept_dt and rcept_no.
    """
    if api_name not in {"majorstock", "elestock"}:
        return [r for r in rows if isinstance(r, dict)]

    latest_by_key: dict[tuple[str, str], tuple[tuple[str, str, int], dict[str, Any]]] = {}
    passthrough: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        key = _ownership_holder_key(row, idx)
        if key is None:
            passthrough.append(row)
            continue
        sort_key = (_normalize_date(row.get("rcept_dt")), str(row.get("rcept_no") or ""), idx)
        current = latest_by_key.get(key)
        if current is None or sort_key > current[0]:
            latest_by_key[key] = (sort_key, row)

    selected = [item[1] for item in latest_by_key.values()]
    selected.extend(passthrough)
    return sorted(
        selected,
        key=lambda r: (_normalize_date(r.get("rcept_dt")), str(r.get("rcept_no") or "")),
        reverse=True,
    )


def filter_ownership_payload(api_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get("list")
    if api_name not in {"majorstock", "elestock"} or not isinstance(rows, list):
        return payload
    filtered = latest_ownership_rows(api_name, rows)
    if len(filtered) == len([r for r in rows if isinstance(r, dict)]):
        return payload
    result = dict(payload)
    result["list"] = filtered
    result["_project_filter"] = {
        "name": OWNERSHIP_LATEST_VERSION,
        "reason": "OpenDART DS004 exposes corp_code-only requests; keep latest row per corp_code+repror.",
        "source_row_count": len(rows),
        "filtered_row_count": len(filtered),
    }
    return result


def _ownership_holder_key(row: dict[str, Any], idx: int) -> tuple[str, str] | None:
    corp_code = str(row.get("corp_code") or "").strip()
    repror = str(row.get("repror") or "").strip()
    if not corp_code or not repror:
        rcept_no = str(row.get("rcept_no") or "").strip()
        if not rcept_no:
            return None
        return (corp_code or "__unknown_corp__", f"__missing_repror__:{rcept_no}:{idx}")
    return (corp_code, repror)


def _normalize_date(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())[:8]
