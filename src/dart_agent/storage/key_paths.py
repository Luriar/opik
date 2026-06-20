from __future__ import annotations

from pathlib import PurePosixPath


def normalize_relative_path(path: str, label: str) -> str:
    normalized = str(PurePosixPath(path))
    parts = PurePosixPath(normalized).parts
    if normalized in {"", "."}:
        return ""
    if normalized.startswith("/") or ".." in parts:
        raise ValueError(f"invalid {label}: {path}")
    return normalized


def join_prefix_and_path(base_prefix: str, path: str) -> str:
    """Join storage prefix and root-relative object path without duplicating overlap."""
    prefix = normalize_relative_path(base_prefix.strip("/"), "storage prefix") if base_prefix else ""
    object_path = normalize_relative_path(path, "object path")
    if not prefix:
        return object_path

    prefix_parts = PurePosixPath(prefix).parts
    object_parts = PurePosixPath(object_path).parts
    overlap = 0
    for size in range(min(len(prefix_parts), len(object_parts)), 0, -1):
        if prefix_parts[-size:] == object_parts[:size]:
            overlap = size
            break
    return "/".join((*prefix_parts, *object_parts[overlap:]))
