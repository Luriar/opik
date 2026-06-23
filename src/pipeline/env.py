"""Environment loading helpers for daily pipeline settings."""

from __future__ import annotations

import os
from pathlib import Path


IGNORED_LEGACY_KEYS = {"KRX_ID", "KRX_PW"}


def load_project_env(project_root: Path) -> list[str]:
    """Load .env key/value pairs without legacy authenticated KRX credentials."""
    env_path = project_root / ".env"
    warnings: list[str] = []
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key in IGNORED_LEGACY_KEYS:
                continue
            if key and key not in os.environ:
                os.environ[key] = value
    return warnings
