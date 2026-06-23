"""OPIK checkpoint manager.

Single API for reading/writing checkpoint files (JSON on local disk).
Used by uploaders and extractor to track progress and enable resume.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger("opik.checkpoint")


class Checkpoint:
    """Manages a single checkpoint file.

    Usage:
        ck = Checkpoint("backfill_naver")
        ck.write({"last_date": "2026-06-15", "total": 37000})
        data = ck.read()
    """

    def __init__(self, name: str, base_dir: str | Path | None = None):
        if base_dir is None:
            base_dir = Path(__file__).parent
        self._path = Path(base_dir) / f".{name}_checkpoint.json"

    def read(self) -> dict[str, Any]:
        """Read checkpoint. Returns empty dict if file missing or corrupt."""
        try:
            if self._path.exists():
                with open(self._path, encoding="utf-8") as f:
                    return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Checkpoint read error ({self._path}): {e}")
        return {}

    def write(self, data: dict[str, Any]) -> None:
        """Atomically write checkpoint data."""
        tmp = self._path.with_suffix(".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            tmp.replace(self._path)
        except OSError as e:
            logger.error(f"Checkpoint write error ({self._path}): {e}")

    def get(self, key: str, default: Any = None) -> Any:
        """Read a single key from checkpoint."""
        return self.read().get(key, default)

    def update(self, **kwargs: Any) -> None:
        """Merge new keys into existing checkpoint."""
        data = self.read()
        data.update(kwargs)
        self.write(data)

    @property
    def path(self) -> Path:
        return self._path
