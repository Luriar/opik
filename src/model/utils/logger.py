"""Structured logging utilities for pipeline steps."""

from __future__ import annotations

import logging
from pathlib import Path


LOG_FORMAT = (
    "%(asctime)s %(levelname)s run_id=%(run_id)s step=%(step)s "
    "status=%(status)s message=%(message)s"
)


class PipelineLogAdapter(logging.LoggerAdapter):
    """Logger adapter that guarantees required pipeline log fields."""

    def process(
        self,
        msg: str,
        kwargs: dict[str, object],
    ) -> tuple[str, dict[str, object]]:
        """Attach default run metadata to every log record."""
        extra = dict(self.extra)
        extra.update(kwargs.pop("extra", {}))
        extra.setdefault("run_id", "unknown")
        extra.setdefault("step", "unknown")
        extra.setdefault("status", "unknown")
        kwargs["extra"] = extra
        return msg, kwargs


def get_logger(
    name: str = "ai_trading_system",
    run_id: str = "unknown",
    log_file: str | Path | None = None,
    level: int = logging.INFO,
) -> PipelineLogAdapter:
    """Create a reusable logger with required pipeline metadata fields."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    formatter = logging.Formatter(LOG_FORMAT)

    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    if log_file is not None:
        file_path = Path(log_file)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if not any(
            isinstance(handler, logging.FileHandler)
            and Path(handler.baseFilename) == file_path.resolve()
            for handler in logger.handlers
        ):
            file_handler = logging.FileHandler(file_path, encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return PipelineLogAdapter(logger, {"run_id": run_id})
