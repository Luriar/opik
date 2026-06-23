"""OPIK shared configuration.

Single source of truth for environment variables and S3 paths.
Import this instead of os.getenv("S3_BUCKET", ...) scattered across files.
"""

from __future__ import annotations

import os
from pathlib import Path


# ── S3 ──────────────────────────────────────────────────────────────────────
S3_BUCKET = os.getenv("S3_BUCKET", "s3-opik-bucket")
S3_REGION = os.getenv("S3_REGION", "ap-northeast-2")

# ── Telegram ────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ── AWS credentials (from env or .env file) ─────────────────────────────────
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")


def load_dotenv() -> None:
    """Load .env file from project root, if present.

    Searches in order: OPIK_ROOT env var -> script dir -> cwd.
    Does NOT override already-set env vars (os.environ.setdefault).
    """
    candidates = []
    if root := os.getenv("OPIK_ROOT"):
        candidates.append(Path(root) / ".env")
    try:
        candidates.append(Path(__file__).parent / ".env")
    except NameError:
        pass
    candidates.append(Path(".env"))

    for env_path in candidates:
        if env_path.exists():
            with open(env_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        os.environ.setdefault(k, v)
            return
