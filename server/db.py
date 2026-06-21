"""
OPIK SQLite Database Layer
subscribers / conversations / briefing_recipients
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("OPIK_DB_PATH", "/data/opik/opik.db")

_lock = threading.Lock()
_conn: Optional[sqlite3.Connection] = None


def get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA foreign_keys=ON")
    return _conn


def init_db():
    """Create tables if they don't exist."""
    conn = get_conn()
    with _lock:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS subscribers (
            chat_id        INTEGER PRIMARY KEY,
            username       TEXT,
            first_name     TEXT,
            role           TEXT NOT NULL DEFAULT 'user',
            approved       INTEGER NOT NULL DEFAULT 0,
            subscribed_at  TEXT NOT NULL DEFAULT (datetime('now')),
            last_active    TEXT
        );

        CREATE TABLE IF NOT EXISTS briefing_recipients (
            chat_id        INTEGER PRIMARY KEY,
            subscribed_at  TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (chat_id) REFERENCES subscribers(chat_id)
        );

        CREATE TABLE IF NOT EXISTS conversations (
            session_id     TEXT PRIMARY KEY,
            chat_id        INTEGER NOT NULL,
            turns          TEXT NOT NULL DEFAULT '[]',
            context_summary TEXT NOT NULL DEFAULT '',
            created_at     TEXT NOT NULL DEFAULT (datetime('now')),
            last_active    TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_conv_chat
            ON conversations(chat_id, last_active);
        """)
        conn.commit()
    logger.info("DB initialized at %s", DB_PATH)


# ── Subscribers ──────────────────────────────────────────────────────────

def upsert_subscriber(chat_id: int, username: str = "",
                      first_name: str = "") -> dict:
    """Register or update a subscriber. Returns the row as dict."""
    conn = get_conn()
    with _lock:
        conn.execute("""
            INSERT INTO subscribers (chat_id, username, first_name, last_active)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(chat_id) DO UPDATE SET
                username   = COALESCE(NULLIF(excluded.username, ''), username),
                first_name = COALESCE(NULLIF(excluded.first_name, ''), first_name),
                last_active = excluded.last_active
        """, (chat_id, username, first_name))
        conn.commit()
        row = conn.execute(
            "SELECT * FROM subscribers WHERE chat_id = ?", (chat_id,)
        ).fetchone()
        return dict(row) if row else {}


def get_subscriber(chat_id: int) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM subscribers WHERE chat_id = ?", (chat_id,)
    ).fetchone()
    return dict(row) if row else None


def is_approved(chat_id: int) -> bool:
    sub = get_subscriber(chat_id)
    return sub is not None and sub.get("approved", 0) == 1


def approve_subscriber(chat_id: int, role: str = "user") -> bool:
    conn = get_conn()
    with _lock:
        cur = conn.execute(
            "UPDATE subscribers SET approved=1, role=? WHERE chat_id=?",
            (role, chat_id)
        )
        conn.commit()
        return cur.rowcount > 0


def list_approved_subscribers() -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM subscribers WHERE approved=1 ORDER BY subscribed_at"
    ).fetchall()
    return [dict(r) for r in rows]


# ── Briefing Recipients ──────────────────────────────────────────────────

def add_briefing_recipient(chat_id: int) -> bool:
    conn = get_conn()
    with _lock:
        try:
            conn.execute(
                "INSERT INTO briefing_recipients (chat_id) VALUES (?)",
                (chat_id,)
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False


def remove_briefing_recipient(chat_id: int) -> bool:
    conn = get_conn()
    with _lock:
        cur = conn.execute(
            "DELETE FROM briefing_recipients WHERE chat_id = ?", (chat_id,)
        )
        conn.commit()
        return cur.rowcount > 0


def get_briefing_recipients() -> list[int]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT chat_id FROM briefing_recipients ORDER BY subscribed_at"
    ).fetchall()
    return [r["chat_id"] for r in rows]


# ── Conversations ────────────────────────────────────────────────────────

def save_conversation(session_id: str, chat_id: int,
                      turns: list[dict], context_summary: str = ""):
    conn = get_conn()
    with _lock:
        conn.execute("""
            INSERT INTO conversations (session_id, chat_id, turns, context_summary, last_active)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(session_id) DO UPDATE SET
                turns           = excluded.turns,
                context_summary = excluded.context_summary,
                last_active     = excluded.last_active
        """, (session_id, chat_id, json.dumps(turns, ensure_ascii=False),
              context_summary))
        conn.commit()


def load_conversation(session_id: str) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM conversations WHERE session_id = ?", (session_id,)
    ).fetchone()
    if not row:
        return None
    d = dict(row)
    d["turns"] = json.loads(d.get("turns", "[]"))
    return d


def load_conversation_by_chat(chat_id: int, limit: int = 5) -> list[dict]:
    """Recent sessions for a chat user."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT * FROM conversations
        WHERE chat_id = ?
        ORDER BY last_active DESC
        LIMIT ?
    """, (chat_id, limit)).fetchall()
    results = []
    for r in rows:
        d = dict(r)
        d["turns"] = json.loads(d.get("turns", "[]"))
        results.append(d)
    return results


def delete_conversation(session_id: str):
    conn = get_conn()
    with _lock:
        conn.execute("DELETE FROM conversations WHERE session_id = ?",
                     (session_id,))
        conn.commit()


# ── Bootstrap ────────────────────────────────────────────────────────────

def seed_initial_users():
    """Ensure the two known users are approved and get briefings."""
    known = [
        (6409771651, "junho", "Admin"),
        (1122918055, "choimihae", "User"),
    ]
    for chat_id, username, role in known:
        upsert_subscriber(chat_id, username=username, first_name=username)
        approve_subscriber(chat_id, role="admin" if role == "Admin" else role)
        add_briefing_recipient(chat_id)
        logger.info("Seeded user %d (%s)", chat_id, username)
    logger.info("Seed complete")
