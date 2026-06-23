from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection

from dart_agent.storage.base import StoredObject


def upsert_raw_file_reference(conn: Connection, stored: StoredObject) -> int:
    row = conn.execute(
        text(
            """
            INSERT INTO raw_file_reference (
                storage_backend,
                object_path,
                physical_uri,
                content_hash,
                file_size
            )
            VALUES (
                :storage_backend,
                :object_path,
                :physical_uri,
                :content_hash,
                :file_size
            )
            ON CONFLICT (storage_backend, object_path)
            DO UPDATE SET
                physical_uri = EXCLUDED.physical_uri,
                content_hash = EXCLUDED.content_hash,
                file_size = EXCLUDED.file_size,
                collected_at = CURRENT_TIMESTAMP
            RETURNING id
            """
        ),
        {
            "storage_backend": stored.storage_backend,
            "object_path": stored.object_path,
            "physical_uri": stored.physical_uri,
            "content_hash": stored.content_hash,
            "file_size": stored.file_size,
        },
    ).one()
    return int(row.id)
