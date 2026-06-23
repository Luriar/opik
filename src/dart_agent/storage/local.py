from __future__ import annotations

from pathlib import Path

from dart_agent.hashing import sha256_bytes
from dart_agent.storage.base import StoredObject
from dart_agent.storage.key_paths import join_prefix_and_path, normalize_relative_path


class LocalStorage:
    backend = "local"

    def __init__(self, base_path: str, base_prefix: str = "") -> None:
        self.base_path = Path(base_path)
        self.base_prefix = normalize_relative_path(base_prefix.strip("/"), "storage prefix") if base_prefix else ""

    def _object_path(self, path: str) -> str:
        return join_prefix_and_path(self.base_prefix, path)

    def _resolve(self, path: str) -> Path:
        return self.base_path / self._object_path(path)

    def write_bytes(self, path: str, data: bytes, content_type: str | None = None) -> StoredObject:
        object_path = self._object_path(path)
        target = self.base_path / object_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return StoredObject(
            storage_backend=self.backend,
            object_path=object_path,
            physical_uri=f"local://{object_path}",
            content_hash=sha256_bytes(data),
            file_size=len(data),
        )

    def read_bytes(self, path: str) -> bytes:
        return self._resolve(path).read_bytes()

    def exists(self, path: str) -> bool:
        return self._resolve(path).exists()

    def delete(self, path: str) -> None:
        resolved = self._resolve(path)
        if resolved.exists():
            resolved.unlink()

    def list_keys(self, prefix: str) -> list[str]:
        search_root = self._resolve(prefix)
        if not search_root.exists():
            return []
        result: list[str] = []
        for p in search_root.rglob("*"):
            if not p.is_file():
                continue
            rel = p.relative_to(self.base_path).as_posix()
            if self.base_prefix and rel.startswith(self.base_prefix + "/"):
                rel = rel[len(self.base_prefix) + 1:]
            result.append(rel)
        return result

    def uri(self, path: str) -> str:
        return f"local://{self._object_path(path)}"
