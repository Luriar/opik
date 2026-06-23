from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class StoredObject:
    storage_backend: str
    object_path: str
    physical_uri: str
    content_hash: str
    file_size: int


class Storage(Protocol):
    backend: str

    def write_bytes(self, path: str, data: bytes, content_type: str | None = None) -> StoredObject:
        raise NotImplementedError

    def read_bytes(self, path: str) -> bytes:
        raise NotImplementedError

    def exists(self, path: str) -> bool:
        raise NotImplementedError

    def delete(self, path: str) -> None:
        raise NotImplementedError

    def list_keys(self, prefix: str) -> list[str]:
        """prefix 하위 객체들의 object_path 목록(base_prefix 제거)을 반환한다.

        마커 set-difference 증분(complete/_done 마커 비교)에 쓴다.
        """
        raise NotImplementedError

    def uri(self, path: str) -> str:
        raise NotImplementedError
