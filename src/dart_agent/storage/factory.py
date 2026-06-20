from __future__ import annotations

from dart_agent.config import Settings
from dart_agent.storage.base import Storage
from dart_agent.storage.local import LocalStorage
from dart_agent.storage.s3 import S3Storage


def build_storage(settings: Settings) -> Storage:
    if settings.storage_backend == "local":
        return LocalStorage(settings.local_storage_base_path, base_prefix=settings.s3_base_prefix)
    if settings.storage_backend == "s3":
        if not settings.s3_bucket:
            raise ValueError("S3_BUCKET is required when STORAGE_BACKEND=s3")
        return S3Storage(
            bucket=settings.s3_bucket,
            base_prefix=settings.s3_base_prefix,
            region_name=settings.aws_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )
    raise ValueError(f"unsupported storage backend: {settings.storage_backend}")
