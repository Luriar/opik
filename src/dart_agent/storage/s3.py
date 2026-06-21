from __future__ import annotations

from dart_agent.hashing import sha256_bytes
from dart_agent.storage.base import StoredObject
from dart_agent.storage.key_paths import join_prefix_and_path, normalize_relative_path


class S3Storage:
    backend = "s3"

    def __init__(
        self,
        bucket: str,
        base_prefix: str,
        region_name: str,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ) -> None:
        import boto3
        from botocore.config import Config

        self.bucket = bucket
        self.base_prefix = normalize_relative_path(base_prefix.strip("/"), "storage prefix") if base_prefix else ""
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
        self.client = session.client(
            "s3",
            config=Config(
                connect_timeout=10,
                read_timeout=60,
                max_pool_connections=32,
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )

    def _key(self, path: str) -> str:
        return join_prefix_and_path(self.base_prefix, path)

    def write_bytes(self, path: str, data: bytes, content_type: str | None = None) -> StoredObject:
        extra_args = {"ContentType": content_type} if content_type else {}
        key = self._key(path)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, **extra_args)
        return StoredObject(
            storage_backend=self.backend,
            object_path=key,
            physical_uri=f"s3://{self.bucket}/{key}",
            content_hash=sha256_bytes(data),
            file_size=len(data),
        )

    def read_bytes(self, path: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=self._key(path))
        return response["Body"].read()

    def exists(self, path: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(path))
            return True
        except self.client.exceptions.ClientError:
            return False

    def delete(self, path: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=self._key(path))

    def list_keys(self, prefix: str) -> list[str]:
        full_prefix = self._key(prefix)
        result: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                result.append(self._strip_base_prefix(obj["Key"]))
        return result

    def _strip_base_prefix(self, key: str) -> str:
        if self.base_prefix and key.startswith(self.base_prefix + "/"):
            return key[len(self.base_prefix) + 1:]
        return key

    def uri(self, path: str) -> str:
        return f"s3://{self.bucket}/{self._key(path)}"
