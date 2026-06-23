"""OPIK shared S3 client factory.

Single boto3 client reused across all modules.
Replaces scattered `boto3.client("s3", ...)` calls with one import.
"""

from __future__ import annotations

import functools

import boto3
from botocore.config import Config

from opik_config import S3_BUCKET, S3_REGION


@functools.cache
def get_s3_client(max_pool_connections: int = 20) -> "boto3.client":
    """Return a cached boto3 S3 client.

    max_pool_connections controls the urllib3 connection pool size.
    Use higher values (50) for batch extraction, lower (10) for one-off scripts.
    """
    return boto3.client(
        "s3",
        region_name=S3_REGION,
        config=Config(max_pool_connections=max_pool_connections),
    )


# Convenience: default client for most use cases
s3 = get_s3_client()
