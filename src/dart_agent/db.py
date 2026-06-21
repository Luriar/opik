from __future__ import annotations

from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@lru_cache(maxsize=8)
def engine_from_url(url: str) -> Engine:
    # detail_collector가 batch를 ThreadPoolExecutor로 동시 처리하므로 worker 수만큼
    # 동시 connection이 필요하다. pool_size/max_overflow를 동시성 상한(>=concurrency)보다
    # 넉넉히 둬 connection 대기 병목을 막는다.
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=20,
        max_overflow=20,
        pool_recycle=1800,
        future=True,
    )

