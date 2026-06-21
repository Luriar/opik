"""Gold 계층 — Silver report.json을 목적별 row(공통 메타/유형별 fact/RAG/관계)로 분해.

진입점:
  - run_gold_embed_parquet : Silver _done 기준 증분. facts/rag_chunk Parquet + e5 임베딩 Parquet
  - run_gold_latest_context: 기업별 최신 요약 캐시
핵심 변환기:
  - GoldProcessor(report.json -> GoldRowSet)  ← 순수 변환(테스트 가능)
"""
from dart_agent.gold.build import run_gold_backfill, run_gold_embed_parquet, run_gold_incremental
from dart_agent.gold.compaction import run_gold_compaction
from dart_agent.gold.context import run_gold_latest_context
from dart_agent.gold.entities import EntityIndex, NameResolver, NullResolver, ResolvedEntity
from dart_agent.gold.processor import GOLD_VERSION, GoldProcessor, GoldRowSet
from dart_agent.gold.writer import GoldWriter

__all__ = [
    "run_gold_embed_parquet",
    "run_gold_backfill",
    "run_gold_incremental",
    "run_gold_compaction",
    "run_gold_latest_context",
    "GoldProcessor",
    "GoldRowSet",
    "GoldWriter",
    "EntityIndex",
    "NameResolver",
    "NullResolver",
    "ResolvedEntity",
    "GOLD_VERSION",
]
