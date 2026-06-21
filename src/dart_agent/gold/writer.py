"""Gold writer — row 집합을 facts/rag_chunk Parquet + e5 임베딩 Parquet으로 적재한다.

  - Parquet(정본·분석): 데이터셋별 파티션에 part-{run_id}.parquet. 소파일은 컴팩션 DAG가 병합.
  - 임베딩 Parquet(RAG 입력): embeddable rag_chunk을 e5로 임베딩해 gold/rag/embedding(모델/버전
    파티션)에 저장. FAISS/Delta 인덱싱은 다운스트림(수집팀)이 이 Parquet으로 수행.

멱등: chunk_id/fact_id가 결정적이라 재실행해도 같은 row로 수렴한다.
"""
from __future__ import annotations

import io
import json
import logging
import hashlib
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from dart_agent.gold.processor import GOLD_VERSION, GoldRowSet
from dart_agent.storage.paths import GoldPaths

log = logging.getLogger(__name__)

# RAG 임베딩에서 제외하는 보고서 유형(설계 결정: DISCLOSURE는 DART 구조화 API 부재 →
# 일반공시는 RAG 미제공. 내용 인용은 원문 URL로만). docs/gold-rag/field_roles.draft.yaml 참조.
_EMBED_EXCLUDE_TYPES = {"DISCLOSURE"}
INDEX_VERSION = "faiss-sidecar.v1"


def _safe(value: Any) -> str:
    s = str(value or "").strip()
    return s if s else "NA"


# 데이터셋 → 파티션 경로 빌더(row 필드에서 파티션 값을 읽는다).
def _partition_path(dataset: str, row: dict, run_id: str) -> str:
    g = GoldPaths
    if dataset == "report_registry":
        return g.report_registry(_safe(row.get("rcept_year")), _safe(row.get("rcept_month")), _safe(row.get("report_type")), run_id)
    if dataset == "company_snapshot":
        return g.company_snapshot(_safe(row.get("snapshot_date")), run_id)
    if dataset == "document_text":
        return g.document_text(_safe(row.get("report_type")), _safe(row.get("rcept_year")), _safe(row.get("rcept_month")), run_id)
    if dataset == "financial_statement":
        return g.fact_financial_statement(_safe(row.get("bsns_year")), _safe(row.get("reprt_code")), _safe(row.get("fs_div")), run_id)
    if dataset == "regular_structured":
        return g.fact_regular_structured(_safe(row.get("table_name")), _safe(row.get("rcept_year")), _safe(row.get("rcept_month")), run_id)
    if dataset == "material_event":
        return g.fact_material_event(_safe(row.get("event_type")), _safe(row.get("rcept_year")), _safe(row.get("rcept_month")), run_id)
    if dataset == "ownership":
        return g.fact_ownership(_safe(row.get("ownership_type")), _safe(row.get("rcept_year")), _safe(row.get("rcept_month")), run_id)
    if dataset == "securities":
        return g.fact_securities(_safe(row.get("securities_type")), _safe(row.get("rcept_year")), _safe(row.get("rcept_month")), run_id)
    if dataset == "rag_document":
        return g.rag_document(_safe(row.get("report_type")), _safe(row.get("rcept_year")), _safe(row.get("rcept_month")), run_id)
    if dataset == "rag_chunk":
        return g.rag_chunk(_safe(row.get("report_type")), _safe(row.get("rcept_year")), _safe(row.get("rcept_month")), run_id)
    if dataset == "entity_relation":
        return g.entity_relation(_safe(row.get("relation_type")), _safe(row.get("rcept_year")), _safe(row.get("rcept_month")), run_id)
    raise ValueError(f"unknown dataset: {dataset}")


def _to_parquet_bytes(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows)
    # 전부 None인 컬럼은 null 타입이 되어 parquet 기록이 깨질 수 있다 → string으로 캐스팅.
    for i, field_ in enumerate(table.schema):
        if pa.types.is_null(field_.type):
            table = table.set_column(i, field_.name, table.column(i).cast(pa.string()))
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


class GoldWriter:
    def __init__(self, storage, run_id: str, *, embedder=None, gold_version: str = GOLD_VERSION,
                 ingest_mode: str = "backfill"):
        self.storage = storage
        self.run_id = run_id
        self.embedder = embedder
        self.gold_version = gold_version
        # backfill 베이스 vs incremental 델타 분리(FAISS base+delta 안정화). 임베딩 파티션 경로에 반영된다.
        self.ingest_mode = ingest_mode
        self._buf: dict[str, list[dict]] = defaultdict(list)

    def add(self, rs: GoldRowSet) -> None:
        for name, rows in rs.datasets().items():
            if rows:
                self._buf[name].extend(rows)

    # ── Parquet ────────────────────────────────────────────────────────
    def flush_parquet(self) -> dict[str, int]:
        written: dict[str, int] = {}
        for dataset, rows in self._buf.items():
            by_path: dict[str, list[dict]] = defaultdict(list)
            for r in rows:
                by_path[_partition_path(dataset, r, self.run_id)].append(r)
            for path, prows in by_path.items():
                self.storage.write_bytes(path, _to_parquet_bytes(prows), content_type="application/octet-stream")
                written[path] = len(prows)
        return written

    # ── 임베딩 Parquet (FAISS/Delta 다운스트림 입력) ─────────────────────
    def flush_embeddings_parquet(self) -> dict[str, int]:
        """embeddable rag_chunk을 e5로 임베딩해 gold/rag/embedding Parquet에 저장한다.

        pgvector 대신 Parquet을 1차 산출물로 둔다(임베딩 결과는 모델/버전 파티션).
        chunk_id·content_hash를 함께 저장해 다운스트림(FAISS/Delta)이 멱등 재빌드 가능.
        """
        # per-type 정책: DISCLOSURE는 RAG 제외(벡터 미생성). SECURITIES_REGISTRATION은
        # processor의 포인터형 청크를 그대로 임베딩(종류·제목·일자→URL 인용용).
        chunks = [
            c for c in self._buf.get("rag_chunk", [])
            if c.get("embeddable") and c.get("rcept_dt")
            and str(c.get("report_type")) not in _EMBED_EXCLUDE_TYPES
        ]
        if not chunks or self.embedder is None:
            return {"embedded": 0, "files": 0}
        vectors = self.embedder.embed([c["chunk_text"] for c in chunks])
        embedding_created_at = datetime.now(timezone.utc).isoformat()
        model_safe = self.embedder.model.replace("/", "_")
        by_path: dict[str, list[dict]] = defaultdict(list)
        for c, vec in zip(chunks, vectors):
            content_hash = c["content_hash"]
            embedding_id = hashlib.sha256(
                f"{c['chunk_id']}|{self.embedder.model}|{self.embedder.version}|{self.embedder.dim}|ip|{content_hash}".encode("utf-8")
            ).hexdigest()
            row = {
                "embedding_id": embedding_id,
                "chunk_id": c["chunk_id"],
                "rcept_no": c["rcept_no"],
                "corp_code": c["corp_code"],
                "stock_code": c.get("stock_code"),
                "report_type": c.get("report_type"),
                "base_report_type": c.get("base_report_type"),
                "rcept_dt": c.get("rcept_dt"),
                "is_latest": bool(c.get("is_latest", True)),
                "original_rcept_no": c.get("original_rcept_no"),
                "latest_rcept_no": c.get("latest_rcept_no"),
                "is_amended": c.get("is_amended"),
                "amendment_seq": c.get("amendment_seq"),
                # 다운스트림 필터/rerank 메타(임베딩 본문 아님): API 출처·중요도·결정적 키워드.
                "api_name": c.get("api_name"),
                "api_group": c.get("api_group"),
                "keywords": list(c.get("keywords") or ["미분류"]),
                "dart_view_url": c.get("dart_view_url"),
                "source_uri": c.get("source_uri"),
                "source_system": c.get("source_system"),
                "source_silver_dataset": c.get("source_silver_dataset"),
                "source_record_key": c.get("source_record_key"),
                "source_outer_rcept_no": c.get("source_outer_rcept_no"),
                "source_outer_dart_view_url": c.get("source_outer_dart_view_url"),
                "source_fact_ids": list(c.get("source_fact_ids") or []),
                "content_hash": content_hash,
                "embedding_provider": getattr(self.embedder, "provider", None),
                "embedding_model": self.embedder.model,
                "embedding_version": self.embedder.version,
                "dim": self.embedder.dim,
                "metric": "ip",
                "embedding_metric": "ip",
                "normalized": True,
                "index_version": INDEX_VERSION,
                "valid_from": embedding_created_at,
                "valid_to": None,
                "embedding_created_at": embedding_created_at,
                "embedding": [float(x) for x in vec],
            }
            path = GoldPaths.embedding(model_safe, self.embedder.version,
                                       _safe(c.get("rcept_year")), _safe(c.get("rcept_month")), self.run_id,
                                       ingest_mode=self.ingest_mode)
            by_path[path].append(row)
        for path, prows in by_path.items():
            self.storage.write_bytes(path, _to_parquet_bytes(prows), content_type="application/octet-stream")
        # incremental 델타는 manifest(changelog)에 기록 → FAISS가 watermark 이후만 add.
        if self.ingest_mode == "incremental":
            self._write_delta_manifest(sorted(by_path.keys()), len(chunks), embedding_created_at)
        return {"embedded": len(chunks), "files": len(by_path)}

    def _write_delta_manifest(self, parquet_paths: list[str], chunk_count: int, created_at: str) -> None:
        """증분 임베딩 1배치의 델타 로그. FAISS base+delta 동기화 watermark(run_id) 기준점."""
        partitions = sorted({p.rsplit("/", 1)[0] for p in parquet_paths})
        manifest = {
            "run_id": self.run_id,
            "ingest_mode": self.ingest_mode,
            "embedding_provider": getattr(self.embedder, "provider", None),
            "embedding_model": self.embedder.model,
            "embedding_version": self.embedder.version,
            "dim": self.embedder.dim,
            "metric": "ip",
            "index_version": INDEX_VERSION,
            "chunk_count": chunk_count,
            "partitions": partitions,        # compaction-safe: 벡터의 현 상태는 이 파티션에서 chunk_id로 upsert
            "parquet_paths": parquet_paths,  # 작성 시점(컴팩션 전) part 경로
            "created_at": created_at,
        }
        self.storage.write_bytes(
            GoldPaths.embedding_delta_manifest(self.ingest_mode, self.run_id),
            json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode("utf-8"),
            content_type="application/json")

    # ── manifest + gold 마커 ───────────────────────────────────────────
    def write_manifest(self, parquet_written: dict[str, int]) -> str:
        manifest = {
            "run_id": self.run_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "gold_version": self.gold_version,
            "datasets": {k: len(v) for k, v in self._buf.items()},
            "parquet_paths": parquet_written,
        }
        path = GoldPaths.manifest(self.run_id)
        self.storage.write_bytes(path, json.dumps(manifest, ensure_ascii=False).encode("utf-8"),
                                 content_type="application/json")
        return path
