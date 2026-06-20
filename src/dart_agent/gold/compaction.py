"""Gold Parquet 컴팩션 — 증분이 만든 작은 part-*.parquet를 파티션별 1파일로 병합.

근실시간(분 주기)은 파티션마다 part-{run_id}.parquet를 계속 쌓는다. 그대로 두면 소파일이
누적돼 조회(planning)·S3 LIST/메타 비용이 커진다. 이 배치가 파티션 디렉터리 단위로:
  여러 part/compacted .parquet → 자연키 dedup(최신 유지) → compacted-{run_id}.parquet 1개로 병합
하고 원본을 삭제한다.

동시성: 병합 시작 시점에 list한 파일만 삭제한다. 그 후 증분이 쓴 새 part는 list에 없어 보존되며
다음 컴팩션이 흡수한다(멱등·무손실). manifest/_done/_quarantine/dim 디렉터리는 건드리지 않는다.
"""
from __future__ import annotations

import io
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from dart_agent.config import get_settings
from dart_agent.storage import GoldPaths, build_storage

log = logging.getLogger(__name__)

# 파티션 디렉터리(경로 일부) → dedup 자연키 컬럼. 가장 먼저 매칭되는 것을 쓴다.
_DEDUP_KEYS: list[tuple[str, list[str]]] = [
    ("/report_registry/", ["doc_id"]),
    ("/company_snapshot/", ["corp_code"]),
    ("/dim/company_dictionary/", ["corp_code"]),
    ("/document_text/", ["doc_id", "section_no"]),
    ("/facts/financial_statement/", ["fact_id"]),
    ("/facts/regular_structured/", ["fact_id"]),
    ("/facts/material_event/", ["event_id"]),
    ("/facts/ownership/", ["ownership_fact_id"]),
    ("/facts/securities/", ["securities_fact_id"]),
    ("/rag/rag_document/", ["rag_doc_id"]),
    ("/rag/rag_chunk/", ["chunk_id"]),
    ("/rag/entity_relation/", ["relation_id"]),
    ("/rag/embedding/", ["chunk_id"]),
]
# 컴팩션 제외 디렉터리(마커/매니페스트).
_SKIP_DIRS = ("/manifest/", "/_done/", "/_quarantine/")


def _dedup_keys(directory: str) -> list[str] | None:
    for marker, keys in _DEDUP_KEYS:
        if marker in directory + "/":
            return keys
    return None


def _concat(tables: list[pa.Table]) -> pa.Table:
    # part 파일마다 컬럼 집합이 조금 다를 수 있어 schema promote로 통합.
    try:
        return pa.concat_tables(tables, promote_options="default")
    except TypeError:  # 구버전 pyarrow 호환.
        return pa.concat_tables(tables, promote=True)


def run_gold_compaction(min_parts_to_merge: int = 2, storage=None) -> dict[str, Any]:
    """파티션 디렉터리별로 part 파일을 1개로 병합(dedup)하고 원본을 삭제한다.

    storage: 테스트 주입용. None이면 설정에서 만든다.
    """
    if storage is None:
        storage = build_storage(get_settings())
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    keys = [k for k in storage.list_keys(GoldPaths.root())
            if k.endswith(".parquet") and not any(s in k for s in _SKIP_DIRS)]
    by_dir: dict[str, list[str]] = defaultdict(list)
    for k in keys:
        by_dir[k.rsplit("/", 1)[0]].append(k)

    merged_dirs = compacted_rows = deleted_files = skipped_dirs = 0
    for directory, files in by_dir.items():
        if len(files) < min_parts_to_merge:
            skipped_dirs += 1
            continue
        files = sorted(files)  # run_id 타임스탬프 정렬 → 뒤가 최신(keep='last').
        tables = [pq.read_table(io.BytesIO(storage.read_bytes(f))) for f in files]
        merged = _concat(tables)
        dedup = _dedup_keys(directory)
        if dedup and all(c in merged.column_names for c in dedup):
            df = merged.to_pandas().drop_duplicates(subset=dedup, keep="last")
            merged = pa.Table.from_pandas(df, preserve_index=False)
        buf = io.BytesIO()
        pq.write_table(merged, buf, compression="snappy")
        storage.write_bytes(f"{directory}/compacted-{run_id}.parquet", buf.getvalue(),
                            content_type="application/octet-stream")
        for f in files:  # 시작 시점 list한 원본만 삭제(이후 새 part는 보존).
            storage.delete(f)
            deleted_files += 1
        merged_dirs += 1
        compacted_rows += merged.num_rows

    log.info("gold compaction: merged_dirs=%s deleted=%s rows=%s", merged_dirs, deleted_files, compacted_rows)
    return {"merged_dirs": merged_dirs, "deleted_files": deleted_files,
            "compacted_rows": compacted_rows, "skipped_dirs": skipped_dirs}
