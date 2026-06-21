"""Gold Parquet compaction - merge small part-*.parquet into 1 per partition.

Concurrency: only deletes files listed before merge started. New parts written
during merge are safe (will be collected by next run).
"""
from __future__ import annotations

import io
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from dart_agent.config import get_settings
from dart_agent.storage import GoldPaths, build_storage

log = logging.getLogger(__name__)

_DEDUP_KEYS = [
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

_SKIP_DIRS = ("/manifest/", "/_done/", "/_quarantine/")


def _dedup_keys(directory):
    for marker, keys in _DEDUP_KEYS:
        if marker in directory + "/":
            return keys
    return None


def _cast_to_string(arr):
    """Cast an array to string, handling complex types like list<>."""
    if pa.types.is_string(arr.type) or pa.types.is_large_string(arr.type):
        return arr
    try:
        return arr.cast(pa.string())
    except (pa.ArrowNotImplementedError, pa.ArrowInvalid):
        # For list, struct, or other complex types, convert via pandas
        s = arr.to_pandas()
        return pa.array(s.astype(str), type=pa.string())


def _unify_schemas(tables):
    col_types = {}
    for t in tables:
        for col in t.column_names:
            col_types.setdefault(col, set()).add(t.schema.field(col).type)

    conflict_cols = set()
    for col, types in col_types.items():
        if len(types) > 1:
            conflict_cols.add(col)

    if not conflict_cols:
        return tables

    result = []
    for t in tables:
        has_conflict = False
        for c in conflict_cols:
            if c in t.column_names:
                has_conflict = True
                break
        if has_conflict:
            cols = {}
            for cn in t.column_names:
                if cn in conflict_cols:
                    cols[cn] = _cast_to_string(t.column(cn))
                else:
                    cols[cn] = t.column(cn)
            t = pa.table(cols)
        result.append(t)
    return result


def _concat(tables):
    try:
        return pa.concat_tables(tables, promote_options="default")
    except (TypeError, pa.ArrowTypeError):
        unified = _unify_schemas(tables)
        return pa.concat_tables(unified, promote_options="default")


def run_gold_compaction(min_parts_to_merge=2, storage=None):
    if storage is None:
        storage = build_storage(get_settings())
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    keys = []
    for k in storage.list_keys(GoldPaths.root()):
        if k.endswith(".parquet"):
            skip = False
            for s in _SKIP_DIRS:
                if s in k:
                    skip = True
                    break
            if not skip:
                keys.append(k)

    by_dir = defaultdict(list)
    for k in keys:
        by_dir[k.rsplit("/", 1)[0]].append(k)

    merged_dirs = 0
    compacted_rows = 0
    deleted_files = 0
    skipped_dirs = 0

    for directory, files in by_dir.items():
        if len(files) < min_parts_to_merge:
            skipped_dirs += 1
            continue
        files = sorted(files)
        tables = []
        for f in files:
            buf = io.BytesIO(storage.read_bytes(f))
            tab = pq.read_table(buf)
            tables.append(tab)
        merged = _concat(tables)
        dedup = _dedup_keys(directory)
        if dedup:
            all_ok = True
            for c in dedup:
                if c not in merged.column_names:
                    all_ok = False
            if all_ok:
                df = merged.to_pandas()
                df = df.drop_duplicates(subset=dedup, keep="last")
                merged = pa.Table.from_pandas(df, preserve_index=False)
        buf = io.BytesIO()
        pq.write_table(merged, buf, compression="snappy")
        dst_key = "%s/compacted-%s.parquet" % (directory, run_id)
        storage.write_bytes(dst_key, buf.getvalue(),
                            content_type="application/octet-stream")
        for f in files:
            storage.delete(f)
            deleted_files += 1
        merged_dirs += 1
        compacted_rows += merged.num_rows

    log.info(
        "gold compaction: merged_dirs=%s deleted=%s rows=%s",
        merged_dirs, deleted_files, compacted_rows
    )
    return {
        "merged_dirs": merged_dirs,
        "deleted_files": deleted_files,
        "compacted_rows": compacted_rows,
        "skipped_dirs": skipped_dirs,
    }
