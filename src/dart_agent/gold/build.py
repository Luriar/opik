"""Gold 오케스트레이션 — Silver report.json → Gold(facts/rag_chunk Parquet + e5 임베딩 Parquet).

진입점:
  - run_gold_embed_parquet : Silver _done 기준 증분. facts/rag_chunk Parquet + e5 임베딩 Parquet
                             (gold/rag/embedding, 모델/버전 파티션) 생성. FAISS/Delta 인덱싱은
                             이 Parquet을 입력으로 다운스트림(수집팀)에서 수행.

DART 호출이 없어 quota/차단과 무관(순수 변환 + Parquet write).
멱등: chunk_id/fact_id 결정적, gold _done 마커(정체성 경로)로 skip.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from dart_agent.config import get_settings
from dart_agent.db import engine_from_url
from dart_agent.gold.embedding import build_embedding_provider
from dart_agent.gold.entities import EntityIndex
from dart_agent.gold.processor import GOLD_VERSION, GoldProcessor
from dart_agent.gold.validation import validate_report
from dart_agent.gold.writer import GoldWriter
from dart_agent.silver.transform import SILVER_VERSION
from dart_agent.storage import GoldPaths, SilverPaths, build_storage

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Gold _done 마커 (Silver→Gold 증분/재처리 기준)
# ─────────────────────────────────────────────────────────────────────────────

def _embed_identity(embedder, sink: str) -> tuple[str, str, str]:
    """done 마커 경로 정체성 (sink, model, version). embedder None이면 'none'."""
    return sink, getattr(embedder, "model", None) or "none", getattr(embedder, "version", None) or "none"


def _write_gold_done(storage, corp_code, rcept_no, *, silver_version, counts, embedder,
                     embedding_sink: str = "parquet") -> None:
    """처리 완료 마커를 정체성 경로(sv/gv/sink/model/ver)에 기록한다.

    버전 정체성을 경로에 두므로, done 판정은 마커 파일 read 없이 `list_keys` + set 차집합으로 한다
    (마커 본문은 감사용 메타일 뿐, 증분 hot path에서 읽지 않는다).
    """
    sink, model, version = _embed_identity(embedder, embedding_sink)
    body = {"counts": counts, "processed_at": datetime.now(timezone.utc).isoformat()}
    storage.write_bytes(
        GoldPaths.done_marker(silver_version or "none", GOLD_VERSION, sink, model, version,
                              corp_code, rcept_no),
        json.dumps(body, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        content_type="application/json",
    )


def _gold_done_exists(storage, silver_version, embedder, sink, corp_code, rcept_no) -> bool:
    """단건 done 판정(증분 per-row) — 정체성 경로 마커의 존재만 본다(read 없음)."""
    s, model, version = _embed_identity(embedder, sink)
    return storage.exists(
        GoldPaths.done_marker(silver_version, GOLD_VERSION, s, model, version, corp_code, rcept_no))


def _list_marker_pairs(storage, prefix: str) -> set[tuple[str, str]]:
    """prefix 하위 마커 경로에서 (corp_code, rcept_no) 집합을 모은다(파일 read 없이 경로만 파싱)."""
    pairs: set[tuple[str, str]] = set()
    for key in storage.list_keys(prefix):
        cc = _kv(key, "corp_code")
        rn = key.rsplit("/", 1)[-1].removeprefix("rcept_no=").removesuffix(".json")
        if cc and rn:
            pairs.add((cc, rn))
    return pairs


def _list_current_silver_done_pairs(storage, silver_version: str) -> set[tuple[str, str]]:
    """Collect current Silver done markers without reading marker bodies."""
    pairs = _list_marker_pairs(storage, SilverPaths.done_prefix_for_version(silver_version))
    if silver_version == "v2":
        for key in storage.list_keys(SilverPaths.done_prefix()):
            if "/sv=" in key:
                continue
            cc = _kv(key, "corp_code")
            rn = key.rsplit("/", 1)[-1].removeprefix("rcept_no=").removesuffix(".json")
            if cc and rn:
                pairs.add((cc, rn))
    return pairs


def _scan_pending_targets(storage, want: set[str] | None, done_prefix: str,
                          limit: int, silver_version: str = SILVER_VERSION) -> tuple[list[tuple[str, str]], int]:
    """Silver _done 와 Gold done(정체성 경로)의 set 차집합으로 '아직 처리 안 된' 대상만 limit개 반환.

    done-set은 `done_prefix` 한 번의 `list_keys`로 수집한다(정체성이 경로에 인코딩돼 있어 마커
    파일을 열 필요가 없다 → per-file read 0). silver 마커도 한 번 list. 즉 list 2회 + 메모리 차집합.
    list 순서를 보존해 limit 절단은 결정적이다. 반환: (targets, skipped).
    """
    done = _list_marker_pairs(storage, done_prefix)
    targets: list[tuple[str, str]] = []
    skipped = 0
    for cc, rn in sorted(_list_current_silver_done_pairs(storage, silver_version)):
        if want is not None and cc not in want:
            continue
        if (cc, rn) in done:
            skipped += 1
            continue
        targets.append((cc, rn))
        if len(targets) >= limit:
            break
    return targets[:limit], skipped


def _read_silver(storage, corp_code, report_type, rcept_no) -> tuple[dict, str] | None:
    path = SilverPaths.report(corp_code, report_type, rcept_no)
    return _read_silver_path(storage, path)


def _read_silver_path(storage, path: str) -> tuple[dict, str] | None:
    if not storage.exists(path):
        return None
    try:
        report = json.loads(storage.read_bytes(path).decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        log.warning("gold: silver read failed [%s]: %s", path, exc)
        return None
    return report, storage.uri(path)


def _read_silver_from_done(storage, engine, corp_code: str, rcept_no: str,
                           silver_version: str = SILVER_VERSION) -> tuple[dict, str] | None:
    """Load Silver by the done marker's silver_path, with legacy fallbacks."""
    marker_paths = [
        SilverPaths.done_marker_for_version(silver_version, corp_code, rcept_no),
        SilverPaths.done_marker(corp_code, rcept_no),
    ]
    for marker_path in marker_paths:
        if not storage.exists(marker_path):
            continue
        try:
            marker = json.loads(storage.read_bytes(marker_path).decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            log.warning("gold: silver done marker read failed [%s]: %s", marker_path, exc)
            continue
        silver_path = marker.get("silver_path")
        if silver_path:
            loaded = _read_silver_path(storage, str(silver_path))
            if loaded is not None:
                return loaded

    report_type = _report_type_of(engine, rcept_no)
    loaded = _read_silver(storage, corp_code, report_type, rcept_no)
    if loaded is not None:
        return loaded

    prefix = f"{SilverPaths.reports_prefix()}corp_code={corp_code}/"
    suffix = f"/rcept_no={rcept_no}/report.json"
    for key in storage.list_keys(prefix):
        if key.endswith(suffix):
            loaded = _read_silver_path(storage, key)
            if loaded is not None:
                return loaded
    return None


def _quarantine(storage, report, result) -> None:
    """게이트 ERROR 공시를 격리(적재·done 마커 안 함). 다음 run/full이 재평가하도록 둔다."""
    meta = report.get("_meta") or {}
    corp_code, rcept_no = str(meta.get("corp_code")), str(meta.get("rcept_no"))
    payload = {"corp_code": corp_code, "rcept_no": rcept_no, "issues": result.issues,
               "quarantined_at": datetime.now(timezone.utc).isoformat()}
    storage.write_bytes(GoldPaths.quarantine_marker(corp_code, rcept_no),
                        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8"),
                        content_type="application/json")


def _finalize_parquet(writer) -> dict[str, Any]:
    """배치 마감 — facts/rag_chunk Parquet + e5 임베딩 Parquet."""
    parquet = writer.flush_parquet()
    embeddings = writer.flush_embeddings_parquet()
    writer.write_manifest(parquet)
    return {"parquet_files": len(parquet), "parquet_rows": sum(parquet.values()),
            **{f"embed_{k}": v for k, v in embeddings.items()}}


def run_gold_embed_parquet(corp_codes: list[str] | None = None, limit: int = 50_000,
                           chunk_size: int = 50,
                           max_runtime_seconds: int | None = None,
                           ingest_mode: str = "backfill") -> dict[str, Any]:
    """retarget 진입점 — Silver → Gold Parquet(facts/rag_chunk) + e5 임베딩 Parquet.

    임베딩 결과물을 pgvector가 아니라 Parquet(gold/rag/embedding, 모델/버전 파티션)으로 저장한다.
    FAISS/Delta 인덱싱은 이 Parquet을 입력으로 다운스트림(수집팀)에서 수행한다.
    corp_codes를 주면 해당 기업만, 없으면 Silver _done 마커 기준 전체.

    증분: gold _done 마커(embedding_sink="parquet" + 동일 model/version) 기준으로 이미 임베딩된
    보고서는 skip한다. 재실행·일배치가 신규/변경분만 처리하고 전량 재임베딩을 방지한다.
    마커는 배치 임베딩 Parquet flush가 성공한 뒤에만 쓰므로(마커 존재 = 산출물 존재), 중간
    크래시로 마커만 남아 산출물이 누락되는 false-positive skip이 생기지 않는다.
    """
    settings = get_settings()
    storage = build_storage(settings)
    engine = engine_from_url(settings.service_db_url)
    embedder = build_embedding_provider(settings)
    resolver = EntityIndex.from_engine(engine)
    processor = GoldProcessor(resolver=resolver)
    run_start = datetime.now(timezone.utc)
    base_run_id = run_start.strftime("%Y%m%dT%H%M%SZ")

    # Silver _done와 Gold embed-done(정체성 경로 sink=parquet/model/ver)의 set 차집합으로
    # "아직 이 모델로 임베딩 안 된" 보고서만 target. limit은 (전체 silver가 아니라) not-done 기준
    # 캡 → 일배치 limit이 작아도 신규/변경분 누락 없음. done-set은 list 1회로 수집(per-file read 0).
    want = set(corp_codes) if corp_codes else None
    done_prefix = GoldPaths.done_prefix(SILVER_VERSION, GOLD_VERSION, "parquet",
                                        getattr(embedder, "model", None) or "none",
                                        getattr(embedder, "version", None) or "none")
    targets, skipped = _scan_pending_targets(storage, want, done_prefix, limit)

    built = failed = silver_missing = quarantined = 0
    started_at = time.monotonic()
    stopped_by_runtime_budget = False
    sinks: list[dict] = []
    for batch_idx in range(0, len(targets), chunk_size):
        if max_runtime_seconds is not None and time.monotonic() - started_at >= max_runtime_seconds:
            stopped_by_runtime_budget = True
            break
        batch = targets[batch_idx: batch_idx + chunk_size]
        writer = GoldWriter(storage, f"{base_run_id}_{batch_idx // chunk_size:04d}", embedder=embedder,
                            ingest_mode=ingest_mode)
        # 마커는 flush 성공 후에만 쓴다 → 처리된 보고서를 모아 뒀다가 배치 끝에 기록.
        pending: list[tuple[str, str, Any, dict[str, int]]] = []
        for corp_code, rcept_no in batch:
            try:
                loaded = _read_silver_from_done(storage, engine, corp_code, rcept_no)
                if loaded is None:
                    silver_missing += 1
                    continue
                report, source_uri = loaded
                result = validate_report(report)
                if not result.ok:
                    _quarantine(storage, report, result)
                    quarantined += 1
                    continue
                rs = processor.process(report, source_uri=source_uri, generated_at=run_start.isoformat())
                writer.add(rs)
                meta = report.get("_meta") or {}
                pending.append((str(meta.get("corp_code")), str(meta.get("rcept_no")),
                                meta.get("silver_version"), rs.counts()))
                built += 1
            except Exception as exc:  # noqa: BLE001
                log.error("gold_embed_parquet failed for %s/%s: %s", corp_code, rcept_no, exc)
                failed += 1
        sinks.append(_finalize_parquet(writer))
        # flush 성공 후 done 마커 기록 — 이제 마커는 "임베딩 Parquet 적재 완료"를 의미.
        for cc, rn, sv, counts in pending:
            _write_gold_done(storage, cc, rn, silver_version=sv, counts=counts,
                             embedder=embedder, embedding_sink="parquet")
    attempted = built + silver_missing + quarantined + failed
    return {"targets": len(targets), "built": built, "skipped": skipped,
            "silver_missing": silver_missing, "quarantined": quarantined, "failed": failed,
            "batches": len(sinks), "embedding_model": getattr(embedder, "model", None),
            "stopped_by_runtime_budget": stopped_by_runtime_budget,
            "remaining_targets": max(0, len(targets) - attempted), "sinks": sinks}


def run_gold_backfill(corp_codes: list[str] | None = None, limit: int = 50_000,
                      chunk_size: int = 50) -> dict[str, Any]:
    """Backfill/repair Gold Parquet from all Silver _done targets that are not Gold _done yet."""
    return run_gold_embed_parquet(corp_codes=corp_codes, limit=limit, chunk_size=chunk_size,
                                  ingest_mode="backfill")


def run_gold_incremental(corp_codes: list[str] | None = None, limit: int = 2_000,
                         chunk_size: int = 20,
                         max_runtime_seconds: int = 540) -> dict[str, Any]:
    """Micro-batch Gold Parquet for the Bronze -> Silver -> Gold incremental path."""
    return run_gold_embed_parquet(
        corp_codes=corp_codes,
        limit=limit,
        chunk_size=chunk_size,
        max_runtime_seconds=max_runtime_seconds,
        ingest_mode="incremental",
    )


def _kv(path: str, key: str) -> str | None:
    token = f"{key}="
    for part in path.split("/"):
        if part.startswith(token):
            return part[len(token):]
    return None


def _report_type_of(engine, rcept_no: str) -> str:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT report_type FROM disclosure WHERE rcept_no = :r"), {"r": rcept_no}
        ).first()
    return str(row.report_type) if row and row.report_type else "DISCLOSURE"
