"""저장 object path 단일 출처.

각 수집 단계가 Bronze/Silver/Gold에 저장하는 root 기준 object path를 이 한 곳에서 만든다.
호출부(workflows)는 경로 문자열을 직접 조립하지 않고 여기 메서드만 쓴다.

object path 규약:
    {layer_prefix}/<artifact>/<partition...>/<file>

    layer_prefix는 env 변수로 제어한다:
      BRONZE_PREFIX  (기본: bronze/dart)
      SILVER_PREFIX  (기본: silver/dart)
      GOLD_PREFIX    (기본: gold/dart)

- object path는 로컬/S3 공통이며, backend별 실제 위치(physical_uri)는 storage adapter가 변환한다.
- partition은 OpenDART 식별자(observed_date, corp_cls, rcept_dt, rcept_no, corp_code, bsns_year, reprt_code 등)
  를 `key=value` 형태로 둬서 사람이 읽고 prefix로 조회하기 쉽게 한다.

계층 역할:
    Bronze  — API 응답 원본 보존 (append-only, immutable). 폴더 구조로 수집 완결도 확인 가능.
    Silver  — Bronze를 취합한 보고서 단위 JSON. rcept_no 하나당 report.json 하나.
              사람이 읽기 쉬운 형태, Gold/VectorDB 입력으로 사용.
    Gold    — Silver → facts/rag_chunk/embedding Parquet.

경로 규약이 바뀌면 이 모듈과 docs/data-model/storage-paths.md를 함께 고치고 docs/history/change-log.md에 이력을 남긴다.
"""

from __future__ import annotations

import os

from dart_agent.dates import DateWindow

_FALLBACK_BRONZE = "bronze/dart"
_FALLBACK_SILVER = "silver/dart"
_FALLBACK_GOLD = "gold/dart"


def _bronze_prefix() -> str:
    raw = os.getenv("BRONZE_PREFIX", "").strip().strip("/")
    return raw if raw else _FALLBACK_BRONZE


def _silver_prefix() -> str:
    raw = os.getenv("SILVER_PREFIX", "").strip().strip("/")
    return raw if raw else _FALLBACK_SILVER


def _gold_prefix() -> str:
    raw = os.getenv("GOLD_PREFIX", "").strip().strip("/")
    return raw if raw else _FALLBACK_GOLD


class BronzePaths:
    """Bronze 계층 object path 빌더.

    폴더 구조 자체가 수집 완결도를 나타낸다.
    어떤 날짜/기업/보고서까지 수집이 완료됐는지 prefix 조회로 확인 가능하도록 설계한다.

    파일이 존재한다 = 해당 API 응답이 수집 완료됨.
    파일이 없다 = 미수집 또는 NoData.
    """

    # ─── DS001 기업 마스터 / 공시 목록 ───────────────────────────────────

    @staticmethod
    def corp_code(observed_date: str) -> str:
        """DS001 corpCode.xml ZIP 원본(고유번호 마스터)."""
        return f"{_bronze_prefix()}/corp_code/observed_date={observed_date}/corpCode.zip"

    @staticmethod
    def listed_company(observed_date: str) -> str:
        """상장사 명단 산출에 사용한 corpCode.xml(압축해제본)."""
        return f"{_bronze_prefix()}/listed_company/observed_date={observed_date}/corp_code.xml"

    @staticmethod
    def disclosure_list(
        corp_cls: str,
        rcept_window: DateWindow,
        page_no: int,
        *,
        ingest_mode: str,
        run_id: str,
    ) -> str:
        """DS001 list.json 공시검색 페이지 원본.

        이 메서드는 하루 단위 DateWindow만 허용한다.
        기간 backfill은 workflow에서 일자별로 쪼개서 여러 번 호출한다.
        """
        if page_no <= 0:
            raise ValueError("page_no must be >= 1")

        rcept_dt_from = rcept_window.start.strftime("%Y%m%d")
        rcept_dt_to = rcept_window.end.strftime("%Y%m%d")

        if rcept_dt_from != rcept_dt_to:
            raise ValueError(
                "disclosure_list path requires a single-day DateWindow. "
                f"got rcept_dt_from={rcept_dt_from}, rcept_dt_to={rcept_dt_to}. "
                "Split range backfill by day in workflow."
            )

        return (
            f"{_bronze_prefix()}/list/"
            f"corp_cls={corp_cls}/"
            f"rcept_dt={rcept_dt_from}/"
            f"ingest_mode={ingest_mode}/"
            f"run_id={run_id}/"
            f"page_no={page_no:06d}.json"
        )

    @staticmethod
    def disclosure_list_prefix(corp_cls: str | None = None, rcept_dt: str | None = None) -> str:
        """DS001 list.json prefix. S3 Bronze replay/repair에서 기존 list 파일을 찾는 데 쓴다."""
        parts = [f"{_bronze_prefix()}/list"]
        if corp_cls:
            parts.append(f"corp_cls={corp_cls}")
        if rcept_dt:
            parts.append(f"rcept_dt={rcept_dt}")
        return "/".join(parts) + "/"

    @staticmethod
    def company_overview(corp_code: str) -> str:
        """DS001 /api/company.json 기업개황 원본.

        기업 기본정보 (대표자, 주소, 사업내용, 회계월 등). COMPANY_OVERVIEW job.
        """
        return f"{_bronze_prefix()}/company_overview/corp_code={corp_code}/response.json"

    # ─── DS001 원문 ──────────────────────────────────────────────────────

    @staticmethod
    def document(rcept_no: str) -> str:
        """DS001 document.xml 원문 ZIP. DISCLOSURE_DOCUMENT job (선택적 수집)."""
        return f"{_bronze_prefix()}/document/rcept_no={rcept_no}/original.zip"

    # ─── DS003 재무정보 ──────────────────────────────────────────────────

    @staticmethod
    def financial_statement(corp_code: str, bsns_year: str, reprt_code: str, fs_div: str) -> str:
        """DS003 fnlttSinglAcntAll.json 재무제표 원본. FINANCIAL_STATEMENT_ALL job."""
        return (
            f"{_bronze_prefix()}/financials/"
            f"corp_code={corp_code}/"
            f"bsns_year={bsns_year}/"
            f"reprt_code={reprt_code}/"
            f"fs_div={fs_div}/"
            f"response.json"
        )

    # ─── DS002 / DS004 구조화 API (FINANCIAL 패턴: corp_code+bsns_year+reprt_code) ─

    @staticmethod
    def structured_report(
        api_group: str,
        api_name: str,
        corp_code: str,
        bsns_year: str | None = None,
        reprt_code: str | None = None,
    ) -> str:
        """DS002/DS004 구조화 API 응답 원본. STRUCTURED_REPORT job.

        파라미터 패턴:
          - FINANCIAL: corp_code + bsns_year + reprt_code  (정기보고서 주요정보)
          - HOLDING:   corp_code만                          (지분공시 등 연간 파라미터 없음)
        """
        parts = [
            f"{_bronze_prefix()}/structured/{api_group}/{api_name}",
            f"corp_code={corp_code}",
        ]
        if bsns_year:
            parts.append(f"bsns_year={bsns_year}")
        if reprt_code:
            parts.append(f"reprt_code={reprt_code}")

        return "/".join(parts) + "/response.json"

    # ─── DS005 주요사항보고서 (EVENT 패턴: corp_code+bgn_de+end_de) ──────

    @staticmethod
    def event_report(
        api_group: str,
        api_name: str,
        corp_code: str,
        bgn_de: str,
        end_de: str,
    ) -> str:
        """DS005 주요사항보고서 API 응답 원본. EVENT_REPORT job.

        기간 파라미터(bgn_de/end_de) 기준으로 파티션.
        """
        return (
            f"{_bronze_prefix()}/event_report/{api_group}/{api_name}/"
            f"corp_code={corp_code}/"
            f"bgn_de={bgn_de}/"
            f"end_de={end_de}/"
            f"response.json"
        )

    # ─── DS006 증권신고서 (EVENT 패턴: corp_code+bgn_de+end_de) ─────────

    @staticmethod
    def securities_report(
        api_group: str,
        api_name: str,
        corp_code: str,
        bgn_de: str,
        end_de: str,
    ) -> str:
        """DS006 증권신고서 API 응답 원본. SECURITIES_REPORT job.

        기간 파라미터(bgn_de/end_de) 기준으로 파티션.
        """
        return (
            f"{_bronze_prefix()}/securities_report/{api_group}/{api_name}/"
            f"corp_code={corp_code}/"
            f"bgn_de={bgn_de}/"
            f"end_de={end_de}/"
            f"response.json"
        )

    # ─── 수집 완결 마커 ──────────────────────────────────────────────────

    @staticmethod
    def complete_marker(corp_code: str, rcept_no: str) -> str:
        """공시 1건의 Bronze 수집 완결 마커.

        이 공시의 expected Bronze 산출물이 전부 present(또는 013 nodata)일 때
        completion 단계가 작성한다. Silver는 이 마커를 증분 처리 기준으로 쓴다.
        corp_code 파티션으로 RAG/기업단위 접근과 prefix 조회에 맞춘다.
        """
        return f"{_bronze_prefix()}/complete/corp_code={corp_code}/rcept_no={rcept_no}.json"

    @staticmethod
    def complete_prefix() -> str:
        """complete 마커 전체 prefix(list_keys용)."""
        return f"{_bronze_prefix()}/complete/"


class SilverPaths:
    """Silver 계층 object path 빌더.

    Silver = Bronze를 취합한 보고서 단위 JSON.
    rcept_no 하나당 report.json 하나. 사람이 보고 분석 가능한 형태.

    경로 패턴:
        {silver_prefix}/reports/corp_code={corp_code}/rcept_dt={rcept_dt}/rcept_no={rcept_no}/report.json
        {silver_prefix}/company_overview/corp_code={corp_code}/overview.json

    설계 근거:
        - corp_code 최상위 파티션: RAG builder가 기업 단위로 접근하므로 필수
        - rcept_dt 하위 파티션: 날짜 범위 조회 및 수집 완결도 확인
        - rcept_no 최하위 파티션: 보고서 단위 idempotent 갱신 가능
        - 파일 하나 = 보고서 하나 → 소파일 우려 있으나 Gold에서 Parquet으로 병합
    """

    @staticmethod
    def report(corp_code: str, report_type: str, rcept_no: str) -> str:
        """공시 보고서 Silver JSON.

        하나의 rcept_no에 대한 완전한 보고서 (메타 + 재무 + 구조화 항목 등).
        파티션: corp_code(RAG 기업 단위) > report_type(타입별 접근). rcept_dt는 경로에서 빼고
        report.json _meta에 둔다 — 날짜 조회는 Gold parquet/vector의 컬럼·메타 필터로 한다.
        """
        return (
            f"{_silver_prefix()}/reports/"
            f"corp_code={corp_code}/"
            f"report_type={report_type}/"
            f"rcept_no={rcept_no}/"
            f"report.json"
        )

    @staticmethod
    def reports_prefix() -> str:
        """Silver reports root prefix for discovery fallback."""
        return f"{_silver_prefix()}/reports/"

    @staticmethod
    def company_overview(corp_code: str) -> str:
        """기업개황 Silver JSON. DS001 /api/company.json을 Silver 포맷으로 정제한 것."""
        return f"{_silver_prefix()}/company_overview/corp_code={corp_code}/overview.json"

    @staticmethod
    def done_marker(corp_code: str, rcept_no: str) -> str:
        """Silver 처리 완료 마커.

        report.json 생성을 끝낸 공시에 대해 Silver가 작성한다. Gold 증분 처리와
        Silver 재처리(silver_version 불일치 시) 판단의 기준이 된다.
        """
        return f"{_silver_prefix()}/_done/corp_code={corp_code}/rcept_no={rcept_no}.json"

    @staticmethod
    def done_prefix() -> str:
        """Silver _done 마커 전체 prefix(list_keys용)."""
        return f"{_silver_prefix()}/_done/"

    @staticmethod
    def done_marker_for_version(silver_version: str, corp_code: str, rcept_no: str) -> str:
        """Versioned Silver done marker used for read-free incremental checks."""
        return (
            f"{_silver_prefix()}/_done/"
            f"sv={silver_version}/"
            f"corp_code={corp_code}/"
            f"rcept_no={rcept_no}.json"
        )

    @staticmethod
    def done_prefix_for_version(silver_version: str) -> str:
        """Versioned Silver done marker prefix."""
        return f"{_silver_prefix()}/_done/sv={silver_version}/"


class GoldPaths:
    """Gold 계층 object path 빌더.

    Gold = Silver report.json을 '목적별 row grain'으로 분해한 Parquet 데이터셋 + RAG 청크.
    보고서 파일 단위(per-corp blob)가 아니라, 공통 메타 / 유형별 fact / RAG / 관계로 분리한다.

    설계 근거(파티션):
        - 분석 쿼리는 다기업·기간 조회가 기본 → 기간(rcept_year/month) 또는 회계연도 파티션 +
          corp_code는 '컬럼'. corp_code를 메인 파티션으로 쓰면 (기업 수 × 날짜) 소파일 폭발.
        - 근실시간(3~10분) 마이크로배치는 part-{run_id}.parquet 로 적재하고, 컴팩션 DAG가
          파티션 단위로 주기적으로 병합한다(manifest/run_id 로 재현·추적).
        - RAG는 chunk 단위 임베딩 Parquet(gold/rag/embedding)이 정본·다운스트림(FAISS/Delta) 입력.

    경로 규약: {gold_prefix}/<dataset>/<partition...>/part-{run_id}.parquet

    데이터셋:
        report_registry  company_snapshot  document_text
        facts/{financial_statement, regular_structured, material_event, ownership, securities}
        rag/{rag_document, rag_chunk, entity_relation, embedding}
        serving/{latest_company_context}
        dim/{company_dictionary}      ← 코스피·코스닥+전체 법인 사전(name↔corp_code 재연결용)
    """

    # ─── 공통 메타 / 기업 ────────────────────────────────────────────────

    @staticmethod
    def report_registry(rcept_year: str, rcept_month: str, report_type: str, run_id: str) -> str:
        """모든 보고서 공통 메타(외부 report.json 1건 = 1 row)."""
        return (
            f"{_gold_prefix()}/report_registry/"
            f"rcept_year={rcept_year}/rcept_month={rcept_month}/report_type={report_type}/"
            f"part-{run_id}.parquet"
        )

    @staticmethod
    def company_snapshot(snapshot_date: str, run_id: str) -> str:
        """기업 개요 스냅샷(corp_code + snapshot_date = 1 row)."""
        return f"{_gold_prefix()}/company_snapshot/snapshot_date={snapshot_date}/part-{run_id}.parquet"

    @staticmethod
    def company_dictionary(snapshot_date: str, run_id: str) -> str:
        """코스피·코스닥 + 전체 법인 사전. entity_relation 재연결(name↔corp_code)의 단일 출처."""
        return f"{_gold_prefix()}/dim/company_dictionary/snapshot_date={snapshot_date}/part-{run_id}.parquet"

    @staticmethod
    def document_text(report_type: str, rcept_year: str, rcept_month: str, run_id: str) -> str:
        """원문/본문 텍스트(doc_id + section_no = 1 row). 원문계열만(생성텍스트는 rag_document)."""
        return (
            f"{_gold_prefix()}/document_text/"
            f"report_type={report_type}/rcept_year={rcept_year}/rcept_month={rcept_month}/"
            f"part-{run_id}.parquet"
        )

    # ─── facts (유형별 구조화) ───────────────────────────────────────────

    @staticmethod
    def fact_financial_statement(bsns_year: str, reprt_code: str, fs_div: str, run_id: str) -> str:
        """정기보고서 재무제표 계정 1개 = 1 row."""
        return (
            f"{_gold_prefix()}/facts/financial_statement/"
            f"bsns_year={bsns_year}/reprt_code={reprt_code}/fs_div={fs_div}/"
            f"part-{run_id}.parquet"
        )

    @staticmethod
    def fact_regular_structured(table_name: str, rcept_year: str, rcept_month: str, run_id: str) -> str:
        """정기보고서 구조화 항목(structured.<table_name> row 1개 = 1 row)."""
        return (
            f"{_gold_prefix()}/facts/regular_structured/"
            f"table_name={table_name}/rcept_year={rcept_year}/rcept_month={rcept_month}/"
            f"part-{run_id}.parquet"
        )

    @staticmethod
    def fact_material_event(event_type: str, rcept_year: str, rcept_month: str, run_id: str) -> str:
        """주요사항보고서 이벤트(event_reports.<event_type> row 1개 = 1 row)."""
        return (
            f"{_gold_prefix()}/facts/material_event/"
            f"event_type={event_type}/rcept_year={rcept_year}/rcept_month={rcept_month}/"
            f"part-{run_id}.parquet"
        )

    @staticmethod
    def fact_ownership(ownership_type: str, rcept_year: str, rcept_month: str, run_id: str) -> str:
        """지분/임원·주요주주(내부 row 기준; rcept_year/month는 내부 row의 rcept_dt 기준)."""
        return (
            f"{_gold_prefix()}/facts/ownership/"
            f"ownership_type={ownership_type}/rcept_year={rcept_year}/rcept_month={rcept_month}/"
            f"part-{run_id}.parquet"
        )

    @staticmethod
    def fact_securities(securities_type: str, rcept_year: str, rcept_month: str, run_id: str) -> str:
        """증권신고서 발행 정보(securities.<type> row 1개 = 1 row; 빈 배열은 row 미생성)."""
        return (
            f"{_gold_prefix()}/facts/securities/"
            f"securities_type={securities_type}/rcept_year={rcept_year}/rcept_month={rcept_month}/"
            f"part-{run_id}.parquet"
        )

    # ─── RAG ─────────────────────────────────────────────────────────────

    @staticmethod
    def rag_document(report_type: str, rcept_year: str, rcept_month: str, run_id: str) -> str:
        """RAG 문서(원문 또는 구조화→설명문). rag_doc 1개 = 1 row."""
        return (
            f"{_gold_prefix()}/rag/rag_document/"
            f"report_type={report_type}/rcept_year={rcept_year}/rcept_month={rcept_month}/"
            f"part-{run_id}.parquet"
        )

    @staticmethod
    def rag_chunk(report_type: str, rcept_year: str, rcept_month: str, run_id: str) -> str:
        """RAG 검색 단위(chunk 1개 = 1 row). VectorDB 적재 입력."""
        return (
            f"{_gold_prefix()}/rag/rag_chunk/"
            f"report_type={report_type}/rcept_year={rcept_year}/rcept_month={rcept_month}/"
            f"part-{run_id}.parquet"
        )

    @staticmethod
    def entity_relation(relation_type: str, rcept_year: str, rcept_month: str, run_id: str) -> str:
        """보고서에서 언급된 기업/인물/주요주주/거래상대방 관계 1개 = 1 row."""
        return (
            f"{_gold_prefix()}/rag/entity_relation/"
            f"relation_type={relation_type}/rcept_year={rcept_year}/rcept_month={rcept_month}/"
            f"part-{run_id}.parquet"
        )

    @staticmethod
    def embedding(model: str, version: str, rcept_year: str, rcept_month: str, run_id: str,
                  ingest_mode: str = "backfill") -> str:
        """chunk embedding 결과(chunk 1개 = embedding 1개).

        파티션: model/version(재임베딩 분리) → ingest_mode(backfill 베이스 vs incremental 델타 분리)
        → 접수 연/월. ingest_mode 분리로 FAISS가 backfill을 base 인덱스로 1회 빌드하고 incremental은
        델타로 add하여, 3분 증분에도 전량 재빌드 없이 안정적으로 유지할 수 있다.
        주의: ingest_mode 파티션이 없는 레거시 경로(`.../version=X/rcept_year=...`)는 backfill 베이스로 간주한다.
        """
        return (
            f"{_gold_prefix()}/rag/embedding/"
            f"model={model}/version={version}/ingest_mode={ingest_mode}/"
            f"rcept_year={rcept_year}/rcept_month={rcept_month}/"
            f"part-{run_id}.parquet"
        )

    @staticmethod
    def embedding_delta_manifest(ingest_mode: str, run_id: str) -> str:
        """증분 임베딩 델타 매니페스트(append-only 로그, run 단위 1파일).

        FAISS가 watermark(마지막 처리 run_id) 이후의 manifest만 읽어 델타를 add하는 changelog.
        경로가 `embedding`(parquet)과 분리돼 있고 .json이라 compaction(.parquet만 병합) 대상에서 제외된다.
        컴팩션으로 part 파일이 합쳐져도 manifest는 보존되며, 벡터의 단일 출처는 ingest_mode 파티션의
        현재 상태(chunk_id로 upsert)다 — manifest는 "언제 무엇이 추가됐나"의 추적용이다.
        """
        return (
            f"{_gold_prefix()}/rag/embedding_delta/"
            f"ingest_mode={ingest_mode}/run_id={run_id}.json"
        )

    # ─── serving (서비스용 캐시, 정본 아님) ───────────────────────────────

    @staticmethod
    def latest_company_context(snapshot_date: str, run_id: str) -> str:
        """기업별 최신 요약 캐시(corp_code + snapshot_date = 1 row)."""
        return (
            f"{_gold_prefix()}/serving/latest_company_context/"
            f"snapshot_date={snapshot_date}/part-{run_id}.parquet"
        )

    # ─── 처리 완결 마커 / manifest ───────────────────────────────────────

    @staticmethod
    def _safe_model(model: str | None) -> str:
        return (model or "none").replace("/", "_")

    @staticmethod
    def done_marker(silver_version: str, gold_version: str, sink: str, model: str | None,
                    version: str, corp_code: str, rcept_no: str) -> str:
        """Gold 처리 완료 마커. 버전 정체성(silver/gold version·sink·embed model/version)을 **경로에
        인코딩**한다 → done 판정을 마커 파일 read 없이 `list_keys` + set 차집합으로 수행(O(n) read 제거)."""
        return (
            f"{_gold_prefix()}/_done/"
            f"sv={silver_version}/gv={gold_version}/sink={sink}/"
            f"model={GoldPaths._safe_model(model)}/ver={version}/"
            f"corp_code={corp_code}/rcept_no={rcept_no}.json"
        )

    @staticmethod
    def done_prefix(silver_version: str | None = None, gold_version: str | None = None,
                    sink: str | None = None, model: str | None = None,
                    version: str | None = None) -> str:
        """done 마커 list_keys prefix. 상위부터 연속으로 주어진 정체성 컴포넌트까지의 최심 prefix를
        만든다(중간이 None이면 거기서 멈춤). set-difference 대상 done-set을 1회 list로 수집하는 데 쓴다."""
        parts = [f"{_gold_prefix()}/_done"]
        for key, val in (
            ("sv", silver_version), ("gv", gold_version), ("sink", sink),
            ("model", None if model is None else GoldPaths._safe_model(model)), ("ver", version),
        ):
            if val is None:
                break
            parts.append(f"{key}={val}")
        return "/".join(parts) + "/"

    @staticmethod
    def quarantine_marker(corp_code: str, rcept_no: str) -> str:
        """인코딩/품질 게이트 ERROR로 격리된 공시 마커(적재 안 함, 가시성·재시도용)."""
        return f"{_gold_prefix()}/_quarantine/corp_code={corp_code}/rcept_no={rcept_no}.json"

    @staticmethod
    def manifest(run_id: str) -> str:
        """run 단위 manifest(어떤 파티션에 part 파일을 썼는지 — 컴팩션·재현 추적)."""
        return f"{_gold_prefix()}/manifest/run_id={run_id}/manifest.json"

    @staticmethod
    def root() -> str:
        """Gold prefix 루트(컴팩션이 part 파일을 스캔할 때 사용)."""
        return f"{_gold_prefix()}/"
