from __future__ import annotations

from dataclasses import dataclass
import re

from dart_agent.opendart.report_apis import (
    MATERIAL_EVENT_API_NAMES,
    OWNERSHIP_REPORT_API_NAMES,
    REGULAR_REPORT_API_NAMES,
    REPORT_APIS,
    SECURITIES_REPORT_API_NAMES,
)
from dart_agent.storage.paths import BronzePaths


CORRECTION_MARKERS = (
    "[기재정정]",
    "[첨부정정]",
    "[첨부추가]",
    "[변경등록]",
    "[연장결정]",
    "[발행조건확정]",
    "[정정명령부과]",
    "[정정제출요구]",
)

REGULAR_REPORT_CODES = {
    "사업보고서": "11011",
    "반기보고서": "11012",
    "분기보고서": "11014",
    "1분기보고서": "11013",
    "3분기보고서": "11014",
}

SECURITIES_REPORT_NAMES = (
    "증권신고서",
    "일괄신고서",
    "일괄신고추가서류",
    "투자설명서",
)


@dataclass(frozen=True)
class DetailJobSpec:
    priority: int
    job_type: str
    api_group: str
    api_name: str
    bsns_year: str | None = None
    reprt_code: str | None = None
    bgn_de: str | None = None
    end_de: str | None = None
    # False면 rcept_no를 제외하고 enqueue한다. 구조화 API는 회사/기간 단위 결과라
    # 공시 1건마다 새 job을 만들지 않고 corp_code(+기간) 기준으로 dedup한다.
    rcept_scoped: bool = True


def is_correction_report(report_name: str) -> bool:
    return any(marker in report_name for marker in CORRECTION_MARKERS)


def normalize_report_name(report_name: str) -> str:
    value = report_name
    for marker in CORRECTION_MARKERS:
        value = value.replace(marker, "")
    return re.sub(r"\s+", " ", value).strip()


def report_type_for(
    report_name: str,
    pblntf_ty: str | None = None,
    pblntf_detail_ty: str | None = None,
) -> str:
    normalized = normalize_report_name(report_name)
    if pblntf_ty == "A" or any(name in normalized for name in REGULAR_REPORT_CODES):
        return "REGULAR"
    if pblntf_ty == "D" or "대량보유" in normalized or "임원" in normalized:
        return "OWNERSHIP"
    if pblntf_ty == "B" or "주요사항보고서" in normalized:
        return "MATERIAL_EVENT"
    if pblntf_ty == "C" or any(name in normalized for name in SECURITIES_REPORT_NAMES):
        return "SECURITIES_REGISTRATION"
    if pblntf_detail_ty:
        return pblntf_detail_ty
    return "DISCLOSURE"


def detail_jobs_for_disclosure(
    report_name: str,
    rcept_dt: str,
    pblntf_ty: str | None = None,
    pblntf_detail_ty: str | None = None,
    collect_mode: str = "structured",
) -> list[DetailJobSpec]:
    """공시 1건에 대해 만들 상세 수집 job 목록을 정한다.

    collect_mode:
      structured = 구조화 API(DS002~DS006) + Bronze 저장. 원문 job 미생성(기본).
      document   = 원문 document.xml ZIP만 수집.
      both       = 구조화 + 원문 둘 다.

    DS005 EVENT 패턴:
      rcept_dt 당일(bgn_de=end_de=rcept_dt)로 조회. 공시 접수일에 발생한 주요사항을 수집.
      한 회사/날짜 조합이 중복 감지되면 request_hash dedup으로 job이 생성되지 않는다.
    """
    normalized = normalize_report_name(report_name)
    report_type = report_type_for(report_name, pblntf_ty, pblntf_detail_ty)
    want_document = collect_mode in ("document", "both")
    want_structured = collect_mode in ("structured", "both")

    jobs: list[DetailJobSpec] = []

    if want_document:
        jobs.append(
            DetailJobSpec(
                priority=50,
                job_type="DISCLOSURE_DOCUMENT",
                api_group="DS001",
                api_name="document",
            )
        )

    if not want_structured:
        return jobs

    reprt_code = infer_reprt_code(normalized)
    if pblntf_ty == "A" or reprt_code is not None:
        bsns_year = infer_bsns_year(normalized, rcept_dt)
        # DS003 단일회사 전체 재무제표
        jobs.append(
            DetailJobSpec(
                priority=20,
                job_type="FINANCIAL_STATEMENT_ALL",
                api_group="DS003",
                api_name="fnlttSinglAcntAll",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
            )
        )
        # DS002 정기보고서 주요정보(구조화)
        if reprt_code is not None:
            for api_name in REGULAR_REPORT_API_NAMES:
                spec = REPORT_APIS[api_name]
                jobs.append(
                    DetailJobSpec(
                        priority=30,
                        job_type="STRUCTURED_REPORT",
                        api_group=spec.api_group,
                        api_name=spec.api_name,
                        bsns_year=bsns_year,
                        reprt_code=reprt_code,
                        rcept_scoped=False,
                    )
                )

    if report_type == "OWNERSHIP":
        # DS004 지분공시 종합정보(구조화, corp_code 단위)
        for api_name in OWNERSHIP_REPORT_API_NAMES:
            spec = REPORT_APIS[api_name]
            jobs.append(
                DetailJobSpec(
                    priority=30,
                    job_type="STRUCTURED_REPORT",
                    api_group=spec.api_group,
                    api_name=spec.api_name,
                    rcept_scoped=False,
                )
            )

    if report_type == "MATERIAL_EVENT":
        # DS005 주요사항보고서(EVENT 패턴: 접수일 당일 기준 조회)
        for api_name in MATERIAL_EVENT_API_NAMES:
            spec = REPORT_APIS[api_name]
            jobs.append(
                DetailJobSpec(
                    priority=35,
                    job_type="EVENT_REPORT",
                    api_group=spec.api_group,
                    api_name=spec.api_name,
                    bgn_de=rcept_dt,
                    end_de=rcept_dt,
                    rcept_scoped=False,
                )
            )

    if report_type == "SECURITIES_REGISTRATION":
        # DS006 증권신고서(EVENT 패턴: 접수일 당일 기준 조회)
        for api_name in SECURITIES_REPORT_API_NAMES:
            spec = REPORT_APIS[api_name]
            jobs.append(
                DetailJobSpec(
                    priority=34,
                    job_type="SECURITIES_REPORT",
                    api_group=spec.api_group,
                    api_name=spec.api_name,
                    bgn_de=rcept_dt,
                    end_de=rcept_dt,
                    rcept_scoped=False,
                )
            )

    return jobs


def bronze_artifacts_for_disclosure(
    report_name: str,
    rcept_dt: str,
    corp_code: str,
    rcept_no: str,
    *,
    pblntf_ty: str | None = None,
    pblntf_detail_ty: str | None = None,
    collect_mode: str = "structured",
) -> list[tuple[str, str, dict]]:
    """이 공시가 만들어야 할 Bronze 산출물의 (kind, object_path, job_match) 목록.

    detail_jobs_for_disclosure가 정한 expected job을 BronzePaths 경로로 변환한다.
    completion 단계는 경로의 S3 존재(정상 응답 또는 013/014 nodata marker)로 완결성을 판정한다.
    job_match는 legacy 호출 호환용 메타이며, collect_job DONE은 현재 completion 근거가 아니다.
    expected는 report_name+rcept_dt만으로 결정론적이라 DB 없이도 재현 가능하다.
    (company_overview는 회사 단위라 공시 완결성 expected에서 제외한다.)
    """
    specs = detail_jobs_for_disclosure(
        report_name, rcept_dt, pblntf_ty, pblntf_detail_ty, collect_mode
    )
    artifacts: list[tuple[str, str, dict]] = []
    for spec in specs:
        if spec.job_type == "FINANCIAL_STATEMENT_ALL":
            match = {"job_type": "FINANCIAL_STATEMENT_ALL", "rcept_no": rcept_no}
            for fs_div in ("CFS", "OFS"):
                artifacts.append((
                    f"financial:{fs_div}",
                    BronzePaths.financial_statement(corp_code, spec.bsns_year, spec.reprt_code, fs_div),
                    match,
                ))
        elif spec.job_type == "STRUCTURED_REPORT":
            artifacts.append((
                f"structured:{spec.api_name}",
                BronzePaths.structured_report(
                    spec.api_group, spec.api_name, corp_code,
                    bsns_year=spec.bsns_year, reprt_code=spec.reprt_code,
                ),
                {"job_type": "STRUCTURED_REPORT", "corp_code": corp_code, "api_name": spec.api_name,
                 "bsns_year": spec.bsns_year, "reprt_code": spec.reprt_code},
            ))
        elif spec.job_type == "EVENT_REPORT":
            artifacts.append((
                f"event:{spec.api_name}",
                BronzePaths.event_report(spec.api_group, spec.api_name, corp_code, spec.bgn_de, spec.end_de),
                {"job_type": "EVENT_REPORT", "corp_code": corp_code, "api_name": spec.api_name,
                 "bgn_de": spec.bgn_de, "end_de": spec.end_de},
            ))
        elif spec.job_type == "SECURITIES_REPORT":
            artifacts.append((
                f"securities:{spec.api_name}",
                BronzePaths.securities_report(spec.api_group, spec.api_name, corp_code, spec.bgn_de, spec.end_de),
                {"job_type": "SECURITIES_REPORT", "corp_code": corp_code, "api_name": spec.api_name,
                 "bgn_de": spec.bgn_de, "end_de": spec.end_de},
            ))
        elif spec.job_type == "DISCLOSURE_DOCUMENT":
            artifacts.append((
                "document",
                BronzePaths.document(rcept_no),
                {"job_type": "DISCLOSURE_DOCUMENT", "rcept_no": rcept_no},
            ))
    return artifacts


def infer_reprt_code(report_name: str) -> str | None:
    if "1분기보고서" in report_name:
        return "11013"
    if "3분기보고서" in report_name:
        return "11014"
    for key, code in REGULAR_REPORT_CODES.items():
        if key in report_name:
            return code
    return None


def infer_bsns_year(report_name: str, rcept_dt: str) -> str:
    match = re.search(r"\((\d{4})\.\d{2}\)", report_name)
    if match:
        return match.group(1)
    return rcept_dt[0:4]
