"""OpenDART 구조화 상세 API 레지스트리.

원문(document.xml)을 직접 파싱하지 않고, OpenDART의 구조화 API(DS002~DS006)에서
바로 구조화 데이터를 받아 적재하기 위한 엔드포인트 정의다.

각 엔드포인트의 요청 URL/필수 파라미터는 OpenDART 개발가이드 detail 페이지에서 확인한 값만 등록한다.
추측한 엔드포인트는 넣지 않는다(공식 확인분만).

파라미터 패턴:
- FINANCIAL: corp_code + bsns_year + reprt_code (DS002 정기보고서 주요정보, DS003 재무정보)
- HOLDING:   corp_code                          (DS004 지분공시 종합정보)
- EVENT:     corp_code + bgn_de + end_de         (DS005 주요사항보고서, DS006 증권신고서)

참고: https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS002 (DS002~DS006 동일 구조)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ParamPattern(str, Enum):
    FINANCIAL = "FINANCIAL"
    HOLDING = "HOLDING"
    EVENT = "EVENT"


@dataclass(frozen=True)
class ReportApiSpec:
    api_name: str       # 레지스트리 키이자 rate-limit/quota 식별자
    api_group: str      # DS002 ~ DS006
    endpoint: str       # 실제 요청 파일명 (예: alotMatter.json)
    pattern: ParamPattern
    title: str          # 한글 명칭
    api_id: str | None = None          # OpenDART guide apiId(공식 detail 페이지 추적용)
    schema_group: str | None = None    # Silver에서 같은 성격/응답군끼리 묶는 그룹


def _spec(
    api_name: str,
    api_group: str,
    endpoint: str,
    pattern: ParamPattern,
    title: str,
    *,
    api_id: str | None = None,
    schema_group: str | None = None,
) -> ReportApiSpec:
    return ReportApiSpec(
        api_name=api_name,
        api_group=api_group,
        endpoint=endpoint,
        pattern=pattern,
        title=title,
        api_id=api_id,
        schema_group=schema_group,
    )


# 공식 개발가이드 detail 페이지에서 엔드포인트/파라미터를 확인한 항목만 등록한다.
REPORT_APIS: dict[str, ReportApiSpec] = {
    # DS002 정기보고서 주요정보 (corp_code + bsns_year + reprt_code)
    "alotMatter": _spec("alotMatter", "DS002", "alotMatter.json", ParamPattern.FINANCIAL, "배당에 관한 사항"),
    "irdsSttus": _spec("irdsSttus", "DS002", "irdsSttus.json", ParamPattern.FINANCIAL, "증자(감자) 현황"),
    "tesstkAcqsDspsSttus": _spec(
        "tesstkAcqsDspsSttus", "DS002", "tesstkAcqsDspsSttus.json", ParamPattern.FINANCIAL,
        "자기주식 취득 및 처분 현황",
    ),
    "hyslrSttus": _spec("hyslrSttus", "DS002", "hyslrSttus.json", ParamPattern.FINANCIAL, "최대주주 현황"),
    # endpoint 정정(2026-06-15): 101 부적절접근 → DART 표준 ~Sttus 패턴. api_name 키는 collect_job 식별자라 유지.
    "hyslrChgHist": _spec("hyslrChgHist", "DS002", "hyslrChgSttus.json", ParamPattern.FINANCIAL, "최대주주 변동 현황"),
    "exctvSttus": _spec("exctvSttus", "DS002", "exctvSttus.json", ParamPattern.FINANCIAL, "임원 현황"),
    "emplyMttrs": _spec("emplyMttrs", "DS002", "empSttus.json", ParamPattern.FINANCIAL, "직원 등 현황"),
    "piTickCrtfcList": _spec("piTickCrtfcList", "DS002", "stockTotqySttus.json", ParamPattern.FINANCIAL, "주식의 총수 등"),
    # DS004 지분공시 종합정보 (corp_code)
    "majorstock": _spec("majorstock", "DS004", "majorstock.json", ParamPattern.HOLDING, "대량보유 상황보고"),
    "elestock": _spec("elestock", "DS004", "elestock.json", ParamPattern.HOLDING, "임원ㆍ주요주주 소유보고"),
    # DS005 주요사항보고서 주요정보 (corp_code + bgn_de + end_de)
    # 공식 개발가이드 main/detail 페이지 기준 36개 전체 등록(2026-06-18 확인).
    # 기존 collect_job/S3 경로 호환을 위해 일부 api_name 키는 레거시명을 유지하고, endpoint만 공식명으로 둔다.
    "dfOcr": _spec("dfOcr", "DS005", "dfOcr.json", ParamPattern.EVENT, "부도발생",
                   api_id="2020019", schema_group="default_credit"),
    "stkrtbdTrfDecsn": _spec("stkrtbdTrfDecsn", "DS005", "stkrtbdTrfDecsn.json", ParamPattern.EVENT,
                             "주권 관련 사채권 양도 결정", api_id="2020049", schema_group="asset_equity_transfer"),
    "bnkCrnc": _spec("bnkCrnc", "DS005", "ctrcvsBgrq.json", ParamPattern.EVENT, "회생절차 개시신청",
                     api_id="2020021", schema_group="insolvency_restructuring"),
    "dsRsOcr": _spec("dsRsOcr", "DS005", "dsRsOcr.json", ParamPattern.EVENT, "해산사유 발생",
                     api_id="2020022", schema_group="distress_legal_shutdown"),
    "nwShIssuDcrs": _spec("nwShIssuDcrs", "DS005", "piicDecsn.json", ParamPattern.EVENT, "유상증자 결정",
                          api_id="2020023", schema_group="capital_increase"),
    "fricDecsn": _spec("fricDecsn", "DS005", "fricDecsn.json", ParamPattern.EVENT, "무상증자 결정",
                       api_id="2020024", schema_group="capital_increase"),
    "pifricDecsn": _spec("pifricDecsn", "DS005", "pifricDecsn.json", ParamPattern.EVENT, "유무상증자 결정",
                         api_id="2020025", schema_group="capital_increase"),
    "crrstkRdctnDcrs": _spec("crrstkRdctnDcrs", "DS005", "crDecsn.json", ParamPattern.EVENT, "감자 결정",
                             api_id="2020026", schema_group="capital_reduction"),
    "bnkMngtPcbg": _spec("bnkMngtPcbg", "DS005", "bnkMngtPcbg.json", ParamPattern.EVENT,
                         "채권은행 등의 관리절차 개시", api_id="2020027", schema_group="insolvency_restructuring"),
    "lwstLg": _spec("lwstLg", "DS005", "lwstLg.json", ParamPattern.EVENT, "소송 등의 제기",
                    api_id="2020028", schema_group="distress_legal_shutdown"),
    "ovLstDecsn": _spec("ovLstDecsn", "DS005", "ovLstDecsn.json", ParamPattern.EVENT,
                        "해외 증권시장 주권등 상장 결정", api_id="2020029", schema_group="overseas_listing"),
    "ovDlstDecsn": _spec("ovDlstDecsn", "DS005", "ovDlstDecsn.json", ParamPattern.EVENT,
                         "해외 증권시장 주권등 상장폐지 결정", api_id="2020030", schema_group="overseas_listing"),
    "ovLst": _spec("ovLst", "DS005", "ovLst.json", ParamPattern.EVENT, "해외 증권시장 주권등 상장",
                   api_id="2020031", schema_group="overseas_listing"),
    "ovDlst": _spec("ovDlst", "DS005", "ovDlst.json", ParamPattern.EVENT, "해외 증권시장 주권등 상장폐지",
                    api_id="2020032", schema_group="overseas_listing"),
    "cvbdIssuDcrs": _spec("cvbdIssuDcrs", "DS005", "cvbdIsDecsn.json", ParamPattern.EVENT,
                          "전환사채권 발행결정", api_id="2020033", schema_group="bond_issuance"),
    "bdwtnIssuDcrs": _spec("bdwtnIssuDcrs", "DS005", "bdwtIsDecsn.json", ParamPattern.EVENT,
                           "신주인수권부사채권 발행결정", api_id="2020034", schema_group="bond_issuance"),
    "exbdIssuDcrs": _spec("exbdIssuDcrs", "DS005", "exbdIsDecsn.json", ParamPattern.EVENT,
                          "교환사채권 발행결정", api_id="2020035", schema_group="bond_issuance"),
    "bnkMngtPcsp": _spec("bnkMngtPcsp", "DS005", "bnkMngtPcsp.json", ParamPattern.EVENT,
                         "채권은행 등의 관리절차 중단", api_id="2020036", schema_group="insolvency_restructuring"),
    "wdCocobdIsDecsn": _spec("wdCocobdIsDecsn", "DS005", "wdCocobdIsDecsn.json", ParamPattern.EVENT,
                             "상각형 조건부자본증권 발행결정", api_id="2020037", schema_group="bond_issuance"),
    "astInhtrfEtcPtbkOpt": _spec("astInhtrfEtcPtbkOpt", "DS005", "astInhtrfEtcPtbkOpt.json",
                                 ParamPattern.EVENT, "자산양수도(기타), 풋백옵션",
                                 api_id="2020018", schema_group="asset_equity_transfer"),
    "otcprStkInvscrTrfDecsn": _spec("otcprStkInvscrTrfDecsn", "DS005", "otcprStkInvscrTrfDecsn.json",
                                    ParamPattern.EVENT, "타법인 주식 및 출자증권 양도결정",
                                    api_id="2020047", schema_group="asset_equity_transfer"),
    "tgastTrfDecsn": _spec("tgastTrfDecsn", "DS005", "tgastTrfDecsn.json", ParamPattern.EVENT,
                           "유형자산 양도 결정", api_id="2020045", schema_group="asset_equity_transfer"),
    "tgastInhDecsn": _spec("tgastInhDecsn", "DS005", "tgastInhDecsn.json", ParamPattern.EVENT,
                           "유형자산 양수 결정", api_id="2020044", schema_group="asset_equity_transfer"),
    "otcprStkInvscrInhDecsn": _spec("otcprStkInvscrInhDecsn", "DS005", "otcprStkInvscrInhDecsn.json",
                                    ParamPattern.EVENT, "타법인 주식 및 출자증권 양수결정",
                                    api_id="2020046", schema_group="asset_equity_transfer"),
    "bsnesDspsIssDsps": _spec("bsnesDspsIssDsps", "DS005", "bsnTrfDecsn.json", ParamPattern.EVENT,
                              "영업양도 결정", api_id="2020043", schema_group="asset_equity_transfer"),
    "bsnInhDecsn": _spec("bsnInhDecsn", "DS005", "bsnInhDecsn.json", ParamPattern.EVENT, "영업양수 결정",
                         api_id="2020042", schema_group="asset_equity_transfer"),
    "tsstkAqTrctrCcDecsn": _spec("tsstkAqTrctrCcDecsn", "DS005", "tsstkAqTrctrCcDecsn.json",
                                 ParamPattern.EVENT, "자기주식취득 신탁계약 해지 결정",
                                 api_id="2020041", schema_group="treasury_stock"),
    "tsstkAqTrctrCnsDecsn": _spec("tsstkAqTrctrCnsDecsn", "DS005", "tsstkAqTrctrCnsDecsn.json",
                                  ParamPattern.EVENT, "자기주식취득 신탁계약 체결 결정",
                                  api_id="2020040", schema_group="treasury_stock"),
    "tsstkDpDecsn": _spec("tsstkDpDecsn", "DS005", "tsstkDpDecsn.json", ParamPattern.EVENT,
                          "자기주식 처분 결정", api_id="2020039", schema_group="treasury_stock"),
    "tsstkAqDecsn": _spec("tsstkAqDecsn", "DS005", "tsstkAqDecsn.json", ParamPattern.EVENT,
                          "자기주식 취득 결정", api_id="2020038", schema_group="treasury_stock"),
    "exShtrnIssDsps": _spec("exShtrnIssDsps", "DS005", "stkExtrDecsn.json", ParamPattern.EVENT,
                            "주식교환·이전 결정", api_id="2020053", schema_group="corporate_reorganization"),
    "cmpDvmgDecsn": _spec("cmpDvmgDecsn", "DS005", "cmpDvmgDecsn.json", ParamPattern.EVENT,
                          "회사분할합병 결정", api_id="2020052", schema_group="corporate_reorganization"),
    "dvsnIssDsps": _spec("dvsnIssDsps", "DS005", "cmpDvDecsn.json", ParamPattern.EVENT, "회사분할 결정",
                         api_id="2020051", schema_group="corporate_reorganization"),
    "mgIssDsps": _spec("mgIssDsps", "DS005", "cmpMgDecsn.json", ParamPattern.EVENT, "회사합병 결정",
                       api_id="2020050", schema_group="corporate_reorganization"),
    "stkrtbdInhDecsn": _spec("stkrtbdInhDecsn", "DS005", "stkrtbdInhDecsn.json", ParamPattern.EVENT,
                             "주권 관련 사채권 양수 결정", api_id="2020048", schema_group="asset_equity_transfer"),
    "bsnSp": _spec("bsnSp", "DS005", "bsnSp.json", ParamPattern.EVENT, "영업정지",
                   api_id="2020020", schema_group="distress_legal_shutdown"),
    # DS006 증권신고서 주요정보 (corp_code + bgn_de + end_de)
    "estkRs":           _spec("estkRs",             "DS006", "estkRs.json",         ParamPattern.EVENT, "지분증권"),
    "bdRs":             _spec("bdRs",               "DS006", "bdRs.json",           ParamPattern.EVENT, "채무증권"),
    "stkdpRs":          _spec("stkdpRs",            "DS006", "stkdpRs.json",        ParamPattern.EVENT, "증권예탁증권"),
    "mgRs":             _spec("mgRs",               "DS006", "mgRs.json",           ParamPattern.EVENT, "합병"),
    "extrRs":           _spec("extrRs",             "DS006", "extrRs.json",         ParamPattern.EVENT, "주식의 포괄적교환·이전"),
    "dvRs":             _spec("dvRs",               "DS006", "dvRs.json",           ParamPattern.EVENT, "분할"),
}

# 정기보고서(REGULAR)일 때 함께 수집할 DS002 구조화 API.
REGULAR_REPORT_API_NAMES: tuple[str, ...] = (
    "alotMatter",       # 배당에 관한 사항
    "irdsSttus",        # 증자(감자) 현황
    "tesstkAcqsDspsSttus",  # 자기주식 취득 및 처분 현황
    "hyslrSttus",       # 최대주주 현황
    "hyslrChgHist",     # 최대주주 변동 현황
    "exctvSttus",       # 임원 현황
    "emplyMttrs",       # 직원 등 현황
    "piTickCrtfcList",  # 주식의 총수 등
)

# 지분공시(OWNERSHIP)일 때 함께 수집할 DS004 구조화 API.
OWNERSHIP_REPORT_API_NAMES: tuple[str, ...] = (
    "majorstock",
    "elestock",
)

# 주요사항보고서(MATERIAL_EVENT)일 때 수집할 DS005 구조화 API.
# 2026-06-18 DART 공식 가이드 DS005 main/detail 페이지 기준 36개 전체.
MATERIAL_EVENT_API_NAMES: tuple[str, ...] = (
    "dfOcr",            # 부도발생
    "stkrtbdTrfDecsn",  # 주권 관련 사채권 양도 결정
    "bnkCrnc",          # 회생절차 개시신청
    "dsRsOcr",          # 해산사유 발생
    "nwShIssuDcrs",     # 유상증자 결정
    "fricDecsn",        # 무상증자 결정
    "pifricDecsn",      # 유무상증자 결정
    "crrstkRdctnDcrs",  # 감자 결정
    "bnkMngtPcbg",      # 채권은행 등의 관리절차 개시
    "lwstLg",           # 소송 등의 제기
    "ovLstDecsn",       # 해외 증권시장 주권등 상장 결정
    "ovDlstDecsn",      # 해외 증권시장 주권등 상장폐지 결정
    "ovLst",            # 해외 증권시장 주권등 상장
    "ovDlst",           # 해외 증권시장 주권등 상장폐지
    "cvbdIssuDcrs",     # 전환사채권 발행결정
    "bdwtnIssuDcrs",    # 신주인수권부사채권 발행결정
    "exbdIssuDcrs",     # 교환사채권 발행결정
    "bnkMngtPcsp",      # 채권은행 등의 관리절차 중단
    "wdCocobdIsDecsn",  # 상각형 조건부자본증권 발행결정
    "astInhtrfEtcPtbkOpt",  # 자산양수도(기타), 풋백옵션
    "otcprStkInvscrTrfDecsn",  # 타법인 주식 및 출자증권 양도결정
    "tgastTrfDecsn",    # 유형자산 양도 결정
    "tgastInhDecsn",    # 유형자산 양수 결정
    "otcprStkInvscrInhDecsn",  # 타법인 주식 및 출자증권 양수결정
    "bsnesDspsIssDsps", # 영업양도 결정
    "bsnInhDecsn",      # 영업양수 결정
    "tsstkAqTrctrCcDecsn",  # 자기주식취득 신탁계약 해지 결정
    "tsstkAqTrctrCnsDecsn", # 자기주식취득 신탁계약 체결 결정
    "tsstkDpDecsn",     # 자기주식 처분 결정
    "tsstkAqDecsn",     # 자기주식 취득 결정
    "exShtrnIssDsps",   # 주식교환·이전 결정
    "cmpDvmgDecsn",     # 회사분할합병 결정
    "dvsnIssDsps",      # 회사분할 결정
    "mgIssDsps",        # 회사합병 결정
    "stkrtbdInhDecsn",  # 주권 관련 사채권 양수 결정
    "bsnSp",            # 영업정지
)

MATERIAL_EVENT_REPORT_TYPE_SLUGS: dict[str, str] = {
    "dfOcr": "default_occurrence",
    "stkrtbdTrfDecsn": "bond_rights_transfer_decision",
    "bnkCrnc": "rehabilitation_procedure_application",
    "dsRsOcr": "dissolution_reason_occurrence",
    "nwShIssuDcrs": "paid_in_capital_increase_decision",
    "fricDecsn": "free_capital_increase_decision",
    "pifricDecsn": "paid_and_free_capital_increase_decision",
    "crrstkRdctnDcrs": "capital_reduction_decision",
    "bnkMngtPcbg": "creditor_bank_management_start",
    "lwstLg": "lawsuit_filing",
    "ovLstDecsn": "overseas_listing_decision",
    "ovDlstDecsn": "overseas_delisting_decision",
    "ovLst": "overseas_listing",
    "ovDlst": "overseas_delisting",
    "cvbdIssuDcrs": "convertible_bond_issuance_decision",
    "bdwtnIssuDcrs": "bond_with_warrant_issuance_decision",
    "exbdIssuDcrs": "exchangeable_bond_issuance_decision",
    "bnkMngtPcsp": "creditor_bank_management_stop",
    "wdCocobdIsDecsn": "write_down_coco_bond_issuance_decision",
    "astInhtrfEtcPtbkOpt": "asset_transfer_putback_option",
    "otcprStkInvscrTrfDecsn": "other_company_stock_transfer_decision",
    "tgastTrfDecsn": "tangible_asset_transfer_decision",
    "tgastInhDecsn": "tangible_asset_acquisition_decision",
    "otcprStkInvscrInhDecsn": "other_company_stock_acquisition_decision",
    "bsnesDspsIssDsps": "business_transfer_decision",
    "bsnInhDecsn": "business_acquisition_decision",
    "tsstkAqTrctrCcDecsn": "treasury_stock_trust_cancellation_decision",
    "tsstkAqTrctrCnsDecsn": "treasury_stock_trust_contract_decision",
    "tsstkDpDecsn": "treasury_stock_disposal_decision",
    "tsstkAqDecsn": "treasury_stock_acquisition_decision",
    "exShtrnIssDsps": "share_exchange_transfer_decision",
    "cmpDvmgDecsn": "company_split_merger_decision",
    "dvsnIssDsps": "company_split_decision",
    "mgIssDsps": "company_merger_decision",
    "stkrtbdInhDecsn": "bond_rights_acquisition_decision",
    "bsnSp": "business_suspension",
}


def material_event_report_type(api_name: str) -> str:
    """Silver report_type partition for a DS005 material-event API."""
    slug = MATERIAL_EVENT_REPORT_TYPE_SLUGS[api_name]
    return f"005{slug}"

# 증권신고서(SECURITIES_REGISTRATION)일 때 수집할 DS006 구조화 API.
SECURITIES_REPORT_API_NAMES: tuple[str, ...] = (
    "estkRs",    # 지분증권
    "bdRs",      # 채무증권
    "stkdpRs",   # 증권예탁증권
    "mgRs",      # 합병
    "extrRs",    # 주식의 포괄적교환·이전
    "dvRs",      # 분할
)


def build_report_params(
    spec: ReportApiSpec,
    *,
    corp_code: str,
    bsns_year: str | None = None,
    reprt_code: str | None = None,
    bgn_de: str | None = None,
    end_de: str | None = None,
) -> dict[str, str]:
    """엔드포인트 패턴에 맞는 요청 파라미터(crtfc_key 제외)를 만든다."""
    if not corp_code:
        raise ValueError("corp_code is required")
    if spec.pattern == ParamPattern.FINANCIAL:
        if not bsns_year or not reprt_code:
            raise ValueError(f"{spec.api_name} requires bsns_year and reprt_code")
        return {"corp_code": corp_code, "bsns_year": bsns_year, "reprt_code": reprt_code}
    if spec.pattern == ParamPattern.HOLDING:
        return {"corp_code": corp_code}
    if spec.pattern == ParamPattern.EVENT:
        if not bgn_de or not end_de:
            raise ValueError(f"{spec.api_name} requires bgn_de and end_de")
        return {"corp_code": corp_code, "bgn_de": bgn_de, "end_de": end_de}
    raise ValueError(f"unknown param pattern: {spec.pattern}")
