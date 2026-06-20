"""Silver 구조화 필드 → 임베딩 역할(role) 맵 — docs/gold-rag/field_roles.draft.yaml의 코드 실행본.

설계(사용자 결정 2026-06-18):
  - embed : 서술/문맥 문자열 필드만 '라벨: 값'으로 직렬화 → e5 임베딩 대상.
  - fact  : 숫자/비율/금액/날짜 → facts Parquet(SQL) + 필터 메타. **임베딩 제외**.
  - skip  : 식별자/무의미.
  - 빈 값('', '-', None)·숫자(날짜 포함)-only는 drop. 서술이 전부 비면 record 청크를 만들지 않는다.
  - importance/keywords는 '기준 있는' 결정적 산출만:
      importance = 유형/섹션 고정 점수(아래 표). keywords = 유형 라벨 + API 제목 + 큐레이션 동의어.
      자유 추출(LLM/TF-IDF) 금지 — 재처리 멱등(§2.4)을 위해.
"""
from __future__ import annotations

import re

from dart_agent.gold import ksic
from dart_agent.opendart.report_apis import REPORT_APIS

# role 맵 변경 시 반드시 버전을 올린다. 같은 원문이라도 chunk_text가 달라져 기존 embedding 재사용 불가.
FIELD_MAPPING_VERSION = "field_roles.v3"


def canonical_report_type(report_type: str | None) -> str:
    """Return the analytical report family used by Gold/RAG rules."""
    rt = str(report_type or "").strip()
    if rt.startswith("005"):
        return "MATERIAL_EVENT"
    return rt or "DISCLOSURE"

# ── importance: 유형/섹션 고정 점수(field_roles.draft.yaml §importance). rerank 메타, 임베딩 안 함.
_TYPE_IMPORTANCE = {"MATERIAL_EVENT": 0.9, "OWNERSHIP": 0.6,
                    "SECURITIES_REGISTRATION": 0.3, "REGULAR": 0.5, "DISCLOSURE": 0.0}
_SECTION_IMPORTANCE = {
    "exctvSttus": 0.7, "hyslrChgHist": 0.7, "financials": 0.7,
    "hyslrSttus": 0.6, "tesstkAcqsDspsSttus": 0.5, "alotMatter": 0.5,
    "emplyMttrs": 0.5, "irdsSttus": 0.5, "piTickCrtfcList": 0.4,
    "majorstock": 0.6, "elestock": 0.6, "company_overview": 0.4,
}

# ── 유형 한글 라벨(keywords 기본 항목).
_TYPE_LABEL = {"REGULAR": "정기보고서", "MATERIAL_EVENT": "주요사항보고서", "OWNERSHIP": "지분공시",
               "SECURITIES_REGISTRATION": "증권신고서", "DISCLOSURE": "공시"}

# ── keywords 큐레이션 동의어(api/section → 키워드). 결정적. 자유추출 아님.
_KEYWORD_SYNONYMS: dict[str, list[str]] = {
    "mgIssDsps": ["합병", "M&A"], "exbdIssuDcrs": ["교환사채", "EB"],
    "cvbdIssuDcrs": ["전환사채", "CB"], "bdwtnIssuDcrs": ["신주인수권부사채", "BW"],
    "nwShIssuDcrs": ["유상증자"], "crrstkRdctnDcrs": ["감자"], "dvsnIssDsps": ["분할"],
    "majorstock": ["대량보유", "5%룰"], "elestock": ["임원·주요주주", "특정증권소유"],
    "tesstkAcqsDspsSttus": ["자기주식"], "alotMatter": ["배당"], "hyslrChgHist": ["최대주주변동"],
    "hyslrSttus": ["최대주주"], "exctvSttus": ["임원"], "irdsSttus": ["증자", "감자"],
    "emplyMttrs": ["직원"], "piTickCrtfcList": ["주식총수"], "company_overview": ["기업개요"],
}

# ── material_event(DS005) 공통 서술 필드(_shared).
_SHARED_MATERIAL = {
    "fdpp_op": "자금조달: 운영자금", "fdpp_dtrp": "자금조달: 채무상환자금",
    "fdpp_ocsa": "자금조달: 타법인증권 취득자금", "fdpp_fclt": "자금조달: 시설자금",
    "fdpp_bsninh": "자금조달: 영업양수자금", "fdpp_etc": "자금조달: 기타자금",
    "ex_sm_r": "증권신고서 미제출/모집 판단 사유",
}

# ── embed-role 필드: api/section → {field: 한글라벨}. 여기 없는 필드는 비임베딩(fact/skip).
# 주의: 배당/증감자/주식총수 같은 "피벗 metric 표"(se='구분' 행 = 지표명)는 record당 임베딩하면
#       '배당 구분: 주당액면가액(원)' 같은 무의미 청크가 양산된다 → per-record 제외(요약은 summary_card).
#       그런 표는 EMBED_FIELDS에 두지 않고, 서술이 풍부한 record만 둔다(is_per_record_api 참고).
EMBED_FIELDS: dict[str, dict[str, str]] = {
    "company_overview": {
        "induty_code": "업종",
        "induty_name": "업종",
        "main_business": "주요사업",
        "business_summary": "회사 개요",
        "corp_summary": "회사 개요",
        "biz_summary": "주요사업",
    },
    "exctvSttus": {"nm": "성명", "ofcps": "직위", "chrg_job": "담당업무", "rgist_exctv_at": "등기임원 여부",
                   "mxmm_shrholdr_relate": "최대주주와의 관계", "hffc_pd": "재직기간",
                   "tenure_end_on": "임기 만료일", "main_career": "주요경력"},
    "hyslrChgHist": {"change_cause": "변동원인", "mxmm_shrholdr_nm": "최대주주명"},
    "hyslrSttus": {"nm": "성명", "relate": "관계", "stock_knd": "주식 종류", "rm": "비고"},
    "tesstkAcqsDspsSttus": {"acqs_mth1": "취득방법(대분류)", "acqs_mth2": "취득방법(중분류)",
                            "acqs_mth3": "취득방법(소분류)", "stock_knd": "주식 종류", "rm": "비고"},
    "exbdIssuDcrs": {"bd_knd": "사채 종류", "bdis_mthn": "발행방법",
                     "ex_prc_dmth": "교환가액 산정방식", "extg": "교환대상"},
    "mgIssDsps": {"mg_mth": "합병방법", "mg_stn": "합병형태", "mg_pp": "합병목적",
                  "mg_rt_bs": "합병비율 산출근거", "mgptncmp_cmpnm": "합병 상대회사명",
                  "mgptncmp_mbsn": "상대회사 주요사업", "mgptncmp_rl_cmpn": "상대회사와의 관계",
                  "exevl_bs_rs": "외부평가 근거·사유"},
    "majorstock": {"report_resn": "보고사유", "report_tp": "보고구분", "repror": "보고자"},
    "elestock": {"repror": "보고자", "isu_exctv_ofcps": "임원 직위",
                 "isu_exctv_rgist_at": "등기 여부", "isu_main_shrholdr": "주요주주 구분"},
}

# per-record 청크를 만들 '서술 풍부' API(allowlist). 피벗 metric 표(alotMatter/irdsSttus/
# piTickCrtfcList/emplyMttrs 등)는 제외 — record당 무의미 청크 양산 방지(요약은 summary_card가 담당).
_PER_RECORD = {"exctvSttus", "hyslrChgHist", "hyslrSttus", "tesstkAcqsDspsSttus", "majorstock", "elestock"}


def is_per_record_api(api: str) -> bool:
    """이 API의 record를 per-record 청크로 임베딩할지(서술 풍부 or DS005 사건성)."""
    return api in _PER_RECORD or _is_material(api)

# 임베딩 텍스트에서 제외하는 공통 식별/메타 필드(field_roles.draft.yaml common_skip).
_COMMON_SKIP = {"corp_cls", "corp_code", "corp_name", "rcept_no", "rcept_dt", "stlm_dt", "ord",
                "currency", "message", "status", "reprt_code", "bsns_year", "sj_div", "sj_nm"}
# 값이 숫자/비율/금액/날짜-only면 서술 신호가 없으므로 임베딩 텍스트에서 drop(메타/facts로 관리).
_NUMERIC_ONLY = re.compile(r"^[\d,.\s%원주개명년월일\-+()~/:]+$")


def _is_material(api: str) -> bool:
    spec = REPORT_APIS.get(api)
    return bool(spec and spec.api_group == "DS005")


def is_material_report_type(report_type: str | None) -> bool:
    return canonical_report_type(report_type) == "MATERIAL_EVENT"


def _clean(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip().replace("\n", " ")
    if s in ("", "-") or _NUMERIC_ONLY.match(s):
        return None
    return s


def _clean_field(api: str, field: str, value: object) -> str | None:
    if api == "company_overview" and field == "induty_code":
        return ksic.industry_name(value)
    return _clean(value)


def render_record_text(api: str, record: dict) -> str:
    """record의 embed-role 필드만 '라벨: 값 / 라벨: 값'으로 직렬화. 서술이 없으면 ''(빈 청크 금지)."""
    labels: dict[str, str] = {}
    if _is_material(api):
        labels.update(_SHARED_MATERIAL)
    labels.update(EMBED_FIELDS.get(api, {}))
    parts = [f"{label}: {v}" for field, label in labels.items() if (v := _clean_field(api, field, record.get(field)))]
    # 미매핑 material_event API 폴백(_pattern): 길이>40 & 숫자-only 아닌 서술 필드만.
    if _is_material(api):
        for field, raw in record.items():
            if field in labels or field in _COMMON_SKIP:
                continue
            v = _clean(raw)
            if v and len(v) >= 2:
                parts.append(f"{field}: {v}")
    return " / ".join(parts)


def importance_of(report_type: str, section: str | None = None) -> float:
    if section and section in _SECTION_IMPORTANCE:
        return _SECTION_IMPORTANCE[section]
    return _TYPE_IMPORTANCE.get(canonical_report_type(report_type), 0.4)


def keywords_of(report_type: str, api: str | None) -> list[str]:
    """결정적 keywords — 유형 라벨 + API 제목 + 큐레이션 동의어. (자유추출 아님)."""
    kws: set[str] = set()
    family = canonical_report_type(report_type)
    if family in _TYPE_LABEL:
        kws.add(_TYPE_LABEL[family])
    if api:
        spec = REPORT_APIS.get(api)
        if spec and spec.title:
            kws.add(spec.title)
        kws.update(_KEYWORD_SYNONYMS.get(api, []))
    return sorted(kws) or ["미분류"]


def api_group_of(api: str | None) -> str | None:
    spec = REPORT_APIS.get(api) if api else None
    return spec.api_group if spec else None


def section_title_of(api: str | None) -> str | None:
    spec = REPORT_APIS.get(api) if api else None
    return spec.title if spec else None
