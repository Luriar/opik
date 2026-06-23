"""Silver 구조화 payload → 사람이 읽는 한국어 설명문(narrative) 생성.

Gold RAG 문서는 원문 텍스트가 없는 보고서(정기/주요사항/지분)도 검색 가능해야 한다.
숫자/표는 절대 그대로 임베딩하지 않고, 여기서 '맥락 문장'으로 바꾼 뒤 chunk → embedding 한다.
정확한 수치 조회는 facts Parquet(SQL)이 담당하고, 벡터는 라우팅(어느 보고서인지)을 담당한다.

원칙:
  - 입력 키가 없거나 "", "-" 면 조용히 건너뛴다(보고서 유형별 payload 편차가 크다).
  - 금액은 원본 정밀도를 facts에 두고, 설명문에는 조/억 단위 가독 표현을 쓴다.
"""
from __future__ import annotations

import re
from typing import Any

from dart_agent.gold.field_roles import canonical_report_type
from dart_agent.opendart.report_apis import REPORT_APIS

# 혼재 날짜 포맷(YYYYMMDD / YYYY-MM-DD / 'YYYY년 MM월 DD일' / YYYY.MM.DD)에서 (y,m,d) 추출.
_DATE_DIGITS = re.compile(r"(\d{4})\D{0,3}(\d{1,2})\D{0,3}(\d{1,2})")


def iso_date(raw: Any) -> str | None:
    """혼재 날짜 문자열 → 'YYYY-MM-DD'. 파싱 불가 시 None('-'·빈값 포함)."""
    if raw is None:
        return None
    m = _DATE_DIGITS.search(str(raw))
    if not m:
        return None
    y, mo, d = (int(g) for g in m.groups())
    if not (1 <= mo <= 12 and 1 <= d <= 31):
        return None
    return f"{y:04d}-{mo:02d}-{d:02d}"

# report_type → 한국어 라벨(요약 카드/문서 제목용).
REPORT_TYPE_KO: dict[str, str] = {
    "REGULAR": "정기보고서",
    "MATERIAL_EVENT": "주요사항보고서",
    "OWNERSHIP": "지분공시",
    "DISCLOSURE": "공시",
    "SECURITIES_REGISTRATION": "증권신고서",
}

# 재무 요약에 쓰는 핵심 계정(account_id → 한국어). 연결(CFS) 우선, 없으면 별도(OFS).
_KEY_ACCOUNTS: list[tuple[str, str]] = [
    ("ifrs-full_Revenue", "매출액"),
    ("dart_OperatingIncomeLoss", "영업이익"),
    ("ifrs-full_ProfitLoss", "당기순이익"),
    ("ifrs-full_Assets", "자산총계"),
    ("ifrs-full_Liabilities", "부채총계"),
    ("ifrs-full_Equity", "자본총계"),
]


def to_int(value: Any) -> int | None:
    """'1,616,913,470,974' / '-33,175' / '' / '-' → int|None. 빈값·하이픈은 None."""
    if value is None:
        return None
    s = str(value).strip().replace(",", "")
    if s in ("", "-"):
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def fmt_krw(value: Any) -> str | None:
    """정수 금액을 조/억 단위 한국어 가독 표현으로. 1,616,913,470,974 → '1조 6,169억원'."""
    n = to_int(value)
    if n is None:
        return None
    neg = n < 0
    n = abs(n)
    jo, rem = divmod(n, 1_0000_0000_0000)
    eok = rem // 1_0000_0000
    parts: list[str] = []
    if jo:
        parts.append(f"{jo:,}조")
    if eok or jo:
        parts.append(f"{eok:,}억원")
    else:
        parts.append(f"{n:,}원")
    text = " ".join(parts)
    return f"-{text}" if neg else text


def _row_account(rows: list[dict], account_id: str) -> dict | None:
    for r in rows:
        if r.get("account_id") == account_id:
            return r
    return None


def _amount(row: dict) -> Any:
    # 당기말/당기 금액 우선, 분기보고서 누적(add)으로 fallback.
    return row.get("thstrm_amount") or row.get("thstrm_add_amount")


def financial_summary_text(meta: dict, financials: dict) -> str | None:
    """정기보고서 재무 핵심을 한 문단으로. 연결 우선, 없으면 별도."""
    rows = financials.get("CFS") or financials.get("OFS") or []
    if not rows:
        return None
    basis = "연결" if financials.get("CFS") else "별도"
    period = next((r.get("thstrm_nm") for r in rows if r.get("thstrm_nm")), "") or ""
    corp = meta.get("corp_name") or ""
    facts: list[str] = []
    for account_id, ko in _KEY_ACCOUNTS:
        row = _row_account(rows, account_id)
        if not row:
            continue
        krw = fmt_krw(_amount(row))
        if krw:
            facts.append(f"{ko} {krw}")
    if not facts:
        return None
    head = f"{corp}의 {period} {basis}기준 재무 요약".strip()
    return f"{head}: " + ", ".join(facts) + "."


def material_event_text(event_type: str, row: dict, corp_name: str) -> str:
    """주요사항보고서 이벤트 1건을 설명문으로. 유형별 핵심 필드 + 공통 fallback."""
    title = REPORT_APIS[event_type].title if event_type in REPORT_APIS else event_type
    bits: list[str] = [f"{corp_name}의 {title}"]
    # 발행 결정 계열(사채/증자 등) 공통 핵심 필드.
    fta = fmt_krw(row.get("bd_fta") or row.get("fdpp_fclt"))
    if fta:
        bits.append(f"발행총액 {fta}")
    if row.get("ex_prc"):
        bits.append(f"교환가액 {str(row['ex_prc']).strip()}원")
    if row.get("extg"):
        bits.append(f"교환대상 {str(row['extg']).strip()}")
    extg_cnt = to_int(row.get("extg_stkcnt"))
    if extg_cnt:
        bits.append(f"{extg_cnt:,}주")
    if row.get("bd_knd"):
        bits.append(str(row["bd_knd"]).strip())
    if row.get("bddd"):
        bits.append(f"이사회결의일 {str(row['bddd']).strip()}")
    if row.get("exrqpd_bgd") and row.get("exrqpd_edd"):
        bits.append(f"교환청구기간 {row['exrqpd_bgd'].strip()}~{row['exrqpd_edd'].strip()}")
    return ", ".join(b for b in bits if b) + "."


def ownership_summary_text(ownership_type: str, rows: list[dict], corp_name: str) -> str | None:
    """지분 공시 요약. 최신 보고자 보유 + 최근 변동 몇 건."""
    if not rows:
        return None
    # 내부 rcept_dt 최신순.
    ordered = sorted(rows, key=lambda r: str(r.get("rcept_dt") or ""), reverse=True)
    if ownership_type == "majorstock":
        latest = ordered[0]
        repror = str(latest.get("repror") or "").strip()
        stkrt = str(latest.get("stkrt") or "").strip()
        stkqy = to_int(latest.get("stkqy"))
        head = f"{corp_name} 대량보유 상황: 최근 보고자 {repror} 보유비율 {stkrt}%"
        if stkqy:
            head += f"({stkqy:,}주)"
        reasons = [str(r.get("report_resn") or "").replace("\n", " ").strip() for r in ordered[:3]]
        reasons = [r for r in reasons if r]
        if reasons:
            head += " / 최근 변동사유: " + "; ".join(reasons)
        return head + "."
    if ownership_type == "elestock":
        latest = ordered[0]
        repror = str(latest.get("repror") or "").strip()
        rate = str(latest.get("sp_stock_lmp_rate") or "").strip()
        return f"{corp_name} 임원·주요주주 특정증권 소유: 최근 보고자 {repror} 소유비율 {rate}%."
    return None


def regular_structured_summary(table_name: str, rows: list[dict], corp_name: str) -> str | None:
    """정기보고서 구조화 표를 짧은 설명문으로. 표 유형별 핵심만."""
    if not rows:
        return None
    title = REPORT_APIS[table_name].title if table_name in REPORT_APIS else table_name
    if table_name == "exctvSttus":
        n = len(rows)
        ceo = next((r.get("nm") for r in rows if "대표이사" in str(r.get("ofcps") or "")), None)
        head = f"{corp_name} 임원 현황: 총 {n}명"
        if ceo:
            head += f", 대표이사 {str(ceo).strip()}"
        return head + "."
    if table_name == "emplyMttrs":
        total = sum(to_int(r.get("sm")) or 0 for r in rows)
        return f"{corp_name} 직원 현황: 합계 약 {total:,}명." if total else None
    if table_name == "hyslrSttus":
        top = max(rows, key=lambda r: to_int(r.get("trmend_posesn_stock_qota_rt")) or 0, default=None)
        if top:
            return (
                f"{corp_name} 최대주주 현황: {str(top.get('nm') or '').strip()} "
                f"지분 {str(top.get('trmend_posesn_stock_qota_rt') or '').strip()}%."
            )
    if table_name == "alotMatter":
        return f"{corp_name} 배당에 관한 사항 {len(rows)}건."
    # 그 외 표는 제목 + 건수만(검색 라우팅용).
    return f"{corp_name} {title} {len(rows)}건."


def summary_card_text(meta: dict, derived_lines: list[str]) -> str:
    """모든 보고서에 1개 생성하는 요약 카드. 숫자만 있는 보고서도 이 카드로 검색·라우팅된다."""
    corp = meta.get("corp_name") or ""
    raw_type = str(meta.get("report_type") or "")
    rtype = REPORT_TYPE_KO.get(canonical_report_type(raw_type), raw_type)
    report_nm = (meta.get("report_nm") or "").strip()
    head = f"[{rtype}] {corp} · {report_nm}"
    body = " ".join(line for line in derived_lines if line)
    return (head + "\n" + body).strip() if body else head
