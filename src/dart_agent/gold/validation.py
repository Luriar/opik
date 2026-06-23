"""Silver→Gold 인코딩/품질 게이트.

Gold Parquet/Vector 품질을 지키기 위해, 변환 전에 Silver report.json을 검사한다.
  - ERROR(격리): mojibake(UTF-8 깨짐) 등 정정 없이 적재하면 전 구간 오염되는 문제.
  - WARN(적재하되 기록): 본문 없음, 전 섹션 empty, 금액 파싱 실패 등 품질 경고.

mojibake 판정: UTF-8 바이트가 latin-1로 오해독된 텍스트는 한글이 거의 없고, latin-1로
재인코딩→utf-8 디코딩 시 한글 비율이 급증한다. 그 차이로 감지한다(오탐 최소화).
숫자 콤마("30,183,696,000")·혼재 날짜는 normalize에서 흡수하므로 ERROR가 아니라 정보성으로 본다.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# 검사할 핵심 사람가독 필드(_meta + company_overview). 여기서 mojibake면 치명적.
_KEY_META_FIELDS = ("corp_name", "report_nm")


def _hangul_ratio(s: str) -> float:
    if not s:
        return 0.0
    hangul = sum(1 for c in s if "가" <= c <= "힣")
    return hangul / len(s)


def try_repair_mojibake(s: str) -> str | None:
    """latin-1 오해독 텍스트를 UTF-8로 복원 시도. 복원 불가면 None."""
    try:
        return s.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return None


def looks_mojibake(s: str | None) -> bool:
    """UTF-8이 latin-1로 깨진 텍스트로 보이면 True."""
    if not s or not isinstance(s, str):
        return False
    repaired = try_repair_mojibake(s)
    if repaired is None:
        return False
    # 복원 후 한글 비율이 뚜렷이 늘면 원본이 깨졌던 것.
    return _hangul_ratio(repaired) > _hangul_ratio(s) + 0.05


@dataclass
class ValidationResult:
    severity: str = "OK"  # OK | WARN | ERROR
    issues: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.severity != "ERROR"

    def _add(self, severity: str, msg: str) -> None:
        self.issues.append(f"[{severity}] {msg}")
        order = {"OK": 0, "WARN": 1, "ERROR": 2}
        if order[severity] > order[self.severity]:
            self.severity = severity


def validate_report(report: dict) -> ValidationResult:
    """Gold 변환 전 게이트. ERROR면 격리(적재 안 함), WARN이면 적재하되 기록."""
    res = ValidationResult()
    meta = report.get("_meta") or {}

    # 1) mojibake — 핵심 메타 + company_overview 이름 + 공시 본문.
    for fld in _KEY_META_FIELDS:
        if looks_mojibake(meta.get(fld)):
            res._add("ERROR", f"mojibake in _meta.{fld}")
    ov = report.get("company_overview") or {}
    if looks_mojibake(ov.get("corp_name")):
        res._add("ERROR", "mojibake in company_overview.corp_name")
    if looks_mojibake(report.get("text")):
        res._add("ERROR", "mojibake in text")

    # 2) 식별자 필수.
    if not meta.get("rcept_no") or not meta.get("corp_code"):
        res._add("ERROR", "missing rcept_no/corp_code")

    # 3) 빈 페이로드 — fact/본문/구조화가 전무하면 검색 가치 낮음(요약카드만 생성됨).
    has_payload = bool(report.get("text")) or bool(report.get("financials")) \
        or any((report.get("structured") or {}).values()) \
        or any((report.get("event_reports") or {}).values()) \
        or any((report.get("ownership") or {}).values()) \
        or any((report.get("securities") or {}).values())
    if not has_payload:
        res._add("WARN", "empty payload (summary card only)")

    # 4) DISCLOSURE인데 본문 없음.
    if str(meta.get("report_type")) == "DISCLOSURE" and not report.get("text"):
        res._add("WARN", "DISCLOSURE without text body")

    return res
