"""상장사 명단 변동 판정과 RAG용 이력 텍스트 생성(순수 로직).

이 모듈은 DB나 네트워크에 의존하지 않는다. corpCode.xml에서 뽑은 현재 상장사 명단과
DB에 저장된 기존 명단을 비교해서 신규상장/상장폐지/재상장/정보변경을 구분하고,
각 변동에 대해 Vector DB ingest에 바로 쓸 수 있는 한국어 searchable_text를 만든다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from dart_agent.collectors.corp_code import CorpCodeRecord


# corpCode.xml에는 시장구분이 없다. corp_cls는 disclosure(list.json 기준)로 후속 enrich하며,
# 매핑은 시장구분이 확인된 경우에만 사용한다.
CORP_CLS_TO_MARKET = {
    "Y": "KOSPI",
    "K": "KOSDAQ",
    "N": "KONEX",
    "E": "ETC",
}

UNKNOWN_MARKET = "UNKNOWN"

# 변동 사유 텍스트. 명단 비교만으로 확정 가능한 1차 사유다(실제 상폐 사유 등은 추후 공시로 enrich).
REASON_LISTED = "corpCode.xml 상장사 명단에 종목코드가 신규 등장(신규 상장 감지)"
REASON_DELISTED = "corpCode.xml 상장사 명단에서 종목코드가 사라짐(상장폐지 감지)"
REASON_RELISTED = "상장폐지 이력이 있는 종목코드가 명단에 다시 등장(재상장 감지)"
REASON_INFO_CHANGED = "상장 유지 중 회사 식별정보(회사명/영문명/고유번호) 변경 감지"

# INFO_CHANGED 비교 대상 필드. 시장구분(corp_cls)은 후속 enrich로 바뀔 수 있어 diff에 넣지 않는다.
IDENTITY_FIELDS = ("corp_code", "corp_name", "corp_eng_name")


@dataclass(frozen=True)
class ListedCompany:
    """corpCode.xml에서 추출한 상장사 1건."""

    stock_code: str
    corp_code: str
    corp_name: str
    corp_eng_name: str | None = None

    @classmethod
    def from_record(cls, record: CorpCodeRecord) -> "ListedCompany":
        if not record.stock_code:
            raise ValueError("listed company requires a stock_code")
        return cls(
            stock_code=record.stock_code,
            corp_code=record.corp_code,
            corp_name=record.corp_name,
            corp_eng_name=record.corp_eng_name,
        )


@dataclass(frozen=True)
class ExistingCompany:
    """DB의 listed_company 1건(비교에 필요한 최소 필드)."""

    stock_code: str
    corp_code: str | None
    corp_name: str
    corp_eng_name: str | None
    status: str


@dataclass(frozen=True)
class RosterEvent:
    """명단 변동 1건. listed_company_event 한 row로 저장된다."""

    stock_code: str
    corp_code: str | None
    corp_name: str
    corp_eng_name: str | None
    event_type: str
    change_reason: str
    change_detail: dict[str, object]
    searchable_text: str


@dataclass(frozen=True)
class RosterDiff:
    listed: list[RosterEvent] = field(default_factory=list)
    delisted: list[RosterEvent] = field(default_factory=list)
    relisted: list[RosterEvent] = field(default_factory=list)
    info_changed: list[RosterEvent] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)

    @property
    def events(self) -> list[RosterEvent]:
        return [*self.listed, *self.relisted, *self.info_changed, *self.delisted]

    @property
    def has_changes(self) -> bool:
        return bool(self.events)

    def summary(self) -> dict[str, int]:
        return {
            "listed": len(self.listed),
            "relisted": len(self.relisted),
            "info_changed": len(self.info_changed),
            "delisted": len(self.delisted),
            "unchanged": len(self.unchanged),
        }


def market_for(corp_cls: str | None) -> str:
    return CORP_CLS_TO_MARKET.get(corp_cls or "", UNKNOWN_MARKET)


def build_searchable_text(
    *,
    event_type: str,
    company: ListedCompany | ExistingCompany,
    observed_date: str,
    corp_cls: str | None,
    change_reason: str,
    change_detail: Mapping[str, object] | None = None,
) -> str:
    """Vector DB ingest용 한국어 요약. 최신순 정렬 + 키워드 검색을 위해
    상태 키워드, 회사명, 종목코드, 고유번호, 시장구분, 일자를 모두 평문으로 포함한다."""
    market = market_for(corp_cls)
    head = {
        "LISTED": "[신규상장]",
        "DELISTED": "[상장폐지]",
        "RELISTED": "[재상장]",
        "INFO_CHANGED": "[기업정보변경]",
    }.get(event_type, "[명단변동]")
    action = {
        "LISTED": "상장사 명단에 신규 등록되었습니다",
        "DELISTED": "상장사 명단에서 제외되었습니다(상장폐지 추정)",
        "RELISTED": "상장사 명단에 다시 등록되었습니다(재상장)",
        "INFO_CHANGED": "회사 식별정보가 변경되었습니다",
    }.get(event_type, "명단이 변동되었습니다")

    parts = [
        f"{head} {company.corp_name}({company.stock_code}) 기업이 "
        f"{observed_date} 기준 {action}.",
        f"종목코드 {company.stock_code}, 고유번호(corp_code) {company.corp_code or '미상'}, "
        f"시장구분 {market}.",
        f"변동사유: {change_reason}.",
    ]
    if event_type == "DELISTED":
        parts.append("상장폐지 이후에도 과거 공시/재무 이력은 삭제하지 않고 보존합니다.")
    if change_detail:
        rendered = ", ".join(
            f"{key}: {value.get('before')!r} -> {value.get('after')!r}"
            if isinstance(value, Mapping)
            else f"{key}: {value!r}"
            for key, value in change_detail.items()
        )
        parts.append(f"변경내용: {rendered}.")
    return " ".join(parts)


def _make_event(
    *,
    event_type: str,
    company: ListedCompany | ExistingCompany,
    observed_date: str,
    corp_cls: str | None,
    change_reason: str,
    change_detail: dict[str, object],
) -> RosterEvent:
    return RosterEvent(
        stock_code=company.stock_code,
        corp_code=company.corp_code,
        corp_name=company.corp_name,
        corp_eng_name=company.corp_eng_name,
        event_type=event_type,
        change_reason=change_reason,
        change_detail=change_detail,
        searchable_text=build_searchable_text(
            event_type=event_type,
            company=company,
            observed_date=observed_date,
            corp_cls=corp_cls,
            change_reason=change_reason,
            change_detail=change_detail,
        ),
    )


def _identity_changes(new: ListedCompany, old: ExistingCompany) -> dict[str, object]:
    changes: dict[str, object] = {}
    new_values = {
        "corp_code": new.corp_code,
        "corp_name": new.corp_name,
        "corp_eng_name": new.corp_eng_name,
    }
    old_values = {
        "corp_code": old.corp_code,
        "corp_name": old.corp_name,
        "corp_eng_name": old.corp_eng_name,
    }
    for key in IDENTITY_FIELDS:
        if new_values[key] != old_values[key]:
            changes[key] = {"before": old_values[key], "after": new_values[key]}
    return changes


def compute_roster_diff(
    new_roster: Mapping[str, ListedCompany],
    existing: Mapping[str, ExistingCompany],
    *,
    observed_date: str,
    corp_cls_lookup: Mapping[str, str] | None = None,
) -> RosterDiff:
    """현재 상장사 명단(new_roster)과 DB 기존 명단(existing)을 비교한다.

    - new_roster: 종목코드 -> ListedCompany (corpCode.xml에서 추출)
    - existing: 종목코드 -> ExistingCompany (DELISTED 포함, 재상장 판정에 필요)
    - corp_cls_lookup: corp_code -> corp_cls (disclosure에서 enrich, 없으면 UNKNOWN 처리)
    """
    corp_cls_lookup = corp_cls_lookup or {}
    diff = RosterDiff()

    for stock_code, company in new_roster.items():
        corp_cls = corp_cls_lookup.get(company.corp_code)
        old = existing.get(stock_code)
        if old is None:
            diff.listed.append(
                _make_event(
                    event_type="LISTED",
                    company=company,
                    observed_date=observed_date,
                    corp_cls=corp_cls,
                    change_reason=REASON_LISTED,
                    change_detail={
                        "stock_code": company.stock_code,
                        "corp_code": company.corp_code,
                        "corp_name": company.corp_name,
                    },
                )
            )
        elif old.status == "DELISTED":
            diff.relisted.append(
                _make_event(
                    event_type="RELISTED",
                    company=company,
                    observed_date=observed_date,
                    corp_cls=corp_cls,
                    change_reason=REASON_RELISTED,
                    change_detail={
                        "stock_code": company.stock_code,
                        "corp_code": company.corp_code,
                        "corp_name": company.corp_name,
                    },
                )
            )
        else:
            identity = _identity_changes(company, old)
            if identity:
                diff.info_changed.append(
                    _make_event(
                        event_type="INFO_CHANGED",
                        company=company,
                        observed_date=observed_date,
                        corp_cls=corp_cls,
                        change_reason=REASON_INFO_CHANGED,
                        change_detail=identity,
                    )
                )
            else:
                diff.unchanged.append(stock_code)

    for stock_code, old in existing.items():
        if old.status != "ACTIVE":
            continue
        if stock_code in new_roster:
            continue
        corp_cls = corp_cls_lookup.get(old.corp_code or "")
        diff.delisted.append(
            _make_event(
                event_type="DELISTED",
                company=old,
                observed_date=observed_date,
                corp_cls=corp_cls,
                change_reason=REASON_DELISTED,
                change_detail={
                    "stock_code": old.stock_code,
                    "corp_code": old.corp_code,
                    "corp_name": old.corp_name,
                },
            )
        )

    return diff
