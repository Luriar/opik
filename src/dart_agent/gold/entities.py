"""기업 사전(EntityIndex) — 이름/코드/시장구분/사업코드를 한 곳에 모아 재연결한다.

용도:
  1. entity_relation 재연결: 보고서에 등장한 거래상대방·최대주주·교환대상 등 '이름'을
     corp_code/stock_code로 매핑해 온전한 엔티티로 만든다. (사용자 요구)
  2. dim/company_dictionary 데이터셋(Gold)·serving 캐시의 단일 출처.

출처:
  - dart_corp_code   : 전체 법인(비상장 포함). 거래상대방·모회사 매칭에 필수.
  - listed_company   : 코스피·코스닥 시장구분(market_type)·상장상태.
  - company_overview : 사업코드(induty_code) 등은 보고서별 Silver에서 가져온다(여기선 코드 보관).

해석 정책:
  - 이름 정규화(법인격·공백·괄호 제거) 후 정확 일치.
  - 동일 정규화명이 여러 corp_code면 '상장사 우선', 그래도 모호하면 None(오연결 방지).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Protocol

# 법인격 접미·접두 토큰. 정규화 시 제거해 "삼성전자(주)" == "주식회사 삼성전자".
_LEGAL_TOKENS = ("주식회사", "(주)", "㈜", "(유)", "유한회사", "(재)", "(사)", "co.,ltd", "co.ltd", "corp", "inc")
_PAREN = re.compile(r"\([^)]*\)")
_NONWORD = re.compile(r"[^0-9a-z가-힣]")


def normalize_company_name(name: str | None) -> str:
    """매칭용 정규화 키. 법인격/괄호/공백/기호 제거 + 소문자."""
    if not name:
        return ""
    s = str(name).strip().lower()
    s = _PAREN.sub("", s)
    for tok in _LEGAL_TOKENS:
        s = s.replace(tok, "")
    return _NONWORD.sub("", s)


@dataclass(frozen=True)
class ResolvedEntity:
    corp_code: str
    corp_name: str
    stock_code: str | None
    market_type: str | None  # KOSPI / KOSDAQ / None(비상장)
    is_listed: bool


class NameResolver(Protocol):
    """이름 → 엔티티 해석기. processor가 의존하는 최소 인터페이스(테스트 주입 가능)."""

    def resolve(self, name: str | None) -> ResolvedEntity | None: ...


class NullResolver:
    """해석 비활성(오프라인/테스트). 항상 None — 관계는 mentioned_name만 기록된다."""

    def resolve(self, name: str | None) -> ResolvedEntity | None:  # noqa: D102
        return None


@dataclass
class DictResolver:
    """테스트/소규모용. {정규화명: ResolvedEntity}."""

    by_norm: dict[str, ResolvedEntity] = field(default_factory=dict)

    def add(self, entity: ResolvedEntity) -> None:
        self.by_norm[normalize_company_name(entity.corp_name)] = entity

    def resolve(self, name: str | None) -> ResolvedEntity | None:
        return self.by_norm.get(normalize_company_name(name))


class EntityIndex:
    """DB(dart_corp_code + listed_company)에서 적재한 전체 법인 사전."""

    def __init__(self, entities: list[ResolvedEntity]):
        self._by_code: dict[str, ResolvedEntity] = {e.corp_code: e for e in entities}
        # 정규화명 → corp_code 목록(동명이인/동일명 법인 다수 가능).
        self._by_norm: dict[str, list[str]] = {}
        for e in entities:
            self._by_norm.setdefault(normalize_company_name(e.corp_name), []).append(e.corp_code)

    @classmethod
    def from_engine(cls, engine) -> "EntityIndex":
        from sqlalchemy import text

        listed: dict[str, dict] = {}
        with engine.connect() as conn:
            for r in conn.execute(
                text("SELECT corp_code, market_type, status FROM listed_company WHERE corp_code IS NOT NULL")
            ).mappings():
                listed[str(r["corp_code"])] = dict(r)
            rows = conn.execute(
                text("SELECT corp_code, corp_name, stock_code FROM dart_corp_code")
            ).mappings().all()
        entities: list[ResolvedEntity] = []
        for r in rows:
            code = str(r["corp_code"])
            lc = listed.get(code)
            market = (lc or {}).get("market_type")
            market = market if market in ("KOSPI", "KOSDAQ") else None
            entities.append(
                ResolvedEntity(
                    corp_code=code,
                    corp_name=str(r["corp_name"]),
                    stock_code=(str(r["stock_code"]) if r["stock_code"] else None),
                    market_type=market,
                    is_listed=lc is not None and (lc.get("status") == "ACTIVE"),
                )
            )
        return cls(entities)

    def resolve(self, name: str | None) -> ResolvedEntity | None:
        norm = normalize_company_name(name)
        if not norm:
            return None
        codes = self._by_norm.get(norm)
        if not codes:
            return None
        if len(codes) == 1:
            return self._by_code[codes[0]]
        # 모호 — 상장사가 정확히 하나면 그걸로, 아니면 연결 보류(오연결 방지).
        listed = [c for c in codes if self._by_code[c].is_listed]
        if len(listed) == 1:
            return self._by_code[listed[0]]
        return None

    def get(self, corp_code: str) -> ResolvedEntity | None:
        return self._by_code.get(corp_code)

    def dictionary_rows(self, snapshot_date: str) -> list[dict]:
        """dim/company_dictionary 데이터셋 row(식별·재연결 사전의 영속 스냅샷).

        사업 속성(induty_code 등)은 company_snapshot이 담당하고, 여기서는 식별/해석 키
        (corp_code↔이름↔stock_code↔시장구분)만 둔다. corp_name_norm으로 이름 재연결을 지원한다.
        """
        return [
            {
                "corp_code": e.corp_code,
                "corp_name": e.corp_name,
                "corp_name_norm": normalize_company_name(e.corp_name),
                "stock_code": e.stock_code,
                "market_type": e.market_type,
                "is_listed": e.is_listed,
                "snapshot_date": snapshot_date,
            }
            for e in self._by_code.values()
        ]

    def __len__(self) -> int:
        return len(self._by_code)
