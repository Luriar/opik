"""KSIC(한국표준산업분류) induty_code → 산업명 사전.

company_overview.induty_code(5자리 세분류)를 사람이 읽는 산업명으로 변환한다.
정확도 우선: 대분류(섹션)는 2자리 division 범위로 항상 정확히 매핑되고, 중분류(2자리)는 확신 가능한
것만 seed로 둔다. 5자리 세분류는 오기재 위험이 커서 기본 seed에 넣지 않고, 운영에서 공식 KSIC CSV를
load_ksic_csv로 주입해 확장한다(코드↔명 정본은 통계청).

반환 정책(industry_name): 세분류(5) → 중분류(2) → 대분류(섹션) 순으로 가장 구체적인 이름.
"""
from __future__ import annotations

import csv

# 대분류(섹션): (division 하한, 상한, 섹션코드, 섹션명). 통계청 KSIC 기준(안정적).
_SECTIONS: list[tuple[int, int, str, str]] = [
    (1, 3, "A", "농업, 임업 및 어업"),
    (5, 8, "B", "광업"),
    (10, 34, "C", "제조업"),
    (35, 35, "D", "전기, 가스, 증기 및 공기 조절 공급업"),
    (36, 39, "E", "수도, 하수 및 폐기물 처리, 원료 재생업"),
    (41, 42, "F", "건설업"),
    (45, 47, "G", "도매 및 소매업"),
    (49, 52, "H", "운수 및 창고업"),
    (55, 56, "I", "숙박 및 음식점업"),
    (58, 63, "J", "정보통신업"),
    (64, 66, "K", "금융 및 보험업"),
    (68, 68, "L", "부동산업"),
    (70, 73, "M", "전문, 과학 및 기술 서비스업"),
    (74, 76, "N", "사업시설 관리, 사업 지원 및 임대 서비스업"),
    (84, 84, "O", "공공행정, 국방 및 사회보장 행정"),
    (85, 85, "P", "교육 서비스업"),
    (86, 87, "Q", "보건업 및 사회복지 서비스업"),
    (90, 91, "R", "예술, 스포츠 및 여가관련 서비스업"),
    (94, 96, "S", "협회 및 단체, 수리 및 기타 개인 서비스업"),
    (97, 98, "T", "가구 내 고용활동 등"),
    (99, 99, "U", "국제 및 외국기관"),
]

# 중분류(2자리) seed — 상장사에 흔하고 확신 가능한 것만. 운영 CSV로 확장.
_DIVISIONS: dict[str, str] = {
    "10": "식료품 제조업", "20": "화학 물질 및 화학제품 제조업; 의약품 제외",
    "21": "의료용 물질 및 의약품 제조업", "22": "고무 및 플라스틱제품 제조업",
    "24": "1차 금속 제조업", "25": "금속 가공제품 제조업; 기계 및 가구 제외",
    "26": "전자부품, 컴퓨터, 영상, 음향 및 통신장비 제조업", "27": "의료, 정밀, 광학 기기 및 시계 제조업",
    "28": "전기장비 제조업", "29": "기타 기계 및 장비 제조업", "30": "자동차 및 트레일러 제조업",
    "35": "전기, 가스, 증기 및 공기 조절 공급업", "41": "종합 건설업",
    "46": "도매 및 상품 중개업", "47": "소매업; 자동차 제외",
    "58": "출판업", "62": "컴퓨터 프로그래밍, 시스템 통합 및 관리업", "63": "정보서비스업",
    "64": "금융업", "65": "보험 및 연금업", "66": "금융 및 보험 관련 서비스업",
    "68": "부동산업", "70": "연구개발업",
}

# 5자리 세분류 — 기본 비움(오기재 방지). load_ksic_csv로 주입.
_SUBCLASSES: dict[str, str] = {}


def _norm(code) -> str:
    return str(code or "").strip()


def section_name(induty_code) -> str | None:
    code = _norm(induty_code)
    if len(code) < 2 or not code[:2].isdigit():
        return None
    div = int(code[:2])
    for lo, hi, _sec, name in _SECTIONS:
        if lo <= div <= hi:
            return name
    return None


def industry_name(induty_code) -> str | None:
    """가장 구체적인 산업명(세분류→중분류→대분류). 없으면 None."""
    code = _norm(induty_code)
    if not code:
        return None
    if code in _SUBCLASSES:
        return _SUBCLASSES[code]
    if code[:2] in _DIVISIONS:
        return _DIVISIONS[code[:2]]
    return section_name(code)


def load_ksic_csv(path: str) -> int:
    """공식 KSIC CSV(code,name)로 세분류/중분류 사전을 확장한다. 적재 건수 반환.

    CSV 형식: 헤더 code,name (code는 2자리 중분류 또는 5자리 세분류).
    """
    n = 0
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            code = _norm(row.get("code"))
            name = (row.get("name") or "").strip()
            if not code or not name:
                continue
            (_DIVISIONS if len(code) == 2 else _SUBCLASSES)[code] = name
            n += 1
    return n
