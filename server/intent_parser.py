"""
Intent Parser — Stage 1 preprocessing for OPIK chatbot.
Extracts structured filters (date range, entities, intent type) from user questions
using Bedrock Haiku for fast (~0.5s) JSON output.

Routes to:
  - report_search      → FAISS semantic search (analyst reports)
  - dart_financial     → DART financials (pure SQL)
  - dart_insider       → DART insider transactions (pure SQL)
  - dart_disclosure    → DART disclosure events (hybrid FAISS + SQL)
  - dart_shareholder   → DART major shareholders (pure SQL)
  - hybrid             → both reports + DART
  - general            → no data needed (greeting, meta-question)
  - refuse             → out-of-scope (investment advice, algorithms, etc.)
"""

import json
import logging
import re
import time
from typing import Optional, Dict, Any, List

import boto3

logger = logging.getLogger("opik.intent")


DART_DISCLOSURE_KEYWORDS = (
    "공시", "dart", "opendart", "전자공시", "사업보고서", "분기보고서", "반기보고서",
    "정기보고서", "주요사항보고서", "증권신고서", "접수번호", "rcept_no", "rcept",
)
DART_FINANCIAL_KEYWORDS = (
    "재무제표", "재무", "매출", "영업이익", "당기순이익", "순이익", "자산", "부채",
    "자본", "per", "pbr", "실적",
)
DART_INSIDER_KEYWORDS = (
    "내부자", "임원 매수", "임원 매도", "임원 거래", "임원매수", "임원매도",
    "임원거래", "주식등의대량보유", "대량보유",
)
DART_SHAREHOLDER_KEYWORDS = (
    "주요주주", "최대주주", "대주주", "주주현황", "지분율", "지분 변동", "지분변동",
)
REPORT_KEYWORDS = (
    "리포트", "증권사", "애널리스트", "목표주가", "목표가", "투자의견",
    "상승여력", "broker", "report",
)


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    compact = re.sub(r"\s+", "", text)
    return any(keyword in text or re.sub(r"\s+", "", keyword) in compact for keyword in keywords)


def infer_dart_intent_override(question: str) -> Optional[str]:
    """Return a deterministic DART intent when the user explicitly asks for DART data.

    Haiku occasionally classifies disclosure questions as report_search. The docs define
    DART disclosures/financials/shareholders/insider trades as separate data sources, so
    explicit DART terms should not depend only on LLM classification.
    """
    text = (question or "").lower()
    if not text:
        return None

    has_disclosure = _contains_any(text, DART_DISCLOSURE_KEYWORDS)
    has_financial = _contains_any(text, DART_FINANCIAL_KEYWORDS)
    has_insider = _contains_any(text, DART_INSIDER_KEYWORDS)
    has_shareholder = _contains_any(text, DART_SHAREHOLDER_KEYWORDS)
    has_dart_signal = has_disclosure or has_financial or has_insider or has_shareholder
    if not has_dart_signal:
        return None

    has_report_signal = _contains_any(text, REPORT_KEYWORDS)
    if has_report_signal and has_dart_signal:
        return "hybrid"
    if has_insider:
        return "dart_insider"
    if has_shareholder:
        return "dart_shareholder"
    if has_financial:
        return "dart_financial"
    return "dart_disclosure"


def apply_dart_intent_override(question: str, result: "IntentResult") -> "IntentResult":
    """Correct LLM intent when explicit DART terms were routed to a non-DART intent."""
    override = infer_dart_intent_override(question)
    if not override or result.is_refusal:
        return result
    if result.intent == override or result.intent == "hybrid":
        return result
    if result.intent.startswith("dart_") and result.intent != "dart_query":
        return result

    original = result.intent
    result.intent = override
    result._raw["intent"] = override
    if override != "hybrid":
        result.search_query = None
        result._raw["search_query"] = None
    logger.info("Intent override: %s -> %s for explicit DART query", original, override)
    return result

INTENT_SYSTEM_PROMPT = """당신은 OPIK 챗봇의 의도 파악기입니다.
사용자 질문을 분석하여 구조화된 JSON으로 반환하세요.

## 데이터 소스
1. analyst_reports — 증권사 분석 리포트 (투자의견, 목표주가, 키워드, 리스크)
2. dart_financials — DART 재무제표 (매출액, 영업이익, 순이익, 자산, 부채, 자본)
3. dart_insider_transactions — DART 임원·주요주주 거래내역 (매수/매도)
4. dart_disclosure_events — DART 공시 이벤트 (공시 종류, 제목, 내용)
5. dart_major_shareholders — DART 주요주주 현황

## intent 분류

### report_search — 데이터 기반 factual 질문 (증권사 리포트)
애널리스트 리포트 검색이 필요한 모든 질문.
- "삼성전자 목표주가 알려줘"
- "한국투자증권에서 SK하이닉스 리포트 있어?"
- "어제 올라온 반도체 리포트 보여줘"
- "최근 리포트 요약해줘"

**주의: "공시"가 포함된 질문은 report_search가 아니라 dart_disclosure입니다.**
"공시 알려줘"는 DART 공시 데이터를 요청하는 것입니다.

### dart_financial — 재무제표 질문
- 매출, 영업이익, 순이익, 자산, 부채, 재무제표, 실적

### dart_insider — 내부자 거래 질문
- 임원 매매, 지분 변동, 주식 매수/매도 (개인 거래)

### dart_disclosure — 공시 질문 (DART 공시 데이터)
"공시" 키워드가 포함된 모든 질문은 무조건 이 intent로 분류하세요.
- "공시 알려줘", "공시 보여줘", "공시 정보 줘", "공시 검색"
- "DART 공시", "공시 이벤트", "공시 내용"
- 날짜와 함께 "공시" → dart_disclosure (report_search가 아님!)

### dart_shareholder — 주요주주 질문
- 주요주주, 최대주주, 주주현황, 지분율

### hybrid — 여러 소스 필요
- 애매하거나 리포트+공시가 모두 필요한 경우

### general — 시스템/잡담
- "안녕하세요", "OPIK이 뭐야?", "어떤 기능 있어?", "고마워"
- 단, OPIK의 기능 범위를 벗어나는 질문은 general이 아닌 refuse로 분류해야 함

### refuse — CRITICAL !!! 최우선 분류, OPIK의 범위를 벗어나는 모든 질문
OPIK은 금융 정보 검색 챗봇입니다. 다음 질문은 무조건 이 intent로 분류하세요:

**투자 조언 (Zone C):**
- "뭐 사는게 좋을까?", "어떤 주식이 좋을까", "추천 종목", "매수 추천"
- "지금 사도 될까?", "팔아야 하나?", "손절해야 하나?"
- "가장 좋은 종목", "제일 나은", "뭐가 좋아"
- "포트폴리오", "비중 조절", "투자 전략", "매매 타이밍"
- "오를까", "내릴까" (예측성 질문)
- "레버리지", "공매도", "선물" (투자 기법 추천)
- "수익률", "돈 벌", "얼마나 오를까"

**OPIK 범위 밖 (Zone D — 기능 밖):**
- 알고리즘/코딩 질문: "정렬 알고리즘", "퀵소트", "코드 작성", "프로그래밍", "Python으로", "자바스크립트", "React", "API 만들기"
- 수학/과학 문제 풀이: "방정식 풀어줘", "미적분", "화학 반응식"
- 일반 지식 Q&A: "역사", "지리", "요리 레시피", "영화 추천"
- 번역: "번역해줘", "영어로 바꿔줘"
- 건강/의료: "병원", "증상", "치료법"
- 법률: "법률 상담", "소송"

**투자 조언과 factual 질문 구분법:**
- "무엇을 해야 하는지" 묻는가? → refuse
- "무엇이 있는지" 묻는가? → report_search 등 해당 intent

**헷갈리기 쉬운 예:**
- "삼성전자 목표주가 알려줘" → report_search (데이터 조회)
- "삼성전자 지금 사도 될까?" → refuse (매매 결정)
- "반도체 섹터 전망 리포트 있어?" → report_search (리포트 검색)
- "반도체 섹터 지금 투자해도 될까?" → refuse (투자 조언)
- "퀵소트 구현해줘" → refuse (OPIK 범위 밖)
- "BFS 알고리즘 설명해줘" → refuse (OPIK 범위 밖)

## 날짜 해석
- 오늘 날짜는 {TODAY_DATE}, 현재 연도는 {TODAY_YEAR}년입니다.
- "오늘", "어제" → date_from/date_to를 {TODAY_DATE} 기준으로 계산
- "이번 주" → date_from/date_to를 이번 주 월~일로 설정
- "최근" → date_from: null, date_to: null, is_recent: true
- "2024년" → date_from: "2024-01-01", date_to: "2024-12-31"
- "2024년 3월" → date_from: "2024-03-01", date_to: "2024-03-31"
- "2024년 3월 15일" → date_from: "2024-03-15", date_to: "2024-03-15"
- "6월 13일", "1월 13일" (연도 없이 월일만) → {TODAY_YEAR}년 기준으로 계산. 6월이 현재 월이므로 {TODAY_YEAR}년.
  예: "6월 13일" → date_from: "{TODAY_YEAR}-06-13", date_to: "{TODAY_YEAR}-06-13"
  예: "1월 13일" → date_from: "{TODAY_YEAR}-01-13", date_to: "{TODAY_YEAR}-01-13"
- "13일" (일자만, follow-up 질문) → date_from/date_to: null, refers_to_previous: true 로 설정
  (서버가 conversation context에서 월/연도를 찾아 완성합니다)
- 구체적 날짜 없으면 null

## 출력 JSON 형식
{
  "intent": "report_search",
  "date_from": "2026-01-01" | null,
  "date_to": "2026-01-31" | null,
  "is_recent": false,
  "companies": ["회사명1", "회사명2"],
  "stock_codes": ["000660", "005930"],
  "securities": ["한국투자증권"],
  "search_query": "FAISS 최적화 검색어" | null,
  "sql_hint": "SQL 설명 (한줄)" | null,
  "refers_to_previous": false,
  "previous_entities": ["이전대화에서 추출한 종목명/키워드"] | null
}

### refers_to_previous — 대화 맥락 참조 감지
다음 표현이 포함되면 refers_to_previous: true로 설정하세요:
- "이 중에서", "그 중에서", "여기서", "이 중", "그 중"
- "아까", "이전에", "방금", "위에서", "앞에서"
- "그 종목", "이 기업", "같은 조건으로"
- "추가로", "더 자세히"
- 비교/필터 요청 ("가장 ~한", "제일 ~한")이 이전 대화의 결과를 대상으로 하는 경우

refers_to_previous가 true이면, previous_entities에 이전 대화의 종목명이나 키워드를 추정하여 넣으세요.
- "이 중에서 가장 저평가된 기업" → previous_entities에 이전 응답에 나온 종목명들을 추정
- "아까 그 종목 목표주가" → previous_entities에 null (서버가 conversation_history에서 찾음)

## 예시
Q: SK하이닉스 목표주가 알려줘
A: {"intent": "report_search", "date_from": null, "date_to": null, "is_recent": true, "companies": ["SK하이닉스"], "stock_codes": [], "securities": [], "search_query": "SK하이닉스 목표주가 투자의견", "sql_hint": null}

Q: 삼성전자 2024년 매출 얼마야
A: {"intent": "dart_financial", "date_from": "2024-01-01", "date_to": "2024-12-31", "is_recent": false, "companies": ["삼성전자"], "stock_codes": ["005930"], "securities": [], "search_query": null, "sql_hint": "SELECT revenue WHERE company=삼성전자 AND year=2024"}

Q: 안녕하세요
A: {"intent": "general", "date_from": null, "date_to": null, "is_recent": false, "companies": [], "stock_codes": [], "securities": [], "search_query": null, "sql_hint": null}

Q: 2026년 1월 3일 리포트 보여줘
A: {"intent": "report_search", "date_from": "2026-01-03", "date_to": "2026-01-03", "is_recent": false, "companies": [], "stock_codes": [], "securities": [], "search_query": null, "sql_hint": null}

Q: 오늘 뭐 사는게 좋을까
A: {"intent": "refuse", "date_from": null, "date_to": null, "is_recent": false, "companies": [], "stock_codes": [], "securities": [], "search_query": null, "sql_hint": null}

Q: 이번 주 추천 종목 알려줘
A: {"intent": "refuse", "date_from": null, "date_to": null, "is_recent": false, "companies": [], "stock_codes": [], "securities": [], "search_query": null, "sql_hint": null}

Q: 삼성전자 지금 사도 될까
A: {"intent": "refuse", "date_from": null, "date_to": null, "is_recent": false, "companies": [], "stock_codes": [], "securities": [], "search_query": null, "sql_hint": null}

Q: 퀵소트 알고리즘 구현해줘
A: {"intent": "refuse", "date_from": null, "date_to": null, "is_recent": false, "companies": [], "stock_codes": [], "securities": [], "search_query": null, "sql_hint": null, "refers_to_previous": false, "previous_entities": null}

Q: 이 중에서 시가총액 가장 낮은 기업 알려줘
A: {"intent": "report_search", "date_from": null, "date_to": null, "is_recent": false, "companies": [], "stock_codes": [], "securities": [], "search_query": "시가총액 낮은 기업", "sql_hint": null, "refers_to_previous": true, "previous_entities": "이전 응답에 나온 기업들"}

Q: 삼성전자 최근 공시랑 목표주가 같이 알려줘
A: {"intent": "hybrid", "date_from": null, "date_to": null, "is_recent": true, "companies": ["삼성전자"], "stock_codes": ["005930"], "securities": [], "search_query": "삼성전자 목표주가 투자의견 공시", "sql_hint": "SELECT disclosure events and analyst reports for 삼성전자"}

Q: 2026년6월 19일 공시 알려줘
A: {"intent": "dart_disclosure", "date_from": "2026-06-19", "date_to": "2026-06-19", "is_recent": false, "companies": [], "stock_codes": [], "securities": [], "search_query": null, "sql_hint": null}

Q: 6월 13일 dart 정보 알려줘
A: {"intent": "dart_disclosure", "date_from": "2026-06-13", "date_to": "2026-06-13", "is_recent": false, "companies": [], "stock_codes": [], "securities": [], "search_query": null, "sql_hint": null}

Q: 공시 알려줘
A: {"intent": "dart_disclosure", "date_from": null, "date_to": null, "is_recent": true, "companies": [], "stock_codes": [], "securities": [], "search_query": null, "sql_hint": null}

Q: 1월 13일 공시 보여줘
A: {"intent": "dart_disclosure", "date_from": "2026-01-13", "date_to": "2026-01-13", "is_recent": false, "companies": [], "stock_codes": [], "securities": [], "search_query": null, "sql_hint": null}

Q: 14일은? (이전 대화에서 6월 맥락이 있는 follow-up)
A: {"intent": "dart_disclosure", "date_from": null, "date_to": null, "is_recent": false, "companies": [], "stock_codes": [], "securities": [], "search_query": null, "sql_hint": null, "refers_to_previous": true, "previous_entities": ["DART 공시"]}

오직 JSON만 출력하세요. 다른 텍스트는 출력하지 마세요."""


class IntentResult:
    """Parsed intent from user question."""
    def __init__(self, data: Dict[str, Any]):
        self.intent = data.get("intent", "general")
        self.date_from = data.get("date_from")
        self.date_to = data.get("date_to")
        self.is_recent = data.get("is_recent", False)
        self.companies = data.get("companies") or []
        self.stock_codes = data.get("stock_codes") or []
        self.securities = data.get("securities") or []
        self.search_query = data.get("search_query")
        self.sql_hint = data.get("sql_hint")
        self.refers_to_previous = data.get("refers_to_previous", False)
        self.previous_entities = data.get("previous_entities")
        self._raw = data

    @property
    def needs_reports(self) -> bool:
        return self.intent in ("report_search", "hybrid")

    @property
    def needs_dart(self) -> bool:
        return self.intent in (
            "dart_financial", "dart_insider", "dart_disclosure",
            "dart_shareholder", "hybrid"
        )

    @property
    def is_general(self) -> bool:
        return self.intent == "general"

    @property
    def is_refusal(self) -> bool:
        return self.intent == "refuse"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "intent": self.intent,
            "date_from": self.date_from,
            "date_to": self.date_to,
            "is_recent": self.is_recent,
            "companies": self.companies,
            "stock_codes": self.stock_codes,
            "securities": self.securities,
            "search_query": self.search_query,
            "sql_hint": self.sql_hint,
            "refers_to_previous": self.refers_to_previous,
            "previous_entities": self.previous_entities,
        }


class IntentParser:
    """Fast intent parsing using Bedrock Haiku (APAC inference profile)."""

    def __init__(
        self,
        model_id: str = "apac.anthropic.claude-3-haiku-20240307-v1:0",
        region: str = "ap-northeast-2",
        max_tokens: int = 256,
    ):
        self.model_id = model_id
        self.region = region
        self.max_tokens = max_tokens
        self._client: Optional[Any] = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client("bedrock-runtime", region_name=self.region)
        return self._client

    def parse(self, question: str) -> IntentResult:
        """Parse a user question into structured intent."""
        t0 = time.time()

        # Inject current date into the system prompt
        from datetime import datetime
        now = datetime.now()
        system_prompt = INTENT_SYSTEM_PROMPT.replace(
            "{TODAY_DATE}", now.strftime("%Y-%m-%d")
        ).replace(
            "{TODAY_YEAR}", str(now.year)
        )

        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": self.max_tokens,
            "system": system_prompt,
            "messages": [{"role": "user", "content": question}],
            "temperature": 0.0,
        })

        try:
            resp = self.client.invoke_model(
                modelId=self.model_id,
                contentType="application/json",
                accept="application/json",
                body=body,
            )
            resp_body = json.loads(resp["body"].read())

            text = ""
            for block in resp_body.get("content", []):
                if block.get("type") == "text":
                    text += block["text"]

            # Parse JSON — may have markdown wrapping
            text = text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[-1].rsplit("\n```", 1)[0]
                if text.startswith("json"):
                    text = text[4:].strip()

            data = json.loads(text)
            result = apply_dart_intent_override(question, IntentResult(data))

            elapsed = (time.time() - t0) * 1000
            logger.info(
                "Intent parsed: intent=%s companies=%s codes=%s (%.0fms)",
                result.intent, result.companies, result.stock_codes, elapsed,
            )
            return result

        except (json.JSONDecodeError, KeyError) as e:
            elapsed = (time.time() - t0) * 1000
            logger.warning("Intent parse failed (%.0fms): %s — falling back to general", elapsed, e)
            return IntentResult({"intent": "general"})

# Singleton
_default_parser: Optional[IntentParser] = None


def get_parser() -> IntentParser:
    global _default_parser
    if _default_parser is None:
        _default_parser = IntentParser()
    return _default_parser
