"""
OPIK /chat 엔드포인트 v2 — opik_server.py에 통합할 코드

기존 /chat 엔드포인트를 이 코드로 교체하거나,
기존 함수에 표시된 # NEW: 주석 부분만 병합하세요.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import json
import re

# NEW: 대화 메모리 임포트
from conversation_store import store as conversation_store

# NEW: 프롬프트 로더
def load_prompt(name: str) -> str:
    with open(f"/root/opik-server/prompts/{name}", "r", encoding="utf-8") as f:
        return f.read()

def inject_date_vars(prompt: str) -> str:
    now = datetime.now()
    weekday_map = ["월", "화", "수", "목", "금", "토", "일"]
    return prompt.replace("{CURRENT_DATE}", now.strftime("%Y-%m-%d")) \
                 .replace("{CURRENT_YEAR}", str(now.year)) \
                 .replace("{CURRENT_DAY_OF_WEEK}", weekday_map[now.weekday()]) \
                 .replace("{CURRENT_TIME}", now.strftime("%H:%M"))


# ── Request/Response Models ──

class ChatRequest(BaseModel):
    query: str
    session_id: str = "default"


class ChatResponse(BaseModel):
    response: str
    session_id: str
    intent: str | None = None
    context_full: bool = False
    turn_count: int = 0
    sources: list[dict] | None = None


# ── 프롬프트 캐싱 (재로딩 방지) ──

SYSTEM_PROMPT = inject_date_vars(load_prompt("system.md"))
INTENT_PARSER_PROMPT = inject_date_vars(load_prompt("intent_parser.md"))
ANSWER_GEN_PROMPT = inject_date_vars(load_prompt("answer_generator.md"))


# ── Bedrock 호출 헬퍼 (기존 코드 재사용 가정) ──

async def call_bedrock(prompt: str, model_id: str = "haiku") -> str:
    """
    Bedrock Claude 호출. 기존 opik_server.py의 bedrock 호출 함수를 사용하세요.
    이 함수는 예시입니다.
    """
    # 기존 코드 사용: await bedrock_client.invoke_model(...)
    raise NotImplementedError("기존 bedrock 호출 함수로 교체하세요")


# ── Intent Parser ──

async def parse_intent(query: str, session_id: str) -> dict:
    """사용자 질문 → intent 분류 + 파라미터 추출"""
    prompt = inject_date_vars(INTENT_PARSER_PROMPT)
    prompt += f"\n\n사용자 질문: {query}"

    raw = await call_bedrock(prompt, model_id="haiku")

    try:
        # JSON 추출 (```json ... ``` 블록 또는 raw JSON)
        json_match = re.search(r'\{[\s\S]*\}', raw)
        if json_match:
            return json.loads(json_match.group())
    except json.JSONDecodeError:
        pass

    # Fallback: 파싱 실패 시 general로 처리
    return {
        "intent": "general",
        "params": {"tickers": [], "brokerages": [], "sectors": [],
                    "time_range": "all", "keywords": [], "refers_to_previous": False},
        "original_query": query,
        "reasoning": "JSON 파싱 실패 — fallback to general"
    }


# ── Answer Generator ──

async def generate_answer(
    query: str,
    intent: str,
    params: dict,
    search_results: str,
    session_id: str
) -> str:
    """검색 결과 + 대화 맥락 → 최종 응답 생성"""

    # NEW: 대화 맥락 주입
    conv_history = conversation_store.get_context_for_prompt(session_id)

    prompt = inject_date_vars(ANSWER_GEN_PROMPT)
    prompt = prompt.replace("{CONVERSATION_HISTORY}", conv_history or "(첫 대화입니다)")
    prompt = prompt.replace("{SEARCH_RESULTS}", search_results or "(검색 결과 없음)")
    prompt += f"\n\n사용자 질문: {query}"
    prompt += f"\n\n분류된 intent: {intent}"

    # NEW: investment_advice_refusal은 바로 refusal 응답
    if intent == "investment_advice_refusal":
        # 검색 없이 바로 거부 응답 생성
        ticker_hint = ""
        if params.get("tickers"):
            ticker_hint = f"{params['tickers'][0]}에 대한 "
        prompt += f"\n\n이 질문은 투자 조언 요청으로 분류되었습니다. "
        prompt += "거부 응답을 생성하고, 대신 {ticker_hint}최근 리포트를 검색해볼지 물어보세요."

    # 모델 선택: Answer Generation은 Sonnet 권장
    answer = await call_bedrock(prompt, model_id="sonnet")

    return answer


# ── /chat 엔드포인트 (기존 코드 교체) ──

async def chat_endpoint_v2(request: ChatRequest) -> ChatResponse:
    query = request.query
    session_id = request.session_id

    # NEW: "새로 시작" 감지 → 세션 초기화
    reset_triggers = ["새로 시작", "처음부터", "리셋", "다시", "초기화"]
    if any(trigger in query for trigger in reset_triggers) and len(query) < 10:
        conversation_store.reset_session(session_id)
        return ChatResponse(
            response="새로운 대화를 시작합니다. 무엇을 도와드릴까요?",
            session_id=session_id,
            intent="general",
            context_full=False,
            turn_count=0
        )

    # Step 1: Intent 분류
    intent_data = await parse_intent(query, session_id)
    intent = intent_data["intent"]
    params = intent_data.get("params", {})

    # NEW: 이전 대화 맥락 참조 감지 → 검색어 보강
    if params.get("refers_to_previous"):
        conv_context = conversation_store.get_context_for_prompt(session_id)
        if conv_context:
            # conversation_history에서 마지막 언급된 종목/섹터를 찾아 params 보강
            # (간단 버전 — 실제로는 intent_parser가 params에 채워야 함)
            pass

    # Step 2: Intent에 따라 데이터 검색 (기존 search 로직 재사용)
    search_results = ""
    sources = []

    if intent == "report_search":
        # 기존 FAISS search 호출
        search_results, sources = await search_faiss(query, params)
    elif intent == "dart_query":
        # 기존 DART query 호출
        search_results, sources = await search_dart(query, params)
    elif intent == "stock_price":
        # 기존 stock price 조회
        search_results, sources = await fetch_stock_price(query, params)
    elif intent == "investment_advice_refusal":
        search_results = ""  # 검색 불필요
    else:  # general
        search_results = ""

    # Step 3: 응답 생성 (검색 결과 + 대화 맥락 포함)
    response_text = await generate_answer(
        query=query,
        intent=intent,
        params=params,
        search_results=search_results,
        session_id=session_id
    )

    # NEW: 대화 저장
    conversation_store.add_turn(session_id, "user", query)
    conversation_store.add_turn(session_id, "assistant", response_text)

    # NEW: 컨텍스트 가득 참 체크
    context_full = conversation_store.is_context_full(session_id)

    # 컨텍스트 가득 참이면 응답에 경고 추가
    if context_full:
        response_text += (
            "\n\n---\n"
            "[대화가 길어져 이전 맥락 일부가 요약되었습니다.]\n"
            "[위 내용은 최근 대화를 바탕으로 한 응답입니다.]\n"
            '[대화를 새로 시작하려면 "새로 시작"이라고 입력해주세요.]'
        )

    session = conversation_store.get_or_create(session_id)

    return ChatResponse(
        response=response_text,
        session_id=session_id,
        intent=intent,
        context_full=context_full,
        turn_count=len(session.turns),
        sources=sources if sources else None
    )


# ── FastAPI 라우터 등록 (기존 app에 추가) ──
# app.add_api_route("/chat", chat_endpoint_v2, methods=["POST"])
# 또는 기존 @app.post("/chat") 함수를 위 코드로 교체
