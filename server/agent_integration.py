"""
Agent Integration — wires server/agents/ into the existing FastAPI server.

Provides:
  - init_agents(): wire global FAISS index, embedder, report IDs to agents
  - v2_chat_handler(): the /v2/chat endpoint implementation
  - get_agent_status(): health-check info about agent readiness

Usage (in opik_server.py):
  from agent_integration import init_agents, v2_chat_handler

  # After FAISS index loads:
  init_agents(faiss_index, report_ids, report_texts, embedder)

  # Add route:
  @app.post("/v2/chat", response_model=ChatResponse)
  async def v2_chat(req: ChatRequest):
      return await v2_chat_handler(req)
"""

import json
import logging
import os
import time
from typing import Optional, List

import boto3

logger = logging.getLogger("opik.agent_integration")

# Agent singletons — lazy init
_safety = None
_intent = None
_report = None
_dart = None
_analysis = None
_composer = None
_supervisor = None
_ready = False

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
AGENT_ENABLED = os.environ.get("OPIK_AGENT_ENABLED", "true").lower() != "false"


def init_agents(faiss_index, report_ids: list, report_texts: dict, embedder):
    """Wire server globals into the agent singletons. Call after FAISS loads."""
    global _safety, _intent, _report, _dart, _analysis, _composer, _supervisor, _ready

    from agents.safety_agent import SafetyAgent
    from agents.intent_agent import IntentAgent
    from agents.report_agent import ReportAgent
    from agents.dart_agent import DartAgent
    from agents.analysis_agent import AnalysisAgent
    from agents.response_composer import ResponseComposer
    from agents.supervisor import SupervisorAgent

    _safety = SafetyAgent()
    _intent = IntentAgent()
    _report = ReportAgent(
        faiss_index=faiss_index,
        report_ids=report_ids,
        report_texts=report_texts,
        embedder=embedder,
    )
    _dart = DartAgent()
    _analysis = AnalysisAgent()
    _composer = ResponseComposer()
    _supervisor = SupervisorAgent()

    _ready = True
    logger.info("Agent framework initialised: 7 agents wired, FAISS=%d vectors",
                faiss_index.ntotal if faiss_index else 0)


def get_agent_status() -> dict:
    """Return agent readiness for /health."""
    return {
        "agent_framework": "v2" if _ready else "not_initialised",
        "agents": {
            "safety": _safety is not None,
            "intent": _intent is not None,
            "report": _report is not None,
            "dart": _dart is not None,
            "analysis": _analysis is not None,
            "composer": _composer is not None,
            "supervisor": _supervisor is not None,
        },
        "enabled": AGENT_ENABLED,
    }


def _format_date_browse(date_str: str, results: list) -> str:
    """Format date-based browse results directly — no LLM summarise needed."""
    if not results:
        return f"해당 날짜({date_str})의 증권사 리포트 데이터가 없습니다."

    lines = [f"*{date_str} 증권사 리포트* ({len(results)}건)", ""]
    for r in results[:20]:
        reason = r.get("reason", "") or ""
        kw = r.get("keywords", "") or ""
        risk = r.get("risks", "") or ""
        lines.append(f"• {reason}")
        if kw:
            lines.append(f"  키워드: {kw}")
        if risk:
            lines.append(f"  리스크: {risk}")
        lines.append("")

    if len(results) > 20:
        lines.append(f"... 외 {len(results) - 20}건")
    lines.append("※ 본 정보는 증권사 리포트의 사실적 요약이며 투자 권유가 아닙니다.")
    return "\n".join(lines)


def _run_agent_pipeline(user_message: str, session_id: str = "default") -> dict:
    """Run the full agent pipeline for one user message.
    
    session_id is used to fetch conversation context for follow-up resolution.
    """
    """Run the full agent pipeline for one user message.

    Returns dict with keys: answer, sources, intent, confidence, violation_type
    """
    if not _ready:
        return {
            "answer": "Agent system is initialising. Please try again in a moment.",
            "sources": [],
            "intent": "error",
            "confidence": "low",
            "violation_type": None,
        }

    t0 = time.time()

    # DEBUG: log session_id and message length
    logger.info("Agent pipeline: session_id=%s msg_len=%d msg_preview=%r",
                 session_id, len(user_message), user_message[:80])

    # Step 0: Inject conversation context into the message when available.
    # This lets safety and intent agents resolve short references like
    # "이거 자세히 알려줘" or "챕터 2의 서막 내용 알려줘" that refer to
    # reports mentioned in previous turns. Context is prepended as a
    # structured prefix so Haiku can distinguish prior context from the
    # current question.
    _context = ""
    if session_id != "default":
        from conversation_store import store as _conv_store
        _context = _conv_store.get_context_for_prompt(session_id)
    _msg_for_safety = user_message
    if _context:
        logger.info("Agent pipeline: injecting %d chars of conversation context", len(_context))
        _msg_for_safety = (
            "[이전 대화에서 논의된 증권사 리포트에 대한 후속 질문입니다.]\n"
            f"<previous_conversation>\n{_context}\n</previous_conversation>\n\n"
            f"<current_question>\n{user_message}\n</current_question>"
        )

    # Step 1: Safety check.
    # When conversation context exists, skip the safety filter — the user
    # already passed safety on their first turn, and short follow-up messages
    # like "이거 자세히 알려줘" are consistently misclassified as out_of_domain
    # by Haiku even with full context.
    if _context:
        safety_result = {"is_safe": True, "violation_type": None, "redirect_suggestion": ""}
        logger.info("Agent pipeline: skipping safety check (has %d chars of context)", len(_context))
    else:
        safety_result = _safety.check(_msg_for_safety)
    if not safety_result.get("is_safe", True):
        answer = _safety.build_refusal_message(
            safety_result.get("violation_type"),
            safety_result.get("redirect_suggestion", ""),
        )
        elapsed = (time.time() - t0) * 1000
        logger.info("Agent pipeline: SAFETY REFUSAL type=%s (%.0fms)",
                     safety_result.get("violation_type"), elapsed)
        return {
            "answer": answer,
            "sources": [],
            "intent": "refused",
            "confidence": "high",
            "violation_type": safety_result.get("violation_type"),
            "elapsed_ms": elapsed,
        }

    # Step 2: Intent parsing.
    #
    # Three cases:
    # 1) context + short msg -> force report_search (Haiku misclassifies follow-ups)
    # 2) NO context + short msg -> try keyword search, fall through to natural re-ask
    #    (handles TTL expiry gracefully - no "session expired" system message)
    # 3) normal message -> full intent parsing via Haiku
    if _context and len(user_message.strip()) <= 60:
        logger.info("Agent pipeline: short msg + context -> forcing report_search intent")
        intent = "report_search"
        params = {
            "tickers": [], "ticker_names": [], "brokerages": [], "sectors": [],
            "time_range": None, "keywords": [user_message.strip()],
            "compare": False, "cause_tracking": False, "interpret": False,
            "is_greeting": False, "response_style": "detailed",
        }
    elif not _context and len(user_message.strip()) <= 60:
        _followup_hints = ["이거", "저거", "그거", "이것", "저것", "그것",
                           "자세히", "더 알려줘", "알려줘", "내용",
                           "이 리포트", "저 리포트", "이 종목", "이 공시"]
        _kw_text = user_message.strip()
        for _w in _followup_hints:
            _kw_text = _kw_text.replace(_w, " ")
        _kw_text = " ".join(_kw_text.split())
        if _kw_text:
            logger.info("Agent pipeline: no-context short msg -> keyword search: %r", _kw_text)
            _search_results = _report.search(_kw_text, top_k=5)
            if _search_results:
                _answer = _report.summarise(_kw_text, _search_results)
                _elapsed = (time.time() - t0) * 1000
                logger.info("Agent pipeline: no-context keyword search: %d results (%.0fms)",
                            len(_search_results), _elapsed)
                return {
                    "answer": _answer,
                    "sources": [r.get("report_id", "") for r in _search_results],
                    "intent": "report_search",
                    "confidence": "medium",
                    "violation_type": None,
                    "elapsed_ms": _elapsed,
                }
            logger.info("Agent pipeline: no-context keyword search found nothing for %r", _kw_text)
        _elapsed = (time.time() - t0) * 1000
        return {
            "answer": (
                "어떤 리포트나 종목을 찾으시는지 구체적으로 말씀해 주시면 검색해 드릴게요.\n\n"
                "예를 들어 '삼성전자 리포트 보여줘', '6월 18일 리포트', "
                "'최근 반도체 공시 알려줘'처럼 말씀해 주세요."
            ),
            "sources": [],
            "intent": "general",
            "confidence": "low",
            "violation_type": None,
            "elapsed_ms": _elapsed,
        }
    else:
        intent_result = _intent.parse(user_message, conversation_context=_context)
        intent = intent_result["intent"]
        params = intent_result.get("intent_params", {})

    # Direct date extraction: "M월 D일" without year → (current_year)-MM-DD
    # Haiku intent parser is unreliable for year inference on month-day patterns.
    _md = __import__("re").search(r"(?<!\d)(\d{1,2})월\s*(\d{1,2})일", user_message)
    if _md and not __import__("re").search(r"\d{4}년", user_message):
        _cy = str(__import__("datetime").datetime.now().year)
        _dm = _md.group(1).zfill(2)
        _dd = _md.group(2).zfill(2)
        _ed = f"{_cy}-{_dm}-{_dd}"
        logger.info("Agent date extract: %s월 %s일 → %s", _md.group(1), _md.group(2), _ed)
        params["date_from"] = _ed
        params["date_to"] = _ed
        if not params.get("ticker_names"):
            params["ticker_names"] = []

    # Step 3: Route and execute
    route = _supervisor.route(True, intent, params)

    logger.info("Agent pipeline: intent=%s route=%s compare=%s cause=%s interpret=%s",
                 intent, route,
                 params.get("compare"),
                 params.get("cause_tracking"),
                 params.get("interpret"))

    answer = ""
    sources = []
    confidence = "medium"

    if route == "general_response":
        if params.get("is_greeting"):
            answer = (
                "안녕하세요! OPIK 금융 정보 챗봇입니다.\n\n"
                "다음과 같은 정보를 검색하실 수 있습니다:\n"
                "• 증권사 애널리스트 리포트 검색 및 요약\n"
                "• DART 공시 이벤트 조회\n"
                "• 애널리스트 의견 비교 및 목표주가 확인\n\n"
                "원하시는 종목명이나 질문을 입력해 주세요."
            )
        else:
            answer = (
                "OPIK은 증권사 애널리스트 리포트와 DART 공시 데이터를 "
                "검색·요약해드리는 금융 정보 챗봇입니다. 무엇을 도와드릴까요?"
            )
        confidence = "high"

    elif route == "report_agent":
        if params.get("date_from") and not params.get("ticker_names"):
            search_results = _report.search_by_date(params["date_from"], params["date_to"], limit=50)
            logger.info("Using date-based browse for %s (%d results)", params["date_from"], len(search_results))
            if search_results:
                answer = _format_date_browse(params["date_from"], search_results)
                sources = [r.get("report_id", "") for r in search_results]
                confidence = "high"
            else:
                answer = f"해당 날짜({params['date_from']})의 증권사 리포트 데이터가 없습니다."
                sources = []
                confidence = "low"
        else:
            search_results = _report.search(user_message, top_k=10)
            if search_results:
                answer = _report.summarise(user_message, search_results)
                sources = [r.get("report_id", "") for r in search_results]
                confidence = "high"
            else:
                ticker_names = params.get("ticker_names", [])
                names_str = ", ".join(ticker_names) if ticker_names else "해당 조건"
                answer = f"{names_str}으로 검색된 애널리스트 리포트가 없습니다."
                confidence = "low"

    elif route == "report_with_analysis":
        if params.get("date_from") and not params.get("ticker_names"):
            search_results = _report.search_by_date(params["date_from"], params["date_to"], limit=50)
            logger.info("Using date-based browse for %s (%d results)", params["date_from"], len(search_results))
            report_summary = _format_date_browse(params["date_from"], search_results) if search_results else ""
        else:
            search_results = _report.search(user_message, top_k=10)
            report_summary = _report.summarise(user_message, search_results) if search_results else ""
        ticker_name = params.get("ticker_names", [""])[0] if params.get("ticker_names") else ""

        analysis = ""
        if params.get("compare") and len(search_results) >= 2:
            analysis = _analysis.compare_reports(search_results, ticker_name)
        elif params.get("cause_tracking"):
            analysis = _analysis.trace_cause(ticker_name, "최근 1주일", search_results, [])

        answer = _composer.compose_chat_response(
            intent="report_search",
            report_summary=report_summary or None,
            analysis=analysis or None,
            sources=[r.get("report_id", "") for r in search_results],
            confidence="medium",
        )
        sources = [r.get("report_id", "") for r in search_results]

    elif route in ("dart_agent", "dart_with_analysis"):
        ticker_names = params.get("ticker_names", [])
        time_range = params.get("time_range")
        dart_result = _dart.query_disclosure_events(
            companies=ticker_names,
            date_from=time_range.get("from") if time_range else None,
            date_to=time_range.get("to") if time_range else None,
        )

        if params.get("interpret") and dart_result:
            interpretation = _dart.interpret_disclosure(dart_result, "")
            answer = _composer.compose_chat_response(
                intent="dart_query",
                dart_summary=f"{dart_result}\n\n[공시 해석]\n{interpretation}",
                confidence="medium",
            )
        else:
            answer = dart_result
            confidence = "high" if dart_result and "데이터가 없습니다" not in dart_result else "low"

    elif route == "hybrid_parallel":
        report_summary = ""
        dart_summary = ""
        if params.get("date_from") and not params.get("ticker_names"):
            search_results = _report.search_by_date(params["date_from"], params["date_to"], limit=50)
            logger.info("Using date-based browse for %s (%d results)", params["date_from"], len(search_results))
            report_summary = _format_date_browse(params["date_from"], search_results) if search_results else ""
        else:
            search_results = _report.search(user_message, top_k=10)
            report_summary = _report.summarise(user_message, search_results) if search_results else ""
            sources = [r.get("report_id", "") for r in search_results]

        ticker_names = params.get("ticker_names", [])
        dart_result = _dart.query_disclosure_events(companies=ticker_names)
        if dart_result and "데이터가 없습니다" not in dart_result:
            dart_summary = dart_result

        answer = _composer.compose_chat_response(
            intent="hybrid",
            report_summary=report_summary or None,
            dart_summary=dart_summary or None,
            sources=sources,
            confidence="medium",
        )

    else:
        answer = "처리할 수 없는 요청입니다."

    elapsed = (time.time() - t0) * 1000
    logger.info("Agent pipeline: intent=%s route=%s (%.0fms, %d sources)",
                 intent, route, elapsed, len(sources))

    return {
        "answer": answer,
        "sources": sources,
        "intent": intent,
        "confidence": confidence,
        "violation_type": None,
        "elapsed_ms": elapsed,
    }


async def v2_chat_handler(req) -> dict:
    """Handler for /v2/chat endpoint. Takes ChatRequest, returns ChatResponse dict."""
    from conversation_store import store as conversation_store

    t0 = time.time()
    session_id = getattr(req, "session_id", "default")

    # Session reset
    _reset_triggers = ["새로 시작", "처음부터", "리셋", "세션 초기화"]
    msg = req.message
    if any(t in msg for t in _reset_triggers) and len(msg) < 15:
        conversation_store.reset_session(session_id)
        return {
            "answer": "새로운 대화를 시작합니다. 무엇을 도와드릴까요?",
            "sources": [],
            "elapsed_ms": round((time.time() - t0) * 1000, 1),
            "intent": {"intent": "general"},
            "dart_results": None,
            "total": 0,
            "page": 1,
            "page_size": 20,
            "has_next": False,
            "context_full": False,
            "turn_count": 0,
        }

    # Fallback: if agents not ready, delegate to old chat
    if not _ready or not AGENT_ENABLED:
        logger.warning("/v2/chat called but agents not ready — returning fallback")
        return {
            "answer": "Agent system is not available. Please use /chat endpoint instead.",
            "sources": [],
            "elapsed_ms": round((time.time() - t0) * 1000, 1),
            "intent": {"intent": "general"},
            "dart_results": None,
            "total": 0,
            "page": 1,
            "page_size": 20,
            "has_next": False,
            "context_full": False,
            "turn_count": 0,
        }

    # Run agent pipeline
    result = _run_agent_pipeline(msg, session_id=session_id)

    # Save conversation turn
    conversation_store.add_turn(session_id, "user", msg)
    conversation_store.add_turn(session_id, "assistant", result["answer"])
    ctx_full = conversation_store.is_context_full(session_id)
    turn_count = conversation_store.get_turn_count(session_id)

    answer = result["answer"]
    if ctx_full:
        answer += (
            "\n\n---\n"
            "[대화가 길어져 이전 맥락 일부가 요약되었습니다.]\n"
            "[위 내용은 최근 대화를 바탕으로 한 응답입니다.]\n"
            '[대화를 새로 시작하려면 "새로 시작"이라고 입력해주세요.]'
        )

    return {
        "answer": answer,
        "sources": [
            {"report_id": s} for s in result.get("sources", [])
        ],
        "elapsed_ms": round(result.get("elapsed_ms", (time.time() - t0) * 1000), 1),
        "intent": {
            "intent": result.get("intent", "general"),
            "confidence": result.get("confidence", "medium"),
        },
        "dart_results": None,
        "total": len(result.get("sources", [])),
        "page": 1,
        "page_size": 20,
        "has_next": False,
        "context_full": ctx_full,
        "turn_count": turn_count,
    }
