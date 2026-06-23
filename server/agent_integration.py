
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


def _format_date_browse(date_from: str, date_to: str, results: list) -> str:
    """Format date-based browse results directly — no LLM summarise needed."""
    if not results:
        return f"해당 날짜({date_from}~{date_to})의 증권사 리포트 데이터가 없습니다."

    # Show date range, or single date if from==to
    if date_from == date_to:
        date_label = date_from
    else:
        # When showing a range (e.g. "최근" = 90 days), highlight latest date
        date_label = f"최근 ({date_from} ~ {date_to}, {date_to} 기준)"
    lines = [f"*{date_label} 증권사 리포트* ({len(results)}건)", ""]
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


# Date-only question patterns for short-circuit detection.
# These prevent Haiku intent parser from miscategorising "오늘 며칠이냐"
# as report_search (which causes FAISS search → hallucinated {today_date}).
_DATE_ONLY_PATTERNS = [
    "오늘 며칠", "며칠이야", "며칠이냐", "오늘 날짜",
    "오늘은 며칠", "오늘이 며칠", "오늘 뭐냐", "날짜 알려줘",
    "오늘 몇일", "오늘이 몇일",
]


def _filter_recent(results: list, max_days: int = 30) -> list:
    """Filter search results to recent reports (within max_days).

    When no date/time_range is specified and the user hasn't asked for
    a specific historical year, old FAISS matches (2022-2023) should be
    dropped to avoid showing stale reports as "today's".
    """
    from datetime import datetime, timedelta
    cutoff = datetime.now() - timedelta(days=max_days)
    filtered = []
    for r in results:
        year = r.get("year")
        month = r.get("month")
        if year and month:
            try:
                rd = datetime(int(year), int(month), 15)
                if rd >= cutoff:
                    filtered.append(r)
            except (ValueError, TypeError):
                filtered.append(r)
        else:
            filtered.append(r)
    return filtered


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
    _is_followup_like = (
        len(user_message.strip()) <= 60
        and any(w in user_message for w in [
            "이거", "저거", "그거", "이것", "저것", "그것",
            "자세히", "더 알려줘", "더 보여줘",
            "이 리포트", "저 리포트", "그 리포트",
            "이 종목", "저 종목", "이 공시"
        ])
    )
    if _context or _is_followup_like:
        safety_result = {"is_safe": True, "violation_type": None, "redirect_suggestion": ""}
        logger.info("Agent pipeline: skipping safety (context=%d followup=%s)",
                     len(_context), _is_followup_like)
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
    # Short message with context: try intent parser first to catch DART/disclosure queries.
    # Only force report_search if the parser doesn't clearly identify a non-report intent.
    if _context and len(user_message.strip()) <= 60:
        _quick_intent = _intent.parse(user_message)
        _qi = _quick_intent["intent"]
        if _qi.startswith("dart_"):
            logger.info("Agent pipeline: short msg with context -> DART intent %s detected, using parser", _qi)
            intent = _qi
            params = _quick_intent.get("intent_params", {})
        elif _qi == "hybrid":
            intent = _qi
            params = _quick_intent.get("intent_params", {})
        else:
            logger.info("Agent pipeline: short msg + context -> forcing report_search intent (parser said %s)", _qi)
            intent = "report_search"
            _pp = _quick_intent.get("intent_params", {})
            params = {
                "tickers": _pp.get("tickers", []),
                "ticker_names": _pp.get("ticker_names", []),
                "brokerages": _pp.get("brokerages", []),
                "sectors": _pp.get("sectors", []),
                "time_range": _pp.get("time_range"),  # preserve IntentAgent time_range
                "keywords": _pp.get("keywords", [user_message.strip()]),
                "compare": _pp.get("compare", False),
                "cause_tracking": _pp.get("cause_tracking", False),
                "interpret": _pp.get("interpret", False),
                "is_greeting": _pp.get("is_greeting", False),
                "response_style": _pp.get("response_style", "detailed"),
            }
    elif not _context and _is_followup_like:
        _followup_hints = ["이거", "저거", "그거", "이것", "저것", "그것",
                           "자세히", "더 알려줘",
                           "이 리포트", "저 리포트", "그 리포트",
                           "이 종목", "저 종목", "이 공시"]
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

    # --- BUGFIX 2: "오늘"/"최근" keyword → force date-based search ---
    # Handle these BEFORE time_range conversion so they always take effect.
    # "최근" = 최근 3개월 (90일), "오늘" = 오늘 날짜.
    _has_date_keyword = False
    if "오늘" in user_message and not params.get("date_from"):
        _today = __import__("datetime").datetime.now()
        _td = _today.strftime("%Y-%m-%d")
        logger.info("Agent pipeline: '오늘' keyword -> force date_from=%s", _td)
        params["date_from"] = _td
        params["date_to"] = _td
        _has_date_keyword = True
    if "최근" in user_message:
        # Always override with 90-day window when user says "최근"
        _now = __import__("datetime").datetime.now()
        _three_mo_ago = (_now - __import__("datetime").timedelta(days=90)).strftime("%Y-%m-%d")
        _today = _now.strftime("%Y-%m-%d")
        logger.info("Agent pipeline: '최근' keyword -> force date range %s~%s (90 days)", _three_mo_ago, _today)
        params["date_from"] = _three_mo_ago
        params["date_to"] = _today
        _has_date_keyword = True

    # --- FIX: Unified time_range → date_from conversion ---
    # IntentAgent returns "time_range" (e.g. {"from": "2026-06-16", "to": "2026-06-23"})
    # for "이번 주", "지난주" etc. Convert here so all routes benefit.
    # Only fires if keyword handlers above didn't already set date_from.
    _tr = params.get("time_range")
    if _tr and not params.get("date_from"):
        params["date_from"] = _tr.get("from")
        params["date_to"] = _tr.get("to")
        logger.info("Agent pipeline: time_range -> date_from=%s date_to=%s",
                     params["date_from"], params["date_to"])

    # --- BUGFIX 1: Pure date-only question detection ---
    # "오늘 며칠이야?", "며칠이냐?" etc. must NOT reach FAISS search.
    _msg_stripped = user_message.replace(" ", "").replace("?", "").replace("!", "").replace(".", "").replace("~", "")
    _is_date_only = False
    for _dp in _DATE_ONLY_PATTERNS:
        _dp_stripped = _dp.replace(" ", "")
        if _dp_stripped in _msg_stripped:
            _is_date_only = True
            break
    if _is_date_only:
        _today = __import__("datetime").datetime.now()
        _elapsed = (time.time() - t0) * 1000
        logger.info("Agent pipeline: date-only question detected -> short-circuit (%r)", user_message[:40])
        return {
            "answer": (
                f"오늘은 {_today.year}년 {_today.month}월 {_today.day}일입니다.\n\n"
                "원하시는 날짜의 증권사 리포트를 보시려면 '6월 18일 리포트'처럼 날짜를 말씀해 주세요."
            ),
            "sources": [],
            "intent": "general",
            "confidence": "high",
            "violation_type": None,
            "elapsed_ms": _elapsed,
        }

    # --- FIX: Auto-date for ticker queries without date ---
    # When user asks about a specific company without specifying date range,
    # default to last 90 days so FAISS/date search has something to work with.
    _tn = params.get("ticker_names", [])
    if _tn and not params.get("date_from") and "최근" not in user_message and "오늘" not in user_message:
        _now = __import__("datetime").datetime.now()
        _three_mo_ago = (_now - __import__("datetime").timedelta(days=90)).strftime("%Y-%m-%d")
        _today = _now.strftime("%Y-%m-%d")
        logger.info("Agent pipeline: ticker query without date -> default 90-day range %s~%s",
                     _three_mo_ago, _today)
        params["date_from"] = _three_mo_ago
        params["date_to"] = _today

    # --- FIX: Disclosure keyword triggers ---
    # When user copy-pastes from chatbot's disclosure table or asks about
    # specific disclosure events, IntentAgent often misclassifies as "general".
    # Force dart_query interpret when disclosure keywords + company detected.
    _disclosure_keywords = [
        "소유상황", "지분 변동", "지분변동", "임원", "주요주주",
        "유상증자", "무상증자", "자기주식", "자사주", "처분", "취득",
        "대량보유", "공시", "보고서", "증권발행", "전환사채", "CB",
        "BW", "교환사채", "파생결합", "ELB", "ELS", "DLB",
    ]
    _msg_for_kw = user_message.replace(" ", "")
    _has_disclosure_kw = any(kw.replace(" ", "") in _msg_for_kw for kw in _disclosure_keywords)
    if _has_disclosure_kw and not intent.startswith("dart_"):
        # Check if there's a company name mentioned
        _known_companies = ["삼성전자", "SK하이닉스", "한국알콜", "대신증권",
            "미래에셋", "NH투자", "삼성증권", "한화투자", "교보증권",
            "키움증권", "하나증권", "IBK투자", "SK증권", "유진투자",
            "iM증권", "비비안", "리드코프", "스틱인베스트먼트",
            "엠투엔", "제일기획", "카카오", "네이버", "현대차",
            "기아", "LG에너지", "셀트리온", "카카오뱅크", "두산",
            "한화", "HD현대", "포스코", "LG화학", "삼성SDI"]
        _found_company = None
        for _cn in _known_companies:
            if _cn.replace(" ", "") in user_message.replace(" ", ""):
                _found_company = _cn
                break
        if _found_company:
            logger.info("Agent pipeline: disclosure kw detected -> force dart_query for %s", _found_company)
            intent = "dart_query"
            params["ticker_names"] = [_found_company]
            params["interpret"] = True
            if not params.get("date_from"):
                _now = __import__("datetime").datetime.now()
                _three_mo_ago = (_now - __import__("datetime").timedelta(days=90)).strftime("%Y-%m-%d")
                _today = _now.strftime("%Y-%m-%d")
                params["date_from"] = _three_mo_ago
                params["date_to"] = _today

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
        if params.get("date_from"):
            search_results = _report.search_by_date(params["date_from"], params["date_to"], limit=50)
            logger.info("Using date-based browse for %s (%d results)", params["date_from"], len(search_results))
            if search_results:
                # If ticker_names specified, filter results by ticker name match
                _tn_list = params.get("ticker_names", [])
                _faiss_fallback_used = False
                _faiss_hits = []
                if _tn_list:
                    _filtered = []
                    for r in search_results:
                        _reason = (r.get("reason", "") or "").upper()
                        _keywords = str(r.get("keywords", "") or "").upper()
                        _search_text = _reason + " " + _keywords
                        for _tn in _tn_list:
                            _tn_upper = _tn.upper()
                            if _tn_upper in _search_text:
                                _filtered.append(r)
                                break
                    if _filtered:
                        logger.info("Ticker filter: %d -> %d results for %s",
                                    len(search_results), len(_filtered), _tn_list)
                        search_results = _filtered
                    else:
                        # Fallback: ticker not found by name in date-browse results
                        # -> do a targeted FAISS search with the company name
                        logger.info("Ticker filter: no match for %s in %d date results, trying FAISS",
                                    _tn_list, len(search_results))
                        _faiss_fallback_used = False
                        _tk_query = " ".join(_tn_list) + " 리포트"
                        _faiss_hits = _report.search(_tk_query, top_k=10)
                        _has_year_ref_fb = bool(__import__("re").search(r"\b(20\d{2})년", user_message))
                        if not _has_year_ref_fb and _faiss_hits:
                            _faiss_hits = _filter_recent(_faiss_hits)
                        if _faiss_hits:
                            logger.info("Ticker FAISS fallback: %d results for %s",
                                        len(_faiss_hits), _tn_list)
                            # Rebuild answer from FAISS results
                            report_summary_fb = _report.summarise(user_message, _faiss_hits)
                            # Auto-compare for FAISS results too
                            _fb_brokerages = set()
                            for _fr in _faiss_hits:
                                _fb = _fr.get("증권사") or _fr.get("brokerage", "")
                                if _fb:
                                    _fb_brokerages.add(_fb)
                            analysis_fb = ""
                            if len(_fb_brokerages) >= 2 and len(_faiss_hits) >= 2:
                                analysis_fb = _analysis.compare_reports(_faiss_hits, _tn_list[0] if _tn_list else "")
                            if analysis_fb:
                                answer = _composer.compose_chat_response(
                                    intent="report_search",
                                    report_summary=report_summary_fb,
                                    analysis=analysis_fb,
                                    sources=[_fr.get("report_id", "") for _fr in _faiss_hits],
                                    confidence="medium",
                                )
                            else:
                                answer = report_summary_fb
                            sources = [_fr.get("report_id", "") for _fr in _faiss_hits]
                            search_results = _faiss_hits
                            _faiss_fallback_used = True
                if not _faiss_fallback_used:
                    answer = _format_date_browse(params["date_from"], params.get("date_to", params["date_from"]), search_results)
                elif not _faiss_hits:
                    # FAISS fallback also returned nothing
                    answer = f"'{' '.join(_tn_list)}' 관련 최근 리포트를 찾을 수 없습니다."
                    sources = []
                    confidence = "low"
                # Related-company context: group by ticker code
                _ticker_groups = {}
                for r in search_results:
                    _tc = str(r.get("종목코드", "")).strip()
                    if _tc and _tc != "None":
                        _tn = (r.get("reason", "") or "")[:80]
                        if _tc not in _ticker_groups:
                            _ticker_groups[_tc] = _tn
                if len(_ticker_groups) >= 3 and not params.get("ticker_names"):
                    _ticker_list = list(_ticker_groups.values())[:5]
                    _more = f" (+{len(_ticker_groups)-5}종목)" if len(_ticker_groups) > 5 else ""
                    answer += f"\n\n[관련 기업 현황] {', '.join(_ticker_list)}{_more} 등 {len(_ticker_groups)}종목의 리포트가 있습니다. 특정 종목에 대해 더 자세히 알고 싶으시면 종목명을 말씀해주세요."
                sources = [r.get("report_id", "") for r in search_results]
                confidence = "high"
            else:
                answer = f"해당 날짜({params['date_from']}~{params.get('date_to', params['date_from'])})의 증권사 리포트 데이터가 없습니다."
                sources = []
                confidence = "low"
        else:
            search_results = _report.search(user_message, top_k=10)
            # --- BUGFIX 3: Recency filter on FAISS results ---
            # Without a date_from, FAISS returns semantically similar reports
            # from any year. Filter to 180 days unless user explicitly asked
            # for a specific historical year.
            _has_year_ref = bool(__import__("re").search(r"\b(20\d{2})년", user_message))
            if not _has_year_ref and search_results:
                _before = len(search_results)
                search_results = _filter_recent(search_results)
                if _before > len(search_results):
                    logger.info("Recency filter: %d → %d results (dropped %d old reports)",
                                _before, len(search_results), _before - len(search_results))
            if search_results:
                report_summary = _report.summarise(user_message, search_results)
                # Auto-compare: 2+ reports from different brokerages -> add comparison
                analysis = ""
                _brokerages = set()
                for r in search_results:
                    _b = r.get("증권사") or r.get("brokerage", "")
                    if _b:
                        _brokerages.add(_b)
                _tk = params.get("ticker_names", [])
                _ticker_name = _tk[0] if _tk else ""
                if len(_brokerages) >= 2 and len(search_results) >= 2:
                    logger.info("Auto-compare: %d brokerages, %d reports for %s",
                                len(_brokerages), len(search_results), _ticker_name)
                    analysis = _analysis.compare_reports(search_results, _ticker_name)
                if analysis:
                    answer = _composer.compose_chat_response(
                        intent="report_search",
                        report_summary=report_summary,
                        analysis=analysis,
                        sources=[r.get("report_id", "") for r in search_results],
                        confidence="medium",
                    )
                else:
                    answer = report_summary
                sources = [r.get("report_id", "") for r in search_results]
                confidence = "high"
            else:
                ticker_names = params.get("ticker_names", [])
                names_str = ", ".join(ticker_names) if ticker_names else "해당 조건"
                answer = f"{names_str}으로 검색된 애널리스트 리포트가 없습니다."
                confidence = "low"

    elif route == "report_with_analysis":
        if params.get("date_from"):
            search_results = _report.search_by_date(params["date_from"], params["date_to"], limit=50)
            logger.info("Using date-based browse for %s (%d results)", params["date_from"], len(search_results))
            report_summary = _format_date_browse(params["date_from"], params.get("date_to", params["date_from"]), search_results) if search_results else ""
        else:
            search_results = _report.search(user_message, top_k=10)
            _has_year_ref = bool(__import__("re").search(r"\b(20\d{2})년", user_message))
            if not _has_year_ref and search_results:
                _before = len(search_results)
                search_results = _filter_recent(search_results)
                if _before > len(search_results):
                    logger.info("Recency filter (analysis): %d → %d results", _before, len(search_results))
            report_summary = _report.summarise(user_message, search_results) if search_results else ""
        ticker_name = params.get("ticker_names", [""])[0] if params.get("ticker_names") else ""

        analysis = ""
        if params.get("compare") and len(search_results) >= 2:
            analysis = _analysis.compare_reports(search_results, ticker_name)
        elif params.get("cause_tracking"):
            # Filter reports to only those mentioning the ticker
            _ct_reports = []
            if ticker_name:
                _tn_upper_ct = ticker_name.upper()
                for r in search_results:
                    _r_ct = (r.get("reason", "") or "").upper()
                    _kw_ct = str(r.get("keywords", "") or "").upper()
                    if _tn_upper_ct in (_r_ct + " " + _kw_ct):
                        _ct_reports.append(r)
                # Fallback: if no ticker-specific reports, use all
                if not _ct_reports:
                    logger.info("Cause tracking: no %s-specific reports in %d, using all",
                                ticker_name, len(search_results))
                    _ct_reports = search_results
                else:
                    logger.info("Cause tracking: filtered %d -> %d reports for %s",
                                len(search_results), len(_ct_reports), ticker_name)
            else:
                _ct_reports = search_results

            # Convert search_by_date results to format trace_cause expects
            # search_by_date returns {report_id, reason: "[brokerage] title", keywords, year, month}
            # trace_cause expects [{date, brokerage, summary}]
            _report_events = []
            for r in _ct_reports:
                _reason = r.get("reason", "") or ""
                _brokerage_ct = ""
                # Extract brokerage from "[brokerage] title" format
                if _reason.startswith("[") and "]" in _reason:
                    _brokerage_ct = _reason[1:_reason.index("]")]
                    _title_ct = _reason[_reason.index("]")+1:].strip()
                else:
                    _title_ct = _reason
                _date_ct = f"{r.get('year', '')}-{str(r.get('month', '')).zfill(2)}"
                _report_events.append({
                    "date": _date_ct,
                    "brokerage": _brokerage_ct,
                    "summary": _title_ct[:200],
                    "keywords": r.get("keywords", ""),
                })

            # Fetch DART disclosures for richer cause analysis
            _dart_events = []
            if ticker_name:
                try:
                    _de = _dart.query_disclosure_events(
                        companies=[ticker_name],
                        date_from=params.get("date_from"),
                        date_to=params.get("date_to"),
                    )
                    if _de and "데이터가 없습니다" not in _de and len(str(_de)) > 80:
                        _lines = _de.strip().split("\n")
                        for _l in _lines:
                            if ":" in _l and len(_l) > 15:
                                _parts = _l.split(":", 1)
                                _dart_events.append({
                                    "date": params.get("date_from", ""),
                                    "event": _parts[0].strip(),
                                    "summary": _parts[1].strip()[:200],
                                })
                        if not _dart_events and _lines:
                            _dart_events.append({
                                "date": params.get("date_from", ""),
                                "event": "공시 이벤트",
                                "summary": _lines[0][:200],
                            })
                    logger.info("Cause tracking: +%d dart events for %s",
                                len(_dart_events), ticker_name)
                except Exception as _ce:
                    logger.warning("Cause tracking dart error: %s", _ce)
            analysis = _analysis.trace_cause(ticker_name, "최근 1주일", _report_events, _dart_events)

        if params.get("cause_tracking") and analysis:
            # Cause tracking: build focused response front and center
            answer = f"## 📈 {ticker_name} 주가 변동 원인 분석\n\n{analysis}"
            if params.get("date_from"):
                answer += f"\n\n---\n\n*분석 기간: {params['date_from']} ~ {params.get('date_to', params['date_from'])}*"
                answer += f"\n*참고 리포트: {len(search_results)}건*"
            answer += "\n\n※ 본 분석은 AI 추론이며 투자 권유가 아닙니다."
        else:
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
        # Use explicit date_from/date_to first (from regex extraction), fallback to time_range
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        if not date_from and time_range:
            date_from = time_range.get("from")
        if not date_to and time_range:
            date_to = time_range.get("to")
        # If still no date but interpret=True, try extracting from conversation context
        if not date_from and not date_to and params.get("interpret") and conversation_context:
            import re as _re2
            _ctxt_dates = _re2.findall(r"(\d{1,2})월\s*(\d{1,2})일", conversation_context)
            if _ctxt_dates:
                _cy = str(__import__("datetime").datetime.now().year)
                _last = _ctxt_dates[-1]
                date_from = f"{_cy}-{_last[0].zfill(2)}-{_last[1].zfill(2)}"
                date_to = date_from
                logger.info("DART interpret: using context date %s", date_from)
        dart_result = _dart.query_disclosure_events(
            companies=ticker_names,
            date_from=date_from,
            date_to=date_to,
        )

        # Auto-interpret disclosures: users always want to know meaning.
        # - Explicit interpret=True -> detailed Sonnet analysis
        # - Default -> basic Haiku impact assessment
        has_data = bool(dart_result and "데이터가 없습니다" not in dart_result and len(str(dart_result)) > 80)
        if has_data:
            if params.get("interpret"):
                interpretation = _dart.summarize_disclosure(dart_result, "")
                answer = _composer.compose_chat_response(
                    intent="dart_query",
                    dart_summary=f"{dart_result}\n\n[공시 해석 (상세)]\n{interpretation}",
                    confidence="medium",
                )
            else:
                interpretation = _dart.interpret_disclosure(dart_result, "")
                answer = _composer.compose_chat_response(
                    intent="dart_query",
                    dart_summary=f"{dart_result}\n\n[공시 의미]\n{interpretation}",
                    confidence="medium",
                )
        else:
            answer = dart_result or "해당 조건의 공시 데이터가 없습니다."
            confidence = "low"

    elif route == "hybrid_parallel":
        report_summary = ""
        dart_summary = ""
        if params.get("date_from") and not params.get("ticker_names"):
            search_results = _report.search_by_date(params["date_from"], params["date_to"], limit=50)
            logger.info("Using date-based browse for %s (%d results)", params["date_from"], len(search_results))
            report_summary = _format_date_browse(params["date_from"], params.get("date_to", params["date_from"]), search_results) if search_results else ""
        else:
            search_results = _report.search(user_message, top_k=10)
            _has_year_ref = bool(__import__("re").search(r"\b(20\d{2})년", user_message))
            if not _has_year_ref and search_results:
                _before = len(search_results)
                search_results = _filter_recent(search_results)
                if _before > len(search_results):
                    logger.info("Recency filter (hybrid): %d → %d results", _before, len(search_results))
            report_summary = _report.summarise(user_message, search_results) if search_results else ""
            sources = [r.get("report_id", "") for r in search_results]

        ticker_names = params.get("ticker_names", [])
        _h_date_from = params.get("date_from")
        _h_date_to = params.get("date_to")
        if not _h_date_from and params.get("time_range"):
            _h_date_from = params["time_range"].get("from")
            _h_date_to = params["time_range"].get("to")
        dart_result = _dart.query_disclosure_events(
            companies=ticker_names,
            date_from=_h_date_from,
            date_to=_h_date_to,
        )
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
