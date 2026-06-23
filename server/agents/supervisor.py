"""
Supervisor Agent — routing decisions + LangGraph orchestration.

Determines which agents to activate based on intent and params.
Supports both LangGraph StateGraph mode and plain function-call mode
(so the framework works without langgraph installed).

Routing logic:
  - unsafe → immediate safety_refusal
  - general → general_response (no data needed)
  - report_search → report_agent
  - dart_query → dart_agent (with optional interpret → analysis)
  - hybrid → report_agent + dart_agent (parallel or sequential)
  - investment_advice → safety_refusal (should have been caught earlier)
"""

import logging
from typing import Optional

logger = logging.getLogger("opik.supervisor")


class SupervisorAgent:
    """Route user requests to the appropriate agent(s)."""

    ROUTES = {
        "general": "general_response",
        "report_search": "report_agent",
        "dart_query": "dart_agent",
        "dart_disclosure": "dart_agent",
        "dart_financial": "dart_agent",
        "dart_insider": "dart_agent",
        "dart_shareholder": "dart_agent",
        "hybrid": "hybrid_parallel",
        "investment_advice": "safety_refusal",
    }

    def route(self, is_safe: bool, intent: str, intent_params: dict) -> str:
        """Determine which agent(s) to activate.

        Returns one of:
          - "safety_refusal"  — blocked by safety
          - "general_response" — simple greeting/FAQ
          - "report_agent"     — FAISS search + Haiku summary
          - "dart_agent"       — DART query + optional interpret
          - "dart_with_analysis" — DART + AnalysisAgent for interpretation
          - "hybrid_parallel"  — report + dart in parallel
        """
        if not is_safe:
            return "safety_refusal"

        route = self.ROUTES.get(intent, "general_response")

        # Sub-routing based on params
        if route == "dart_agent" and intent_params.get("interpret"):
            return "dart_with_analysis"

        if route == "report_agent" and (intent_params.get("compare") or intent_params.get("cause_tracking")):
            return "report_with_analysis"

        return route

    def run(
        self,
        user_message: str,
        safety_agent=None,
        intent_agent=None,
        report_agent=None,
        dart_agent=None,
        analysis_agent=None,
        composer=None,
    ) -> dict:
        """Run the full agent pipeline without LangGraph.

        This is the plain function-call path — works without langgraph installed.
        Returns a dict with 'answer', 'sources', 'intent', 'confidence'.

        For the LangGraph path, use `build_graph()` below.
        """
        # Step 1: Safety check
        safety_result = safety_agent.check(user_message) if safety_agent else {"is_safe": True}
        if not safety_result.get("is_safe", True):
            if composer:
                refusal = composer.compose_refusal(
                    safety_result.get("violation_type", ""),
                    safety_result.get("redirect_suggestion", ""),
                )
            else:
                refusal = "죄송합니다. 이 요청은 처리할 수 없습니다."
            return {
                "answer": refusal,
                "sources": [],
                "intent": "refused",
                "confidence": "high",
                "violation_type": safety_result.get("violation_type"),
            }

        # Step 2: Intent parsing
        intent_result = intent_agent.parse(user_message) if intent_agent else {"intent": "general", "intent_params": {}}
        intent = intent_result["intent"]
        params = intent_result.get("intent_params", {})

        # Step 3: Route
        route = self.route(True, intent, params)

        # Step 4: Execute
        answer = ""
        sources = []
        confidence = "medium"

        if route == "safety_refusal":
            answer = "죄송합니다. 이 요청은 처리할 수 없습니다."

        elif route == "general_response":
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
                    "OPIK은 증권사 애널리스트 리포트와 DART 공시 데이터를 검색·요약해드리는 "
                    "금융 정보 챗봇입니다. 무엇을 도와드릴까요?"
                )

        elif route == "report_agent":
            if report_agent:
                search_results = report_agent.search(user_message, top_k=10)
                answer = report_agent.summarise(user_message, search_results)
                sources = [r.get("report_id", "") for r in search_results]
                confidence = "high" if search_results else "low"

        elif route == "report_with_analysis":
            if report_agent and analysis_agent:
                search_results = report_agent.search(user_message, top_k=10)
                report_summary = report_agent.summarise(user_message, search_results)
                ticker_name = params.get("ticker_names", [""])[0] if params.get("ticker_names") else ""

                if params.get("compare"):
                    analysis = analysis_agent.compare_reports(search_results, ticker_name)
                elif params.get("cause_tracking"):
                    analysis = analysis_agent.trace_cause(
                        ticker_name, "최근 1주일", search_results, []
                    )
                else:
                    analysis = ""

                if composer:
                    answer = composer.compose_chat_response(
                        intent="report_search",
                        report_summary=report_summary,
                        analysis=analysis,
                        sources=[r.get("report_id", "") for r in search_results],
                        confidence="medium",
                    )
                else:
                    answer = f"{report_summary}\n\n{analysis}" if analysis else report_summary
                sources = [r.get("report_id", "") for r in search_results]

        elif route in ("dart_agent", "dart_with_analysis"):
            if dart_agent:
                ticker_names = params.get("ticker_names", [])
                tickers = params.get("tickers", [])
                dart_result = dart_agent.query_disclosure_events(
                    companies=ticker_names,
                    codes=tickers,
                    date_from=params.get("time_range", {}).get("from") if params.get("time_range") else None,
                    date_to=params.get("time_range", {}).get("to") if params.get("time_range") else None,
                )
                answer = dart_result
                confidence = "high" if dart_result and "검색된 공시가 없습니다" not in dart_result else "low"

        elif route == "hybrid_parallel":
            report_result = ""
            dart_result = ""
            if report_agent:
                search_results = report_agent.search(user_message, top_k=10)
                report_result = report_agent.summarise(user_message, search_results)
                sources = [r.get("report_id", "") for r in search_results]
            if dart_agent:
                ticker_names = params.get("ticker_names", [])
                dart_result = dart_agent.query_disclosure_events(companies=ticker_names)

            if composer:
                answer = composer.compose_chat_response(
                    intent="hybrid",
                    report_summary=report_result or None,
                    dart_summary=dart_result or None,
                    sources=sources,
                    confidence="medium",
                )
            else:
                answer = f"{report_result}\n\n{dart_result}"

        return {
            "answer": answer,
            "sources": sources,
            "intent": intent,
            "confidence": confidence,
        }


# LangGraph integration (optional — works without langgraph installed)

def build_supervisor_graph(
    safety_agent,
    intent_agent,
    report_agent,
    dart_agent,
    analysis_agent,
    composer,
):
    """Build LangGraph StateGraph for chat agent pipeline.

    Returns a compiled LangGraph graph if langgraph is installed, otherwise None.
    """
    try:
        from langgraph.graph import StateGraph, END
    except ImportError:
        logger.warning("langgraph not installed — supervisor graph unavailable")
        return None

    from typing import TypedDict, Annotated, Sequence
    from langgraph.graph.message import add_messages

    class ChatState(TypedDict):
        messages: Annotated[Sequence[dict], add_messages]
        user_message: str
        is_safe: bool
        violation_type: Optional[str]
        intent: str
        intent_params: dict
        report_results: list
        dart_results: list
        analysis: str
        response: Optional[str]
        sources: list
        confidence: str
        error: Optional[str]

    def node_safety(state: ChatState) -> ChatState:
        result = safety_agent.check(state["user_message"])
        state["is_safe"] = result.get("is_safe", True)
        state["violation_type"] = result.get("violation_type")
        if not state["is_safe"]:
            state["response"] = composer.compose_refusal(
                state["violation_type"] or "",
                result.get("redirect_suggestion", ""),
            )
            state["confidence"] = "high"
        return state

    def node_intent(state: ChatState) -> ChatState:
        if not state.get("is_safe", True):
            return state
        result = intent_agent.parse(state["user_message"])
        state["intent"] = result["intent"]
        state["intent_params"] = result.get("intent_params", {})
        return state

    def node_report(state: ChatState) -> ChatState:
        if state.get("response"):
            return state
        results = report_agent.search(state["user_message"], top_k=10)
        state["report_results"] = results
        return state

    def node_dart(state: ChatState) -> ChatState:
        if state.get("response"):
            return state
        params = state.get("intent_params", {})
        intent = state.get("intent", "dart_disclosure")
        ticker_names = params.get("ticker_names", [])
        codes = params.get("ticker_codes", [])
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        is_recent = params.get("is_recent", False)

        if intent == "dart_financial":
            result = dart_agent.query_financials(
                companies=ticker_names, codes=codes,
                date_from=date_from, date_to=date_to,
            )
        elif intent == "dart_insider":
            result = dart_agent.query_insider_trades(
                companies=ticker_names, codes=codes,
                date_from=date_from, date_to=date_to,
            )
        elif intent == "dart_shareholder":
            result = dart_agent.query_major_shareholders(
                companies=ticker_names, codes=codes,
                date_from=date_from, date_to=date_to,
            )
        else:
            # dart_disclosure or dart_query
            result = dart_agent.query_disclosure_events(
                companies=ticker_names, codes=codes,
                date_from=date_from, date_to=date_to,
                page=params.get("page", 1),
                page_size=params.get("page_size", 20),
            )
        state["dart_results"] = [result]
        return state

    def node_analysis(state: ChatState) -> ChatState:
        if state.get("response"):
            return state
        params = state.get("intent_params", {})
        if params.get("compare"):
            state["analysis"] = analysis_agent.compare_reports(
                state.get("report_results", []),
                ", ".join(params.get("ticker_names", [])),
            )
        return state

    def node_compose(state: ChatState) -> ChatState:
        if state.get("response"):
            return state
        report_summary = None
        if state.get("report_results"):
            report_summary = report_agent.summarise(
                state["user_message"], state["report_results"]
            )
        dart_summary = None
        if state.get("dart_results"):
            dart_summary = "\n".join(str(r) for r in state["dart_results"])

        state["response"] = composer.compose_chat_response(
            intent=state.get("intent", "general"),
            report_summary=report_summary,
            dart_summary=dart_summary,
            analysis=state.get("analysis"),
            sources=[r.get("report_id", "") for r in state.get("report_results", [])],
            confidence=state.get("confidence", "medium"),
        )
        return state

    def edge_after_safety(state: ChatState) -> str:
        if not state.get("is_safe", True):
            return END
        return "intent"

    def edge_after_intent(state: ChatState) -> str:
        intent = state.get("intent", "general")
        if intent == "general":
            return "compose"
        if intent == "hybrid":
            return "report"  # will go to dart after
        if intent == "report_search":
            return "report"
        if intent in ("dart_query", "dart_disclosure", "dart_financial", "dart_insider", "dart_shareholder"):
            return "dart"
        return "compose"

    g = StateGraph(ChatState)

    g.add_node("safety", node_safety)
    g.add_node("intent", node_intent)
    g.add_node("report", node_report)
    g.add_node("dart", node_dart)
    g.add_node("analysis", node_analysis)
    g.add_node("compose", node_compose)

    g.set_entry_point("safety")
    g.add_conditional_edges("safety", edge_after_safety)
    g.add_conditional_edges("intent", edge_after_intent)
    g.add_edge("report", "analysis")
    g.add_edge("dart", "compose")
    g.add_edge("analysis", "compose")
    g.set_finish_point("compose")

    return g.compile()
