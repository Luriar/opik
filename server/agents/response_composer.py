"""
Response Composer — formats agent outputs into user-facing responses.

Applies consistent formatting rules across all agents:
  - Factual data: original values (numbers, dates, brokerage names)
  - LLM interpretation: always marked as "analysis result"
  - All factual claims: source citations
  - Zone C responses: blocked by SafetyAgent, not handled here

For chat responses, delegates formatting to the agent that produced the output.
For briefing, assembles the full ★/! layout defined in Section 5.1-5.3.
"""

import logging
from datetime import datetime
from typing import Optional, List

logger = logging.getLogger("opik.response_composer")

DISCLAIMER = (
    "\n\n---\n"
    "※ 본 정보는 증권사 리포트 및 DART 공시의 사실적 요약이며 투자 권유가 아닙니다."
)


class ResponseComposer:
    """Formats agent outputs into consistent user-facing responses."""

    def compose_chat_response(
        self,
        intent: str,
        report_summary: Optional[str] = None,
        dart_summary: Optional[str] = None,
        analysis: Optional[str] = None,
        sources: Optional[List[str]] = None,
        confidence: str = "medium",
    ) -> str:
        """Compose a chat response from individual agent outputs."""
        parts = []

        if confidence != "high":
            parts.append(f"[신뢰도: {confidence}]")

        if report_summary:
            parts.append(report_summary)

        if dart_summary:
            if parts:
                parts.append("\n" + "─" * 40)
            parts.append(dart_summary)

        if analysis:
            if parts:
                parts.append("\n" + "─" * 40)
            parts.append(f"## 분석 결과\n{analysis}")

        if sources:
            parts.append("\n## 출처")
            for s in sources[:5]:
                parts.append(f"- {s}")

        parts.append(DISCLAIMER)

        return "\n".join(parts)

    def compose_briefing(
        self,
        date: str,
        star_items: List[dict],
        exclamation_items: List[dict],
        report_count: int = 0,
        dart_count: int = 0,
    ) -> str:
        """Compose the daily ★/! briefing in Telegram HTML format.

        Args:
            date: briefing date in YYYY-MM-DD format
            star_items: [{ticker, 종목명, reports, dart_events, model_pred}]
            exclamation_items: [{ticker, 종목명, dart_event, impact, reason}]
            report_count: total report count for the day
            dart_count: total DART event count for the period
        """
        # Parse date for weekday display
        try:
            dt = datetime.strptime(date, "%Y-%m-%d")
            weekdays = ["월", "화", "수", "목", "금", "토", "일"]
            date_str = f"{dt.strftime('%Y-%m-%d')} ({weekdays[dt.weekday()]})"
        except ValueError:
            date_str = date

        lines = [
            f"[OPIK 브리핑] {date_str} 07:00",
            "",
            f"오늘의 리포트 및 공시 ({report_count}개 리포트 / {dart_count}개 공시)",
            "",
        ]

        # ★ TRIPLE CONSENSUS section
        if star_items:
            lines.append("★ TRIPLE CONSENSUS (리포트 + DART + 모델)")
            lines.append("━" * 30)

            for item in star_items:
                ticker = item.get("ticker", "")
                name = item.get("종목명", ticker)
                model_pred = item.get("model_pred", {})
                pred_price = model_pred.get("pred_close_price", "N/A")
                ranking_score = model_pred.get("ranking_score", 0)

                # Find the BUY report with highest TP for display
                reports = item.get("reports", [])
                best_report = {}
                max_tp = 0
                for r in reports:
                    tp = r.get("목표주가", 0) or 0
                    if tp > max_tp:
                        max_tp = tp
                        best_report = r

                tp_str = f"{max_tp:,.0f}원" if max_tp else "N/A"
                lines.append(
                    f"\n★ {name} (모델: {pred_price}원 / 리포트 TP {tp_str})"
                )

                # Show one representative report
                if best_report:
                    brokerage = best_report.get("증권사", "")
                    opinion = best_report.get("투자의견", "")
                    upside = best_report.get("상승여력_pct", 0)
                    lines.append(f"  ✓ {opinion} ({brokerage})")
                    reason = best_report.get("reason", "")
                    if reason:
                        lines.append(f"  {reason[:120]}")

                # Show DART events
                dart_events = item.get("dart_events", [])
                for de in dart_events[:3]:  # max 3 per stock
                    nm = de.get("report_nm", "")
                    if nm:
                        lines.append(f"  ✓ DART: {nm[:80]}")

                # Show ranking_score
                if ranking_score:
                    lines.append(f"  ✓ 모델: ranking_score {ranking_score:+.3f}")

            lines.append("")

        # ! MAJOR DISCLOSURES section
        if exclamation_items:
            lines.append("! MAJOR DISCLOSURES (B-type)")
            lines.append("━" * 30)

            for item in exclamation_items:
                name = item.get("종목명", item.get("ticker", ""))
                report_nm = item.get("report_nm", "")
                reason = item.get("sentiment_reason", item.get("reason", ""))
                impact = item.get("sentiment", item.get("impact", "neutral"))
                impact_kr = {"positive": "긍정", "negative": "부정", "neutral": "중립"}.get(impact, "중립")

                lines.append(f"\n! {name} [{impact_kr}]")
                lines.append(f"  → {report_nm[:100]}")
                if reason:
                    lines.append(f"     {reason[:80]}")

            lines.append("")

        # Footer
        lines.append("━" * 30)
        lines.append("이외의 종목들에 대한 리포트나 공시가 궁금하시면 질문해주세요.")
        lines.append("")
        lines.append("※ 본 브리핑은 증권사 리포트 및 DART 공시의 사실적 요약이며")
        lines.append("   투자 권유가 아닙니다.")

        # If no consensus at all
        if not star_items and not exclamation_items:
            lines.append("\n오늘은 삼중 신호 일치 종목이 없습니다.")

        return "\n".join(lines)

    def compose_refusal(self, violation_type: str, redirect: str = "") -> str:
        """Compose a refusal response."""
        try:
            from .safety_agent import SafetyAgent
        except ImportError:
            from safety_agent import SafetyAgent
        return SafetyAgent().build_refusal_message(violation_type, redirect)

    def compose_error(self, error_msg: str) -> str:
        """Compose a generic error response."""
        return (
            f"처리 중 오류가 발생했습니다: {error_msg}\n\n"
            "잠시 후 다시 시도해 주시기 바랍니다.\n"
            "동일한 문제가 지속되면 관리자에게 문의해 주세요."
        )


# Singleton
_default_composer: Optional[ResponseComposer] = None


def get_composer() -> ResponseComposer:
    global _default_composer
    if _default_composer is None:
        _default_composer = ResponseComposer()
    return _default_composer
