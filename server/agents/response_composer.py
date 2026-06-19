"""
Response Composer — formats agent outputs into user-facing responses.

Applies consistent formatting rules across all agents:
  - Factual data: original values (numbers, dates, brokerage names)
  - LLM interpretation: always marked as "analysis result"
  - All factual claims: source citations
  - Zone C responses: blocked by SafetyAgent, not handled here

Two-tier response strategy:
  - Simple queries (report search, DART lookup) → template formatting
  - Complex queries (analysis, comparison, cause-tracing) → Sonnet 4.6 natural language
"""

import logging
import re
import os
from datetime import datetime
from typing import Optional, List

import boto3

logger = logging.getLogger("opik.response_composer")

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
SONNET_MODEL = os.environ.get(
    "COMPOSER_MODEL",
    "global.anthropic.claude-sonnet-4-6",
)

DISCLAIMER = (
    "\n\n---\n"
    "※ 본 정보는 증권사 리포트 및 DART 공시의 사실적 요약이며 투자 권유가 아닙니다."
)

RESPONSE_SYSTEM = """당신은 OPIK 금융 정보 비서입니다. 주어진 분석 결과를 투자자가 읽기 쉽게
자연스러운 한국어로 정리하세요.

## 규칙
- 원본 분석의 핵심 내용을 유지하며 과도한 요약은 피할 것
- 수치는 그대로 유지, 증권사명과 날짜는 그대로 표기
- 확정적 표현 금지 ("~입니다" 대신 "~로 분석됩니다", "~로 보입니다")
- 투자 권유 금지
- 형식: 짧은 단락과 구분선(---)을 적절히 사용해 가독성 확보
- 응답 말미에 "※ 위 내용은 AI 분석 결과이며 투자 권유가 아닙니다." 추가"""


def _clean_disclosure_text(raw_text: str, report_nm: str) -> str:
    """Strip DART boilerplate; keep only informational content.

    Removes:
      1. 회사명/보고서명/(YYYY.MM.DD) prefix
      2. report_nm repeated up to 3x at text start
      3. 정정 template blocks (정정신고, 정정일자, 정정관련, 정정사유, 정정사항)

    Returns at most ~110 chars with natural sentence-boundary truncation.
    Returns empty string when only template boilerplate remains.
    """
    if not raw_text:
        return ""
    text = str(raw_text)
    if len(text) < 10:
        return ""

    # ── 1. 회사명/보고서명/(YYYY.MM.DD) prefix ──
    text = re.sub(r"^[^/]+/[^/]+/\(\d{4}\.\d{2}\.\d{2}\)\s*", "", text)
    text = re.sub(r"^\(\d{4}\.\d{2}\.\d{2}\)[^)]+\)\s*", "", text)

    # ── 2. report_nm repeated at text start (up to 3x, space-insensitive) ──
    if report_nm:
        rn_compact = report_nm.replace(" ", "")
        if len(rn_compact) >= 4:
            rn_pat = r"\s*".join(re.escape(c) for c in rn_compact)
            for _ in range(3):
                text = text.lstrip()
                m = re.match(rf"^{rn_pat}\s*", text)
                if m:
                    text = text[m.end():]
                else:
                    break

    # ── 3. Strip 정정 boilerplate ──
    text = re.sub(
        r"정\s*정\s*신\s*고\s*\(\s*보\s*고\s*\)\s*(\d{4}[\-\s년]*\d{2}[\-\s월]*\d{2}일?)?\s*",
        "", text,
    )
    text = re.sub(
        r"정정일자\s*\d{4}[\-\s년]*\d{2}[\-\s월]*\d{2}일?\s*",
        "", text, count=1,
    )
    text = re.sub(
        r"\d+\.\s*정정관련\s*공시서류\s*:?\s*.+?\d+\.\s*정정관련\s*공시서류\s*제출일\s*:?\s*\d{4}[\-\s년]*\d{2}[\-\s월]*\d{2}일?",
        "", text, flags=re.DOTALL,
    )
    text = re.sub(
        r"\d+\.\s*정정사유\s*.*$",
        "", text, flags=re.DOTALL,
    )
    text = re.sub(
        r"\d+\.\s*정정사항\s*정정항목\s*정정전\s*정정후.*$",
        "", text,
    )
    text = re.sub(r"\d+\.\s*정정관련\s*공시서류\s*:?\s*[^\d]*", "", text)
    text = re.sub(
        r"\d+\.\s*정정관련\s*공시서류\s*제출일\s*:?\s*\d{4}[\-\s년]*\d{2}[\-\s월]*\d{2}일?",
        "", text,
    )

    # ── 4. Normalise whitespace ──
    text = re.sub(r"\s+", " ", text).strip(" .-·")

    # ── 5. Discard if only template field numbers remain ──
    if re.match(r"^[\d\.\s\-]+$", text):
        return ""

    # ── 6. Natural sentence break ──
    if len(text) > 110:
        breaks = [m.end() for m in re.finditer(r"[다요]\.\s", text[:130])]
        if breaks:
            best = max(b for b in breaks if b <= 110)
            if best > 15:
                text = text[:best].rstrip()
            else:
                text = text[:100]
        else:
            text = text[:100]

    return text[:120]


class ResponseComposer:
    """Two-tier response formatter: templates for simple, Sonnet for complex."""

    def __init__(
        self,
        sonnet_model: str = SONNET_MODEL,
        region: str = AWS_REGION,
    ):
        self.sonnet_model = sonnet_model
        self.region = region
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client("bedrock-runtime", region_name=self.region)
        return self._client

    # ── Complexity detection ──

    @staticmethod
    def _is_complex(intent: str, raw_output: str) -> bool:
        """Detect whether this response needs Sonnet-level composition.

        Complex = analysis, comparison, cause-tracing, industry synthesis.
        Simple = report search, DART lookup, factual queries.
        """
        complex_intents = {
            "compare", "industry", "cause", "analysis",
            "dart_with_analysis", "report_with_analysis",
        }
        if intent in complex_intents:
            return True
        # Long raw output from analysis agents also qualifies
        if raw_output and len(raw_output) > 500:
            return True
        return False

    # ── Sonnet natural-language composition ──

    def _compose_with_sonnet(self, intent: str, raw_content: str, sources: Optional[List[str]] = None) -> str:
        """Use Sonnet 4.6 to compose a natural Korean response from raw analysis."""
        user_msg = f"의도: {intent}\n\n원본 분석:\n{raw_content[:4000]}"
        if sources:
            user_msg += f"\n\n출처:\n" + "\n".join(f"- {s}" for s in sources[:5])

        try:
            resp = self.client.converse(
                modelId=self.sonnet_model,
                messages=[{"role": "user", "content": [{"text": user_msg}]}],
                system=[{"text": RESPONSE_SYSTEM}],
                inferenceConfig={"maxTokens": 2000, "temperature": 0.4},
            )
            answer = ""
            for block in resp.get("output", {}).get("message", {}).get("content", []):
                if block.get("text"):
                    answer += block["text"]
            if answer:
                logger.info("Sonnet-composed response: %d chars", len(answer))
                return answer
        except Exception as e:
            logger.warning("Sonnet composition failed, falling back to template: %s", e)

        # Fallback: return raw content with disclaimer
        return raw_content + DISCLAIMER

    # ── Chat responses ──

    def compose_chat_response(
        self,
        intent: str,
        report_summary: Optional[str] = None,
        dart_summary: Optional[str] = None,
        analysis: Optional[str] = None,
        sources: Optional[List[str]] = None,
        confidence: str = "medium",
    ) -> str:
        """Compose a chat response. Routes to Sonnet for complex intents."""
        # Assemble raw content
        parts = []
        if report_summary:
            parts.append(report_summary)
        if dart_summary:
            parts.append(dart_summary)
        if analysis:
            parts.append(analysis)

        raw_content = "\n\n".join(parts)

        # Complex intents → Sonnet natural language
        if self._is_complex(intent, raw_content):
            logger.info("Complex intent '%s' — routing to Sonnet", intent)
            return self._compose_with_sonnet(intent, raw_content, sources)

        # Simple intents → template
        result_parts = []
        if confidence != "high":
            result_parts.append(f"[신뢰도: {confidence}]")

        if raw_content:
            result_parts.append(raw_content)

        if sources:
            result_parts.append("\n## 출처")
            for s in sources[:5]:
                result_parts.append(f"- {s}")

        result_parts.append(DISCLAIMER)
        return "\n".join(result_parts)

    # ── BRIEFING ──

    def compose_briefing(
        self,
        date: str,
        star_items: List[dict],
        exclamation_items: List[dict],
        report_count: int = 0,
        dart_count: int = 0,
    ) -> str:
        """Compose daily ★/! briefing per PHASE2_MULTIAGENT_DESIGN.md §5.1."""
        try:
            dt = datetime.strptime(date, "%Y-%m-%d")
            date_display = dt.strftime("%Y.%m.%d")
        except ValueError:
            date_display = date

        lines = [
            f"OPIK Daily Briefing / {date_display}",
            "",
            f"오늘의 리포트 및 공시 ({report_count}개 리포트 / {dart_count}개 공시)",
            "",
            "━" * 30,
            "",
        ]

        star_count = len(star_items)
        excl_count = len(exclamation_items)

        # ── ★ TRIPLE CONSENSUS ──
        if star_items:
            lines.append(f"오늘의 주목할 종목 ({star_count}개)")
            lines.append("")

            for item in star_items:
                ticker = item.get("ticker", "")
                name = item.get("종목명", ticker)
                model_pred = item.get("model_pred", {})
                pred_price = model_pred.get("pred_close_price", "N/A")

                reports = item.get("reports", [])
                best_report = {}
                max_tp = 0
                for r in reports:
                    tp_val = r.get("목표주가", 0)
                    try:
                        tp = float(tp_val) if tp_val is not None else 0
                    except (ValueError, TypeError):
                        tp = 0
                    if tp > max_tp:
                        max_tp = tp
                        best_report = r

                tp_str = f"{max_tp:,.0f}원" if max_tp else "N/A"
                lines.append(f"★ {name} (모델: {pred_price}원 / 리포트 TP {tp_str})")
                lines.append("")

                if best_report:
                    brokerage = best_report.get("증권사", "")
                    opinion = best_report.get("투자의견", "")
                    reason = best_report.get("reason") or best_report.get("_llm_reason", "")
                    risks = best_report.get("risks") or best_report.get("_llm_risks", "")

                    lines.append(f"  [리포트] {brokerage} ({opinion}, TP {tp_str})")
                    if reason:
                        r_text = str(reason)[:200]
                        for i in range(0, len(r_text), 60):
                            lines.append(f"  {r_text[i:i+60]}")
                    if risks:
                        risks_clean = str(risks).strip("[]'\" ")
                        lines.append(f"  → 주의할 점: {risks_clean[:160]}")
                    lines.append("")

                dart_info = item.get("dart_info", {})
                quarterly_text = dart_info.get("quarterly_text", "")
                event_text = dart_info.get("recent_event_text", "")
                event_title = dart_info.get("recent_event_title", "")

                if quarterly_text:
                    lines.append(f"  [공시] {quarterly_text[:150]}")
                if event_title:
                    disp = event_text[:150] if event_text else event_title[:150]
                    lines.append(f"  [공시] {disp}")

                lines.append("")

            lines.append("")

        # ── ! MAJOR DISCLOSURES ──
        if exclamation_items:
            lines.append(f"! 주요 공시 ({excl_count}건)")
            lines.append("")

            for item in exclamation_items[:15]:
                name = (item.get("corp_name") or item.get("종목명")
                        or str(item.get("stock_code", "")).zfill(6))
                report_nm = item.get("report_nm", "")
                display_nm = report_nm.replace("[기재정정]", "").strip()
                display_nm = " ".join(display_nm.split())[:100]

                raw_text = item.get("text", "")
                clean_summary = _clean_disclosure_text(raw_text, report_nm)

                lines.append(f"! {name}")
                if clean_summary:
                    lines.append(f"  [공시] {display_nm} — {clean_summary[:100]}")
                else:
                    lines.append(f"  [공시] {display_nm}")
                lines.append("")

            lines.append("")

        lines.append("━" * 30)
        lines.append("")
        lines.append("이외의 종목들에 대한 리포트나 공시가 궁금하시면 질문해주세요.")
        lines.append("")
        lines.append("※ 본 브리핑은 증권사 리포트 및 DART 공시의 사실적 요약이며")
        lines.append("   투자 권유가 아닙니다.")

        if not star_items and not exclamation_items:
            lines.append("")
            lines.append("오늘은 삼중 신호 일치 종목이 없습니다.")

        result = "\n".join(lines)
        logger.info("Briefing: %d stars, %d excl, %d chars", star_count, excl_count, len(result))
        return result

    def split_for_telegram(self, text: str, max_len: int = 3900) -> List[str]:
        """Split long briefing into Telegram-safe chunks (<4096 char limit)."""
        if len(text) <= max_len:
            return [text]

        chunks = []
        for line in text.split("\n"):
            line_len = len(line) + 1
            if not chunks or len(chunks[-1]) + line_len > max_len:
                chunks.append("")
            chunks[-1] += line + "\n"

        return [c.rstrip("\n") for c in chunks]

    # ── REFUSAL / ERROR ──

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
