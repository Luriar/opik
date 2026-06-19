"""
DART Agent — DART Gold Parquet queries + disclosure interpretation.

Wraps existing dart_query.py engine and adds LLM interpretation for
disclosure events. Supports:
  - query_disclosure_events: DART disclosure event search
  - query_financials: financial statement queries
  - query_insider_trades: insider transaction queries
  - query_major_shareholders: major shareholder queries
  - interpret_disclosure: short Haiku interpretation (title + impact + 3 sentences)
  - summarize_disclosure: detailed Sonnet 4.6 analysis (what/why/watch)
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional, List, Dict

import boto3

from dart_query import query_dart as query_dart_engine

logger = logging.getLogger("opik.dart_agent")

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
DART_MODEL = os.environ.get(
    "DART_MODEL",
    "global.anthropic.claude-haiku-4-5-20251001-v1:0",
)
SUMMARIZE_MODEL = os.environ.get(
    "DART_SUMMARIZE_MODEL",
    "global.anthropic.claude-sonnet-4-6",
)

INTERPRET_PROMPT = """당신은 한국 DART 공시 분석 전문가입니다.
주어진 공시 텍스트를 분석하여 일반 투자자가 이해할 수 있게 요약하세요.

## 출력 형식
```
{이벤트 제목} — {한 줄 요약 (30단어 이내)}
영향도: [긍정적|중립적|부정적]
주요 내용: {3문장 이내 요약}
```

## 중요 규칙
- 재무/법률 용어는 가능한 쉬운 말로 풀어서 설명
- "~할 것으로 보입니다", "~로 해석됩니다" 등 확정적이지 않은 표현 사용
- 투자 판단을 직접 유도하는 표현 금지 ("매수해야 합니다" 등)
- 주주가치에 미치는 영향이 불분명하면 "중립적"으로 분류
- 정보가 불충분하면 "공시 본문만으로 판단이 어렵습니다"라고 솔직히 표기"""

SUMMARIZE_PROMPT = """당신은 한국 DART 공시 분석 전문가입니다. 주어진 공시 텍스트를 깊이 있게 분석하여
투자자가 이 공시의 실질적 의미를 이해할 수 있도록 설명하세요.

## 출력 형식
```
## 공시 요약: {이벤트 제목}

### 무슨 일이 있었나
{공시의 핵심 사실을 2-3문장으로 간결하게 정리. 수치가 있으면 포함}

### 왜 중요한가
{이 공시가 주주가치/기업가치에 미치는 실질적 영향 분석. 긍정/부정/중립 판단과 그 근거}

### 주목할 점
{투자자가 이 공시를 보고 추가로 확인해야 할 사항 2-3개}
```

## 중요 규칙
- "~할 것으로 보입니다", "~로 판단됩니다" 등 확률적 표현 사용
- 재무/법률 용어는 쉬운 말로 풀어서 설명
- 투자 판단 직접 유도 금지
- 정보가 불충분하면 "공시 본문만으로는 판단이 어려우며, 추가 공시를 확인해야 합니다"라고 명시
- 응답 마지막에 "※ 본 분석은 공시 텍스트의 해석이며 투자 권유가 아닙니다." 추가"""


class DartAgent:
    """DART Gold query + disclosure interpretation + summarization."""

    def __init__(
        self,
        model_id: str = DART_MODEL,
        summarize_model_id: str = SUMMARIZE_MODEL,
        region: str = AWS_REGION,
    ):
        self.model_id = model_id
        self.summarize_model_id = summarize_model_id
        self.region = region
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client("bedrock-runtime", region_name=self.region)
        return self._client

    # ── query wrappers (delegate to existing dart_query engine) ──

    def query_disclosure_events(
        self,
        companies: Optional[List[str]] = None,
        codes: Optional[List[str]] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> str:
        """Search DART disclosure events."""
        return query_dart_engine(
            intent="dart_disclosure",
            companies=companies or [],
            codes=codes or [],
            date_from=date_from,
            date_to=date_to,
            page=page,
            page_size=page_size,
        )

    def query_financials(
        self,
        companies: Optional[List[str]] = None,
        codes: Optional[List[str]] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> str:
        """Search DART financial statements."""
        return query_dart_engine(
            intent="dart_financial",
            companies=companies or [],
            codes=codes or [],
            date_from=date_from,
            date_to=date_to,
            page=page,
            page_size=page_size,
        )

    def query_insider_trades(
        self,
        companies: Optional[List[str]] = None,
        codes: Optional[List[str]] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> str:
        """Search insider transactions."""
        return query_dart_engine(
            intent="dart_insider",
            companies=companies or [],
            codes=codes or [],
            date_from=date_from,
            date_to=date_to,
        )

    def query_major_shareholders(
        self,
        companies: Optional[List[str]] = None,
        codes: Optional[List[str]] = None,
    ) -> str:
        """Search major shareholder information."""
        return query_dart_engine(
            intent="dart_shareholder",
            companies=companies or [],
            codes=codes or [],
        )

    # ── LLM interpretation (Haiku — fast, cheap) ──

    def _call_haiku(self, system_prompt: str, user_content: str, max_tokens: int = 600) -> str:
        """Call Haiku via converse API."""
        try:
            resp = self.client.converse(
                modelId=self.model_id,
                messages=[{"role": "user", "content": [{"text": user_content}]}],
                system=[{"text": system_prompt}],
                inferenceConfig={"maxTokens": max_tokens, "temperature": 0.2},
            )
            answer = ""
            for block in resp.get("output", {}).get("message", {}).get("content", []):
                if block.get("text"):
                    answer += block["text"]
            return answer
        except Exception as e:
            logger.error("Haiku call failed: %s", e)
            return ""

    def _call_sonnet(self, system_prompt: str, user_content: str, max_tokens: int = 1500) -> str:
        """Call Sonnet via converse API for detailed analysis."""
        try:
            resp = self.client.converse(
                modelId=self.summarize_model_id,
                messages=[{"role": "user", "content": [{"text": user_content}]}],
                system=[{"text": system_prompt}],
                inferenceConfig={"maxTokens": max_tokens, "temperature": 0.3},
            )
            answer = ""
            for block in resp.get("output", {}).get("message", {}).get("content", []):
                if block.get("text"):
                    answer += block["text"]
            return answer
        except Exception as e:
            logger.error("Sonnet call failed: %s", e)
            return ""

    def interpret_disclosure(self, disclosure_text: str, event_type: str = "") -> str:
        """Short Haiku interpretation: title + impact + 3 sentences."""
        if not disclosure_text or len(disclosure_text) < 20:
            return "공시 텍스트가 충분하지 않아 해석이 어렵습니다."

        text_snippet = disclosure_text[:3000]
        user_content = f"공시유형: {event_type}\n\n공시 내용:\n{text_snippet}"

        answer = self._call_haiku(INTERPRET_PROMPT, user_content, max_tokens=600)
        return answer or "공시 해석 중 오류가 발생했습니다."

    def summarize_disclosure(self, disclosure_text: str, event_type: str = "") -> str:
        """Detailed Sonnet analysis: what happened, why it matters, what to watch."""
        if not disclosure_text or len(disclosure_text) < 20:
            return "공시 텍스트가 충분하지 않아 분석이 어렵습니다."

        text_snippet = disclosure_text[:5000]
        user_content = f"공시유형: {event_type}\n\n공시 내용:\n{text_snippet}"

        answer = self._call_sonnet(SUMMARIZE_PROMPT, user_content, max_tokens=1500)
        return answer or "공시 분석 중 오류가 발생했습니다."

    def interpret_batch(
        self, disclosures: List[dict]
    ) -> List[dict]:
        """Interpret multiple disclosures. Each gets a summary.

        Args:
            disclosures: [{report_nm, text, event_category, ...}]

        Returns:
            Same list with added 'interpretation' and 'impact' fields.
        """
        results = []
        for d in disclosures:
            text = d.get("text", "")
            event_type = d.get("event_category", "")
            interpretation = self.interpret_disclosure(text, event_type)

            # Extract impact from interpretation
            impact = "neutral"
            if "긍정" in interpretation:
                impact = "positive"
            elif "부정" in interpretation:
                impact = "negative"

            results.append({
                **d,
                "interpretation": interpretation,
                "impact": impact,
            })
        return results


# Singleton
_default_dart: Optional[DartAgent] = None


def get_dart_agent() -> DartAgent:
    global _default_dart
    if _default_dart is None:
        _default_dart = DartAgent()
    return _default_dart
