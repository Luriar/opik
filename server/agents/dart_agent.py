"""
DART Agent — DART Gold Parquet queries + disclosure interpretation.

Wraps existing dart_query.py engine and adds LLM interpretation for
disclosure events. Supports:
  - query_disclosure_events: DART disclosure event search
  - query_financials: financial statement queries
  - query_insider_trades: insider transaction queries
  - query_major_shareholders: major shareholder queries
  - interpret_disclosure: LLM-powered disclosure text interpretation (Haiku)
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
    "apac.anthropic.claude-3-haiku-20240307-v1:0",
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


class DartAgent:
    """DART Gold query + disclosure interpretation."""

    def __init__(
        self,
        model_id: str = DART_MODEL,
        region: str = AWS_REGION,
    ):
        self.model_id = model_id
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

    # ── LLM interpretation ──

    def interpret_disclosure(self, disclosure_text: str, event_type: str = "") -> str:
        """Interpret a single disclosure's meaning using Haiku."""
        if not disclosure_text or len(disclosure_text) < 20:
            return "공시 텍스트가 충분하지 않아 해석이 어렵습니다."

        # Truncate to reasonable context window
        text_snippet = disclosure_text[:3000]

        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 600,
            "system": INTERPRET_PROMPT,
            "messages": [{
                "role": "user",
                "content": f"공시유형: {event_type}\n\n공시 내용:\n{text_snippet}",
            }],
            "temperature": 0.2,
        })

        try:
            resp = self.client.invoke_model(
                modelId=self.model_id,
                contentType="application/json",
                accept="application/json",
                body=body,
            )
            resp_body = json.loads(resp["body"].read())
            answer = ""
            for block in resp_body.get("content", []):
                if block.get("type") == "text":
                    answer += block["text"]
            return answer

        except Exception as e:
            logger.error("Disclosure interpret failed: %s", e)
            return "공시 해석 중 오류가 발생했습니다."

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
