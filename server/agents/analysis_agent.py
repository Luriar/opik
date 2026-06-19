"""
Analysis Agent — cross-source synthesis, comparison, cause-tracing.

The main reasoning agent (Opus 4.8):
  - compare_reports: cross-brokerage report comparison
  - industry_analysis: sector-wide report synthesis
  - trace_cause: price movement cause tracing (timeline mapping)

Output is always caveated: analysis ≠ fact, probabilistic language only.
"""

import json
import logging
import os
from typing import Optional, List, Dict

import boto3

logger = logging.getLogger("opik.analysis")

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
ANALYSIS_MODEL = os.environ.get(
    "ANALYSIS_MODEL",
    "global.anthropic.claude-opus-4-8",
)

COMPARE_PROMPT = """당신은 OPIK 금융 분석가입니다. 동일 종목에 대한 여러 증권사 리포트를 비교 분석하세요.

## 출력 형식
```
### {종목명} 증권사 리포트 비교

| 항목 | {증권사A} | {증권사B} |
|------|----------|----------|
| 투자의견 | BUY | BUY |
| 목표주가 | 85,000원 | 82,000원 |
| 상승여력 | +18.1% | +13.9% |
| 핵심 논리 | ... | ... |
| 주요 리스크 | ... | ... |

## 핵심 차이점
{2-3문장으로 증권사 간 의견 차이의 근본 원인 설명}

## 종합
{종합적 관점, consensus/disagreement 지점 강조}
```

## 중요 규칙
- 수치는 그대로, 해석은 "분석 결과"임을 명시
- 확정적 진술 금지 ("~할 것이다" → "~할 것으로 보입니다")
- 투자 판단 유도 금지
- 응답 마지막에 "※ 본 분석은 증권사 리포트의 비교 요약이며 투자 권유가 아닙니다." 추가"""

CAUSE_TRACE_PROMPT = """당신은 OPIK 금융 분석가입니다. 주어진 리포트와 공시 데이터를 바탕으로
주가 움직임의 가능한 요인을 추적하세요.

## 출력 형식
```
### {종목명} 주가 변동 요인 분석 ({기간})

## 타임라인
| 날짜 | 이벤트 | 유형 | 가능한 영향 |
|------|--------|------|------------|
| ... | ... | 리포트/공시 | ... |

## 가능한 주요 요인
{확률적 언어로 2-4개 요인 제시}

## 주의사항
- 이는 과거 데이터 기반 추론이며 확정적 원인이 아닙니다.
- 실제 주가는 복합적 요인으로 움직이므로 단일 원인으로 설명할 수 없습니다.
```

## 중요 규칙
- "~때문입니다" → "~이(가) 주요 요인으로 작용한 것으로 보입니다"
- "확실히" → "가능성이 높습니다"
- 하나의 원인으로 단정하지 말고 복합적 요인을 제시할 것"""

INDUSTRY_PROMPT = """당신은 OPIK 금융 분석가입니다. 특정 섹터의 애널리스트 리포트를 종합하여
섹터 전망을 분석하세요.

## 출력 형식
```
### {섹터명} 섹터 애널리스트 종합

## 섹터 내 종목별 포지션
| 종목 | 증권사 | 의견 | TP | 핵심 키워드 |
|------|--------|------|-----|------------|
| ... | ... | ... | ... | ... |

## 공통 업황 키워드
{3-5개 공통 키워드와 각각에 대한 애널리스트 consensus}

## 섹터 종합 전망
{애널리스트들의 공통된 시각과 차별화된 시각 정리}

## 종목별 차별화 포인트
{각 종목이 섹터 내에서 가지는 상대적 강점/약점}
```

## 중요 규칙
- 애널리스트 의견을 있는 그대로 전달
- 종목 간 순위 매기기 금지 ("가장 좋다" 등)
- 투자 판단 유도 금지"""


class AnalysisAgent:
    """Cross-source synthesis with Opus 4.8 for complex reasoning tasks."""

    def __init__(
        self,
        model_id: str = ANALYSIS_MODEL,
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

    def _call_opus(self, system_prompt: str, user_content: str, max_tokens: int = 2000) -> str:
        """Call Opus via Bedrock converse API."""
        try:
            resp = self.client.converse(
                modelId=self.model_id,
                messages=[{"role": "user", "content": [{"text": user_content}]}],
                system=[{"text": system_prompt}],
                inferenceConfig={"maxTokens": max_tokens, "temperature": 0.3},
            )
            answer = ""
            for block in resp.get("output", {}).get("message", {}).get("content", []):
                if block.get("text"):
                    answer += block["text"]
            in_tok = resp.get("usage", {}).get("inputTokens", 0)
            out_tok = resp.get("usage", {}).get("outputTokens", 0)
            logger.info("Opus call: in=%d out=%d chars=%d", in_tok, out_tok, len(answer))
            return answer or "(분석 결과를 생성하지 못했습니다)"

        except Exception as e:
            logger.error("Opus call failed: %s", e)
            return f"분석 중 오류가 발생했습니다: {e}"

    def compare_reports(
        self, reports: List[dict], ticker_name: str = ""
    ) -> str:
        """Compare multiple broker reports for the same stock."""
        if len(reports) < 2:
            return "비교할 리포트가 2개 이상 필요합니다."

        context = f"종목: {ticker_name}\n\n"
        for i, r in enumerate(reports):
            context += (
                f"### 리포트 {i + 1}\n"
                f"증권사: {r.get('증권사', 'N/A')}\n"
                f"날짜: {r.get('발행일', 'N/A')}\n"
                f"투자의견: {r.get('투자의견', 'N/A')}\n"
                f"목표주가: {r.get('목표주가', 'N/A')}원\n"
                f"현재주가: {r.get('현재주가', 'N/A')}원\n"
                f"핵심 논리: {r.get('reason', 'N/A')}\n"
                f"리스크: {r.get('risks', 'N/A')}\n\n"
            )

        return self._call_opus(COMPARE_PROMPT, context)

    def trace_cause(
        self,
        ticker_name: str,
        date_range: str,
        report_events: List[dict],
        dart_events: List[dict],
    ) -> str:
        """Trace possible causes of a price movement."""
        context = f"종목: {ticker_name}\n기간: {date_range}\n\n"

        context += "### 리포트 이벤트\n"
        for r in report_events:
            context += f"- [{r.get('date', 'N/A')}] {r.get('brokerage', '')} : {r.get('summary', '')}\n"

        context += "\n### DART 공시 이벤트\n"
        for d in dart_events:
            context += f"- [{d.get('date', 'N/A')}] {d.get('event', '')} : {d.get('summary', '')}\n"

        return self._call_opus(CAUSE_TRACE_PROMPT, context, max_tokens=1500)

    def industry_analysis(
        self,
        sector: str,
        sector_reports: List[dict],
    ) -> str:
        """Sector-wide analysis across multiple stocks."""
        context = f"섹터: {sector}\n\n"
        for r in sector_reports:
            context += (
                f"종목: {r.get('종목명', 'N/A')} | "
                f"증권사: {r.get('증권사', 'N/A')} | "
                f"의견: {r.get('투자의견', 'N/A')} | "
                f"TP: {r.get('목표주가', 'N/A')}원 | "
                f"키워드: {r.get('keywords', 'N/A')}\n"
            )

        return self._call_opus(INDUSTRY_PROMPT, context, max_tokens=2500)


# Singleton
_default_analysis: Optional[AnalysisAgent] = None


def get_analysis_agent() -> AnalysisAgent:
    global _default_analysis
    if _default_analysis is None:
        _default_analysis = AnalysisAgent()
    return _default_analysis
