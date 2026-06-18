"""
Safety Agent — Zone C (investment advice) / Zone D (out-of-domain) filtering.

First agent in the LangGraph pipeline. Every user message passes through here
before any other agent runs. Uses Haiku for low-latency classification (~0.3s).

Output: {is_safe: bool, violation_type: str|null, redirect_suggestion: str}
"""

import json
import logging
import os
from typing import Optional

import boto3

logger = logging.getLogger("opik.safety")

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
SAFETY_MODEL = os.environ.get(
    "SAFETY_MODEL",
    "apac.anthropic.claude-3-haiku-20240307-v1:0",
)

SAFETY_SYSTEM_PROMPT = """당신은 OPIK 금융 정보 챗봇의 안전 필터입니다.
사용자 메시지를 분석하여 다음 중 하나로 분류하세요.

## SAFE (is_safe: true) — 정상 처리
- report_search: "삼성전자 리포트 보여줘", "최근 반도체 리포트", "목표주가 알려줘"
- dart_query: "최근 공시 뭐 있어?", "자사주 매입 공시 보여줘"
- hybrid: "삼성전자 리포트랑 공시 같이 보여줘"
- general: "OPIK이 뭐야?", "고마워", "어떻게 써?", "안녕"

## UNSAFE (is_safe: false) — 즉시 거절 + 대안 제시
- buy_recommend: "뭐 사는게 좋을까?", "추천 종목", "지금 살만한 주식"
- sell_recommend: "팔아야 하나?", "손절할까?"
- portfolio: "내 포트폴리오 어때?", "비중 조절"
- timing: "언제 사는게 좋아?", "지금이 바닥이야?"
- guarantee: "확실한 종목", "무조건 오르는", "100%"
- out_of_domain: 코딩, 알고리즘, 요리, 번역, 일반 지식 등 금융 무관

출력은 반드시 아래 JSON 형식만 반환하세요:
{"is_safe": true/false, "violation_type": "buy_recommend"|"sell_recommend"|"portfolio"|"timing"|"guarantee"|"out_of_domain"|null, "redirect_suggestion": "대안 제안 (한국어, 20단어 이내)"}"""


class SafetyAgent:
    """Classifies user messages as safe / unsafe for downstream processing."""

    def __init__(
        self,
        model_id: str = SAFETY_MODEL,
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

    def check(self, user_message: str) -> dict:
        """Run safety check on a user message. Returns {is_safe, violation_type, redirect_suggestion}."""
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 256,
            "system": SAFETY_SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_message}],
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

            text = text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[-1].rsplit("\n```", 1)[0]
                if text.startswith("json"):
                    text = text[4:].strip()

            result = json.loads(text)
            logger.info("Safety check: safe=%s type=%s", result.get("is_safe"), result.get("violation_type"))
            return {
                "is_safe": result.get("is_safe", True),
                "violation_type": result.get("violation_type"),
                "redirect_suggestion": result.get("redirect_suggestion", ""),
            }

        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Safety parse failed: %s — allowing through (safe default)", e)
            return {"is_safe": True, "violation_type": None, "redirect_suggestion": ""}

    def build_refusal_message(self, violation_type: Optional[str], redirect_suggestion: str = "") -> str:
        """Build a polite refusal message for the given violation type."""
        templates = {
            "buy_recommend": (
                "OPIK은 특정 종목의 매수 추천을 제공하지 않는 금융 정보 챗봇입니다.\n\n"
                "대신 다음과 같은 정보를 제공해 드릴 수 있습니다:\n"
                "• 관심 종목에 대한 최근 애널리스트 리포트 검색 및 요약\n"
                "• 증권사별 투자의견·목표주가 비교\n"
                "• 관련 DART 공시 내용 요약\n\n"
                "원하시는 종목명이나 섹터를 말씀해 주시면 관련 정보를 검색해 드리겠습니다."
            ),
            "sell_recommend": (
                "OPIK은 매도 타이밍이나 손절에 대한 조언을 제공하지 않습니다.\n\n"
                "대신 보유 종목에 대한 최근 리포트나 공시 정보를 검색해 드릴 수 있습니다.\n"
                "원하시는 종목명을 말씀해 주세요."
            ),
            "portfolio": (
                "OPIK은 개인 포트폴리오 구성이나 비중 조절에 대한 조언을 제공하지 않습니다.\n\n"
                "대신 관심 섹터나 종목의 최근 리포트를 검색해 드릴 수 있습니다."
            ),
            "timing": (
                "OPIK은 시장 타이밍이나 매매 시점에 대한 조언을 제공하지 않습니다.\n\n"
                "대신 해당 종목에 대한 최근 애널리스트 의견과 공시 정보를 제공해 드릴 수 있습니다."
            ),
            "guarantee": (
                "OPIK은 특정 수익을 보장하는 종목 추천을 제공하지 않습니다.\n"
                "모든 투자에는 리스크가 따릅니다.\n\n"
                "대신 객관적인 리포트 데이터와 공시 정보를 검색해 드릴 수 있습니다."
            ),
            "out_of_domain": (
                "OPIK은 증권사 애널리스트 리포트 및 DART 공시 데이터를 검색·요약해주는 금융 정보 챗봇입니다.\n\n"
                "질문하신 내용은 OPIK의 기능 범위를 벗어납니다. 다음과 같은 작업을 도와드릴 수 있습니다:\n"
                "• 특정 종목/섹터의 애널리스트 리포트 검색 및 요약\n"
                "• DART 공시 이벤트 조회 (실적, 자사주, 주요주주 변동 등)\n"
                "• 애널리스트 의견 비교 및 목표주가 분포 확인\n\n"
                "금융 정보 검색이 필요하시면 말씀해 주세요."
            ),
        }
        base = templates.get(violation_type or "", templates["out_of_domain"])
        if redirect_suggestion:
            base += f"\n\n{redirect_suggestion}"
        return base


# Convenience function for non-LangGraph use
_default_safety: Optional[SafetyAgent] = None


def get_safety_agent() -> SafetyAgent:
    global _default_safety
    if _default_safety is None:
        _default_safety = SafetyAgent()
    return _default_safety


def check_message(user_message: str) -> dict:
    return get_safety_agent().check(user_message)
