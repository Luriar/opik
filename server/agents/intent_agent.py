"""
Intent Agent — classifies user intent and extracts search parameters.

Second agent in the pipeline (after Safety). Uses Haiku for fast classification.
Expands on the existing intent_parser.py with richer intent types for Phase 2:
  - compare: cross-brokerage report comparison (triggers AnalysisAgent)
  - cause_tracking: price movement cause tracing
  - interpret: disclosure interpretation requests

Output: {intent, params: {tickers, brokerages, sectors, time_range, keywords, ...}}
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional

import boto3

logger = logging.getLogger("opik.intent_agent")

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
INTENT_MODEL = os.environ.get(
    "INTENT_MODEL",
    "apac.anthropic.claude-3-haiku-20240307-v1:0",
)

INTENT_SYSTEM_PROMPT = """당신은 OPIK 챗봇의 의도 파악기입니다.
사용자 질문을 분석하여 구조화된 JSON으로 반환하세요. 오늘 날짜는 {TODAY_DATE}입니다.

## Intent 분류
1. **report_search** — 애널리스트 리포트 검색/요약/비교
2. **dart_query** — DART 공시 이벤트/재무/지분 조회
3. **hybrid** — 리포트 + 공시 모두 필요 (두 소스 모두 검색)
4. **general** — 인사, 설명, 기능 문의 (데이터 검색 불필요)

## 특수 플래그 (intent_params 내 boolean)
- **compare**: "A증권사랑 B증권사 비교해줘" → 여러 증권사 리포트 비교 요청
- **cause_tracking**: "왜 올랐어?", "왜 떨어졌어?" → 주가 원인 추적 요청
- **interpret**: "이 공시 무슨 의미야?", "이게 호재야 악재야?" → 공시 해석 요청

## 파라미터 추출
- **tickers**: 종목코드 배열 (ex: ["005930"]) — 모르면 빈 배열
- **ticker_names**: 종목명 배열 (ex: ["삼성전자", "SK하이닉스"])
- **brokerages**: 증권사명 배열 (ex: ["한국투자증권"])
- **sectors**: 섹터명 배열 (ex: ["반도체"])
- **time_range**: {"from": "YYYY-MM-DD", "to": "YYYY-MM-DD"} 또는 null
  - "어제" → 어제 날짜로 변환
  - "최근" → 최근 7일
  - "이번 주" → 이번 주 월~금
  - "1월" → 해당 연도 1월
  - 없으면 null
- **keywords**: 핵심 키워드 배열 (ex: ["HBM", "유상증자"])
- **is_greeting**: 인사말/감사 여부 (true/false)

## response_style
- "brief": 한 줄 요약 원함
- "detailed": 상세 분석 원함 (기본값)
- "table": 표 형태 원함

## 비교 분석 판단 기준
- "A증권사랑 B증권사 비교해줘", "의견 차이", "증권사별로" → compare: true
- "다른 증권사는 뭐라고 해?" → compare: true (cross-brokerage)
- "왜 올랐어?", "왜 떨어졌어?", "무슨 일이야?" → cause_tracking: true
- "이 공시 무슨 의미야?", "해석해줘", "호재야?" → interpret: true

출력은 반드시 아래 JSON 형식만 반환하세요:
{
  "intent": "report_search"|"dart_query"|"hybrid"|"general",
  "params": {
    "tickers": ["005930"],
    "ticker_names": ["삼성전자"],
    "brokerages": ["한국투자증권"],
    "sectors": [],
    "time_range": {"from": "2026-06-01", "to": "2026-06-18"} | null,
    "keywords": ["HBM"],
    "compare": false,
    "cause_tracking": false,
    "interpret": false,
    "is_greeting": false,
    "response_style": "detailed"
  }
}"""


class IntentAgent:
    """Parses user intent and extracts structured search parameters."""

    def __init__(
        self,
        model_id: str = INTENT_MODEL,
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

    def parse(self, user_message: str, conversation_context: str = "") -> dict:
        """Parse user message into structured intent.
        
        If conversation_context is provided, it is prepended to help resolve
        follow-up references like "이거", "저 리포트", etc.
        """
        """Parse user message into structured intent."""
        now = datetime.now()

        # Augment with conversation context for follow-up resolution
        if conversation_context:
            augmented = (
                f"[이전 대화 내용]\n{conversation_context}\n\n"
                f"[현재 질문]\n{user_message}"
            )
        else:
            augmented = user_message

        system_prompt = INTENT_SYSTEM_PROMPT.replace(
            "{TODAY_DATE}", now.strftime("%Y-%m-%d")
        )

        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 512,
            "system": system_prompt,
            "messages": [{"role": "user", "content": augmented}],
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
            intent = result.get("intent", "general")
            params = result.get("params", {})

            logger.info(
                "Intent: %s tickers=%s compare=%s cause=%s interpret=%s",
                intent,
                params.get("tickers"),
                params.get("compare"),
                params.get("cause_tracking"),
                params.get("interpret"),
            )
            return {
                "intent": intent,
                "intent_params": params,
            }

        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Intent parse failed: %s — falling back to general", e)
            return {
                "intent": "general",
                "intent_params": {
                    "tickers": [],
                    "ticker_names": [],
                    "brokerages": [],
                    "sectors": [],
                    "time_range": None,
                    "keywords": [],
                    "compare": False,
                    "cause_tracking": False,
                    "interpret": False,
                    "is_greeting": False,
                    "response_style": "detailed",
                },
            }


# Singleton
_default_intent: Optional[IntentAgent] = None


def get_intent_agent() -> IntentAgent:
    global _default_intent
    if _default_intent is None:
        _default_intent = IntentAgent()
    return _default_intent


def parse_intent(user_message: str) -> dict:
    return get_intent_agent().parse(user_message)
