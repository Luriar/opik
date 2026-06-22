"""
Report Agent — FAISS semantic search + Haiku report summarisation.

Wraps existing FAISS search (opik_server.py /search endpoint logic) and adds
LLM summarisation for user-facing responses. Supports:
  - search_faiss: semantic search over 51,583 report embeddings
  - browse_by_date: date-based report browsing
  - get_report_detail: single report full detail
  - summarise_reports: LLM summary of search results

Output format per report:
  [종목명] (증권사, YYYY-MM-DD)
  투자의견: BUY | 목표주가: 85,000원 | 현재주가: 72,000원 | 상승여력: +18.1%
  핵심 논리: ...  리스크: ...  키워드: ...
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional, List

import boto3
import numpy as np

logger = logging.getLogger("opik.report_agent")

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
REPORT_MODEL = os.environ.get(
    "REPORT_MODEL",
    "apac.anthropic.claude-3-haiku-20240307-v1:0",
)

REPORT_SUMMARY_PROMPT = """당신은 OPIK 금융 정보 챗봇의 리포트 요약 전문가입니다.
검색된 증권사 애널리스트 리포트를 사용자 질문에 맞게 요약하세요.

## 응답 형식
각 리포트별로:
```
[신뢰도: {high|medium|low}]
{종목명} ({증권사}, {날짜})
투자의견: {opinion} | 목표주가: {tp:,}원 | 상승여력: {upside:+.1f}%

핵심 논리: {reason}
리스크: {risks}
키워드: {keywords}

[출처: {증권사} {title}, {날짜}]
```

## 중요 규칙
- 모든 factual 데이터(수치, 날짜, 증권사명)는 검색 결과 그대로 사용
- LLM 해석이 필요한 부분은 "분석 결과"임을 명시
- 데이터가 없는 필드는 "정보 없음"으로 표기
- 투자 추천이나 예측성 발언 금지
- 응답 마지막에 "※ 본 정보는 증권사 리포트의 사실적 요약이며 투자 권유가 아닙니다." 추가"""


class ReportAgent:
    """FAISS search + Haiku report summarisation."""

    def __init__(
        self,
        faiss_index=None,
        report_ids: Optional[List[str]] = None,
        report_texts: Optional[dict] = None,
        model_id: str = REPORT_MODEL,
        region: str = AWS_REGION,
        embedder=None,
    ):
        self.faiss_index = faiss_index
        self.report_ids = report_ids or []
        self.report_texts = report_texts or {}
        self.model_id = model_id
        self.region = region
        self.embedder = embedder
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client("bedrock-runtime", region_name=self.region)
        return self._client

    def set_index(self, faiss_index, report_ids: list, report_texts: dict):
        """Wire up to the global FAISS index (set after server startup)."""
        self.faiss_index = faiss_index
        self.report_ids = report_ids
        self.report_texts = report_texts

    def set_embedder(self, embedder):
        self.embedder = embedder

    def search(self, query: str, top_k: int = 10) -> List[dict]:
        """Search FAISS index. Returns list of {report_id, score, ...}."""
        if self.faiss_index is None or self.faiss_index.ntotal == 0:
            logger.warning("FAISS index not loaded")
            return []
        if self.embedder is None:
            logger.warning("Embedder not set")
            return []

        query_vec = self.embedder.encode(
            ["query: " + query], normalize_embeddings=True
        ).astype(np.float32)

        scores, idxs = self.faiss_index.search(query_vec, top_k)

        results = []
        for score, idx in zip(scores[0], idxs[0]):
            if idx < 0 or idx >= len(self.report_ids):
                continue
            rid = self.report_ids[idx]
            info = self.report_texts.get(rid, {})
            results.append({
                "report_id": rid,
                "score": float(score),
                "종목코드": info.get("종목코드"),
                "reason": info.get("reason"),
                "keywords": info.get("keywords"),
                "risks": info.get("risks"),
                "year": info.get("year"),
                "month": info.get("month"),
            })

        logger.info("FAISS search: '%s' → %d results", query[:60], len(results))
        return results
    def search_by_date(self, date_from: str, date_to: str, limit: int = 50) -> list:
        """Scan S3 gold/structured by 발행일. Used when user specifies an exact date.

        Reads gold/structured/year=YYYY/month=MM/data.parquet, filters by 발행일.
        Returns list of dicts with report_id, reason, keywords, risks, etc.
        """
        import io, pyarrow.parquet as pq
        s3 = boto3.client("s3", region_name=AWS_REGION)
        BUCKET = os.environ.get("S3_BUCKET", "s3-opik-bucket")
        results = []
        fy = int(date_from[:4])
        fm = int(date_from[5:7])
        ty = int(date_to[:4])
        tm = int(date_to[5:7])

        for y in range(fy, ty + 1):
            ms = fm if y == fy else 1
            me = tm if y == ty else 12
            for m in range(ms, me + 1):
                key = f"gold/structured/year={y:04d}/month={m:02d}/data.parquet"
                try:
                    resp = s3.get_object(Bucket=BUCKET, Key=key)
                    buf = io.BytesIO(resp["Body"].read())
                    df = pq.read_table(buf).to_pandas()
                except Exception:
                    continue
                if "발행일" in df.columns:
                    from pandas import Series
                    mask = Series(True, index=df.index)
                    mask = mask & (df["발행일"].astype(str) >= date_from)
                    mask = mask & (df["발행일"].astype(str) <= date_to)
                    df = df[mask].sort_values("발행일", ascending=False)
                for _, row in df.iterrows():
                    rid = str(row.get("report_id", ""))
                    info = self.report_texts.get(rid, {})
                    title = str(row.get("title", "")) if row.get("title") is not None else ""
                    sec = str(row.get("증권사", "")) if row.get("증권사") is not None else ""
                    reason = f"[{sec}] {title}" if sec and title else (title or info.get("reason", ""))
                    results.append({
                        "report_id": rid,
                        "score": 1.0,
                        "종목코드": str(row.get("종목코드", "")) if row.get("종목코드") is not None else None,
                        "reason": str(reason)[:300] if reason else None,
                        "keywords": info.get("keywords"),
                        "risks": info.get("risks"),
                        "year": y,
                        "month": m,
                    })
        logger.info("Date scan: %s~%s → %d results", date_from, date_to, len(results))
        return results[:limit]

    def summarise(self, user_question: str, search_results: List[dict]) -> str:
        """Summarise FAISS results with Haiku."""
        if not search_results:
            return "해당 조건으로 검색된 애널리스트 리포트가 없습니다."

        # Build context from search results
        context_lines = []
        for r in search_results[:10]:
            context_lines.append(
                f"[report_id={r['report_id']} score={r['score']:.3f}] "
                f"종목코드={r.get('종목코드', 'N/A')} "
                f"reason={r.get('reason', '정보 없음')} "
                f"risks={r.get('risks', '정보 없음')} "
                f"keywords={r.get('keywords', '정보 없음')}"
            )
        context = "\n".join(context_lines)

        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1500,
            "system": REPORT_SUMMARY_PROMPT,
            "messages": [{
                "role": "user",
                "content": f"질문: {user_question}\n\n검색 결과:\n{context}",
            }],
            "temperature": 0.3,
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
            logger.error("Report summarise failed: %s", e)
            return "리포트 요약 생성 중 오류가 발생했습니다."


# Singleton
_default_report: Optional[ReportAgent] = None


def get_report_agent() -> ReportAgent:
    global _default_report
    if _default_report is None:
        _default_report = ReportAgent()
    return _default_report
