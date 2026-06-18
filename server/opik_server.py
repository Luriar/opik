"""
OPIK Server — embedding + FAISS search + chatbot with Intent Parsing.
Deployed on EC2 r6g.large (16GB ARM).

Endpoints:
  GET  /health          — health check
  POST /encode          — text -> embedding (multilingual-e5-small, 384-dim)
  POST /search          — query -> top-k FAISS results
  POST /chat            — conversational RAG with 3-stage pipeline
  POST /index/rebuild   — trigger FAISS index rebuild from S3

Start: uvicorn opik_server:app --host 0.0.0.0 --port 8000
"""

import io
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

import boto3
import faiss
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import re
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

# Intent parser — Stage 1 preprocessing
from intent_parser import get_parser, IntentResult

# DART query engine — actual S3 Parquet queries
from dart_query import query_dart as query_dart_engine

# Conversation memory — session-based context management (v2)
from conversation_store import store as conversation_store

# Phase 2a: LangGraph multi-agent framework (optional — toggled by OPIK_AGENT_ENABLED)
from agent_integration import init_agents, v2_chat_handler, get_agent_status

# config
S3_BUCKET = os.environ.get("S3_BUCKET", "s3-opik-bucket")
EMBEDDING_MODEL_NAME = os.environ.get("EMBEDDING_MODEL", "intfloat/multilingual-e5-small")
EMBEDDING_DIM = 384
FAISS_INDEX_PATH = os.environ.get("FAISS_INDEX_PATH", "/data/opik/faiss_index.bin")
FAISS_IDMAP_PATH = os.environ.get("FAISS_IDMAP_PATH", "/data/opik/report_ids.json")
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
BEDROCK_MODEL = os.environ.get("BEDROCK_MODEL", "apac.anthropic.claude-3-haiku-20240307-v1:0")
SEARCH_TOP_K = int(os.environ.get("SEARCH_TOP_K", "10"))
AGENT_ENABLED_STR = os.environ.get("OPIK_AGENT_ENABLED", "true")

os.makedirs("/data/opik", exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("opik")

# global state
embedder: Optional[SentenceTransformer] = None
faiss_index: Optional[faiss.Index] = None
report_ids: List[str] = []
report_texts: dict = {}
index_lock = threading.Lock()


# ──────────────────────────────────────────────
# v2: Refusal message builder (Zone C + Zone D)
# ──────────────────────────────────────────────
def _build_refusal_message(user_message: str) -> str:
    """Build a polite refusal message when user asks for investment advice
    or out-of-scope topics (algorithms, coding, general knowledge, etc.)."""
    msg_lower = user_message.lower().strip()

    # Zone C: Investment advice keywords
    _invest_signals = [
        "사는", "살까", "매수", "매도", "추천", "종목", "주식", "투자",
        "사는게", "살만한", "어떤게", "뭐가", "뭐사", "뭘사",
        "손절", "매매", "비중", "포트폴리오", "오를까", "내릴까",
        "레버리지", "공매도", "선물", "수익률",
    ]
    # Zone D: Out-of-scope non-finance
    _out_of_scope_signals = [
        "알고리즘", "코딩", "정렬", "퀵소트", "버블소트", "BFS", "DFS",
        "프로그래밍", "python", "자바", "자바스크립트", "react", "html", "css",
        "api", "코드", "구현", "컴파일", "디버그", "깃허브", "깃",
        "방정식", "미적분", "화학", "물리", "레시피", "요리", "번역", "영화",
    ]

    is_invest = any(kw in msg_lower for kw in _invest_signals)
    is_oos = any(kw in msg_lower for kw in _out_of_scope_signals)

    if is_oos:
        return (
            "OPIK은 증권사 애널리스트 리포트 및 DART 공시 데이터를 검색·요약해주는 금융 정보 챗봇입니다.\n\n"
            "질문하신 내용은 OPIK의 기능 범위를 벗어납니다. 다음과 같은 작업을 도와드릴 수 있습니다:\n"
            "• 특정 종목/섹터의 애널리스트 리포트 검색 및 요약\n"
            "• DART 공시 이벤트 조회 (실적, 자사주, 주요주주 변동 등)\n"
            "• 애널리스트 의견 비교 및 목표주가 분포 확인\n\n"
            "금융 정보 검색이 필요하시면 말씀해 주세요."
        )
    if is_invest:
        return (
            "OPIK은 투자 조언을 제공하지 않는 금융 정보 챗봇입니다.\n\n"
            "대신 다음과 같은 정보를 제공해 드릴 수 있습니다:\n"
            "• 특정 종목이나 섹터에 대한 최근 애널리스트 리포트 검색\n"
            "• 증권사별 투자의견·목표주가 비교\n"
            "• 관련 DART 공시 내용 요약\n\n"
            "원하시는 종목명이나 섹터를 말씀해 주시면 리포트를 검색해 드리겠습니다."
        )
    # Generic refusal fallback
    return (
        "OPIK은 증권사 애널리스트 리포트 및 DART 공시 데이터를 검색·요약해주는\n"
        "금융 정보 챗봇입니다. 질문하신 내용은 OPIK의 기능 범위를 벗어납니다.\n\n"
        "원하시는 종목명, 섹터, 또는 공시 유형을 말씀해 주시면 검색해 드리겠습니다."
    )


# models
class EncodeRequest(BaseModel):
    texts: List[str]
    prefix: str = "query"


class EncodeResponse(BaseModel):
    embeddings: List[List[float]]
    model: str
    dim: int


class SearchRequest(BaseModel):
    query: str
    top_k: int = 10
    date_from: Optional[str] = None
    date_to: Optional[str] = None


class SearchResult(BaseModel):
    report_id: str
    score: float
    종목코드: Optional[str] = None
    reason: Optional[str] = None
    keywords: Optional[List[str]] = None
    risks: Optional[List[str]] = None
    year: Optional[int] = None
    month: Optional[int] = None


class SearchResponse(BaseModel):
    query: str
    results: List[SearchResult]
    elapsed_ms: float


class ChatRequest(BaseModel):
    message: str
    session_id: str = "default"
    top_k: int = 10
    history: Optional[List[dict]] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    page: int = 1
    page_size: int = 20


class ChatResponse(BaseModel):
    answer: str
    sources: List[SearchResult]
    elapsed_ms: float
    intent: Optional[dict] = None
    dart_results: Optional[List[dict]] = None
    # Pagination metadata
    total: int = 0
    page: int = 1
    page_size: int = 20
    has_next: bool = False
    # v2: conversation metadata
    context_full: bool = False
    turn_count: int = 0


# lifecycle
@asynccontextmanager
async def lifespan(app: FastAPI):
    global embedder, faiss_index, report_ids, report_texts
    logger.info("Loading embedding model: %s", EMBEDDING_MODEL_NAME)
    embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)
    logger.info("Model loaded. Dim: %d", embedder.get_sentence_embedding_dimension())

    if os.path.exists(FAISS_INDEX_PATH) and os.path.exists(FAISS_IDMAP_PATH):
        logger.info("Loading FAISS index from disk...")
        faiss_index = faiss.read_index(FAISS_INDEX_PATH)
        with open(FAISS_IDMAP_PATH) as f:
            report_ids = json.load(f)
        info_path = "/data/opik/report_info.json"
        if os.path.exists(info_path):
            with open(info_path) as f:
                report_texts.update(json.load(f))
        logger.info("FAISS index loaded: %d vectors, %d report infos", faiss_index.ntotal, len(report_texts))
    else:
        logger.info("No local index found. Will build from S3...")
        try:
            build_index_from_s3()
        except Exception as e:
            logger.warning("Could not build index from S3 on startup: %s", e)
            logger.warning("Index will be empty. Call POST /index/rebuild to retry.")

    # Phase 2a: Wire agents to server globals (non-blocking — agents work without it)
    if AGENT_ENABLED_STR != "false":
        try:
            init_agents(faiss_index, report_ids, report_texts, embedder)
            logger.info("Agent framework wired successfully")
        except Exception as e:
            logger.warning("Agent init failed (non-fatal): %s", e)

    yield
    logger.info("Shutting down.")


app = FastAPI(title="OPIK Server", lifespan=lifespan)

# CORS for browser frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# serve static frontend
CHAT_HTML_PATH = os.environ.get("CHAT_HTML_PATH", "/root/opik-server/chat.html")


@app.get("/")
async def root():
    if os.path.exists(CHAT_HTML_PATH):
        return FileResponse(CHAT_HTML_PATH)
    return HTMLResponse("<h1>OPIK Server</h1><p>Frontend not found.</p>")


# health
@app.get("/health")
async def health():
    agent_status = {}
    try:
        agent_status = get_agent_status()
    except Exception:
        pass
    return {
        "status": "ok",
        "model": EMBEDDING_MODEL_NAME,
        "index_size": faiss_index.ntotal if faiss_index else 0,
        "dim": EMBEDDING_DIM,
        "agent_framework": agent_status.get("agent_framework", "not_loaded"),
    }


# embedding
def _prefix_texts(texts: List[str], prefix: str) -> List[str]:
    pfx = prefix.strip().lower()
    if pfx == "query":
        return ["query: " + t for t in texts]
    else:
        return ["passage: " + t for t in texts]


@app.post("/encode", response_model=EncodeResponse)
async def encode(req: EncodeRequest):
    if embedder is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    t0 = time.time()
    prefixed = _prefix_texts(req.texts, req.prefix)
    vectors = embedder.encode(
        prefixed, normalize_embeddings=True, show_progress_bar=False
    )
    elapsed = (time.time() - t0) * 1000
    logger.info("Encoded %d texts in %.0fms", len(req.texts), elapsed)
    return EncodeResponse(
        embeddings=vectors.tolist(),
        model=EMBEDDING_MODEL_NAME,
        dim=int(vectors.shape[1]),
    )


# FAISS search
@app.post("/search", response_model=SearchResponse)
async def search(req: SearchRequest):
    global faiss_index, report_ids
    if faiss_index is None or faiss_index.ntotal == 0:
        raise HTTPException(status_code=503, detail="Index not ready")
    if embedder is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    t0 = time.time()
    query_vec = embedder.encode(
        ["query: " + req.query], normalize_embeddings=True
    )
    query_np = np.array(query_vec, dtype=np.float32)

    with index_lock:
        distances, indices = faiss_index.search(query_np, min(req.top_k, faiss_index.ntotal))

    results = []
    for i in range(len(indices[0])):
        idx = indices[0][i]
        if idx == -1 or idx >= len(report_ids):
            continue
        rid = report_ids[idx]
        score = float(distances[0][i])
        results.append(
            SearchResult(
                report_id=rid,
                score=round(score, 4),
                종목코드=report_texts.get(rid, {}).get("종목코드"),
                reason=report_texts.get(rid, {}).get("reason"),
                keywords=report_texts.get(rid, {}).get("keywords"),
                risks=report_texts.get(rid, {}).get("risks"),
                year=report_texts.get(rid, {}).get("year"),
                month=report_texts.get(rid, {}).get("month"),
            )
        )

    # date filter
    if req.date_from or req.date_to:
        results = _filter_by_date(results, req.date_from, req.date_to)

    elapsed = (time.time() - t0) * 1000
    return SearchResponse(query=req.query, results=results, elapsed_ms=round(elapsed, 1))


# system prompts (v2: file-loaded with inline fallback)
PROMPT_DIR = os.environ.get("PROMPT_DIR", "/root/opik-server/prompts")

def _load_prompt_file(name: str) -> Optional[str]:
    path = os.path.join(PROMPT_DIR, name)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    return None

def _inject_date(prompt: str) -> str:
    from datetime import datetime
    now = datetime.now()
    weekday_map = ["월", "화", "수", "목", "금", "토", "일"]
    return prompt.replace("{CURRENT_DATE}", now.strftime("%Y-%m-%d")) \
                 .replace("{CURRENT_YEAR}", str(now.year)) \
                 .replace("{CURRENT_DAY_OF_WEEK}", weekday_map[now.weekday()]) \
                 .replace("{CURRENT_TIME}", now.strftime("%H:%M"))

SYSTEM_PROMPT_BASE = _inject_date(_load_prompt_file("system.md") or """You are OPIK, a Korean stock market AI analyst.
Answer investment questions based on provided data (analyst reports and/or DART filings).

Rules:
- Answer only based on the provided content. Do not speculate beyond the data.
- Cite sources (report_id or disclosure number) in your answer.
- Write answers in Korean, deliver the key point first then explain details.
- Present investment opinions (Buy/Sell/Hold) as stated in the reports, do not give direct trading advice.
- For financial data (revenue, profit, etc.), provide exact figures from the DART data.
- For insider transactions, summarize the key trades clearly.

CRITICAL — Anti-hallucination rules:
- NEVER add company names, stock codes, or securities to the user's request.
- NEVER say "X와 Y 관련 리포트를 제공드리겠습니다" unless the user specifically asked about X and Y.
- When the user asks for "all reports" or "전체 리포트" from a date range, do NOT imply the results are a curated selection.
- If the question is vague, answer based on the data provided without fabricating user intent.

PAGINATION rules:
- The data provided to you is already paginated — it shows one page of results.
- Each data section ends with a footer like "[페이지 1/5, 총 125건]".
- When there are more pages (page < total), end your response with: "다음 페이지를 보시려면 '다음 페이지' 또는 'N페이지'라고 말씀해 주세요."
- NEVER fabricate page numbers or total counts. Only use the exact page/total from the data footer.
- If the data footer shows only 1 page, do NOT suggest pagination.""")

INTENT_PROMPTS = {
    "report_search": "\nData source: 증권사 분석 리포트. Focus on investment opinions, target prices, and analyst reasoning.",
    "dart_financial": "\nData source: DART 재무제표. Focus on financial figures (revenue, operating profit, net income, assets, liabilities).",
    "dart_insider": "\nData source: DART 임원/주요주주 거래내역. Focus on who traded, how much, and when.",
    "dart_disclosure": "\nData source: DART 공시이벤트. Focus on disclosure type, key content, and potential market impact.",
    "dart_shareholder": "\nData source: DART 주요주주 현황. Focus on shareholder structure changes and ownership percentages.",
    "hybrid": "\nData source: 증권사 분석 리포트 + DART 공시/재무 데이터. Provide comprehensive analysis combining both sources.",
    "general": "\nNo data source available. You have NO report or DART data to ground your answer.\nCRITICAL: If the user asks about investments, stock recommendations, or market analysis, do NOT fabricate company names, stock codes, or recommendations. Instead politely explain what OPIK can help with (e.g., specific company reports, financial data, disclosures) and ask them to specify a company name or date.",
}


def _parse_date(d):
    """Parse date string like '2025-12-01' or '2025-12' or '2025' into (year, month, day)."""
    if not d:
        return None, None, None
    parts = d.strip().split("-")
    y = int(parts[0]) if len(parts) > 0 else None
    m = int(parts[1]) if len(parts) > 1 else None
    day = int(parts[2]) if len(parts) > 2 else None
    return y, m, day


def _filter_by_date(results, date_from, date_to):
    """Filter search results by date range. Each result's year/month must be within bounds."""
    if not date_from and not date_to:
        return results
    fy, fm, fd = _parse_date(date_from)
    ty, tm, td = _parse_date(date_to)
    filtered = []
    for r in results:
        ry = r.year
        rm = r.month
        if ry is None or rm is None:
            filtered.append(r)
            continue
        if fy is not None:
            if ry < fy or (ry == fy and fm is not None and rm < fm):
                continue
        if ty is not None:
            if ry > ty or (ry == ty and tm is not None and rm > tm):
                continue
        filtered.append(r)
    return filtered


def _safe_join(val):
    """Safely join list items, handle non-iterable values."""
    if val is None:
        return ""
    if isinstance(val, list):
        return ", ".join([str(x) for x in val])
    if isinstance(val, str):
        return val
    return str(val)


def _build_context(results: List[SearchResult], max_items: int = None) -> str:
    limit = max_items if max_items is not None else SEARCH_TOP_K
    parts = []
    for r in results[:limit]:
        info = report_texts.get(r.report_id, {})
        kw = _safe_join(info.get("keywords", r.keywords))
        rs = _safe_join(info.get("risks", r.risks))
        reason = info.get("reason", r.reason)
        if reason is not None and not isinstance(reason, str):
            reason = str(reason)
        parts.append(
            "[report_id: {}]\n".format(r.report_id) +
            "종목코드: {}\n".format(r.종목코드 or "") +
            "투자이유: {}\n".format(reason or "") +
            "키워드: {}\n".format(kw) +
            "리스크: {}\n".format(rs)
        )
    return "\n---\n".join(parts)



def _scan_reports_by_date(date_from: str, date_to: str, limit: int = 50,
                          offset: int = 0) -> tuple:
    """Scan gold/structured by 발행일 — day-level filtering for 'all reports' queries.

    Reads gold/structured/year=YYYY/month=MM/data.parquet, filters by 발행일 >= date_from
    and 발행일 <= date_to, enriches with report_texts metadata.

    Returns (results: List[SearchResult], total_count: int) for pagination.
    offset/limit control which slice of results to return.
    """
    s3 = boto3.client("s3", region_name=AWS_REGION)
    all_results = []

    fy = int(date_from[:4]) if date_from else 2020
    fm = int(date_from[5:7]) if date_from and len(date_from) >= 7 else 1
    ty = int(date_to[:4]) if date_to else 2026
    tm = int(date_to[5:7]) if date_to and len(date_to) >= 7 else 12

    logger.info("Scanning structured by 발행일: %s ~ %s (months %04d-%02d ~ %04d-%02d)",
                 date_from, date_to, fy, fm, ty, tm)

    for y in range(fy, ty + 1):
        m_start = fm if y == fy else 1
        m_end = tm if y == ty else 12
        for m in range(m_start, m_end + 1):
            key = f"gold/structured/year={y:04d}/month={m:02d}/data.parquet"
            try:
                resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
                buf = io.BytesIO(resp["Body"].read())
                df = pq.read_table(buf).to_pandas()
            except Exception as e:
                logger.warning("Skipping %s: %s", key, e)
                continue

            # Day-level filter on 발행일
            if "발행일" in df.columns:
                mask = pd.Series(True, index=df.index)
                if date_from:
                    mask = mask & (df["발행일"].astype(str) >= date_from)
                if date_to:
                    mask = mask & (df["발행일"].astype(str) <= date_to)
                df = df[mask].sort_values("발행일", ascending=False)

            for _, row in df.iterrows():
                rid = str(row.get("report_id", ""))
                info = report_texts.get(rid, {})
                title = str(row.get("title", "")) if row.get("title") is not None else ""
                증권사 = str(row.get("증권사", "")) if row.get("증권사") is not None else ""
                reason = f"[{증권사}] {title}" if 증권사 and title else (title or info.get("reason", ""))
                all_results.append(
                    SearchResult(
                        report_id=rid,
                        score=1.0,
                        종목코드=str(row.get("종목코드", "")) if row.get("종목코드") is not None else None,
                        reason=str(reason)[:300] if reason else None,
                        keywords=info.get("keywords"),
                        risks=info.get("risks"),
                        year=int(y),
                        month=int(m),
                    )
                )

    total = len(all_results)
    # Apply pagination slice
    results = all_results[offset:offset + limit]

    logger.info("Date scan complete: %d total, returning %d (offset=%d) for %s~%s",
                 total, len(results), offset, date_from, date_to)
    return results, total


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    global faiss_index, report_ids
    if embedder is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    t0 = time.time()
    intent_info = {}
    dart_results = None

    # v2: Session reset detection
    session_id = req.session_id or "default"
    _reset_triggers = ["새로 시작", "처음부터", "리셋", "세션 초기화"]
    if any(trigger in req.message for trigger in _reset_triggers) and len(req.message) < 15:
        conversation_store.reset_session(session_id)
        return ChatResponse(
            answer="새로운 대화를 시작합니다. 무엇을 도와드릴까요?",
            sources=[],
            elapsed_ms=round((time.time() - t0) * 1000, 1),
            context_full=False,
            turn_count=0,
        )

    # v2: Get conversation history for context injection
    conv_history = conversation_store.get_context_for_prompt(session_id)

    # v4.2: Normalize abbreviated years before intent parsing
    # "26년 1월 5일" → "2026년 1월 5일" (Haiku only understands 4-digit years)
    _original_message = req.message
    _normalized = re.sub(
        r'(?<!\d)(\d{2})년',
        lambda m: f'20{m.group(1)}년' if int(m.group(1)) >= 20 else m.group(0),
        req.message
    )
    if _normalized != req.message:
        req.message = _normalized
        logger.info("Normalized abbreviated year: '%s' → '%s'", _original_message, req.message)

    # Stage 1: Intent Parsing (Haiku, ~1s)
    try:
        parser = get_parser()
        intent = parser.parse(req.message)
        intent_info = intent.to_dict()
        logger.info("Intent: %s | search_query=%s | sql_hint=%s",
                     intent.intent, intent.search_query, intent.sql_hint)
    except Exception as e:
        logger.warning("Intent parsing failed: %s", e)
        intent = IntentResult({
            "intent": "general", "date_from": None, "date_to": None,
            "is_recent": False, "companies": [], "stock_codes": [],
            "securities": [], "search_query": req.message, "sql_hint": None,
        })
        intent_info = intent.to_dict()

    # v2: REFUSAL GATE — intent parser said refuse → return immediately, no search
    if intent.is_refusal:
        logger.warning("Refusal intent detected: %s — blocking search", req.message[:80])
        refusal_msg = _build_refusal_message(req.message)
        return ChatResponse(
            answer=refusal_msg,
            sources=[],
            elapsed_ms=round((time.time() - t0) * 1000, 1),
            intent=intent_info,
            context_full=conversation_store.is_context_full(session_id),
            turn_count=conversation_store.get_turn_count(session_id),
        )

    # v2: SAFETY NET — catch out-of-scope questions missed by intent parser
    # Category 1: Investment advice keywords (Zone C)
    _invest_keywords = ["사는", "살까", "매수", "매도", "추천", "종목", "주식", "투자",
                        "사는게", "살만한", "어떤게", "뭐가", "뭐사", "뭘사",
                        "손절", "매매 타이밍", "비중", "포트폴리오"]
    # Category 2: Out-of-scope non-finance questions (Zone D)
    _out_of_scope_keywords = [
        "알고리즘", "코딩", "정렬", "퀵소트", "버블소트", "BFS", "DFS", "프로그래밍",
        "Python", "자바", "자바스크립트", "React", "HTML", "CSS", "API",
        "코드", "구현", "컴파일", "디버그", "깃허브", "깃",
        "방정식", "미적분", "화학", "물리", "수학 문제",
        "레시피", "요리", "번역", "영화 추천",
    ]
    _all_block_keywords = _invest_keywords + _out_of_scope_keywords

    if intent.intent == "general" and any(kw in req.message for kw in _all_block_keywords):
        logger.warning("Out-of-scope question misrouted to general, refusing: %s", req.message[:80])
        refusal_msg = _build_refusal_message(req.message)
        return ChatResponse(
            answer=refusal_msg,
            sources=[],
            elapsed_ms=round((time.time() - t0) * 1000, 1),
            intent=intent_info,
            context_full=conversation_store.is_context_full(session_id),
            turn_count=conversation_store.get_turn_count(session_id),
        )

    # v2: Date context reconstruction for follow-up questions
    # When a follow-up question references a day ("14일은?", "15일은?")
    # without explicit month/year, pull the recent month from conversation context.
    #
    # v4.2: Broad conversation date context. The intent parser (Haiku) sometimes
    # hallucinates dates for vague follow-ups ("그날", "라고 하면"). The conversation
    # store is more reliable — if the user's message has no explicit date text
    # but the conversation has a recent date, prefer the conversation date.
    date_from = req.date_from or intent.date_from
    date_to = req.date_to or intent.date_to

    # Check if the user message contains explicit date text
    _has_explicit_date = bool(
        re.search(r'\d{4}년|\d{1,2}월\s*\d{1,2}일|\d{4}-\d{2}-\d{2}|어제|오늘|그제', req.message)
    )

    _day_only = re.search(r'(?<!\d)(\d{1,2})일', req.message)
    if intent.refers_to_previous and _day_only and not _has_explicit_date:
        recent_month = conversation_store.get_recent_month(session_id)
        if recent_month:
            day = _day_only.group(1).zfill(2)
            reconstructed = f"{recent_month}-{day}"
            date_from = reconstructed
            date_to = reconstructed
            logger.info("Reconstructed date from conversation context: %s (month=%s, day=%s)",
                         reconstructed, recent_month, day)

    # v4.2: Override intent parser dates when the user message has no explicit date.
    # The intent parser (Haiku) may hallucinate dates for words like "그날".
    # Prefer conversation context which is based on actual message history.
    if not _has_explicit_date and intent.needs_dart:
        recent_full_date = conversation_store.get_recent_full_date(session_id)
        if recent_full_date:
            intent_date = intent.date_from or intent.date_to
            if intent_date:
                logger.info("v4.2 overriding intent parser date %s with conversation date %s",
                             intent_date, recent_full_date)
            date_from = recent_full_date
            date_to = recent_full_date
            logger.info("v4.2 broad date fallback for follow-up: %s", recent_full_date)

    # Stage 2: Data retrieval
    sources = []
    dart_context = ""

    # --- Pagination: reject multi-day ranges ---
    if date_from and date_to and date_from != date_to:
        return ChatResponse(
            answer=(
                "여러 날짜 범위로 한 번에 조회하실 수 없습니다.\n"
                "하루 단위로 조회해 주세요.\n\n"
                "예시:\n"
                f"• '{date_from} 리포트 보여줘'\n"
                f"• '{date_to} 리포트 보여줘'\n\n"
                "특정 날짜 하루만 지정해서 다시 질문해 주세요."
            ),
            sources=[],
            elapsed_ms=round((time.time() - t0) * 1000, 1),
            intent=intent_info,
        )

    page = max(1, req.page)
    page_size = min(max(1, req.page_size), 100)  # cap at 100
    offset = (page - 1) * page_size
    total_count = 0

    if intent.needs_reports and faiss_index is not None and faiss_index.ntotal > 0:
        # Detect "all reports from date range" pattern:
        # no company filter + date range set + no search query = bypass FAISS, scan by date
        is_browse_query = (
            not intent.companies and not intent.stock_codes
            and (date_from or date_to)
            and (not intent.search_query or date_from == date_to)
        )

        if is_browse_query:
            sources, total_count = _scan_reports_by_date(
                date_from, date_to, limit=page_size, offset=offset
            )
            logger.info("Browse query: page=%d/%d, scanned %d reports by date %s~%s",
                         page, max(1, (total_count + page_size - 1) // page_size),
                         len(sources), date_from, date_to)
        else:
            search_query = intent.search_query or req.message

            # v2: Conversation context augmentation — when question references
            # previous results ("이 중에서", "그 중에서"), enrich the search query
            # with entities and context from recent conversation turns.
            if intent.refers_to_previous or (
                conv_history and len(conv_history) > 100 and
                any(kw in req.message for kw in
                    ["이 중", "그 중", "여기서", "이 중에서", "그 중에서",
                     "가장", "제일", "아까", "이전", "방금", "앞서"])
            ):
                ctx = conversation_store.get_recent_summary(session_id)
                if ctx:
                    augment = f"이전 대화 맥락: {ctx[:500]}"
                    search_query = f"{augment} | {search_query}"
                    logger.info("Augmented search query with conversation context (%d chars)",
                                len(augment))

            query_vec = embedder.encode(
                ["query: " + search_query], normalize_embeddings=True
            )
            query_np = np.array(query_vec, dtype=np.float32)
            search_k = min(req.top_k * 2, faiss_index.ntotal)

            with index_lock:
                distances, indices = faiss_index.search(query_np, search_k)

            report_sources = []
            for i in range(len(indices[0])):
                idx = int(indices[0][i])
                if idx == -1 or idx >= len(report_ids):
                    continue
                rid = report_ids[idx]
                report_sources.append(
                    SearchResult(
                        report_id=rid,
                        score=round(float(distances[0][i]), 4),
                        종목코드=report_texts.get(rid, {}).get("종목코드"),
                        reason=report_texts.get(rid, {}).get("reason"),
                        keywords=report_texts.get(rid, {}).get("keywords"),
                        risks=report_texts.get(rid, {}).get("risks"),
                        year=report_texts.get(rid, {}).get("year"),
                        month=report_texts.get(rid, {}).get("month"),
                    )
                )

            if date_from or date_to:
                report_sources = _filter_by_date(report_sources, date_from, date_to)

            sources = report_sources

    if intent.needs_dart:
        dart_context = _query_dart(intent, page=page, page_size=page_size,
                                   date_from=date_from, date_to=date_to)
        # Treat "no data" messages as empty — never pass to LLM
        if dart_context and "데이터가 없습니다" not in dart_context:
            dart_results = [{"source": intent.intent, "summary": dart_context[:500]}]
        else:
            dart_context = ""

    # Stage 3: Build context and answer
    # Determine if this was a browse query (all reports in date range)
    is_browse = (
        not intent.companies and not intent.stock_codes
        and (date_from or date_to)
        and not intent.search_query
    )

    if intent.is_general:
        # v2: Zone D guard — general intent with no data MUST NOT fabricate answers
        # Only respond with help message, never answer from empty context
        help_msg = (
            "OPIK은 증권사 애널리스트 리포트 및 DART 공시 데이터를 검색·요약해주는 금융 정보 챗봇입니다.\n\n"
            "다음과 같은 질문을 해보세요:\n"
            "• \"삼성전자 목표주가 알려줘\"\n"
            "• \"최근 반도체 리포트 있어?\"\n"
            "• \"한국투자증권 SK하이닉스 리포트 보여줘\"\n"
            "• \"오늘 올라온 리포트 요약해줘\"\n"
            "• \"삼성전자 공시 뭐 있어?\"\n\n"
            "구체적인 종목명, 섹터, 또는 증권사 이름을 포함해서 물어보시면\n"
            "더 정확한 검색 결과를 제공해 드릴 수 있습니다."
        )
        return ChatResponse(
            answer=help_msg,
            sources=[],
            elapsed_ms=round((time.time() - t0) * 1000, 1),
            intent=intent_info,
            context_full=conversation_store.is_context_full(session_id),
            turn_count=conversation_store.get_turn_count(session_id),
        )
    elif intent.needs_reports and not intent.needs_dart:
        if not sources:
            date_str = date_from or date_to or "지정된"
            return ChatResponse(
                answer=f"해당 조건({date_str})으로 검색된 리포트가 없습니다.",
                sources=[],
                elapsed_ms=round((time.time() - t0) * 1000, 1),
                intent=intent_info,
                context_full=conversation_store.is_context_full(session_id),
                turn_count=conversation_store.get_turn_count(session_id),
            )
        max_items = len(sources) if is_browse else SEARCH_TOP_K
        context = _build_context(sources, max_items=max_items)
        if is_browse and total_count > 0:
            tpages = max(1, (total_count + page_size - 1) // page_size)
            context += f"\n\n[페이지 {page}/{tpages}, 총 {total_count}건]"
        system_prompt = SYSTEM_PROMPT_BASE + INTENT_PROMPTS["report_search"]
    elif intent.needs_dart and not intent.needs_reports:
        if not dart_context:
            # v4: Empty-data gate — NEVER pass empty data to LLM.
            # LLM hallucinates when given no results but asked to summarize.
            # Return canned response directly, bypassing Bedrock entirely.
            # v4.2: Also check conversation context for the date to show in message.
            _display_date = (date_from or date_to or intent.date_from or intent.date_to
                             or conversation_store.get_recent_full_date(session_id)
                             or "지정된")
            return ChatResponse(
                answer=f"해당 날짜({_display_date})의 DART 공시 데이터가 없습니다.",
                sources=[],
                elapsed_ms=round((time.time() - t0) * 1000, 1),
                intent=intent_info,
                dart_results=[{"source": intent.intent, "summary": "해당 기간의 공시 데이터가 없습니다."}],
                context_full=conversation_store.is_context_full(session_id),
                turn_count=conversation_store.get_turn_count(session_id),
            )
        context = dart_context
        system_prompt = SYSTEM_PROMPT_BASE + INTENT_PROMPTS.get(intent.intent, "")
    else:
        max_items = len(sources) if is_browse else SEARCH_TOP_K
        context = _build_context(sources, max_items=max_items) if sources else ""
        if dart_context:
            context += "\n\n[DART 데이터]\n" + dart_context
        if is_browse and total_count > 0:
            tpages = max(1, (total_count + page_size - 1) // page_size)
            context += f"\n\n[페이지 {page}/{tpages}, 총 {total_count}건]"
        elif not sources and not dart_context:
            # v4: Both report and DART sources are empty.
            # Never pass empty context to LLM — it hallucinates.
            date_str = date_from or date_to or "지정된"
            return ChatResponse(
                answer=f"해당 조건({date_str})으로 검색된 데이터가 없습니다.",
                sources=[],
                elapsed_ms=round((time.time() - t0) * 1000, 1),
                intent=intent_info,
                context_full=conversation_store.is_context_full(session_id),
                turn_count=conversation_store.get_turn_count(session_id),
            )
        system_prompt = SYSTEM_PROMPT_BASE + INTENT_PROMPTS["hybrid"]

    # Stage 4: Bedrock generation
    # v2: Inject conversation history and date into system prompt
    final_system_prompt = system_prompt

    # v2: When question references previous context, add it explicitly so
    # the LLM knows what entities/companies "이 중에서" refers to.
    if intent.refers_to_previous:
        recent_ctx = conversation_store.get_recent_summary(session_id)
        if recent_ctx:
            prev_note = (
                "\n\n[중요: 이 질문은 이전 대화를 참조하고 있습니다. "
                "이전 대화 맥락을 반영하여 답변하세요.]\n"
                f"<previous_conversation_context>\n{recent_ctx[:600]}\n"
                "</previous_conversation_context>"
            )
            context = prev_note + "\n\n" + context

    if conv_history:
        final_system_prompt = final_system_prompt.replace(
            "{CONVERSATION_HISTORY}", conv_history
        )
    final_system_prompt = final_system_prompt.replace(
        "{CONVERSATION_HISTORY}", "(첫 대화입니다)"
    )
    final_system_prompt = _inject_date(final_system_prompt)

    bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)
    user_content = "질문: {}\n\n{}".format(req.message, "참고 데이터:\n" + context if context else "")
    messages = [{"role": "user", "content": user_content}]
    if req.history:
        messages = req.history + messages

    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 2048,
        "system": final_system_prompt,
        "messages": messages,
        "temperature": 0.3,
    })

    resp = bedrock.invoke_model(
        modelId=BEDROCK_MODEL,
        contentType="application/json",
        accept="application/json",
        body=body,
    )
    resp_body = json.loads(resp["body"].read())

    answer = ""
    for block in resp_body.get("content", []):
        if block.get("type") == "text":
            answer += block["text"]

    # v2: Save conversation turns and check context window
    conversation_store.add_turn(session_id, "user", req.message)
    conversation_store.add_turn(session_id, "assistant", answer)
    ctx_full = conversation_store.is_context_full(session_id)
    turn_count = len(conversation_store.get_or_create(session_id).turns)

    if ctx_full:
        answer += (
            "\n\n---\n"
            "[대화가 길어져 이전 맥락 일부가 요약되었습니다.]\n"
            "[위 내용은 최근 대화를 바탕으로 한 응답입니다.]\n"
            '[대화를 새로 시작하려면 "새로 시작"이라고 입력해주세요.]'
        )

    elapsed = (time.time() - t0) * 1000

    # Compute pagination metadata
    total_pages = max(1, (total_count + page_size - 1) // page_size)
    has_next = page < total_pages

    logger.info("Chat complete in %dms (turn %d, ctx_full=%s)", elapsed, turn_count, ctx_full)

    return ChatResponse(
        answer=answer,
        sources=sources[:SEARCH_TOP_K],
        elapsed_ms=round(elapsed, 1),
        intent=intent_info,
        dart_results=dart_results,
        total=total_count,
        page=page,
        page_size=page_size,
        has_next=has_next,
        context_full=ctx_full,
        turn_count=turn_count,
    )


def _query_dart(intent: IntentResult, page: int = 1, page_size: int = 20,
                 date_from: Optional[str] = None, date_to: Optional[str] = None) -> str:
    """Query DART Gold Parquet tables based on parsed intent.

    date_from / date_to override intent.date_from / intent.date_to.
    This is critical for follow-up date reconstruction: the intent parser
    correctly sets date_from=null for day-only questions like "14일은?",
    but the server reconstructs the date from conversation context.
    Without overrides, the dart query would run with no date filter and
    return thousands of irrelevant rows, causing LLM hallucination."""
    return query_dart_engine(
        intent=intent.intent,
        companies=intent.companies,
        codes=intent.stock_codes,
        date_from=date_from if date_from is not None else intent.date_from,
        date_to=date_to if date_to is not None else intent.date_to,
        is_recent=intent.is_recent,
        page=page,
        page_size=page_size,
    )


# ── v2 Agent-powered chat (Phase 2a) ──

@app.post("/v2/chat", response_model=ChatResponse)
async def v2_chat(req: ChatRequest):
    """Chat endpoint powered by the multi-agent framework (Safety → Intent → Agents → Compose).

    Falls back gracefully if agents are not initialised.
    Use OPIK_AGENT_ENABLED=false to disable.
    """
    if embedder is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    try:
        result = await v2_chat_handler(req)
        return ChatResponse(**result)
    except Exception as e:
        logger.exception("/v2/chat error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# index management
@app.post("/index/rebuild")
async def rebuild_index():
    try:
        n = build_index_from_s3()
        return {"status": "ok", "index_size": n, "dim": EMBEDDING_DIM}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/index/status")
async def index_status():
    return {
        "ready": faiss_index is not None and faiss_index.ntotal > 0,
        "size": faiss_index.ntotal if faiss_index else 0,
        "dim": EMBEDDING_DIM,
    }


def build_index_from_s3():
    global faiss_index, report_ids, report_texts

    logger.info("Building FAISS index from S3: s3://%s/gold/embeddings/", S3_BUCKET)
    s3 = boto3.client("s3", region_name=AWS_REGION)

    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="gold/embeddings/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])

    logger.info("Found %d embedding parquet files", len(keys))

    all_embeddings = []
    all_ids = []

    for key in keys:
        try:
            ym = re.search(r"year=(\d{4})/month=(\d{2})", key)
            year = int(ym.group(1)) if ym else None
            month = int(ym.group(2)) if ym else None

            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            buf = io.BytesIO(obj["Body"].read())
            table = pq.read_table(buf)
            df = table.to_pandas()

            for _, row in df.iterrows():
                emb = row["embedding"]
                if emb is None or len(emb) == 0:
                    continue
                all_embeddings.append(np.array(emb, dtype=np.float32))
                rid = row["report_id"]
                all_ids.append(rid)
                report_texts[rid] = {
                    "종목코드": row.get("종목코드"),
                    "reason": row.get("reason"),
                    "keywords": row.get("keywords"),
                    "risks": row.get("risks"),
                    "year": year,
                    "month": month,
                }
        except Exception as e:
            logger.warning("Error reading %s: %s", key, e)

    n = len(all_embeddings)
    if n == 0:
        raise RuntimeError("No embeddings found in S3")

    vectors = np.array(all_embeddings, dtype=np.float32).copy()
    dim = vectors.shape[1]
    faiss.normalize_L2(vectors)

    index = faiss.IndexIDMap(faiss.IndexFlatIP(dim))
    index.add_with_ids(vectors, np.arange(n, dtype=np.int64))

    with index_lock:
        faiss_index = index
        report_ids = all_ids

    faiss.write_index(faiss_index, FAISS_INDEX_PATH)
    with open(FAISS_IDMAP_PATH, "w") as f:
        json.dump(all_ids, f, ensure_ascii=False)

    info_path = "/data/opik/report_info.json"
    with open(info_path, "w") as f:
        json.dump(report_texts, f, ensure_ascii=False)

    logger.info("FAISS index rebuilt: %d vectors, dim=%d", n, dim)
    return n
