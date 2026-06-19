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

# config
S3_BUCKET = os.environ.get("S3_BUCKET", "s3-opik-bucket")
EMBEDDING_MODEL_NAME = os.environ.get("EMBEDDING_MODEL", "intfloat/multilingual-e5-small")
EMBEDDING_DIM = 384
FAISS_INDEX_PATH = os.environ.get("FAISS_INDEX_PATH", "/data/opik/faiss_index.bin")
FAISS_IDMAP_PATH = os.environ.get("FAISS_IDMAP_PATH", "/data/opik/report_ids.json")
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
BEDROCK_MODEL = os.environ.get("BEDROCK_MODEL", "apac.anthropic.claude-3-haiku-20240307-v1:0")
SEARCH_TOP_K = int(os.environ.get("SEARCH_TOP_K", "10"))

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
    elif is_invest:
        return (
            "OPIK은 투자 조언을 제공하지 않습니다.\n\n"
            "저는 증권사 애널리스트 리포트와 DART 공시 데이터를 검색·요약해드리는 정보 챗봇입니다.\n"
            "대신 다음과 같은 도움을 드릴 수 있습니다:\n"
            "• 특정 종목의 애널리스트 의견과 목표주가 확인\n"
            "• 섹터별 최근 리포트 동향 파악\n"
            "• DART 공시 내역 조회\n\n"
            "구체적인 종목명이나 섹터를 알려주시면 관련 정보를 찾아드리겠습니다."
        )
    return ""


# ──────────────────────────────────────────────
# FAISS + embedder
# ──────────────────────────────────────────────
def _load_embedder():
    global embedder
    if embedder is None:
        embedder = SentenceTransformer(EMBEDDING_MODEL_NAME, device="cpu")
        logger.info("Embedder loaded: %s (dim=%d)", EMBEDDING_MODEL_NAME, EMBEDDING_DIM)


def _load_faiss():
    global faiss_index, report_ids, report_texts
    if faiss_index is not None:
        return
    if not os.path.exists(FAISS_INDEX_PATH):
        logger.warning("FAISS index not found at %s", FAISS_INDEX_PATH)
        return
    faiss_index = faiss.read_index(FAISS_INDEX_PATH)
    if os.path.exists(FAISS_IDMAP_PATH):
        with open(FAISS_IDMAP_PATH, "r") as f:
            report_ids = json.load(f)
    logger.info("FAISS index loaded: %d vectors", faiss_index.ntotal)


def _prefix_texts(texts: list, prefix: str = ""):
    if prefix:
        return [f"{prefix}: {t}" for t in texts]
    return texts


def build_index_from_s3():
    global faiss_index, report_ids, report_texts
    s3 = boto3.client("s3", region_name=AWS_REGION)
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix="silver/reports/")
    texts = []
    rids = []
    rtexts = {}
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            try:
                resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
                buf = io.BytesIO(resp["Body"].read())
                data = json.loads(buf.read().decode("utf-8"))
            except Exception as e:
                logger.warning("Skipping %s: %s", key, e)
                continue

            rid = key.split("/")[-1].replace(".json", "")
            text = data.get("text", "") or data.get("reason", "") or ""
            if not text:
                continue
            texts.append("passage: " + text)
            rids.append(rid)
            rtexts[rid] = {
                "종목코드": data.get("stock_code"),
                "reason": data.get("reason"),
                "keywords": data.get("keywords"),
                "risks": data.get("risks"),
                "year": data.get("year"),
                "month": data.get("month"),
                "text": text,
            }

    if not texts:
        logger.warning("No report data found in S3")
        return 0

    _load_embedder()
    embs = embedder.encode(texts, normalize_embeddings=True, show_progress_bar=True)
    dim = embs.shape[1]
    idx = faiss.IndexFlatIP(dim)
    idx.add(np.array(embs, dtype=np.float32))

    with index_lock:
        faiss_index = idx
        report_ids = rids
        report_texts = rtexts

    faiss.write_index(idx, FAISS_INDEX_PATH)
    with open(FAISS_IDMAP_PATH, "w") as f:
        json.dump(rids, f)

    logger.info("FAISS index rebuilt: %d vectors", len(rids))
    return len(rids)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_embedder()
    try:
        _load_faiss()
        if faiss_index is None or faiss_index.ntotal == 0:
            logger.warning("Index will be empty. Call POST /index/rebuild to retry.")
    except Exception as e:
        logger.warning("Could not build index from S3 on startup: %s", e)
        logger.warning("Index will be empty. Call POST /index/rebuild to retry.")
    yield


app = FastAPI(title="OPIK Server", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

SYSTEM_PROMPT_PATH = Path(__file__).parent / "prompts" / "system.md"
try:
    with open(SYSTEM_PROMPT_PATH, "r") as f:
        SYSTEM_PROMPT_BASE = f.read()
except FileNotFoundError:
    SYSTEM_PROMPT_BASE = ""

# LLM prompt fragments for each intent (appended after SYSTEM_PROMPT_BASE)
INTENT_PROMPTS = {
    "report_search": "\n\n당신은 애널리스트 리포트 검색 결과를 요약하고 있습니다.",
    "dart_financial": "\n\n당신은 DART 재무제표 데이터를 요약하고 있습니다.",
    "dart_insider": "\n\n당신은 DART 내부자 거래 데이터를 요약하고 있습니다.",
    "dart_disclosure": "\n\n당신은 DART 공시 이벤트 데이터를 요약하고 있습니다.",
    "dart_shareholder": "\n\n당신은 DART 주요주주 데이터를 요약하고 있습니다.",
    "hybrid": "\n\n당신은 애널리스트 리포트와 DART 데이터를 함께 요약하고 있습니다.",
}


# ──────────────────────────────────────────────
# Pydantic models
# ──────────────────────────────────────────────
class EncodeRequest(BaseModel):
    texts: list
    prefix: str = ""


class SearchRequest(BaseModel):
    query: str
    top_k: int = 5
    date_from: Optional[str] = None
    date_to: Optional[str] = None


class ChatRequest(BaseModel):
    message: str
    session_id: str = "default"
    top_k: int = 5
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    page: int = 1
    page_size: int = 20


class SearchResult(BaseModel):
    report_id: str
    score: float
    종목코드: Optional[str] = None
    reason: Optional[str] = None
    keywords: Optional[str] = None
    risks: Optional[str] = None
    year: Optional[str] = None
    month: Optional[str] = None


class ChatResponse(BaseModel):
    answer: str
    sources: list
    elapsed_ms: float
    intent: Optional[dict] = None
    dart_results: Optional[List[dict]] = None
    total: int = 0
    page: int = 1
    page_size: int = 20
    has_next: bool = False
    context_full: bool = False
    turn_count: int = 0


# ──────────────────────────────────────────────
# Health check
# ──────────────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "model": EMBEDDING_MODEL_NAME,
        "index_size": faiss_index.ntotal if faiss_index else 0,
        "dim": EMBEDDING_DIM,
    }


@app.get("/")
async def root():
    return {"service": "OPIK Server", "version": "3.0"}


# ──────────────────────────────────────────────
# Embedding endpoint
# ──────────────────────────────────────────────
@app.post("/encode")
async def encode(req: EncodeRequest):
    if embedder is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    t0 = time.time()
    prefixed = _prefix_texts(req.texts, req.prefix)
    vecs = embedder.encode(prefixed, normalize_embeddings=True).tolist()
    elapsed = (time.time() - t0) * 1000
    logger.info("Encoded %d texts in %.0fms", len(req.texts), elapsed)
    return {"embeddings": vecs, "dim": EMBEDDING_DIM, "elapsed_ms": round(elapsed, 1)}


# ──────────────────────────────────────────────
# Search endpoint
# ──────────────────────────────────────────────
@app.post("/search")
async def search(req: SearchRequest):
    global faiss_index, report_ids
    if faiss_index is None or faiss_index.ntotal == 0:
        raise HTTPException(status_code=503, detail="Index not ready")
    if embedder is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    t0 = time.time()
    qv = embedder.encode(["query: " + req.query], normalize_embeddings=True)
    qn = np.array(qv, dtype=np.float32)
    k = min(req.top_k, faiss_index.ntotal)

    with index_lock:
        distances, indices = faiss_index.search(qn, k)

    results = []
    for i in range(len(indices[0])):
        idx = int(indices[0][i])
        if idx == -1 or idx >= len(report_ids):
            continue
        rid = report_ids[idx]
        results.append(
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

    elapsed = (time.time() - t0) * 1000
    logger.info("Search '%s' => %d results (%.0fms)", req.query, len(results), elapsed)
    return {"query": req.query, "results": results, "elapsed_ms": round(elapsed, 1)}


# ──────────────────────────────────────────────
# Helper functions for the chat pipeline
# ──────────────────────────────────────────────
def _filter_by_date(sources: list, date_from: Optional[str], date_to: Optional[str]) -> list:
    if not date_from and not date_to:
        return sources
    filtered = []
    for s in sources:
        y = str(s.year or "").strip()
        m = str(s.month or "").strip()
        if not y or not m or y == "None" or m == "None":
            continue
        date_str = f"{y}-{m.zfill(2)}"
        if date_from and date_str < date_from:
            continue
        if date_to and date_str > date_to:
            continue
        filtered.append(s)
    return filtered


def _scan_reports_by_date(date_from: Optional[str], date_to: Optional[str],
                          limit: int = 20, offset: int = 0):
    """Scan all loaded report_texts by date range (not FAISS)."""
    target_date = date_from or date_to
    if not target_date:
        return [], 0
    matches = []
    for rid, info in report_texts.items():
        y = str(info.get("year", "") or "").strip()
        m = str(info.get("month", "") or "").strip()
        if not y or not m or y == "None" or m == "None":
            continue
        date_str = f"{y}-{m.zfill(2)}"
        if date_from and date_str < date_from:
            continue
        if date_to and date_str > date_to:
            continue
        matches.append(SearchResult(
            report_id=rid,
            score=0,
            종목코드=info.get("종목코드"),
            reason=info.get("reason"),
            keywords=info.get("keywords"),
            risks=info.get("risks"),
            year=info.get("year"),
            month=info.get("month"),
        ))
    total = len(matches)
    matches = matches[offset:offset + limit]
    return matches, total


def _build_context(sources: list, max_items: int = 10) -> str:
    lines = []
    for i, s in enumerate(sources[:max_items]):
        lines.append(
            f"[{i+1}] report_id={s.report_id} score={s.score} "
            f"year={s.year} month={s.month} 코드={s.종목코드}"
        )
        if s.reason:
            lines.append(f"    reason: {s.reason}")
        if s.keywords:
            lines.append(f"    keywords: {s.keywords}")
        if s.risks:
            lines.append(f"    risks: {s.risks}")
    return "\n".join(lines)


# ──────────────────────────────────────────────
# Chat endpoint — 3-stage pipeline
# ──────────────────────────────────────────────
@app.post("/chat")
async def chat(req: ChatRequest):
    global faiss_index, report_ids
    if embedder is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    t0 = time.time()
    session_id = req.session_id

    # Get or create conversation session (v2: persistent memory)
    conversation_store.get_or_create(session_id)

    # Store user turn (v2: before processing, so context is available)
    conversation_store.add_turn(session_id, "user", req.message)

    # Stage 1: Intent parsing via Bedrock Haiku
    parser = get_parser()
    try:
        intent = parser.parse(req.message)
        logger.info("Intent: %s | search_query=%s | sql_hint=%s",
                     intent.intent, intent.search_query, intent.sql_hint)
    except Exception as e:
        logger.warning("Intent parsing failed: %s", e)
        intent = IntentResult({
            "intent": "general",
            "companies": [],
            "stock_codes": [],
        })

    # --- Refusal gates ---
    # v2: intent-based refusal (Zone C + Zone D)
    if intent.is_refusal:
        refusal_msg = _build_refusal_message(req.message)
        elapsed = (time.time() - t0) * 1000
        intent_info = intent.to_dict()
        return ChatResponse(
            answer=refusal_msg,
            sources=[],
            elapsed_ms=round(elapsed, 1),
            intent=intent_info,
        )

    # Fallback keyword-based refusal (catches what intent parser might miss)
    msg_lower = req.message.lower().strip()
    _invest_signals = [
        "사는", "살까", "매수", "매도", "추천 종목", "추천종목",
        "뭐 사", "뭘 사", "뭐사", "뭘사",
        "손절", "포트폴리오", "비중 조절", "투자 전략",
        "레버리지", "공매도", "선물 거래",
    ]
    _oos_signals = [
        "알고리즘", "코딩", "구현해", "코드", "프로그래밍",
        "python", "자바", "react", "html", "css", "api",
        "방정식", "미적분", "레시피", "번역",
    ]
    if any(kw in msg_lower for kw in _invest_signals + _oos_signals):
        refusal_msg = _build_refusal_message(req.message)
        elapsed = (time.time() - t0) * 1000
        intent_info = intent.to_dict()
        return ChatResponse(
            answer=refusal_msg,
            sources=[],
            elapsed_ms=round(elapsed, 1),
            intent=intent_info,
        )

    # v2: Conversation history for this session (for downstream context)
    conv_history = conversation_store.get_context_for_prompt(session_id)

    # v3: Date reconstruction from conversation context (follow-up questions)
    # Intent parser correctly sets date_from=null for day-only questions like "14일은?"
    # with refers_to_previous=true. The server reconstructs the full date using
    # get_recent_month() which finds the most recently mentioned YYYY-MM.
    date_from = req.date_from or intent.date_from
    date_to = req.date_to or intent.date_to

    _day_only = re.search(r'(?<!\d)(\d{1,2})일', req.message)
    if intent.refers_to_previous and _day_only and not date_from:
        recent_month = conversation_store.get_recent_month(session_id)
        if recent_month:
            day = _day_only.group(1).zfill(2)
            reconstructed = f"{recent_month}-{day}"
            date_from = reconstructed
            date_to = reconstructed
            logger.info("Reconstructed date from conversation context: %s (month=%s, day=%s)",
                         reconstructed, recent_month, day)

    # Stage 2: Data retrieval
    sources = []
    dart_context = ""
    dart_results = None

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
            intent=intent.to_dict(),
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
            intent=intent.to_dict(),
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
                intent=intent.to_dict(),
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
            date_str = date_from or date_to or intent.date_from or intent.date_to or "지정된"
            return ChatResponse(
                answer=f"해당 날짜({date_str})의 DART 공시 데이터가 없습니다.",
                sources=[],
                elapsed_ms=round((time.time() - t0) * 1000, 1),
                intent=intent.to_dict(),
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
        if not sources and not dart_context:
            # v4: Both report and DART sources are empty.
            # Never pass empty context to LLM — it hallucinates.
            date_str = date_from or date_to or "지정된"
            return ChatResponse(
                answer=f"해당 조건({date_str})으로 검색된 데이터가 없습니다.",
                sources=[],
                elapsed_ms=round((time.time() - t0) * 1000, 1),
                intent=intent.to_dict(),
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
            final_system_prompt += prev_note

    # Inject current date
    from datetime import datetime as dt
    now = dt.now()
    final_system_prompt = final_system_prompt.replace("{CURRENT_DATE}", now.strftime("%Y-%m-%d"))
    final_system_prompt = final_system_prompt.replace("{CURRENT_YEAR}", str(now.year))

    # Inject conversation history
    if conv_history:
        final_system_prompt = final_system_prompt.replace("{CONVERSATION_HISTORY}", conv_history)
    else:
        final_system_prompt = final_system_prompt.replace("{CONVERSATION_HISTORY}", "이전 대화 없음")

    # Build user message with context
    user_msg = f"사용자 질문: {req.message}\n\n검색 결과:\n{context}"

    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1024,
        "system": final_system_prompt,
        "messages": [{"role": "user", "content": user_msg}],
        "temperature": 0.0,
    })

    try:
        client = boto3.client("bedrock-runtime", region_name=AWS_REGION)
        resp = client.invoke_model(
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
        if not answer:
            answer = "죄송합니다. 응답을 생성할 수 없습니다."
    except Exception as e:
        logger.error("Bedrock invocation failed: %s", e)
        answer = f"죄송합니다. 응답 생성 중 오류가 발생했습니다."

    # v2: Store assistant turn in conversation memory
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

    intent_info = intent.to_dict()

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
        "index_size": faiss_index.ntotal if faiss_index else 0,
        "report_count": len(report_ids),
        "dim": EMBEDDING_DIM,
        "model": EMBEDDING_MODEL_NAME,
    }


# serve frontend
FRONTEND_PATH = Path(__file__).parent / "frontend" / "index.html"


@app.get("/frontend")
async def frontend():
    if FRONTEND_PATH.exists():
        return FileResponse(FRONTEND_PATH)
    return HTMLResponse("<h1>OPIK Server Frontend not found</h1>", status_code=404)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("opik_server:app", host="0.0.0.0", port=8000, reload=False)
