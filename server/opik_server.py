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
import requests
from fastapi import FastAPI, HTTPException, Request
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
from source_links import source_line, source_url_from_metadata, strip_ungrounded_dart_urls

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
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
DART_RAG_EMBEDDING_PREFIX = (
    "gold/dart/rag/embedding/model=intfloat_multilingual-e5-small/version=v1/"
)

from db import (upsert_subscriber, is_approved, approve_subscriber,
                add_briefing_recipient, remove_briefing_recipient,
                list_approved_subscribers, init_db, seed_initial_users,
                get_subscriber)

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

def _search_faiss_for_companies(companies: list, base_query: str, top_k: int = 20) -> list:
    """Search FAISS separately for each company and merge deduplicated results.
    
    When a user asks "삼성전자랑 SK하이닉스 비교해줘", a single FAISS query with
    both names produces poor semantic matches. Instead, search each company
    separately with a clean query and merge the results.
    """
    global faiss_index, report_ids, embedder
    if faiss_index is None or embedder is None:
        return []
    
    all_results = []
    seen_ids = set()
    per_company_k = max(10, top_k // max(1, len(companies)))
    
    for company in companies:
        query = f"{company} {base_query}" if base_query else company
        logger.info("Multi-ticker FAISS search: company=%s query=%s k=%d", company, query[:80], per_company_k)
        try:
            query_vec = embedder.encode(
                ["query: " + query], normalize_embeddings=True
            )
            query_np = np.array(query_vec, dtype=np.float32)
            search_k = min(per_company_k, faiss_index.ntotal)
            with index_lock:
                distances, indices = faiss_index.search(query_np, search_k)
            
            for i in range(len(indices[0])):
                idx = int(indices[0][i])
                if idx == -1 or idx >= len(report_ids):
                    continue
                rid = report_ids[idx]
                if rid in seen_ids:
                    continue
                seen_ids.add(rid)
                all_results.append(_search_result_from_metadata(
                    rid,
                    score=round(float(distances[0][i]), 4),
                ))
        except Exception as e:
            logger.warning("Multi-ticker search failed for %s: %s", company, e)
    
    # Sort by score descending
    all_results.sort(key=lambda r: r.score, reverse=True)
    logger.info("Multi-ticker search: %d companies → %d unique results", len(companies), len(all_results))
    return all_results


def _detect_analysis_request(message: str) -> Optional[str]:
    """Detect analysis-type questions from message keywords.
    Returns: "compare", "cause_tracking", "interpret", or None.
    Called BEFORE the refusal gate so analysis queries bypass Haiku's refuse classification."""
    msg = message.strip()
    _compare_kw = ["비교", "차이", "증권사별", "의견 차이", "다른 증권사", "증권사 차이",
                   "어디가 더", "어느 증권사", "어떤 증권사가"]
    if any(kw in msg for kw in _compare_kw):
        return "compare"
    _cause_kw = ["왜 올랐", "왜 떨어졌", "왜 내렸", "원인", "급등", "급락",
                 "하락 이유", "상승 이유", "왜 급", "무슨 일이"]
    if any(kw in msg for kw in _cause_kw):
        return "cause_tracking"
    _interpret_kw = ["해석", "공시 뜻", "무슨 의미", "어떤 의미",
                     "호재", "악재", "무슨 영향", "영향 분석",
                     "이 공시", "이게 무슨", "무슨 뜻"]
    if any(kw in msg for kw in _interpret_kw):
        return "interpret"
    return None


def _parse_brokerage_from_reason(reason: str) -> str:
    """Extract 증권사 name from reason field. Format: '[한국투자증권] 실적 전망...' → '한국투자증권'"""
    if not reason:
        return ""
    m = re.match(r'\[([^\]]+)\]', str(reason))
    if m:
        return m.group(1)
    return ""


def _convert_sources(sources: list) -> list:
    """Convert SearchResult objects to dicts compatible with agent pipeline.
    Enriches with parsed fields: 증권사 (from reason), 발행일 (from year/month)."""
    converted = []
    for s in sources:
        if hasattr(s, '__dict__'):
            d = {}
            for k, v in s.__dict__.items():
                if not k.startswith('_'):
                    d[k] = v
            # Enrich: parse 증권사 from reason field
            reason = d.get('reason', '')
            brokerage = _parse_brokerage_from_reason(reason)
            if brokerage:
                d['증권사'] = brokerage
            # Enrich: construct 발행일 from year/month if available
            if d.get('year') and d.get('month') and not d.get('발행일'):
                d['발행일'] = f"{d['year']}-{str(d['month']).zfill(2)}"
            converted.append(d)
        elif isinstance(s, dict):
            # Already a dict — apply same enrichment
            if 'reason' in s and '증권사' not in s:
                s['증권사'] = _parse_brokerage_from_reason(s.get('reason', ''))
            if s.get('year') and s.get('month') and '발행일' not in s:
                s['발행일'] = f"{s['year']}-{str(s['month']).zfill(2)}"
            converted.append(s)
    return converted


def _run_analysis_with_data(analysis_type: str, sources: list, 
                            intent_info: dict, req_message: str,
                            dart_results=None) -> Optional[str]:
    """Run V2 agent pipeline analysis on retrieved data.
    Falls back to None if analysis fails — caller should then use V1 path."""
    try:
        import agent_integration
        converted = _convert_sources(sources)

        # Extract ticker info from intent or message
        companies = intent_info.get("companies", [])
        ticker_name = companies[0] if companies else _extract_ticker_from_message(req_message)

        # v5: For "compare" with multiple companies, search FAISS per-company.
        # A single FAISS query with merged company names ("삼성전자 SK하이닉스 비교")
        # produces poor matches. Search each company separately.
        if analysis_type == "compare" and len(companies) >= 2:
            logger.info("Compare: multi-ticker search for %d companies: %s", len(companies), companies)
            multi_results = _search_faiss_for_companies(companies, "", top_k=20)
            if multi_results:
                converted = _convert_sources(multi_results)
                logger.info("Compare: multi-ticker search returned %d results", len(converted))

        # If no sources from Stage 2, try direct FAISS search with ticker
        if not converted and ticker_name:
            logger.info("Analysis: Stage 2 had 0 sources, trying direct FAISS with ticker=%s", ticker_name)
            converted = agent_integration._report.search(ticker_name, top_k=20)
        if not converted:
            logger.warning("Analysis: no converted sources for %s", analysis_type)
            return None

        if analysis_type == "compare":
            # compare_reports expects List[dict] — pass converted directly
            # (Stage 2 already fetched data for all companies in the query)
            result = agent_integration._analysis.compare_reports(converted, ticker_name)
            if result:
                return agent_integration._composer.compose_chat_response(
                intent=analysis_type,
                analysis=result,
                sources=converted[:10] if converted else [],
            )
        elif analysis_type == "cause_tracking":
            result = agent_integration._analysis.trace_cause(
                ticker_name=ticker_name,
                date_range=None,
                report_events=converted,
                dart_events=dart_results or [],
            )
            if result:
                return agent_integration._composer.compose_chat_response(
                intent=analysis_type,
                analysis=result,
                sources=converted[:10] if converted else [],
            )
        elif analysis_type == "interpret":
            raw_text = ""
            if dart_results and len(dart_results) > 0:
                first = dart_results[0]
                raw_text = first.get("report_nm", "") if hasattr(first, 'get') else str(first)
            elif converted:
                first = converted[0]
                raw_text = first.get("text", first.get("reason", "")) if isinstance(first, dict) else str(first)
            if raw_text:
                result = agent_integration._dart.interpret_disclosure(raw_text, ticker_name or "종목")
                if result:
                    return agent_integration._composer.compose_chat_response(
                intent=analysis_type,
                analysis=result,
                sources=converted[:10] if converted else [],
            )
        return None
    except Exception as e:
        logger.warning("Analysis pipeline failed (%s): %s", analysis_type, e)
        import traceback
        traceback.print_exc()
        return None


def _extract_ticker_from_message(message: str) -> Optional[str]:
    """Extract ticker name from a message like '삼성전자 왜 올랐어?'."""
    import re
    # Common Korean stock names
    _known_tickers = [
        "삼성전자", "SK하이닉스", "LG에너지솔루션", "삼성바이오로직스",
        "현대차", "기아", "셀트리온", "POSCO홀딩스", "NAVER", "카카오",
        "카카오뱅크", "KB금융", "신한지주", "하나금융지주",
        "삼성SDI", "LG화학", "현대모비스", "삼성물산", "SK텔레콤",
        "KT", "LG전자", "한화에어로스페이스", "두산에너빌리티",
        "HD현대중공업", "삼성중공업", "한화오션", "HMM",
        "대한항공", "하이브", "JYP Ent.", "에코프로", "포스코퓨처엠",
    ]
    for name in _known_tickers:
        if name in message:
            return name
    return None



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
    score: Optional[float] = None
    종목코드: Optional[str] = None
    reason: Optional[str] = None
    keywords: Optional[List[str]] = None
    risks: Optional[List[str]] = None
    year: Optional[int] = None
    month: Optional[int] = None
    source_type: Optional[str] = None
    rcept_no: Optional[str] = None
    rcept_dt: Optional[str] = None
    corp_name: Optional[str] = None
    report_nm: Optional[str] = None
    url: Optional[str] = None


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


def _search_result_from_metadata(report_id: str, score: Optional[float] = None,
                                 overrides: Optional[dict] = None) -> SearchResult:
    info = report_texts.get(str(report_id), {}) or {}
    data = {
        "report_id": str(report_id),
        "score": score,
        "종목코드": info.get("종목코드"),
        "reason": info.get("reason"),
        "keywords": info.get("keywords"),
        "risks": info.get("risks"),
        "year": info.get("year"),
        "month": info.get("month"),
        "source_type": info.get("source_type"),
        "rcept_no": info.get("rcept_no"),
        "rcept_dt": info.get("rcept_dt"),
        "corp_name": info.get("corp_name"),
        "report_nm": info.get("report_nm"),
        "url": source_url_from_metadata(info),
    }
    if overrides:
        for key, value in overrides.items():
            if value is not None:
                data[key] = value
    if not data.get("url"):
        data["url"] = source_url_from_metadata(data)
    return SearchResult(**data)


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

    init_db()
    seed_initial_users()
    conversation_store.restore_all()

    _telegram_polling_stop = threading.Event()
    _telegram_polling_thread = threading.Thread(
        target=_telegram_polling_loop,
        args=(_telegram_polling_stop,),
        daemon=True,
        name="telegram_polling"
    )
    _telegram_polling_thread.start()

    yield

    _telegram_polling_stop.set()
    _telegram_polling_thread.join(timeout=5)
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
        results.append(_search_result_from_metadata(
            rid,
            score=round(score, 4),
        ))

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
- If a DART item includes "DART URL:", copy that exact URL below the corresponding item.
- NEVER invent a DART receipt number (rcpNo) or URL. Receipt numbers are assigned sequentially across all companies per filing date, so a guessed number links to an unrelated company. Only reproduce a URL that was provided with the item; if none was provided, show no URL.
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


def _filter_dart_sources_by_company(results: List[SearchResult], intent) -> List[SearchResult]:
    """Drop DART chunks belonging to a different company than the user targeted.

    FAISS semantic search can surface the same report type (e.g. 임원·주요주주
    소유상황보고서) filed by OTHER companies; with the company name missing from the
    embedded chunk the LLM then mislabels them as the queried company (the
    현대차증권→미래에셋 leak). Analyst-report chunks are left untouched — only DART
    chunks are company-scoped here, and only when a company was actually named.
    """
    companies = list(getattr(intent, "companies", None) or [])
    codes = list(getattr(intent, "stock_codes", None) or [])
    if not companies and not codes:
        return results  # no specific company targeted → nothing to scope

    target_corp = None
    target_stocks = {str(c).strip().lstrip("0") for c in codes if str(c).strip()}
    try:
        from dart_query import _find_company
        comp = _find_company(companies, codes)
        if comp:
            if comp.get("corp_code"):
                target_corp = str(comp["corp_code"]).lstrip("0")
            if comp.get("stock_code"):
                target_stocks.add(str(comp["stock_code"]).lstrip("0"))
    except Exception as e:
        logger.debug("company resolve for DART filter failed: %s", e)

    if not target_corp and not target_stocks:
        return results  # couldn't resolve identity → don't risk dropping data

    kept, dropped = [], 0
    for r in results:
        if getattr(r, "source_type", None) != "dart":
            kept.append(r)
            continue
        info = report_texts.get(r.report_id, {}) or {}
        rc = str(info.get("corp_code", "") or "").lstrip("0")
        rs = str((r.종목코드 or info.get("종목코드") or "")).lstrip("0")
        if (target_corp and rc and rc == target_corp) or (rs and rs in target_stocks):
            kept.append(r)
        else:
            dropped += 1
    if dropped:
        logger.info("Dropped %d cross-company DART chunk(s) (target corp=%s stocks=%s)",
                    dropped, target_corp, target_stocks)
    return kept


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
        url = r.url or source_url_from_metadata(info)
        source_ref = source_line({"url": url}, indent="") if url else ""
        if reason is not None and not isinstance(reason, str):
            reason = str(reason)
        corp_name = r.corp_name or info.get("corp_name") or ""
        parts.append(
            "[report_id: {}]\n".format(r.report_id) +
            "기업명: {}\n".format(corp_name) +
            "종목코드: {}\n".format(r.종목코드 or "") +
            "공시번호: {}\n".format(r.rcept_no or info.get("rcept_no") or "") +
            "공시명: {}\n".format(r.report_nm or info.get("report_nm") or "") +
            "투자이유: {}\n".format(reason or "") +
            "키워드: {}\n".format(kw) +
            "리스크: {}\n".format(rs) +
            source_ref
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

    # Delta-first: structured Delta 테이블을 한 번에 읽어 발행일로 필터(파티션 스캔 대체).
    # Delta가 비었거나 실패하면 아래 월 파티션 parquet 스캔으로 폴백(무회귀).
    try:
        from agents.data_helper import read_gold_data
        _ddf = read_gold_data("structured")
    except Exception:
        _ddf = None
    if _ddf is not None and len(_ddf) > 0 and "발행일" in _ddf.columns:
        d = _ddf.copy()
        d["발행일_s"] = d["발행일"].astype(str).str.replace("-", "").str.replace(".", "")
        if date_from:
            d = d[d["발행일_s"] >= date_from.replace("-", "")]
        if date_to:
            d = d[d["발행일_s"] <= date_to.replace("-", "")]
        d = d.sort_values("발행일", ascending=False)
        for _, row in d.iterrows():
            rid = str(row.get("report_id", ""))
            info = report_texts.get(rid, {})
            title = str(row.get("title", "")) if row.get("title") is not None else ""
            증권사 = str(row.get("증권사", "")) if row.get("증권사") is not None else ""
            reason = f"[{증권사}] {title}" if 증권사 and title else (title or info.get("reason", ""))
            ymd = str(row.get("발행일_s", ""))
            all_results.append(
                SearchResult(
                    report_id=rid,
                    score=1.0,
                    종목코드=str(row.get("종목코드", "")) if row.get("종목코드") is not None else None,
                    reason=str(reason)[:300] if reason else None,
                    keywords=info.get("keywords"),
                    risks=info.get("risks"),
                    year=int(ymd[:4]) if len(ymd) >= 4 else 0,
                    month=int(ymd[4:6]) if len(ymd) >= 6 else 0,
                )
            )
        total = len(all_results)
        results = all_results[offset:offset + limit]
        logger.info("Date scan complete (Delta): %d total, returning %d (offset=%d) for %s~%s",
                     total, len(results), offset, date_from, date_to)
        return results, total

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

    # v3: Analysis pre-check — detect analysis requests BEFORE refusal gate.
    # The intent parser may classify cause_tracking/interpret as "refuse" because
    # Haiku doesn't have those intent types. Override refusal for analysis queries.
    _analysis_type = _detect_analysis_request(_original_message if '_original_message' in dir() else req.message)

    # v2: REFUSAL GATE — intent parser said refuse → return immediately, no search
    # v3 EXCEPTION: analysis requests (compare/cause_tracking/interpret) skip this gate
    if intent.is_refusal and not _analysis_type:
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

    if intent.is_refusal and _analysis_type:
        logger.info("Analysis request (%s) bypassed refusal gate — intent was 'refuse' but keywords detected", _analysis_type)
        # Override intent to proceed with data retrieval
        intent_info["intent"] = "report_search"

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

    if intent.intent == "general" and any(kw in req.message for kw in _all_block_keywords) and not _analysis_type:
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

    # v5: Date context reconstruction for follow-up questions (revised 2026-06-19)
    # Multi-stage algorithm for short follow-ups like "6일은?", "그날은?":
    #   Stage 1: explicit full date in message → use it (no override)
    #   Stage 2: DART broad fallback — if needs_dart + no explicit date → use last full date
    #   Stage 3: Day-only reconstruction — if message has just a day ("6일") + recent month
    #            in conversation → RECONSTRUCT {month}-{day}. THIS TAKES PRIORITY.
    #   Stage 4: Report broad fallback — if needs_reports + no date at all → use recent month
    #
    # Key fix: Stage 3 no longer requires intent.refers_to_previous.
    # Haiku often fails to set refers_to_previous for short follow-ups like "6일은?".
    # Stage 3 runs AFTER Stage 2 so the reconstructed day overrides the broad fallback.
    date_from = req.date_from or intent.date_from
    date_to = req.date_to or intent.date_to

    # Direct date extraction: "M월 D일" without year → (current_year)-MM-DD
    # Haiku intent parser is unreliable for year inference on month-day patterns.
    # Extract dates directly from user text to guarantee correct year.
    _month_day_pat = re.search(r'(?<!\d)(\d{1,2})월\s*(\d{1,2})일', req.message)
    if _month_day_pat and not re.search(r'\d{4}년', req.message):
        from datetime import datetime as _dt
        _cur_year = str(_dt.now().year)
        _m = _month_day_pat.group(1).zfill(2)
        _d = _month_day_pat.group(2).zfill(2)
        _extracted = f"{_cur_year}-{_m}-{_d}"
        logger.info("Direct date: '%s월 %s일' → %s (overriding Haiku intent)",
                     _month_day_pat.group(1), _month_day_pat.group(2), _extracted)
        date_from = _extracted
        date_to = _extracted
        intent_info["date_from"] = _extracted
        intent_info["date_to"] = _extracted

    # Check if the user message contains explicit date text
    _has_explicit_date = bool(
        re.search(r'\d{4}년|\d{1,2}월\s*\d{1,2}일|\d{4}-\d{2}-\d{2}|어제|오늘|그제', req.message)
    )

    _day_only = re.search(r'(?<!\d)(\d{1,2})일', req.message)

    # Stage 2: Broad DART fallback (runs FIRST, may be overridden by Stage 3)
    if not _has_explicit_date and intent.needs_dart:
        recent_full_date = conversation_store.get_recent_full_date(session_id)
        if recent_full_date:
            logger.info("v5 broad DART fallback: %s (intent=%s)", recent_full_date, intent.intent)
            date_from = recent_full_date
            date_to = recent_full_date

    # Stage 3: Day-only reconstruction (ALWAYS takes priority over Stage 2)
    # "6일은?" after "2026년 1월 3일" → reconstruct "2026-01-06"
    # No longer requires intent.refers_to_previous.
    if _day_only and not _has_explicit_date:
        recent_month = conversation_store.get_recent_month(session_id)
        if recent_month:
            day = _day_only.group(1).zfill(2)
            reconstructed = f"{recent_month}-{day}"
            prev_full = conversation_store.get_recent_full_date(session_id) or "none"
            logger.info("v5 Reconstructed day-only date: %s (month=%s, day=%s, prev_full=%s)",
                         reconstructed, recent_month, day, prev_full)
            date_from = reconstructed
            date_to = reconstructed
            # Reflect the reconstructed date in intent_info
            intent_info["date_from"] = reconstructed
            intent_info["date_to"] = reconstructed

    # Stage 4: Report broad fallback — if still no date but intent wants reports
    if not date_from and not date_to and not _has_explicit_date and intent.needs_reports:
        recent_month = conversation_store.get_recent_month(session_id)
        if recent_month and not _day_only:
            date_from = recent_month + "-01"
            date_to = recent_month + "-31"
            logger.info("v5 report fallback to recent month: %s", recent_month)

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
                report_sources.append(_search_result_from_metadata(
                    rid,
                    score=round(float(distances[0][i]), 4),
                ))

            if date_from or date_to:
                report_sources = _filter_by_date(report_sources, date_from, date_to)

            # Anti-leakage: when a company is named, DART chunks must match it.
            sources = _filter_dart_sources_by_company(report_sources, intent)

    if intent.needs_dart:
        dart_context = _query_dart(intent, page=page, page_size=page_size,
                                   date_from=date_from, date_to=date_to)
        # Treat "no data" messages as empty — never pass to LLM
        if dart_context and "데이터가 없습니다" not in dart_context:
            dart_results = [{"source": intent.intent, "summary": dart_context[:500]}]
        else:
            dart_context = ""

    # v3: Analysis routing for complex queries (compare/cause_tracking/interpret)
    # When the user wants analysis beyond simple search, delegate to the V2 agent pipeline.
    # NOTE: _analysis_type is already set in the pre-check section above.
    if _analysis_type and AGENT_ENABLED_STR != "false":
        logger.info("Analysis routing activated: type=%s", _analysis_type)
        analysis_result = _run_analysis_with_data(
            _analysis_type, sources, intent_info,
            _original_message if '_original_message' in dir() else req.message,
            dart_results)
        if analysis_result:
            return ChatResponse(
                answer=analysis_result,
                sources=sources,
                elapsed_ms=round((time.time() - t0) * 1000, 1),
                intent=intent_info,
                context_full=conversation_store.is_context_full(session_id),
                turn_count=conversation_store.get_turn_count(session_id),
            )
        logger.info("Analysis pipeline returned None — falling back to standard response")

    # Stage 3: Build context and answer
    # Determine if this was a browse query (all reports in date range)
    is_browse = (
        not intent.companies and not intent.stock_codes
        and (date_from or date_to)
        and not intent.search_query
    )

    if intent.is_general:
        # v3: Greeting detection — for simple greetings, respond warmly
        _greeting_kw = ["안녕", "하이", "헬로", "반가", "고마워", "감사", "ㅎㅇ"]
        _original_for_greeting = getattr(req, 'message', '')
        _is_simple_greeting = any(kw in _original_for_greeting for kw in _greeting_kw) and len(_original_for_greeting) < 20

        if _is_simple_greeting:
            greeting_msg = (
                "안녕하세요! OPIK 금융 정보 챗봇입니다.\n\n"
                "증권사 애널리스트 리포트와 DART 공시 데이터를 검색·요약해 드립니다.\n\n"
                "예를 들어 이런 질문을 해보세요:\n"
                "• \"삼성전자 목표주가 알려줘\"\n"
                "• \"최근 반도체 리포트 있어?\"\n"
                "• \"삼성전자랑 SK하이닉스 비교해줘\"\n"
                "• \"오늘 올라온 리포트 요약해줘\"\n\n"
                "무엇을 도와드릴까요?"
            )
            return ChatResponse(
                answer=greeting_msg,
                sources=[],
                elapsed_ms=round((time.time() - t0) * 1000, 1),
                intent=intent_info,
                context_full=conversation_store.is_context_full(session_id),
                turn_count=conversation_store.get_turn_count(session_id),
            )

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
                answer=(
                f"해당 날짜({_display_date})의 DART 공시 데이터가 없습니다.\n\n"
                "DART 공시 데이터는 2026년 3월까지 적재되어 있습니다.\n"
                "최신 공시는 3월 이전 날짜로 조회해 주세요."
            ),
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

    # Grounding guardrail: drop any DART URL whose receipt number was not in the
    # data context. A fabricated rcpNo deep-links to an unrelated company's filing
    # on dart.fss.or.kr (e.g. a 현대차증권 answer pointing to a 미래에셋증권 doc).
    if context:
        answer, _removed_urls = strip_ungrounded_dart_urls(answer, context)
        if _removed_urls:
            logger.warning("Stripped %d ungrounded DART URL(s) from answer: %s",
                           len(_removed_urls), _removed_urls)

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




# -- Telegram ------------------------------------------------------------

def _send_telegram(chat_id, text, parse_mode="HTML"):
    if not TELEGRAM_BOT_TOKEN:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        for i in range(0, len(text), 4000):
            chunk = text[i:i + 4000]
            r = requests.post(url, json={
                "chat_id": chat_id, "text": chunk,
                "parse_mode": parse_mode, "disable_web_page_preview": True
            }, timeout=15)
            if r.status_code != 200:
                logger.error("TG send failed: %s", r.text)
                return False
        return True
    except Exception as e:
        logger.exception("TG send error: %s", e)
        return False


def _handle_telegram_command(chat_id, text, username, first_name):
    cmd = text.strip().lower()
    if cmd == "/start":
        upsert_subscriber(chat_id, username=username, first_name=first_name)
        if is_approved(chat_id):
            return "OPIK에 다시 오신 것을 환영합니다!"
        return "OPIK 봇에 오신 것을 환영합니다! 승인 대기 중입니다."
    elif cmd == "/subscribe":
        upsert_subscriber(chat_id, username=username, first_name=first_name)
        if not is_approved(chat_id):
            return "먼저 승인이 필요합니다."
        if add_briefing_recipient(chat_id):
            return "매일 아침 7시 브리핑을 구독했습니다."
        return "이미 구독 중입니다."
    elif cmd == "/unsubscribe":
        if remove_briefing_recipient(chat_id):
            return "구독이 해지되었습니다."
        return "구독 중이 아닙니다."
    elif cmd == "/status":
        sub = get_subscriber(chat_id)
        if not sub:
            return "등록되지 않았습니다."
        return "승인됨" if sub.get("approved") else "대기 중"
    elif cmd.startswith("/approve") and is_approved(chat_id):
        parts = text.strip().split()
        if len(parts) == 2 and parts[1].isdigit():
            target = int(parts[1])
            if approve_subscriber(target):
                add_briefing_recipient(target)
                _send_telegram(target, "관리자가 승인했습니다.")
                return f"승인: {target}"
            return "승인 실패"
        return "사용법: /approve <chat_id>"
    elif cmd == "/help":
        return "OPIK: /start /subscribe /unsubscribe /status /help"
    return None


def _process_telegram_message(chat_id, text, username, first_name):
    upsert_subscriber(chat_id, username=username, first_name=first_name)
    reply = _handle_telegram_command(chat_id, text, username, first_name)
    if reply is not None:
        _send_telegram(chat_id, reply)
        return
    if not is_approved(chat_id):
        _send_telegram(chat_id, "승인 대기 중입니다.")
        return

    # Show typing indicator
    if TELEGRAM_BOT_TOKEN:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendChatAction",
                json={"chat_id": chat_id, "action": "typing"}, timeout=5
            )
        except Exception:
            pass

    t0 = time.time()
    try:
        # Context injection for follow-up messages is handled by
        # _run_agent_pipeline() in agent_integration.py. Pass the
        # original message so v10's short-message detection works correctly.
        _augmented_text = text
        FakeReq = type("FakeReq", (), {
            "message": _augmented_text, "session_id": f"telegram_{chat_id}"
        })
        import asyncio
        r = asyncio.run(v2_chat_handler(FakeReq()))
        answer = r.get("answer", "응답 실패")
    except Exception as e:
        logger.exception("Agent failed %d: %s", chat_id, e)
        answer = "내부 오류"
    logger.info("TG done in %dms", round((time.time()-t0)*1000))
    _send_telegram(chat_id, answer)


def _telegram_polling_loop(stop_event):
    offset = 0
    logger.info("Telegram polling started")
    while not stop_event.is_set():
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            r = requests.get(url, params={
                "offset": offset, "timeout": 30, "allowed_updates": ["message"]
            }, timeout=35)
            data = r.json()
            if not data.get("ok"):
                stop_event.wait(5)
                continue
            for upd in data.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message")
                if not msg:
                    continue
                ch = msg.get("chat", {})
                cid = ch.get("id")
                txt = (msg.get("text") or "").strip()
                if not cid or not txt:
                    continue
                logger.info("TG msg from %d: %s", cid, txt[:100])
                _process_telegram_message(cid, txt, ch.get("username",""), ch.get("first_name",""))
        except Exception as e:
            logger.warning("TG poll err: %s", e)
            stop_event.wait(5)
    logger.info("Telegram polling stopped")


@app.post("/telegram/webhook")
async def telegram_webhook(req: Request):
    if not TELEGRAM_BOT_TOKEN:
        raise HTTPException(503)
    body = await req.json()
    msg = body.get("message", {})
    if not msg:
        return {"status": "ignored"}
    ch = msg.get("chat", {})
    cid = ch.get("id")
    txt = (msg.get("text") or "").strip()
    if not cid or not txt:
        return {"status": "ignored"}
    _process_telegram_message(cid, txt, ch.get("username",""), ch.get("first_name",""))
    return {"status": "ok"}


# index management
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

    logger.info("Building FAISS index from S3: s3://%s/gold/embeddings/ + %s",
                S3_BUCKET, DART_RAG_EMBEDDING_PREFIX)
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
    new_report_texts = {}

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
                rid = str(row["report_id"])
                all_ids.append(rid)
                new_report_texts[rid] = {
                    "종목코드": row.get("종목코드"),
                    "reason": row.get("reason"),
                    "keywords": row.get("keywords"),
                    "risks": row.get("risks"),
                    "year": year,
                    "month": month,
                    "source_type": "report",
                }
        except Exception as e:
            logger.warning("Error reading %s: %s", key, e)

    keys_dart = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=DART_RAG_EMBEDDING_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys_dart.append(obj["Key"])
    logger.info("Found %d DART embedding parquet files", len(keys_dart))

    # The DART embedding parquet has corp_code but NO corp_name column. Resolve
    # names from company_master so each chunk carries its real company — without
    # it the LLM fills the blank with whatever company the user asked about.
    try:
        from dart_query import corp_code_to_name_map
        dart_corp_names = corp_code_to_name_map()
        logger.info("Loaded %d corp_code→name mappings for DART enrichment", len(dart_corp_names))
    except Exception as e:
        logger.warning("corp_code→name map load failed (DART chunks will lack names): %s", e)
        dart_corp_names = {}

    dart_loaded = 0
    for key in keys_dart:
        try:
            ym = re.search(r"rcept_year=(\d{4})/rcept_month=(\d{2})", key)
            year = int(ym.group(1)) if ym else None
            month = int(ym.group(2)) if ym else None

            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            buf = io.BytesIO(obj["Body"].read())
            table = pq.read_table(buf)
            df = table.to_pandas()

            for _, row in df.iterrows():
                is_latest = row.get("is_latest")
                if is_latest is None or not bool(is_latest):
                    continue
                valid_to = row.get("valid_to")
                if valid_to is not None and str(valid_to) != "None":
                    continue

                emb = row["embedding"]
                if emb is None or len(emb) == 0:
                    continue

                chunk_id = str(row["chunk_id"])
                all_embeddings.append(np.array(emb, dtype=np.float32))
                all_ids.append(chunk_id)

                rcept_no = str(row.get("rcept_no", "")) if row.get("rcept_no") is not None else ""
                rcept_dt = str(row.get("rcept_dt", "")) if row.get("rcept_dt") is not None else ""
                base_type = str(row.get("base_report_type", "")) if row.get("base_report_type") is not None else ""
                dart_kw = row.get("keywords")
                source_url = source_url_from_metadata(row)

                corp_code = str(row.get("corp_code", "")) if row.get("corp_code") is not None else None
                corp_name = str(row.get("corp_name", "")) if row.get("corp_name") is not None else None
                if (not corp_name or corp_name.lower() in ("", "none", "nan")) and corp_code:
                    corp_name = dart_corp_names.get(corp_code) or dart_corp_names.get(corp_code.lstrip("0"))

                reason_parts = [f"[DART {base_type}]"]
                if corp_name:
                    reason_parts.append(f"기업: {corp_name}")
                if rcept_dt:
                    reason_parts.append(f"접수일: {rcept_dt}")
                if rcept_no:
                    reason_parts.append(f"공시번호: {rcept_no}")

                new_report_texts[chunk_id] = {
                    "종목코드": str(row.get("stock_code", "")) if row.get("stock_code") is not None else None,
                    "reason": " ".join(reason_parts),
                    "keywords": [str(x) for x in dart_kw] if dart_kw is not None and len(dart_kw) > 0 else None,
                    "risks": None,
                    "year": year,
                    "month": month,
                    "source_type": "dart",
                    "rcept_no": rcept_no or None,
                    "rcept_dt": rcept_dt or None,
                    "corp_code": corp_code,
                    "corp_name": corp_name or None,
                    "report_nm": str(row.get("report_nm", "")) if row.get("report_nm") is not None else None,
                    "base_report_type": base_type or None,
                    "dart_view_url": source_url,
                    "source_url": source_url,
                    "source_uri": str(row.get("source_uri", "")) if row.get("source_uri") is not None else None,
                }
                dart_loaded += 1
        except Exception as e:
            logger.warning("Error reading DART %s: %s", key, e)

    logger.info("Loaded DART embeddings: %d", dart_loaded)

    n = len(all_embeddings)
    if n == 0:
        raise RuntimeError("No embeddings found in S3")

    dim = all_embeddings[0].shape[0]
    if dim != EMBEDDING_DIM:
        logger.warning("Embedding dimension mismatch: expected=%d actual=%d", EMBEDDING_DIM, dim)

    vectors = np.array(all_embeddings, dtype=np.float32)
    ids = np.arange(n, dtype=np.int64)

    new_index = faiss.IndexIDMap(faiss.IndexFlatIP(dim))
    new_index.add_with_ids(vectors, ids)

    os.makedirs(os.path.dirname(FAISS_INDEX_PATH), exist_ok=True)
    faiss.write_index(new_index, FAISS_INDEX_PATH)
    with open(FAISS_IDMAP_PATH, "w", encoding="utf-8") as f:
        json.dump(all_ids, f, ensure_ascii=False)
    with open("/data/opik/report_info.json", "w", encoding="utf-8") as f:
        json.dump(new_report_texts, f, ensure_ascii=False, default=str)

    faiss_index = new_index
    report_ids = all_ids
    report_texts = new_report_texts
    logger.info("FAISS index built from S3: %d vectors, dim=%d", n, dim)
    return n
