"""RAG chunk 분할 + 안정적 chunk_id 생성.

원칙(Agent.MD §2.4):
  - chunk_id는 재실행해도 동일해야 한다(결정적). → upsert 멱등, 재임베딩 skip 가능.
  - 같은 보고서가 재처리되면 같은 chunk_id가 나오고, 내용이 바뀌면 content_hash로 감지한다.

토큰 예산(e5-small RAG):
  - e5-small 컨텍스트 = 512 토큰. 초과분은 sentence-transformers가 조용히 truncation한다.
    → 청크 뒷부분이 임베딩에서 누락돼 의미검색 recall이 깨진다(특히 긴 원문 text).
  - 그래서 char가 아니라 "토큰 예산"으로 자른다. tiktoken 의존 없이(한국어) 보수적으로 근사한다.
    한글 음절은 토큰/char가 높아 1로 보고, 그 외는 0.5/char(라틴/숫자 과대평가=안전)로 센다.
  - "passage: " 프리픽스·특수토큰·근사오차 여유까지 빼고 예산을 512보다 낮게 잡는다.
"""
from __future__ import annotations

import hashlib

# e5-small 512 토큰 - "passage: "(~4) - 특수토큰(2) - overlap·근사오차 여유. 청크 본문 토큰 상한.
# 한 청크 최대 ≈ _MAX_TOKENS + _OVERLAP_TOKENS = 448(est) → 프리픽스 포함해도 512 미만(truncation 방지).
_MAX_TOKENS = 400
_OVERLAP_TOKENS = 48

_HANGUL_START, _HANGUL_END = "가", "힣"


def approx_tokens(text: str) -> int:
    """e5(XLM-R sentencepiece) 근사 토큰 수 — 한국어 truncation 방지를 위한 보수적 추정.

    한글 음절 ≈ 1 토큰, 그 외 문자(라틴/숫자/공백/문장부호) ≈ 0.5 토큰/char(실제 ~0.25라 과대평가=안전).
    정밀 토크나이저 없이 청크 예산·token_count 메타용으로만 쓴다.
    """
    if not text:
        return 0
    hangul = sum(1 for ch in text if _HANGUL_START <= ch <= _HANGUL_END)
    other = len(text) - hangul
    return hangul + (other + 1) // 2


def split_text(text: str, max_tokens: int = _MAX_TOKENS, overlap_tokens: int = _OVERLAP_TOKENS) -> list[str]:
    """문단/문장 경계를 우선 존중하며 토큰 예산(max_tokens) 이하 chunk로 자른다(겹침 overlap_tokens).

    e5-small 512 토큰 한계를 넘지 않도록 토큰 기준으로 분할한다(초과 시 truncation으로 내용 손실).
    """
    text = (text or "").strip()
    if not text:
        return []
    if approx_tokens(text) <= max_tokens:
        return [text]

    units = _to_units(text, max_tokens)

    chunks: list[str] = []
    cur: list[str] = []
    cur_tok = 0
    for unit in units:
        ut = approx_tokens(unit)
        if cur and cur_tok + ut > max_tokens:
            chunks.append(" ".join(cur))
            # 겹침: 직전 청크의 뒤쪽 단위들을 overlap_tokens 예산까지만 이월(문맥 연속성).
            keep: list[str] = []
            kt = 0
            for u in reversed(cur):
                t = approx_tokens(u)
                if kt + t > overlap_tokens:
                    break
                keep.insert(0, u)
                kt += t
            cur = keep + [unit]
            cur_tok = sum(approx_tokens(u) for u in cur)
        else:
            cur.append(unit)
            cur_tok += ut
    if cur:
        chunks.append(" ".join(cur))
    return chunks


def _to_units(text: str, max_tokens: int) -> list[str]:
    """문단→문장으로 분해하되, 단위 하나가 max_tokens를 넘으면 강제로 더 자른다(truncation 방지)."""
    units: list[str] = []
    for para in text.split("\n"):
        para = para.strip()
        if not para:
            continue
        if approx_tokens(para) <= max_tokens:
            units.append(para)
            continue
        buf = ""
        for sent in _split_sentences(para):
            if approx_tokens(sent) > max_tokens:  # 단일 문장이 예산 초과 → 강제 분할.
                if buf:
                    units.append(buf.strip())
                    buf = ""
                units.extend(_hard_split(sent, max_tokens))
                continue
            if buf and approx_tokens(buf) + approx_tokens(sent) > max_tokens:
                units.append(buf.strip())
                buf = sent
            else:
                buf = f"{buf} {sent}".strip()
        if buf.strip():
            units.append(buf.strip())
    return units


def _hard_split(s: str, max_tokens: int) -> list[str]:
    """토크나이저 없이 긴 문장을 토큰 예산 이하 조각으로(추정 토큰밀도로 char 윈도 산정)."""
    s = s.strip()
    if not s:
        return []
    density = approx_tokens(s) / len(s)  # 토큰/char
    win = max(1, int(max_tokens / max(density, 1e-6)))
    return [s[i:i + win].strip() for i in range(0, len(s), win) if s[i:i + win].strip()]


def _split_sentences(text: str) -> list[str]:
    out, buf = [], ""
    for ch in text:
        buf += ch
        if ch in ".。!?\n" or buf.endswith("다 "):
            out.append(buf.strip())
            buf = ""
    if buf.strip():
        out.append(buf.strip())
    return [s for s in out if s]


def make_chunk_id(corp_code: str, rcept_no: str, rag_doc_type: str, index: int) -> str:
    """기본 chunk_id. 외부 보고서(rcept_no) 단위 문서에 사용."""
    return f"dart:{corp_code}:{rcept_no}:{rag_doc_type}:{index:03d}"


def make_chunk_id_keyed(corp_code: str, inner_rcept_no: str, rag_doc_type: str, natural_key: str, index: int) -> str:
    """내부 row 기준(ownership 등) chunk_id. 자연키 해시로 동일 row의 중복/충돌 방지."""
    h = hashlib.sha1(natural_key.encode("utf-8")).hexdigest()[:8]  # noqa: S324 - 식별자용, 보안용 아님
    return f"dart:{corp_code}:{inner_rcept_no}:{rag_doc_type}:{h}:{index:03d}"
