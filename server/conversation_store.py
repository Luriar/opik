"""
OPIK Conversation Memory Store
세션 기반 대화 맥락 관리. 인메모리 LRU 저장소 + 컨텍스트 윈도우 관리.
"""

from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import re
import threading
from typing import Optional


@dataclass
class ConversationTurn:
    role: str          # "user" | "assistant"
    content: str       # 원본 메시지
    summary: str       # 요약 (오래된 턴만 채워짐)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ConversationSession:
    session_id: str
    turns: list = field(default_factory=list)
    context_summary: str = ""          # 오래된 턴의 누적 요약
    created_at: datetime = field(default_factory=datetime.now)
    last_active: datetime = field(default_factory=datetime.now)


class ConversationStore:
    """스레드 안전한 LRU 기반 대화 저장소"""

    def __init__(self, max_sessions=100, max_turns_per_session=20,
                 context_window_tokens=8000, session_ttl_minutes=60):
        self._sessions = OrderedDict()
        self._lock = threading.Lock()
        self.max_sessions = max_sessions
        self.max_turns = max_turns_per_session
        self.context_window_tokens = context_window_tokens
        self.session_ttl = timedelta(minutes=session_ttl_minutes)

    def get_or_create(self, session_id: str) -> ConversationSession:
        with self._lock:
            # 만료된 세션 정리
            self._evict_expired()

            if session_id not in self._sessions:
                # LRU eviction
                if len(self._sessions) >= self.max_sessions:
                    self._sessions.popitem(last=False)
                self._sessions[session_id] = ConversationSession(
                    session_id=session_id
                )
            else:
                self._sessions.move_to_end(session_id)

            self._sessions[session_id].last_active = datetime.now()
            return self._sessions[session_id]

    def add_turn(self, session_id: str, role: str, content: str):
        session = self.get_or_create(session_id)
        turn = ConversationTurn(
            role=role,
            content=content,
            summary="",
            timestamp=datetime.now()
        )
        session.turns.append(turn)

        # 초과 턴은 요약 후 압축
        if len(session.turns) > self.max_turns:
            self._compress_old_turns(session)

        # Persist to SQLite (fire-and-forget, best-effort)
        self._persist_session(session_id, session)

    def get_context_for_prompt(self, session_id: str) -> str:
        """프롬프트에 주입할 대화 맥락 생성"""
        session = self.get_or_create(session_id)
        if not session.turns:
            return ""

        parts = []

        # 누적 요약
        if session.context_summary:
            parts.append(
                "<conversation_summary>\n"
                f"{session.context_summary}\n"
                "</conversation_summary>"
            )

        # 최근 3왕복 (6턴) 그대로 포함
        recent_turns = session.turns[-6:]
        if recent_turns:
            parts.append("<recent_conversation>")
            for turn in recent_turns:
                role_label = "사용자" if turn.role == "user" else "OPIK"
                text = turn.summary if turn.summary else turn.content
                parts.append(f"[{role_label}]: {text}")
            parts.append("</recent_conversation>")

        return "\n".join(parts)

    def is_context_full(self, session_id: str) -> bool:
        """컨텍스트 윈도우 가득 참 여부"""
        session = self.get_or_create(session_id)
        turn_count = len(session.turns)
        summary_count = sum(1 for t in session.turns if t.summary)
        total_chars = sum(
            len(t.summary or t.content) for t in session.turns
        )

        return (
            turn_count > 20
            or summary_count >= 10
            or total_chars > self.context_window_tokens * 4  # char ≈ token/4
        )

    def reset_session(self, session_id: str):
        """대화 세션 초기화"""
        with self._lock:
            if session_id in self._sessions:
                del self._sessions[session_id]

    def get_turn_count(self, session_id: str) -> int:
        """현재 세션의 턴 수 반환 (없으면 0)"""
        with self._lock:
            session = self._sessions.get(session_id)
            return len(session.turns) if session else 0

    def get_recent_summary(self, session_id: str) -> str:
        """이전 대화에서 검색에 활용할 요약 정보 반환.
        마지막 사용자 질문 + 최근 논의된 종목/키워드/날짜를 포함."""
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return ""
            # 마지막 3왕복 (6턴)에서 사용자 발언만 추출
            recent_turns = session.turns[-6:]
            user_turns = [t for t in recent_turns if t.role == "user"]
            if not user_turns:
                return ""
            # 키워드 추출
            kw = self._extract_keywords(recent_turns)
            parts = []
            if kw.get("tickers"):
                parts.append("종목: " + ", ".join(kw["tickers"]))
            if kw.get("topics"):
                parts.append("주제: " + ", ".join(kw["topics"]))
            if kw.get("recent_dates"):
                parts.append("최근 날짜: " + ", ".join(kw["recent_dates"]))
            # 마지막 사용자 질문
            last_q = user_turns[-1].content[:200]
            if last_q:
                parts.append(f"이전 질문: {last_q}")
            return " | ".join(parts)

    def get_recent_month(self, session_id: str) -> Optional[str]:
        """가장 최근 대화 턴에서 언급된 YYYY-MM 반환. follow-up 날짜 질문에 사용.

        sorted(dates, reverse=True)가 아닌 턴 역순으로 찾는 이유:
        "6월 13일" → "1월 13일" → "14일은?" 대화에서 sorted()는 문자열 정렬로
        "2026-06-13" > "2026-01-13"이 되어 잘못된 월을 반환한다.
        턴 순서 기준으로 가장 최근 언급된 날짜의 월을 반환해야 한다."""
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return None
            recent_turns = session.turns[-6:]
            # 턴을 역순으로 순회 — 가장 최근 턴의 날짜 우선
            for turn in reversed(recent_turns):
                kw = self._extract_keywords([turn])
                for d in kw.get("recent_dates", []):
                    if len(d) >= 7:
                        return d[:7]
            return None

    def get_recent_full_date(self, session_id: str) -> Optional[str]:
        """대화 컨텍스트에서 가장 최근 언급된 YYYY-MM-DD 반환.
        get_recent_month()와 달리 완전한 날짜(10자)를 반환한다.
        "이거밖에 없어?", "더 보여줘" 같은 follow-up이 날짜 없이 이전 결과를
        참조할 때 DART 쿼리의 date filter로 사용된다."""
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return None
            recent_turns = session.turns[-6:]
            for turn in reversed(recent_turns):
                kw = self._extract_keywords([turn])
                for d in kw.get("recent_dates", []):
                    if len(d) == 10:  # YYYY-MM-DD
                        return d
            return None

    def _compress_old_turns(self, session: ConversationSession):
        """오래된 턴을 규칙 기반으로 압축 (LLM 요약 없이)"""
        old_turns = session.turns[:-6]  # 최근 3왕복 제외
        if not old_turns:
            return

        keywords = self._extract_keywords(old_turns)
        summary_parts = []

        if keywords["tickers"]:
            summary_parts.append(f"관심 종목: {', '.join(keywords['tickers'])}")
        if keywords["sectors"]:
            summary_parts.append(f"관심 섹터: {', '.join(keywords['sectors'])}")
        if keywords["topics"]:
            summary_parts.append(f"논의 주제: {', '.join(keywords['topics'])}")

        new_summary = "이전 대화 요약 — " + "; ".join(summary_parts) + "."
        if session.context_summary:
            session.context_summary += " " + new_summary
        else:
            session.context_summary = new_summary

        # 오래된 턴 제거
        session.turns = session.turns[-6:]

    def _extract_keywords(self, turns: list) -> dict:
        """간단한 키워드 추출 (규칙 기반, LLM 호출 없음).
        종목명, 섹터, 토픽, 최근 언급된 날짜를 추출."""
        tickers = set()
        sectors = {"반도체", "2차전지", "바이오", "자동차", "인터넷", "게임",
                    "엔터", "금융", "철강", "화학", "건설", "유통", "로봇",
                    "AI", "인공지능", "클라우드", "방산", "조선", "해운"}
        topics = set()
        recent_dates = set()

        # 잘 알려진 종목명 매칭
        known_tickers = [
            "삼성전자", "SK하이닉스", "LG에너지솔루션", "삼성바이오로직스",
            "현대차", "기아", "셀트리온", "카카오", "네이버", "카카오뱅크",
            "POSCO", "포스코", "LG화학", "삼성SDI", "현대모비스",
            "KB금융", "신한지주", "하나금융지주", "삼성물산", "SK",
            "KT", "LG전자", "SK텔레콤", "두산", "한화", "HD현대"
        ]

        topic_keywords = [
            "목표주가", "투자의견", "실적", "PER", "PBR", "EPS",
            "밸류에이션", "배당", "M&A", "공시", "내부자",
            "영업이익", "매출", "주가", "상승", "하락"
        ]

        all_text = " ".join(t.content for t in turns)

        for ticker in known_tickers:
            if ticker in all_text:
                tickers.add(ticker)

        for sector in sectors:
            if sector in all_text:
                tickers.add(sector)

        for topic in topic_keywords:
            if topic in all_text:
                topics.add(topic)

        # 날짜 추출 — follow-up 질문에서 월/연도 맥락 재구성에 사용
        # 패턴: "2026년 6월 13일", "6월 13일", "2026-06-13", "2026-01-13"
        date_patterns = [
            r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일',  # 2026년 6월 13일
            r'(\d{4})-(\d{2})-(\d{2})',                  # 2026-06-13
            r'(?<!\d)(\d{1,2})월\s*(\d{1,2})일(?!\d)',   # 6월 13일 (연도 없음)
        ]
        for pat in date_patterns:
            for m in re.finditer(pat, all_text):
                groups = m.groups()
                if len(groups) == 3:
                    recent_dates.add(f"{groups[0]}-{groups[1].zfill(2)}-{groups[2].zfill(2)}")
                elif len(groups) == 2:
                    # 월일만 있는 경우: 현재 연도 사용
                    from datetime import datetime
                    y = datetime.now().year
                    recent_dates.add(f"{y}-{groups[0].zfill(2)}-{groups[1].zfill(2)}")

        return {
            "tickers": list(tickers)[:5],
            "sectors": list(sectors & tickers)[:3],
            "topics": list(topics)[:5],
            "recent_dates": list(recent_dates)[:5],
        }

    def _persist_session(self, session_id: str, session: ConversationSession):
        """Best-effort persist to SQLite."""
        try:
            from db import save_conversation
            chat_id = 0
            if session_id.startswith("telegram_"):
                try:
                    chat_id = int(session_id.split("_", 1)[1])
                except ValueError:
                    chat_id = 0
            turns_json = [
                {"role": t.role, "content": t.content,
                 "summary": t.summary, "timestamp": t.timestamp.isoformat()}
                for t in session.turns
            ]
            save_conversation(session_id, chat_id, turns_json, session.context_summary)
        except Exception:
            pass

    def restore_all(self):
        """Restore active conversations from SQLite on startup."""
        try:
            from db import list_approved_subscribers, load_conversation_by_chat
            subscribers = list_approved_subscribers()
            for sub in subscribers:
                chat_id = sub["chat_id"]
                sessions = load_conversation_by_chat(chat_id, limit=1)
                for s in sessions:
                    sid = s["session_id"]
                    cs = ConversationSession(
                        session_id=sid,
                        context_summary=s.get("context_summary", ""),
                    )
                    cs.last_active = datetime.fromisoformat(
                        s.get("last_active", datetime.now().isoformat())
                    ) if s.get("last_active") else datetime.now()
                    for t in s.get("turns", []):
                        ts = t.get("timestamp", "")
                        try:
                            ts_dt = datetime.fromisoformat(ts) if ts else datetime.now()
                        except (ValueError, TypeError):
                            ts_dt = datetime.now()
                        cs.turns.append(ConversationTurn(
                            role=t.get("role", "user"),
                            content=t.get("content", ""),
                            summary=t.get("summary", ""),
                            timestamp=ts_dt,
                        ))
                    with self._lock:
                        if sid not in self._sessions:
                            self._sessions[sid] = cs
            import logging
            logging.getLogger(__name__).info("Restored %d sessions", len(self._sessions))
        except Exception:
            pass

    def _evict_expired(self):
        """TTL 만료 세션 제거"""
        now = datetime.now()
        expired = [
            sid for sid, s in self._sessions.items()
            if now - s.last_active > self.session_ttl
        ]
        for sid in expired:
            del self._sessions[sid]


# 글로벌 싱글톤
store = ConversationStore()
