"""OPIK — 네이버 금융 증권사 리포트 수집기

실제 HTML 구조 (2026-06-11 확인):
    <tr>
        <td> <a class="stock_item" href="/item/main.naver?code=287840">인투셀</a> </td>
        <td> <a href="company_read.naver?nid=93508">임상 진입에 따른 기술 가시화</a> </td>
        <td> 미래에셋증권 </td>
        <td class="file"> <a href="https://stock.pstatic.net/...pdf"> </td>
        <td class="date"> 26.06.11 </td>
        <td class="date"> 218 </td>
    </tr>

- 인코딩: 실제 EUC-KR (meta 태그는 UTF-8이라 선언하지만 무시)
- PDF CDN: stock.pstatic.net
- 종목코드: /item/main.naver?code=XXXXXX
- 상세: company_read.naver?nid=XXXXX
- 날짜: YY.MM.DD
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from datetime import date, datetime, timedelta
from typing import Optional

import aiohttp
import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

# ── 상수 ──────────────────────────────────────────────────────────────
BASE_URL = "https://finance.naver.com/research/company_list.naver"
DETAIL_URL = "https://finance.naver.com/research/company_read.naver"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
]

# 증권사명 정규화 (네이버 표기 → 내부코드)
FIRM_MAP: dict[str, str] = {
    "미래에셋증권": "miraeasset",
    "삼성증권": "samsung",
    "NH투자증권": "nh",
    "KB증권": "kb",
    "한국투자증권": "koreainv",
    "신한투자증권": "shinhan",
    "하나증권": "hana",
    "키움증권": "kiwoom",
    "대신증권": "daishin",
    "유안타증권": "yuanta",
    "메리츠증권": "meritz",
    "신영증권": "shinyoung",
    "한화투자증권": "hanwha",
    "교보증권": "kyobo",
    "현대차증권": "hyundai",
    "DB금융투자": "db",
    "SK증권": "sk",
    "IBK투자증권": "ibk",
    "LS증권": "ls",
    "BNK투자증권": "bnk",
    "이베스트투자증권": "ebest",
    "케이프투자증권": "cape",
    "다올투자증권": "daol",
    "부국증권": "bookook",
    "유진투자증권": "eugene",
    "상상인증권": "sangsangin",
    "한양증권": "hanyang",
    "흥국증권": "heungkuk",
}

FIRM_NAME_KR: dict[str, str] = {v: k for k, v in FIRM_MAP.items()}


def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENTS[0],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })
    return s


# ── 데이터 클래스 ──────────────────────────────────────────────────────
from dataclasses import dataclass, asdict


@dataclass
class ReportMeta:
    report_id: str
    source: str = "naver"
    securities_firm: str = ""
    title: str = ""
    stock_code: Optional[str] = None
    stock_name: Optional[str] = None
    publish_date: Optional[str] = None
    analyst: Optional[str] = None
    pdf_url: Optional[str] = None
    detail_url: Optional[str] = None
    nid: Optional[str] = None
    category: str = "company"
    pdf_s3_key: Optional[str] = None
    pdf_size_bytes: Optional[int] = None
    pdf_hash_md5: Optional[str] = None
    collected_at: str = ""
    parse_status: str = "pending"

    def to_dict(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v is not None}


# ── 날짜 유틸 ─────────────────────────────────────────────────────────

def _parse_date_cell(raw: str) -> Optional[str]:
    """YY.MM.DD → YYYY-MM-DD"""
    raw = raw.strip()
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{2})", raw)
    if m:
        y = int(m.group(1))
        year = 2000 + y if y < 90 else 1900 + y
        return f"{year}-{m.group(2)}-{m.group(3)}"
    return None


# ── Row 파싱 (모듈 레벨, 클래스 인스턴스 없이 사용 가능) ─────────────

def _parse_table_rows(soup: BeautifulSoup) -> list[Tag]:
    """리서치 테이블의 tr 목록"""
    for sel in ["table.type_1 tbody tr", "div.box_type_m table tbody tr", "table.Nnavi tbody tr"]:
        rows = soup.select(sel)
        if rows:
            return [r for r in rows if r.select_one("td.file") or r.select_one("a.stock_item")]
    all_trs = soup.select("tr")
    return [tr for tr in all_trs if len(tr.select("td")) >= 5]


def _parse_row(tr: Tag) -> Optional[dict]:
    """실제 HTML 구조 파싱:
    td[0]: 종목, td[1]: 제목+nid, td[2]: 증권사, td[3].file: PDF, td[4].date: 날짜, td[5].date: 조회수
    """
    tds = tr.select("td")
    if len(tds) < 5:
        return None

    # td[0] 종목
    stock_name = None
    stock_code = None
    stock_a = tds[0].select_one("a.stock_item") or tds[0].select_one("a")
    if stock_a:
        stock_name = stock_a.get("title") or stock_a.get_text(strip=True)
        code_m = re.search(r"code=(\d{6})", stock_a.get("href", ""))
        if code_m:
            stock_code = code_m.group(1)

    # td[1] 제목 + nid
    title = ""
    nid = None
    title_a = tds[1].select_one("a")
    if title_a:
        title = title_a.get_text(strip=True)
        nid_m = re.search(r"nid=(\d+)", title_a.get("href", ""))
        if nid_m:
            nid = nid_m.group(1)

    if not title:
        return None

    # td[2] 증권사
    firm_raw = tds[2].get_text(strip=True)
    firm_code = FIRM_MAP.get(firm_raw, firm_raw.lower().replace(" ", ""))
    firm_name_kr = firm_raw

    # td[3] PDF — 여러 CDN 도메인/패턴 수용
    pdf_url = None
    pdf_a = tds[3].select_one("a")
    if pdf_a:
        pdf_href = pdf_a.get("href", "").strip()
        if pdf_href and not pdf_href.startswith("#") and not pdf_href.startswith("javascript:"):
            pdf_url = pdf_href
            # stock.pstatic.net 외 도메인 발견 시 로깅
            if "stock.pstatic.net" not in pdf_href:
                logger.debug("새 PDF 도메인: %s", pdf_href[:120])

    # td[4] 날짜
    date_raw = tds[4].get_text(strip=True)
    pub_date = _parse_date_cell(date_raw)
    if not pub_date:
        return None

    # NID 기반 report_id
    seed = f"naver|{nid or title}|{pub_date}"
    report_id = hashlib.sha256(seed.encode()).hexdigest()[:16]

    return {
        "report_id": report_id,
        "source": firm_name_kr,
        "securities_firm": firm_code,
        "증권사": firm_name_kr,
        "title": title,
        "stock_code": stock_code,
        "stock_name": stock_name,
        "종목명": stock_name or "",
        "publish_date": pub_date,
        "발행일": pub_date,
        "pdf_url": pdf_url,
        "detail_url": f"{DETAIL_URL}?nid={nid}" if nid else None,
        "nid": nid,
        "category": "company",
        "collected_at": datetime.now().isoformat(),
    }


def _has_next(soup: BeautifulSoup, current_page: int) -> bool:
    pagination = soup.select_one("div.paging, table.Nnavi")
    if not pagination:
        return False
    for a in pagination.select("a"):
        m = re.search(r"page=(\d+)", a.get("href", ""))
        if m and int(m.group(1)) > current_page:
            return True
    return False


# ── 단일 페이지 HTML 파싱 (asyncio.to_thread 용) ──────────────────────

def _parse_page_html(html: str, since: date, today: date) -> tuple[list[dict], bool]:
    """HTML 문자열을 받아 since 이상인 메타 리스트를 반환.
    Returns: (metas, has_more) — has_more는 이 페이지 이후에도 데이터가 더 있는지.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = _parse_table_rows(soup)
    if not rows:
        return [], False

    metas: list[dict] = []
    for tr in rows:
        meta = _parse_row(tr)
        if not meta:
            continue
        pub_date = meta.get("publish_date")
        meta_date = datetime.strptime(pub_date, "%Y-%m-%d").date() if pub_date else None
        if meta_date is None:
            continue
        if meta_date < since:
            # 이 페이지부터 과거 데이터 진입 → 이후 페이지 없음
            return metas, False
        if since <= meta_date <= today:
            if meta.get("증권사") == "신한투자증권":
                continue
            metas.append(meta)

    # 이 페이지 끝까지 since 범위 안 → 다음 페이지 확인 필요
    return metas, _has_next(soup, 0)  # current_page=0 → 무조건 다음 있으면 True


# ── 병렬 비동기 수집 (Phase 1 백필용) ─────────────────────────────────

async def fetch_all_since_async(
    since: date,
    page_batch: int = 20,
    max_conn: int = 30,
) -> list[dict]:
    """aiohttp로 페이지를 병렬 페칭하며 since ~ 오늘까지 모든 리포트 수집.

    page_batch: 한 번에 동시 요청할 페이지 수 (기본 20)
    max_conn: TCP 커넥션 풀 크기 (기본 30)

    1369페이지 기준: 69라운드 × ~0.3초 = 약 20~25초 (순차 770초 → 30배 이상 단축)
    """
    today = date.today()
    all_metas: list[dict] = []
    page = 1
    round_num = 0
    t0 = time.perf_counter()

    connector = aiohttp.TCPConnector(limit=max_conn + 5, limit_per_host=max_conn)
    async with aiohttp.ClientSession(
        headers={
            "User-Agent": USER_AGENTS[0],
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        },
        connector=connector,
    ) as session:

        async def _fetch_one(p: int) -> tuple[int, Optional[str]]:
            """단일 페이지 HTML 가져오기"""
            url = f"{BASE_URL}?page={p}"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=25)) as resp:
                    if resp.status != 200:
                        logger.debug("페이지 %d: HTTP %d", p, resp.status)
                        return p, None
                    # 네이버 실제 인코딩 감지
                    raw = await resp.read()
                    # 간단한 인코딩 추론
                    try:
                        text = raw.decode("euc-kr")
                    except UnicodeDecodeError:
                        text = raw.decode("utf-8", errors="replace")
                    return p, text
            except Exception as e:
                logger.debug("페이지 %d 요청 실패: %s", p, e)
                return p, None

        while True:
            pages_in_batch = list(range(page, page + page_batch))
            round_num += 1

            # 병렬 페치
            html_results = await asyncio.gather(*[_fetch_one(p) for p in pages_in_batch])

            # 페이지 순서대로 파싱 (순서 보장)
            done = False
            batch_count = 0
            for p, html in sorted(html_results, key=lambda x: x[0]):
                if html is None:
                    if p == page:  # 첫 페이지 실패면 중단
                        done = True
                        break
                    continue

                metas, has_more = await asyncio.to_thread(_parse_page_html, html, since, today)
                all_metas.extend(metas)
                batch_count += 1

                if not has_more:
                    done = True
                    break

            count = len(all_metas)
            elapsed = time.perf_counter() - t0
            logger.info("라운드 %d (페이지 %d-%d) → 누적 %d건 (%.1f초)",
                         round_num, page, page + len(pages_in_batch) - 1, count, elapsed)

            if done:
                break

            page += page_batch
            # 배치 간 짧은 지연 (서버 부하 분산)
            await asyncio.sleep(0.1)

    elapsed = time.perf_counter() - t0
    total_pages = page + pages_in_batch[-1] - 1 if page >= 2 else 1
    logger.info("fetch_all_since(%s) → %d라운드 %d건 (%.1f초)",
                 since.isoformat(), round_num, len(all_metas), elapsed)
    return all_metas


# ── 수집기 (개별일 용, 순차) ──────────────────────────────────────────

class NaverCollector:
    """네이버 금융 리서치 리포트 수집기 (개별일 + 순차 백필)"""

    def __init__(self):
        self.session = _build_session()

    def fetch_list(self, target_date: Optional[date] = None) -> list[dict]:
        """지정일의 리포트 목록 수집 (당일: None)"""
        target = target_date or date.today()
        results: list[dict] = []
        page = 1

        while True:
            url = f"{BASE_URL}?page={page}"
            try:
                resp = self.session.get(url, timeout=30)
                resp.encoding = resp.apparent_encoding or "euc-kr"
            except Exception as e:
                logger.error("페이지 %d 요청 실패: %s", page, e)
                break

            if resp.status_code != 200:
                logger.error("HTTP %d (page=%d)", resp.status_code, page)
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            rows = _parse_table_rows(soup)
            if not rows:
                break

            reached_past = False
            for tr in rows:
                meta = _parse_row(tr)
                if not meta:
                    continue
                pub_date = meta.get("publish_date")
                meta_date = datetime.strptime(pub_date, "%Y-%m-%d").date() if pub_date else None
                if meta_date is None:
                    continue
                if meta_date == target:
                    if meta.get("증권사") == "신한투자증권":
                        continue
                    results.append(meta)
                elif meta_date < target:
                    reached_past = True
                    break

            if reached_past:
                break
            if not _has_next(soup, page):
                break

            page += 1
            time.sleep(0.3)

        logger.info("네이버 %s → %d건", target.isoformat(), len(results))
        return results

    def fetch_all_since(self, since: date) -> list[dict]:
        """순차 페이지 순회하며 since ~ 오늘까지 수집 (동기, 레거시)"""
        today = date.today()
        results: list[dict] = []
        page = 1
        pages_fetched = 0

        while True:
            url = f"{BASE_URL}?page={page}"
            try:
                resp = self.session.get(url, timeout=30)
                resp.encoding = resp.apparent_encoding or "euc-kr"
            except Exception as e:
                logger.error("페이지 %d 요청 실패: %s", page, e)
                break

            if resp.status_code != 200:
                logger.error("HTTP %d (page=%d)", resp.status_code, page)
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            rows = _parse_table_rows(soup)
            if not rows:
                break

            pages_fetched += 1
            reached_past = False
            for tr in rows:
                meta = _parse_row(tr)
                if not meta:
                    continue
                pub_date = meta.get("publish_date")
                meta_date = datetime.strptime(pub_date, "%Y-%m-%d").date() if pub_date else None
                if meta_date is None:
                    continue
                if meta_date < since:
                    reached_past = True
                    break
                if since <= meta_date <= today:
                    if meta.get("증권사") == "신한투자증권":
                        continue
                    results.append(meta)

            if reached_past:
                break
            if not _has_next(soup, page):
                break

            page += 1
            if pages_fetched % 10 == 0:
                logger.info("페이지 %d 진행 중... (누적 %d건)", page, len(results))
            time.sleep(0.15)

        logger.info("fetch_all_since(%s) → %d페이지 %d건", since.isoformat(), page, len(results))
        return results

    # ── PDF 다운로드 ──────────────────────────────────────────────────
    def download_pdf(self, url: str) -> Optional[bytes]:
        import random
        time.sleep(random.uniform(1.5, 3.0))
        try:
            resp = self.session.get(url, timeout=60)
            if resp.status_code != 200:
                logger.error("PDF 다운로드 실패 HTTP %d: %s", resp.status_code, url)
                return None
            return resp.content
        except Exception as e:
            logger.error("PDF 다운로드 예외: %s → %s", url, e)
            return None

    def collect_today(self) -> list[dict]:
        return self.fetch_list(date.today())

    def collect_date(self, d: date) -> list[dict]:
        return self.fetch_list(d)
