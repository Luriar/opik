"""OPIK — 한국투자증권 리서치 리포트 수집기

실제 HTML 구조 (2026-06-12 확인):
    <ul class="view_area line">
        <li>
            <a class="view_con" onclick="doDetail('156243')">
                <div class="head blue">기업Note</div>
                <div class="body">
                    <span class="body_tit">삼양식품 (003230):해외로 확장하는 라면시장</span>
                </div>
                <span class="tit_info">
                    <em>최광현</em>
                    <em>2026.06.12</em>
                </span>
            </a>
            <a class="pdf_btn" onclick="prePdfFileView('?category1=05&category2=01','20260611195859857_ko.pdf','01','2026.06.12','N','Y','N')">
        </li>
    </ul>

- 인코딩: EUC-KR
- PDF 다운로드: https://file.truefriend.com/servlet/Download?file_path=research/research05/&file_name=XXXX.pdf
- 페이지네이션: ?jkGubun=10&currentPage=N
- 카테고리 매핑: common_2021.js doFiledownload 함수 참조
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import Optional
from urllib.parse import parse_qs, urlencode

import aiohttp
import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

# ── 상수 ──────────────────────────────────────────────────────────────
BASE_URL = "https://securities.koreainvestment.com"
MAIN_URL = f"{BASE_URL}/main.jsp"
LIST_URL = f"{BASE_URL}/main/research/research/Strategy.jsp"
PDF_HOST = "https://file.truefriend.com"
PDF_SERVLET = f"{PDF_HOST}/servlet/Download"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
]

FILE_SERVER_HOST = "https://file.koreainvestment.com"

# category1 + category2 → filepath 매핑 (common_2021.js doFiledownload)
CATEGORY_FILEPATH_MAP: dict[str, str] = {
    "01_01": "research/research01",
    "01_02": "research/research01",
    "01_03": "research/research01",
    "01_04": "research/research01",
    "01_05": "research/research01",
    "01_06": "research/research12",
    "02_01": "research/research02",
    "02_02": "research/research02",
    "02_03": "research/research02",
    "02_04": "research/research02",
    "02_06": "research/research02",
    "02_08": "research/research02",
    "02_09": "research/research02",
    "02_10": "research/research02",
    "02_11": "research/research02",
    "02_12": "research/research02",
    "02_13": "research/research02",
    "02_14": "research/research02",
    "03_01": "research/research03",
    "03_02": "research/research03",
    "03_03": "research/research03",
    "04_00": "research/research04",
    "04_01": "research/research01",
    "04_02": "research/research01",
    "04_03": "research/research01",
    "05_00": "research/research05",
    "05_01": "research/research05",
    "06_01": "research/research06",
    "06_02": "research/research06",
    "07_01": "research/research07",
    "08_03": "research/research08",
    "08_04": "research/research08",
    "08_05": "research/research08",
    "09_00": "research/research11",
    "10_01": "research/research10",
    "10_04": "research/research10",
    "10_06": "research/research_emailcomment",
    "13_01": "research/research11",
    "14_01": "research/research14",
    "15_01": "research/research01",
    "16_01": "research/research15",
    "17_00": "research/research17",
}

# 분석가명 정규화
ANALYST_MAP: dict[str, str] = {
    "최광현": "choigwanghyun",
    "정명호": "jeongmyungho",
    "김진영": "kimjinyoung",
    "원재민": "wonjaemin",
    "오세웅": "osewoong",
    "백재승": "baekjaeseung",
    "장윤호": "jangyunho",
    "원지민": "wonjimin",
    "이재모": "leejaemo",
    "배성수": "baeseongsu",
    "김성환": "kimseonghwan",
    "박세라": "paksera",
}


def _get_filepath(category1: str, category2: str = "01") -> str:
    """category1, category2 → filepath 매핑"""
    key = f"{category1}_{category2}"
    if key in CATEGORY_FILEPATH_MAP:
        return CATEGORY_FILEPATH_MAP[key]
    # fallback: category1만으로 매핑 시도
    for k, v in CATEGORY_FILEPATH_MAP.items():
        if k.startswith(f"{category1}_"):
            return v
    return f"research/research{category1.zfill(2)}"


def _build_pdf_url(filepath: str, filename: str) -> str:
    """PDF 다운로드 URL 구성 (doFiledownload 로직)"""
    # filepath 예: "?category1=05&category2=01"
    if filepath.startswith("?") or filepath.startswith("&"):
        filepath_clean = filepath.lstrip("?&")
        params = filepath_clean.split("&")
        parsed = {}
        for p in params:
            if "=" in p:
                k, v = p.split("=", 1)
                parsed[k] = v
        cat1 = parsed.get("category1", "05")
        cat2 = parsed.get("category2", "01")
        mapped_path = _get_filepath(cat1, cat2)
    elif filepath.startswith("http"):
        # 절대 URL인 경우
        return f"{filepath}/{filename}"
    else:
        mapped_path = filepath

    filename_enc = filename  # 서블릿이 처리하므로 인코딩 불필요
    return f"{PDF_SERVLET}?file_path={mapped_path}/&file_name={filename_enc}"


# ── 데이터 클래스 ──────────────────────────────────────────────────────

@dataclass
class ReportMeta:
    report_id: str
    source: str = "koreainvest"
    securities_firm: str = "koreainv"
    title: str = ""
    analyst: Optional[str] = None
    publish_date: Optional[str] = None
    pdf_url: Optional[str] = None
    detail_id: Optional[str] = None
    category: str = "company"
    category_head: str = ""
    pdf_s3_key: Optional[str] = None
    collected_at: str = ""

    def to_dict(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v is not None}


# ── 세션 / HTTP 유틸 ─────────────────────────────────────────────────

def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENTS[0],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })
    return s


def _init_session(session: requests.Session) -> bool:
    """세션 초기화 — main.jsp 방문하여 쿠키 획득"""
    try:
        resp = session.get(MAIN_URL, timeout=30)
        resp.encoding = resp.apparent_encoding or "euc-kr"
        return resp.status_code == 200
    except Exception as e:
        logger.error("세션 초기화 실패: %s", e)
        return False


# ── HTML 파싱 ──────────────────────────────────────────────────────────

def _parse_report_items(html: str) -> list[dict]:
    """Strategy.jsp HTML에서 리포트 목록 추출"""
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict] = []

    # 리포트 리스트 영역 찾기
    view_area = soup.select_one("ul.view_area.line")
    if not view_area:
        logger.debug("view_area.line not found")
        return items

    lis = view_area.select("li")
    for li in lis:
        try:
            item = _parse_single_item(li)
            if item:
                items.append(item)
        except Exception as e:
            logger.debug("아이템 파싱 오류: %s", e)
            continue

    return items


def _parse_single_item(li: Tag) -> Optional[dict]:
    """<li> 하나에서 리포트 정보 추출"""
    # 상세보기 링크 → detail_id
    view_con = li.select_one("a.view_con")
    if not view_con:
        return None

    onclick = view_con.get("onclick", "")
    detail_id = None
    m = re.search(r"doDetail\('(\d+)'\)", onclick)
    if m:
        detail_id = m.group(1)

    # 카테고리 헤드
    head_div = view_con.select_one("div.head")
    head_text = head_div.get_text(strip=True) if head_div else ""

    # 제목
    title_span = view_con.select_one("span.body_tit")
    title = title_span.get_text(strip=True) if title_span else ""
    if not title:
        return None

    # 분석가, 날짜
    info_spans = view_con.select("span.tit_info em")
    analyst = info_spans[0].get_text(strip=True) if len(info_spans) >= 1 else None
    pub_date_raw = info_spans[-1].get_text(strip=True) if len(info_spans) >= 2 else None

    # 날짜 변환: 2026.06.12 → 2026-06-12
    pub_date = None
    if pub_date_raw:
        m = re.match(r"(\d{4})\.(\d{2})\.(\d{2})", pub_date_raw)
        if m:
            pub_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # PDF 파라미터
    pdf_btn = li.select_one("a.pdf_btn")
    pdf_filepath = None
    pdf_filename = None
    if pdf_btn:
        onclick_pdf = pdf_btn.get("onclick", "")
        # prePdfFileView('?category1=05&category2=01','20260611195859857_ko.pdf','01','2026.06.12','N','Y','N')
        m = re.search(
            r"(?:prePdfFileView|pdfFileView)\s*\(\s*'([^']*)'\s*,\s*'([^']*)'",
            onclick_pdf,
        )
        if m:
            pdf_filepath = m.group(1)
            pdf_filename = m.group(2)

    # PDF URL 구성
    pdf_url = None
    if pdf_filepath and pdf_filename:
        pdf_url = _build_pdf_url(pdf_filepath, pdf_filename)

    # report_id 생성 (detail_id 기반)
    seed = f"koreainvest|{detail_id or title}|{pub_date or ''}"
    report_id = hashlib.sha256(seed.encode()).hexdigest()[:16]

    return {
        "report_id": report_id,
        "source": "한국투자증권",
        "securities_firm": "koreainv",
        "증권사": "한국투자증권",
        "title": title,
        "analyst": analyst or "",
        "publish_date": pub_date,
        "발행일": pub_date,
        "pdf_url": pdf_url,
        "detail_id": detail_id,
        "category": "company",
        "category_head": head_text,
        "pdf_filepath": pdf_filepath,
        "pdf_filename": pdf_filename,
        "collected_at": datetime.now().isoformat(),
    }


def _get_total_count(html: str) -> int:
    """전체 리포트 건수 추출"""
    m = re.search(r"전체건수\s*<span>(\d+)</span>건", html)
    if m:
        return int(m.group(1))
    return 0


def _get_last_page(html: str) -> int:
    """마지막 페이지 번호 추출"""
    pages = re.findall(r"goPage\('?(\d+)'?\)", html)
    if pages:
        return max(int(p) for p in pages)
    return 1


# ── 동기 수집기 ────────────────────────────────────────────────────────

class KoreaInvestCollector:
    """한국투자증권 리서치 리포트 수집기"""

    def __init__(self):
        self.session = _build_session()

    def _ensure_session(self) -> bool:
        """세션 유효성 확인 및 재초기화"""
        try:
            # 간단히 main.jsp 호출로 세션 확인
            resp = self.session.get(MAIN_URL, timeout=15)
            return resp.status_code == 200
        except Exception:
            return _init_session(self.session)

    def fetch_page(self, page: int = 1, jk_gubun: int = 10) -> tuple[list[dict], int, int]:
        """특정 페이지의 리포트 목록 수집

        Returns:
            (items, total_count, last_page)
        """
        url = f"{LIST_URL}?jkGubun={jk_gubun}&currentPage={page}"
        try:
            resp = self.session.get(url, timeout=30)
            resp.encoding = resp.apparent_encoding or "euc-kr"
        except Exception as e:
            logger.error("페이지 %d 요청 실패: %s", page, e)
            return [], 0, 0

        if resp.status_code != 200:
            logger.error("HTTP %d (page=%d)", resp.status_code, page)
            return [], 0, 0

        html = resp.text

        # 에러 페이지 체크
        if "BoxError" in html or "조회가 불가능" in html:
            # 세션 재초기화 후 재시도
            logger.warning("에러 페이지 감지, 세션 재초기화...")
            _init_session(self.session)
            try:
                resp = self.session.get(url, timeout=30)
                resp.encoding = resp.apparent_encoding or "euc-kr"
                html = resp.text
            except Exception:
                return [], 0, 0

        total = _get_total_count(html)
        last_page = _get_last_page(html)
        items = _parse_report_items(html)

        return items, total, last_page

    def fetch_list(
        self,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        jk_gubun: int = 10,
    ) -> list[dict]:
        """지정 기간의 리포트 전체 수집 (자동 페이지네이션)

        Args:
            from_date: 시작일 (YYYY.MM.DD 형식)
            to_date: 종료일 (YYYY.MM.DD 형식)
            jk_gubun: 리서치 구분 (10=기업/산업)
        """
        results: list[dict] = []
        page = 1
        seen_ids: set[str] = set()

        # fromDate/toDate가 있으면 searchForm 파라미터로 추가
        extra_params = ""
        if from_date:
            extra_params += f"&fromDate={from_date}"
        if to_date:
            extra_params += f"&toDate={to_date}"

        while True:
            url = f"{LIST_URL}?jkGubun={jk_gubun}&currentPage={page}{extra_params}"
            try:
                resp = self.session.get(url, timeout=30)
                resp.encoding = resp.apparent_encoding or "euc-kr"
            except Exception as e:
                logger.error("페이지 %d 요청 실패: %s", page, e)
                break

            if resp.status_code != 200:
                logger.error("HTTP %d (page=%d)", resp.status_code, page)
                break

            html = resp.text
            if "BoxError" in html:
                logger.warning("세션 만료, 재초기화...")
                _init_session(self.session)
                continue

            items = _parse_report_items(html)
            if not items:
                break

            # 중복 제거
            new_items = []
            for item in items:
                rid = item["report_id"]
                if rid not in seen_ids:
                    seen_ids.add(rid)
                    new_items.append(item)

            results.extend(new_items)

            total = _get_total_count(html)
            last_page = _get_last_page(html)

            logger.info("페이지 %d/%d → %d건 (누적 %d건, 전체 %d건)",
                         page, last_page, len(new_items), len(results), total)

            if page >= last_page:
                break

            page += 1
            time.sleep(0.3)

        logger.info("한국투자증권 수집 완료: %d건", len(results))
        return results

    def download_pdf(self, url: str) -> Optional[bytes]:
        """PDF 다운로드 (file.truefriend.com 서블릿)"""
        import random
        time.sleep(random.uniform(0.5, 1.5))
        try:
            resp = self.session.get(url, timeout=60)
            if resp.status_code != 200:
                logger.error("PDF 다운로드 실패 HTTP %d: %s", resp.status_code, url[:100])
                return None
            data = resp.content
            if len(data) < 1024:
                return None
            return data
        except Exception as e:
            logger.error("PDF 다운로드 예외: %s → %s", url[:100], e)
            return None

    def close(self):
        self.session.close()


# ── 비동기 병렬 수집 (백필용) ─────────────────────────────────────────

async def _fetch_page_async(
    session: aiohttp.ClientSession,
    page: int,
    jk_gubun: int = 10,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    search_date: str = "all",
) -> Optional[str]:
    """비동기 단일 페이지 HTML 가져오기"""
    url = f"{LIST_URL}?jkGubun={jk_gubun}&currentPage={page}&searchDate={search_date}"
    if from_date:
        url += f"&fromDate={from_date}"
    if to_date:
        url += f"&toDate={to_date}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                return None
            raw = await resp.read()
            try:
                text = raw.decode("euc-kr")
            except UnicodeDecodeError:
                text = raw.decode("utf-8", errors="replace")
            return text
    except Exception as e:
        logger.debug("비동기 페이지 %d 요청 실패: %s", page, e)
        return None


async def _parse_page_async(html: str) -> list[dict]:
    """비동기 HTML 파싱 (스레드 풀 사용)"""
    return await asyncio.to_thread(_parse_report_items, html)


async def fetch_all_async(
    jk_gubun: int = 10,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    search_date: str = "all",
    page_batch: int = 15,
    max_conn: int = 20,
) -> list[dict]:
    """비동기 병렬 페이지 수집 — 전체 리포트 수집

    Args:
        jk_gubun: 리서치 구분 (10=기업/산업)
        from_date: 시작일 (YYYY.MM.DD 형식, 선택)
        to_date: 종료일 (YYYY.MM.DD 형식, 선택)
        search_date: 기간 프리셋 (all=전체, 3month=3개월, week=1주 등)
        page_batch: 동시 요청할 페이지 수
        max_conn: 최대 TCP 커넥션
    """
    t0 = time.perf_counter()
    all_items: list[dict] = []
    seen_ids: set[str] = set()
    page = 1
    round_num = 0

    # 쿠키 파일 사용
    cookie_jar = aiohttp.CookieJar()
    connector = aiohttp.TCPConnector(limit=max_conn + 5, limit_per_host=max_conn)

    async with aiohttp.ClientSession(
        headers={
            "User-Agent": USER_AGENTS[0],
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        },
        connector=connector,
        cookie_jar=cookie_jar,
    ) as session:
        # 세션 초기화: main.jsp 방문
        try:
            async with session.get(MAIN_URL, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                await resp.read()
        except Exception as e:
            logger.error("비동기 세션 초기화 실패: %s", e)

        last_page = None

        while True:
            pages_in_batch = list(range(page, page + page_batch))
            round_num += 1

            html_results = await asyncio.gather(
                *[_fetch_page_async(session, p, jk_gubun, from_date, to_date, search_date) for p in pages_in_batch]
            )

            # 페이지 순서대로 파싱
            for p, html in zip(pages_in_batch, html_results):
                if html is None:
                    if p == page:  # 첫 페이지 실패 → 중단
                        logger.error("첫 페이지(%d) 실패로 중단", p)
                        return all_items
                    continue

                if "BoxError" in html:
                    logger.warning("페이지 %d 에러, 세션 재시도...", p)
                    # 세션 재초기화
                    try:
                        async with session.get(MAIN_URL, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                            await resp.read()
                    except Exception:
                        pass
                    # 재시도
                    html = await _fetch_page_async(session, p, jk_gubun)
                    if html is None or "BoxError" in html:
                        continue

                # 마지막 페이지 확인 (첫 배치에서만)
                if last_page is None:
                    total = _get_total_count(html)
                    lp = _get_last_page(html)
                    if lp > 0:
                        last_page = lp
                        logger.info("전체 %d건, %d페이지", total, last_page)

                items = await _parse_page_async(html)
                for item in items:
                    rid = item["report_id"]
                    if rid not in seen_ids:
                        seen_ids.add(rid)
                        all_items.append(item)

            elapsed = time.perf_counter() - t0
            logger.info("라운드 %d (페이지 %d-%d) → 누적 %d건 (%.1f초)",
                         round_num, page, pages_in_batch[-1], len(all_items), elapsed)

            if last_page and page + page_batch > last_page:
                break

            page += page_batch
            await asyncio.sleep(0.1)

    elapsed = time.perf_counter() - t0
    logger.info("fetch_all_async → %d건, %d라운드 (%.1f초)", len(all_items), round_num, elapsed)
    return all_items


async def download_pdf_async(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    url: str,
    retries: int = 2,
) -> Optional[bytes]:
    """비동기 PDF 다운로드"""
    for attempt in range(retries + 1):
        try:
            async with semaphore:
                await asyncio.sleep(0.05)
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    if resp.status != 200:
                        if attempt < retries:
                            await asyncio.sleep(1.5 * (2 ** attempt))
                            continue
                        return None
                    data = await resp.read()
                    if len(data) < 1024:
                        if attempt < retries:
                            continue
                        return None
                    return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < retries:
                await asyncio.sleep(1.5 * (2 ** attempt))
                continue
            logger.debug("PDF 다운로드 실패: %s → %s", url[:80], e)
            return None
    return None


async def download_all_pdfs_async(
    items: list[dict],
    workers: int = 15,
) -> dict[str, Optional[bytes]]:
    """비동기 병렬 PDF 다운로드"""
    semaphore = asyncio.Semaphore(workers)
    connector = aiohttp.TCPConnector(limit=workers + 10, limit_per_host=workers + 5)

    # PDF 다운로드는 별도 세션 필요 없음
    async with aiohttp.ClientSession(
        headers={
            "User-Agent": USER_AGENTS[0],
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
        },
        connector=connector,
    ) as session:
        tasks = {}
        for item in items:
            url = item.get("pdf_url")
            if url:
                tasks[item["report_id"]] = download_pdf_async(session, semaphore, url)

        if not tasks:
            return {}

        results = dict(zip(tasks.keys(), await asyncio.gather(*tasks.values())))
    return results
