"""
한국투자증권 기업/산업 리포트 수집 (Playwright, 본문 전체)
- 목록 페이지에서 메타데이터 + 상세 id 추출 → 상세 페이지에서 본문 전체 수집
- PDF 없음(로그인 필요) → 본문 텍스트를 silver에 직접 적재 (파싱상태: text_only)
- 실행: python extract/extract_한국투자증권.py --start-date 2025-06-11 --end-date 2026-06-11
- 사전 설치: pip install playwright beautifulsoup4 boto3 python-dotenv pandas lxml
            playwright install chromium
"""
import argparse
import os
import re
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import boto3
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

# ── 고정 설정 ──────────────────────────────────────────────
BASE = "https://securities.koreainvestment.com/main/research/research"
LIST_URL   = f"{BASE}/Strategy.jsp"
DETAIL_URL = f"{BASE}/StrategyDetail.jsp"
S3_BUCKET = os.getenv("S3_BUCKET", "s3-opik-bucket")
BROKER_NAME = "한국투자증권"
PUT_EXTRA = {}


def extract_stock_code(title):
    """제목에서 종목코드 추출. 예: 'LG생활건강 (051900):...' → '051900'"""
    m = re.search(r'\((\d{6})\)', title)
    return m.group(1) if m else ""


def load_krx_master():
    """KRX 상장법인 종목코드 → 종목명 딕셔너리. 실패 시 빈 딕셔너리."""
    try:
        from io import BytesIO
        import pandas as pd
        resp = requests.post(
            "https://kind.krx.co.kr/corpgeneral/corpList.do",
            data={"method": "download", "searchType": "13"}, timeout=10,
        )
        df = pd.read_html(BytesIO(resp.content), header=0)[0]
        df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)
        print(f"KRX 마스터 로드 완료: {len(df)}개 종목")
        return dict(zip(df["종목코드"], df["회사명"]))
    except Exception as e:
        print(f"KRX 마스터 로드 실패 (종목명 변환 불가): {e}")
        return {}


def normalize_date(value):
    for fmt in ("%Y-%m-%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y.%m.%d")
        except ValueError:
            pass
    raise ValueError(f"날짜 형식 오류: {value}")


def parse_args():
    yesterday = datetime.now() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="한국투자증권 리포트 수집 (본문 전체)")
    parser.add_argument("--start-date", default=yesterday.strftime("%Y.%m.%d"))
    parser.add_argument("--end-date",   default=yesterday.strftime("%Y.%m.%d"))
    parser.add_argument("--headful", action="store_true", help="브라우저 창 표시")
    args = parser.parse_args()
    args.start_date = normalize_date(args.start_date)
    args.end_date   = normalize_date(args.end_date)
    if args.start_date > args.end_date:
        raise ValueError("start-date는 end-date보다 늦을 수 없습니다.")
    return args


def list_url(page):
    return (
        f"{LIST_URL}?jkGubun=10&category_sub=&category1=05&category2=01"
        f"&focusYN=&fromDate=&toDate=&searchDate=all&searchColumn=all"
        f"&searchValue=&rowsPerPages=10&currentPage={page}"
    )


def parse_list(html):
    """목록 한 페이지의 항목(메타 + 상세 id) 추출."""
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for li in soup.select("ul.view_area.line li"):
        a = li.select_one("a.view_con")
        if not a:
            continue
        # onclick="...doDetail('156236')..." 에서 id 추출
        m = re.search(r"doDetail\('(\d+)'\)", a.get("onclick", ""))
        detail_id = m.group(1) if m else ""

        tit_info = li.select_one("span.tit_info")
        parts = tit_info.get_text(" ", strip=True).split() if tit_info else []
        reg_date = parts[-1] if parts else ""

        title    = (li.select_one("span.body_tit") or li).get_text(strip=True)
        category = (li.select_one("div.head") or li).get_text(strip=True)

        items.append({
            "detail_id": detail_id,
            "reg_date":  reg_date,
            "title":     title,
            "category":  category,
        })
    return items


def fetch_detail_body(page, detail_id):
    """상세 페이지 본문 전체 텍스트 추출. 리다이렉트 감지 시 재시도."""
    target = f"{DETAIL_URL}?jkGubun=10&id={detail_id}"
    for attempt in (1, 2):
        try:
            page.goto(target, wait_until="domcontentloaded", timeout=30000)
            # 세션 만료 리다이렉트 감지
            if "install_non_activex" in page.url or "login" in page.url:
                page.go_back()
                page.wait_for_timeout(2000)
                page.goto(target, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_selector("#content", timeout=15000)
            soup = BeautifulSoup(page.content(), "html.parser")
            content = soup.select_one("#content")
            return content.get_text("\n", strip=True) if content else ""
        except Exception as e:
            if attempt == 1:
                page.wait_for_timeout(2000)
            else:
                raise e
    return ""


def main():
    args = parse_args()
    s3   = boto3.client("s3")
    code_to_name = load_krx_master()

    manifest = []
    seen_ids = set()

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=not args.headful)
            page = browser.new_page()
    
            cur_page = 1
            stop = False
            while not stop:
                print(f"목록 페이지 {cur_page} 요청 중...")
                html = None
                for attempt in (1, 2):
                    try:
                        page.goto(list_url(cur_page), wait_until="domcontentloaded", timeout=30000)
                        page.wait_for_selector("ul.view_area.line li", timeout=15000)
                        html = page.content()
                        break
                    except Exception as e:
                        if attempt == 1:
                            print(f"  렌더 지연 → 재시도: {e}")
                            page.wait_for_timeout(2000)
                        else:
                            print(f"목록 페이지 {cur_page}: 항목 없음(마지막 도달 추정) → 종료")
                if html is None:
                    break
    
                items = parse_list(html)
                if not items:
                    print("항목 없음 → 종료")
                    break
    
                for it in items:
                    reg_date = it["reg_date"]
                    if reg_date > args.end_date:
                        continue
                    if reg_date < args.start_date:
                        print(f"[{reg_date}] 시작일({args.start_date}) 이전 도달 → 종료")
                        stop = True
                        break
    
                    # 종목코드 없는 산업/시황 리포트 skip — extract before report_id for uniqueness
                    stock_code = extract_stock_code(it["title"])
                    if not stock_code:
                        continue
    
                    report_id = hashlib.md5(
                        f"{BROKER_NAME}_{stock_code}_{it['title']}_{reg_date}".encode()
                    ).hexdigest()
    
                    if report_id in seen_ids:
                        continue
                    seen_ids.add(report_id)
    
                    # 상세 페이지에서 본문 전체 수집
                    body = ""
                    if it["detail_id"]:
                        try:
                            body = fetch_detail_body(page, it["detail_id"])
                        except Exception as e:
                            print(f"  본문 수집 실패 [{it['title']}]: {e}")
    
                    s3_key = f"silver/한국투자증권/{reg_date.replace('.', '-')}/{report_id}.json"
                    silver = {
                        "report_id": report_id,
                        "source":    BROKER_NAME,
                        "발행일":    reg_date,
                        "제목":      it["title"],
                        "본문":      body,
                        "category":  it["category"],
                    }
                    try:
                        s3.put_object(
                            Bucket=S3_BUCKET, Key=s3_key,
                            Body=json.dumps(silver, ensure_ascii=False).encode(),
                            **PUT_EXTRA,
                        )
                    except Exception as e:
                        print(f"  S3 적재 실패 [{it['title']}]: {e}")
                        s3_key = ""
    
                    manifest.append({
                        "report_id": report_id,
                        "source":    BROKER_NAME,
                        "종목코드":  stock_code,
                        "종목명":    code_to_name.get(stock_code, ""),
                        "증권사":    BROKER_NAME,
                        "발행일":    reg_date,
                        "s3_key":   s3_key,
                        "파싱상태":  "text_only" if s3_key else "pdf_missing",
                    })
    
                print(f"목록 페이지 {cur_page} 완료 — 누적 {len(manifest)}건")
                cur_page += 1
    
        finally:
            browser.close()

    key = (
        f"manifest/한국투자증권_{args.start_date.replace('.', '-')}_"
        f"{args.end_date.replace('.', '-')}_{datetime.now():%Y%m%d%H%M%S}.json"
    )
    try:
        s3.put_object(
            Bucket=S3_BUCKET, Key=key,
            Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode(),
            **PUT_EXTRA,
        )
        print(f"수집 완료: 총 {len(manifest)}건 → s3://{S3_BUCKET}/{key}")
    except Exception as e:
        print(f"manifest 저장 실패: {e}")
        print(json.dumps(manifest[:3], ensure_ascii=F