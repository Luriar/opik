"""
신한투자증권 기업분석 리포트 수집 (로컬 실행)
- 수집 범위: 특정 날짜 range
- 실행: python extract/extract_신한투자증권.py --start-date 2026-06-01 --end-date 2026-06-11
- 적재: 팀 공용(타 계정) S3 버킷 — .env의 자격증명 사용
"""
import argparse
import os
import re
import time
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

import requests
import boto3
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")   # .env에서 AWS 키 / 버킷명 로드

# ── 고정 설정 ──────────────────────────────────────────────
LIST_URL = "https://bbs2.shinhansec.com/bbs/list/gicompanyanalyst"
S3_BUCKET = os.getenv("S3_BUCKET", "s3-opik-bucket")  # 팀 공용 버킷 이름
BROKER_NAME = "신한투자증권"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Referer": "https://www.shinhansec.com/siw/insights/industry/gicompanyanalyst/view.do",
}

# 타 계정 버킷이 "버킷 정책으로 내 계정에 쓰기 허용"한 방식이면 아래 ACL 필요할 수 있음
# (버킷 소유자가 객체에 접근 못 하는 문제 방지. 버킷의 Object Ownership이
#  'Bucket owner enforced'면 ACL 자체가 무시되므로 빈 dict 그대로 두면 됨)
PUT_EXTRA = {}   # 필요 시: {"ACL": "bucket-owner-full-control"}


def normalize_date(value):
    """YYYY-MM-DD 또는 YYYY.MM.DD를 신한 API 형식(YYYY.MM.DD)으로 정규화."""
    for fmt in ("%Y-%m-%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y.%m.%d")
        except ValueError:
            pass
    raise ValueError(f"날짜 형식 오류: {value} (YYYY-MM-DD 또는 YYYY.MM.DD)")


def parse_args():
    yesterday = datetime.now() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="신한투자증권 기업분석 리포트 수집")
    parser.add_argument(
        "--start-date",
        default=yesterday.strftime("%Y.%m.%d"),
        help="수집 시작일(포함). YYYY-MM-DD 또는 YYYY.MM.DD. 기본값: 어제",
    )
    parser.add_argument(
        "--end-date",
        default=yesterday.strftime("%Y.%m.%d"),
        help="수집 종료일(포함). YYYY-MM-DD 또는 YYYY.MM.DD. 기본값: 어제",
    )
    args = parser.parse_args()
    args.start_date = normalize_date(args.start_date)
    args.end_date = normalize_date(args.end_date)
    if args.start_date > args.end_date:
        raise ValueError("start-date는 end-date보다 늦을 수 없습니다.")
    return args


def get_json(sess, params):
    resp = sess.get(LIST_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def extract_stock_code(title):
    """제목에서 종목코드 추출. 예: '(005930) 삼성전자' → '005930'"""
    m = re.search(r'\((\d{6})\)', title or "")
    return m.group(1) if m else "000000"


def make_report_id(item, reg_date):
    stock_code = extract_stock_code(item.get('f1', ''))
    return hashlib.md5(
        f"{BROKER_NAME}_{stock_code}_{item.get('f1', '')}_{reg_date}".encode()
    ).hexdigest()


def main():
    args = parse_args()
    s3 = boto3.client("s3")
    sess = requests.Session()
    sess.headers.update(HEADERS)

    manifest = []
    seen_ids = set()        # 페이지네이션 오작동(같은 페이지 반복) 감지용
    seen_report_ids = {}
    cur_page, start_page, start_id = 1, 1, None
    stop = False

    while not stop:
        # 신한 프론트 JS와 동일: curPage/startPage를 같이 증가시키고 startId는 매 응답에서 갱신
        params = {
            "v": int(time.time() * 1000),
            "curPage": cur_page,
            "startPage": start_page,
        }
        if start_id:
            params["startId"] = start_id

        try:
            data = get_json(sess, params)
        except Exception as e:
            print(f"목록 요청 실패: page={cur_page}, startPage={start_page}, startId={start_id}: {e}")
            break

        items = data.get("list", [])

        # 종료 조건 1: 빈 페이지 / 종료 조건 2: 첫 항목이 이미 본 것 (페이지가 안 넘어감)
        if not items or items[0].get("fn") in seen_ids:
            print(f"페이지 {cur_page}: 더 이상 새 항목 없음 → 종료")
            break

        for it in items:
            fn = it.get("fn")
            if fn in seen_ids:
                continue
            seen_ids.add(fn)

            reg_date = it.get("f0", "")          # "2026.06.11" — 문자열 비교 가능
            if reg_date > args.end_date:
                continue
            if reg_date < args.start_date:
                print(f"[{reg_date}] 시작일({args.start_date}) 이전 도달 → 수집 종료")
                stop = True
                break

            report_id = make_report_id(it, reg_date)
            if report_id in seen_report_ids:
                print(
                    f"  중복 report_id 경고: {report_id} "
                    f"(이전 fn={seen_report_ids[report_id]}, 현재 fn={fn})"
                )
            seen_report_ids[report_id] = fn

            # PDF 다운로드 (f3 = 직접 다운로드 URL)
            s3_key = ""
            pdf_url = it.get("f3", "")
            if pdf_url:
                try:
                    pdf = sess.get(pdf_url, timeout=15)
                    pdf.raise_for_status()
                    if "pdf" in pdf.headers.get("Content-Type", "").lower():
                        s3_key = (
                            f"bronze/{BROKER_NAME}/{reg_date.replace('.', '-')}/"
                            f"{report_id}.pdf"
                        )
                        s3.put_object(Bucket=S3_BUCKET, Key=s3_key,
                                      Body=pdf.content, **PUT_EXTRA)
                    else:
                        print(f"  PDF 아님 [{it.get('f1')}]: {pdf.headers.get('Content-Type')}")
                except Exception as e:
                    print(f"  PDF 실패 [{it.get('f1')}]: {e}")
                time.sleep(1)                    # 서버 부담 완화

            manifest.append({
                "report_id": report_id,
                "source": BROKER_NAME,
                "종목명": it.get("f2", ""),
                "증권사": BROKER_NAME,
                "발행일": reg_date,
                "s3_key": s3_key,
                "파싱상태": "pending" if s3_key else "pdf_missing",
            })

        print(f"페이지 {cur_page} 완료 — 누적 {len(manifest)}건")

        page_ids = data.get("pageInfo", {}).get("pages", [])
        if len(page_ids) > 1:
            start_id = page_ids[1]
        cur_page += 1
        start_page += 1
        time.sleep(1.5)

    # manifest 저장
    key = (
        f"manifest/shinhan_{args.start_date.replace('.', '-')}_"
        f"{args.end_date.replace('.', '-')}_{datetime.now():%Y%m%d%H%M%S}.json"