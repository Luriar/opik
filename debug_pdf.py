"""진단: 2020년 vs 2024년 Naver 리포트 PDF URL 비교

1. S3 manifest 확인 (오래된 날짜의 pdf_url 값)
2. Naver HTML에서 td[3] raw 확인 (사용자 PC에서 직접)

사용법: python debug_pdf.py
"""
import json
import os
import re
import sys

import boto3
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET", "s3-opik-bucket")
S3_REGION = os.getenv("S3_REGION", "ap-northeast-2")
s3 = boto3.client("s3", region_name=S3_REGION)


def check_s3_manifest(date_str: str, firm: str):
    """S3에서 해당 날짜/증권사의 manifest 확인"""
    key = f"bronze/{firm}/{date_str}/_manifest.json"
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = json.loads(resp["Body"].read().decode("utf-8"))
        print(f"\n=== S3 manifest: {key} ===")
        print(f"  총 {len(data)}건")
        for entry in data[:3]:
            rid = entry.get("report_id", "?")[:8]
            status = entry.get("파싱상태", "?")
            s3k = entry.get("s3_key", "null")
            print(f"  [{rid}...] 상태={status} s3_key={s3k}")
    except s3.exceptions.NoSuchKey:
        print(f"\n  [없음] {key}")
    except Exception as e:
        print(f"\n  [오류] {key}: {e}")


def check_naver_html():
    """Naver HTML에서 td[3] PDF 컬럼 직접 확인"""
    UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    HEADERS = {"User-Agent": UA}

    pages = [
        ("https://finance.naver.com/research/company_list.naver?page=1", "최신 (page 1)"),
        ("https://finance.naver.com/research/company_list.naver?page=500", "중간 (page 500)"),
        ("https://finance.naver.com/research/company_list.naver?page=1300", "오래된 (page 1300)"),
    ]

    seen_domains = set()
    for url, label in pages:
        try:
            resp = requests.get(url, headers=HEADERS)
            resp.encoding = "euc-kr"
        except Exception as e:
            print(f"\n{label}: 요청 실패 - {e}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.select_one("table.type_1") or soup.select_one("table")
        if not table:
            print(f"\n{label}: 테이블 없음")
            continue

        rows = table.select("tr")
        data_rows = [tr for tr in rows if tr.select("td") and len(tr.select("td")) >= 5]

        print(f"\n=== {label} ({len(data_rows)} rows) ===")

        stock_pstatic = 0
        other_domain = 0
        no_a_tag = 0
        no_href = 0

        for tr in data_rows[:15]:
            tds = tr.select("td")
            pdf_td = tds[3]
            pdf_a = pdf_td.select_one("a")

            if pdf_a:
                href = pdf_a.get("href", "").strip()
                if href and not href.startswith("#") and not href.startswith("javascript:"):
                    if "stock.pstatic.net" in href:
                        stock_pstatic += 1
                    else:
                        other_domain += 1
                        domain = re.search(r'https?://([^/]+)', href)
                        dom = domain.group(1) if domain else "?"
                        if dom not in seen_domains:
                            seen_domains.add(dom)
                            date_raw = tds[4].get_text(strip=True)
                            print(f"  새 도메인: {dom} | 날짜={date_raw} | {href[:120]}")
                else:
                    no_href += 1
            else:
                no_a_tag += 1

        print(f"  stock.pstatic.net: {stock_pstatic}")
        print(f"  다른 CDN/도메인: {other_domain}")
        print(f"  a태그 없음: {no_a_tag}")
        print(f"  a태그 있으나 href 없음: {no_href}")

        # td[3] raw HTML 샘플 (처음 3개)
        print(f"  td[3] raw HTML 샘플:")
        for i, tr in enumerate(data_rows[:3]):
            td_html = str(tr.select("td")[3])
            date_raw = tr.select("td")[4].get_text(strip=True)
            firm = tr.select("td")[2].get_text(strip=True)
            title = tr.select("td")[1].get_text(strip=True)[:40]
            print(f"    [{date_raw}] {firm}: {title}")
            print(f"      td[3]: {td_html[:200]}")

    if seen_domains:
        print(f"\n=== 발견된 새 CDN 도메인 ({len(seen_domains)}개) ===")
        for d in sorted(seen_domains):
            print(f"  {d}")


if __name__ == "__main__":
    print("=" * 60)
    print("1. S3 Manifest 확인 (오래된 날짜)")
    print("=" * 60)
    # 오래된 날짜의 주요 증권사 manifest 확인
    check_s3_manifest("2020-06-15", "미래에셋증권")
    check_s3_manifest("2022-01-10", "키움증권")
    check_s3_manifest("2023-06-15", "삼성증권")
    check_s3_manifest("2024-05-16", "미래에셋증권")  # 작동 시작 지점
    check_s3_manifest("2025-06-10", "미래에셋증권")  # 확실히 작동하는 날짜

    print("\n" + "=" * 60)
    print("2. Naver HTML 직접 확인")
    print("=" * 60)
    check_naver_html()
