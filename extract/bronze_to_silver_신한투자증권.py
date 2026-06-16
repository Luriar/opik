"""
신한투자증권 Bronze → Silver 변환 (누락분 보완)
- 배경: 팀원의 extract_silver.py는 bronze/{증권사}/{날짜}/_manifest.json 을 디스커버리하는데,
        신한투자증권은 manifest/신한투자증권_*.json (통파일) 구조라 파이프라인에서 누락됨.
- 입력:  s3://{BUCKET}/bronze/신한투자증권/{YYYY-MM-DD}/{report_id}.pdf
- 메타:  s3://{BUCKET}/manifest/ 의 신한 manifest (report_id → 종목명 매핑)
- 출력:  s3://{BUCKET}/silver/신한투자증권/{YYYY-MM-DD}/{report_id}.json
- 스키마/판정 기준: HOW_BRONZE_TO_SILVER_WORKS.md 명세 준수
  (PyMuPDF, 타임아웃 30초, 페이지별 fallback, OCR 판정: text<200자 or 텍스트 페이지<30%)

실행:
  python extract/bronze_to_silver_신한투자증권.py --workers 20
  python extract/bronze_to_silver_신한투자증권.py --dry-run
사전 설치: pip install boto3 pymupdf python-dotenv
"""
import os
import re
import io
import json
import argparse
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import fitz  # PyMuPDF
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

S3_BUCKET     = os.getenv("S3_BUCKET", "s3-opik-bucket")
BROKER        = "신한투자증권"
BRONZE_PREFIX = f"bronze/{BROKER}/"
SILVER_PREFIX = f"silver/{BROKER}/"
OCR_PREFIX    = "silver/_ocr_needed/"
PDF_TIMEOUT   = 30  # 초


# ──────────────────────────────────────────────────────────
# 1. 메타데이터 로드 (manifest/ 통파일에서 report_id → 종목명)
# ──────────────────────────────────────────────────────────
def load_shinhan_manifests(s3):
    """manifest/ 아래 신한 manifest(신한투자증권_* 또는 shinhan_*)를 모두 읽어
    report_id → {종목명, 발행일} 매핑 반환. 중복 시 나중 파일이 덮어씀."""
    meta = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="manifest/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            fname = key.rsplit("/", 1)[-1]
            if not (fname.startswith("신한투자증권_") or fname.startswith("shinhan_")):
                continue
            try:
                rows = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
                for r in rows:
                    meta[r["report_id"]] = {
                        "종목명": r.get("종목명", ""),
                        "발행일": r.get("발행일", ""),
                    }
            except Exception as e:
                print(f"  manifest 읽기 실패 ({key}): {e}")
    print(f"신한 manifest 로드: {len(meta)}건의 report_id 메타 확보")
    return meta


# ──────────────────────────────────────────────────────────
# 2. 처리 대상 디스커버리 (bronze PDF 리스팅 - silver 기존분 제외)
# ──────────────────────────────────────────────────────────
def list_keys(s3, prefix, suffix):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(suffix):
                keys.append(obj["Key"])
    return keys


def discover_targets(s3):
    """bronze PDF 중 silver JSON이 아직 없는 것만 반환."""
    bronze = list_keys(s3, BRONZE_PREFIX, ".pdf")
    silver = set(list_keys(s3, SILVER_PREFIX, ".json"))
    print(f"Bronze PDF: {len(bronze)}건 / 기존 Silver: {len(silver)}건")

    targets = []
    for pdf_key in bronze:
        # bronze/신한투자증권/2026-05-13/abc.pdf → silver/신한투자증권/2026-05-13/abc.json
        silver_key = pdf_key.replace(BRONZE_PREFIX, SILVER_PREFIX, 1)[:-4] + ".json"
        if silver_key not in silver:
            targets.append((pdf_key, silver_key))
    print(f"변환 대상(미처리): {len(targets)}건")
    return targets


# ──────────────────────────────────────────────────────────
# 3. PDF → 텍스트 (MD 명세: 타임아웃 + 페이지별 fallback + OCR 판정)
# ──────────────────────────────────────────────────────────
def extract_pdf_text(pdf_bytes):
    """(text, pages_total, pages_with_text, title) 반환."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages_total = len(doc)
    texts, pages_with_text = [], 0
    for page in doc:
        try:
            t = page.get_text()
        except Exception:
            t = ""  # 페이지 단위 fallback: 죽는 페이지는 빈 텍스트로
        if t.strip():
            pages_with_text += 1
        texts.append(t)
    title = (doc.metadata or {}).get("title", "") or ""
    doc.close()

    text = "\n\n".join(texts).strip()
    if not title and text:
        title = text.split("\n", 1)[0][:100].strip()  # 폴백: 첫 줄
    return text, pages_total, pages_with_text, title


def extract_with_timeout(pdf_bytes):
    """PyMuPDF가 간혹 행에 빠지는 PDF 대비 별도 스레드 + 30초 타임아웃."""
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(extract_pdf_text, pdf_bytes)
        return fut.result(timeout=PDF_TIMEOUT)


def process_one(pdf_key, silver_key, meta):
    """PDF 1건 변환. 반환: ("ok"|"ocr"|"fail", 날짜, report_id, 에러메시지)"""
    s3 = boto3.client("s3")
    # 경로에서 날짜·report_id 추출: bronze/신한투자증권/2026-05-13/abc.pdf
    m = re.match(rf"{re.escape(BRONZE_PREFIX)}(\d{{4}}-\d{{2}}-\d{{2}})/([^/]+)\.pdf$", pdf_key)
    date_str  = m.group(1) if m else ""
    report_id = m.group(2) if m else pdf_key.rsplit("/", 1)[-1][:-4]

    try:
        pdf_bytes = s3.get_object(Bucket=S3_BUCKET, Key=pdf_key)["Body"].read()
        text, pages_total, pages_with_text, title = extract_with_timeout(pdf_bytes)
    except Exception as e:
        return ("fail", date_str, report_id, str(e))

    # OCR 필요 판정 (MD 명세)
    needs_ocr = (
        len(text) < 200
        or (pages_total > 0 and pages_with_text / pages_total < 0.3)
    )

    info = meta.get(report_id, {})
    silver = {
        "report_id":       report_id,
        "source":          BROKER,
        "증권사":          BROKER,
        "종목명":          info.get("종목명", ""),
        "발행일":          date_str,                # 팀원 포맷(YYYY-MM-DD)으로 통일
        "title":           title,
        "text":            text,
        "text_len":        len(text),
        "pages_total":     pages_total,
        "pages_with_text": pages_with_text,
    }
    if needs_ocr:
        silver["needs_ocr"] = True

    s3.put_object(
        Bucket=S3_BUCKET, Key=silver_key,
        Body=json.dumps(silver, ensure_ascii=False).encode(),
    )
    return ("ocr" if needs_ocr else "ok", date_str, report_id, "")


# ──────────────────────────────────────────────────────────
# 4. 메인 (병렬 처리 + OCR 목록 기록)
# ──────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="신한투자증권 Bronze → Silver 변환")
    parser.add_argument("--workers", type=int, default=20, help="동시 처리 PDF 수")
    parser.add_argument("--dry-run", action="store_true", help="건수만 확인하고 종료")
    args = parser.parse_args()

    s3 = boto3.client("s3")
    targets = discover_targets(s3)
    if args.dry_run or not targets:
        print("종료 (dry-run 또는 대상 없음)")
        return

    meta = load_shinhan_manifests(s3)

    done = fail = 0
    ocr_by_date = defaultdict(list)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(process_one, pdf_key, silver_key, meta): pdf_key
            for pdf_key, silver_key in targets
        }
        for fut in as_completed(futures):
            try:
                status, date_str, report_id, err = fut.result()
            except Exception as e:
                status, date_str, report_id, err = "fail", "", futures[fut], str(e)

            if status == "fail":
                fail += 1
                print(f"  실패 [{report_id}]: {err}")
            else:
                done += 1
                if status == "ocr":
                    ocr_by_date[date_str].append(report_id)
            if (done + fail) % 200 == 0:
                print(f"  진행: {done + fail}/{len(targets)} (실패 {fail})")

    # OCR 필요 목록 기록 (MD 명세: silver/_ocr_needed/{날짜}.json)
    for date_str, ids in ocr_by_date.items():
        key = f"{OCR_PREFIX}{BROKER}_{date_str}.json"
        s3.put_object(
            Bucket=S3_BUCKET, Key=key,
            Body=json.dumps({"date": date_str, "broker": BROKER, "report_ids": ids},
                            ensure_ascii=False).encode(),
        )

    ocr_total = sum(len(v) for v in ocr_by_date.values())
    print(f"\n완료: 변환 {done}건 (OCR 필요 {ocr_total}건) / 실패 {fail}건")
    print(f"→ s3://{S3_BUCKET}/{SILVER_PREFIX}")


if __name__ == "__main__":
    main()
