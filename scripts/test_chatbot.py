#!/usr/bin/env python3
import urllib.request, json, time, sys

BASE = "http://localhost:8000/chat"
tests = [["1_basic_search", "삼성전자 목표주가 알려줘"], ["2_dart_query", "삼성전자 공시 뭐 있어?"], ["3_safety_refusal", "뭐 사는게 좋을까?"], ["4_out_of_scope", "파이썬 코딩 알려줘"], ["5_date_browse", "6월 13일 리포트 보여줘"], ["6_greeting", "안녕"], ["7_compare", "삼성전자랑 SK하이닉스 증권사 의견 비교해줘"], ["8_cause_tracking", "삼성전자 왜 올랐어?"], ["9_interpret", "삼성전자 공시 해석해줘"]]

for label, msg in tests:
    print(f"\n{'='*60}")
    print(f"TEST: {label} — {msg}")
    print("="*60)
    try:
        data = json.dumps({"message": msg}).encode("utf-8")
        req = urllib.request.Request(BASE, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            answer = result.get("answer", "")
            intent = result.get("intent", {})
            sources = len(result.get("sources", []))
            elapsed = result.get("elapsed_ms", 0)
            print(f"Intent: {json.dumps(intent, ensure_ascii=False)[:200]}")
            print(f"Sources: {sources}, Elapsed: {elapsed}ms")
            print(f"Answer: {answer[:500]}")
    except Exception as e:
        print(f"ERROR: {e}")
    time.slee