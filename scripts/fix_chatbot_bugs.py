#!/usr/bin/env python3
"""
Fix chatbot bugs from live user feedback (2026-06-19).

Bug 1: Date follow-up "6일은?" → returned wrong month data
  Root cause: intent.refers_to_previous not set by Haiku for short follow-ups,
  and broad fallback overrode the reconstructed date.
  Fix: remove refers_to_previous requirement, run day reconstruction AFTER broad fallback.

Bug 2: Compare ("삼성전자랑 SK하이닉스 비교") returned 1 wrong source
  Root cause: single FAISS query with merged company names produces poor matches.
  Fix: search FAISS separately per company, merge+deduplicate results.

Bug 3: _convert_sources missing fields for compare_reports
  Root cause: converted dicts lack 증권사/발행일/투자의견/목표주가 fields.
  Fix: parse 증권사 from reason field format "[증권사명] 제목".
"""

import re

SERVER_PATH = "/sessions/keen-laughing-carson/mnt/Documents/opik/server/opik_server.py"

with open(SERVER_PATH, "r", encoding="utf-8") as f:
    content = f.read()

original = content

# ── Fix 1: Date follow-up reconstruction (lines 758-797) ──
old_date_block = """    # v2: Date context reconstruction for follow-up questions
    # When a follow-up question references a day ("14일은?", "15일은?")
    # without explicit month/year, pull the recent month from conversation context.
    #
    # v4.2: Broad conversation date context. The intent parser (Haiku) sometimes
    # hallucinates dates for vague follow-ups ("그날", "라고 하면"). The conversation
    # store is more reliable — if the user's message has no explicit date text
    # but the conversation has a recent date, prefer the conversation date.
    date_from = req.date_from or intent.date_from
    date_to = req.date_to or intent.date_to

    # Check if the user message contains explicit date text
    _has_explicit_date = bool(
        re.search(r'\d{4}년|\d{1,2}월\s*\d{1,2}일|\d{4}-\d{2}-\d{2}|어제|오늘|그제', req.message)
    )

    _day_only = re.search(r'(?<!\d)(\d{1,2})일', req.message)
    if intent.refers_to_previous and _day_only and not _has_explicit_date:
        recent_month = conversation_store.get_recent_month(session_id)
        if recent_month:
            day = _day_only.group(1).zfill(2)
            reconstructed = f"{recent_month}-{day}"
            date_from = reconstructed
            date_to = reconstructed
            logger.info("Reconstructed date from conversation context: %s (month=%s, day=%s)",
                         reconstructed, recent_month, day)

    # v4.2: Override intent parser dates when the user message has no explicit date.
    # The intent parser (Haiku) may hallucinate dates for words like "그날".
    # Prefer conversation context which is based on actual message history.
    if not _has_explicit_date and intent.needs_dart:
        recent_full_date = conversation_store.get_recent_full_date(session_id)
        if recent_full_date:
            intent_date = intent.date_from or intent.date_to
            if intent_date:
                logger.info("v4.2 overriding intent parser date %s with conversation date %s",
                             intent_date, recent_full_date)
            date_from = recent_full_date
            date_to = recent_full_date
            logger.info("v4.2 broad date fallback for follow-up: %s", recent_full_date)"""

new_date_block = """    # v5: Date context reconstruction for follow-up questions (revised 2026-06-19)
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
            logger.info("v5 report fallback to recent month: %s", recent_month)"""

if old_date_block in content:
    content = content.replace(old_date_block, new_date_block)
    print("✓ Fix 1 applied: Date follow-up reconstruction (v5)")
else:
    print("✗ Fix 1 FAILED: Could not find the date block")
    # Show what's around that area
    idx = content.find("v2: Date context reconstruction")
    if idx > 0:
        print(f"  Found at offset {idx}, snippet: {content[idx:idx+200]}")

# ── Fix 2: Multi-ticker FAISS search for compare ──
# Add _search_faiss_for_companies function right before _detect_analysis_request
old_detect_fn = """def _detect_analysis_request(message: str) -> Optional[str]:"""

new_search_fn = """def _search_faiss_for_companies(companies: list, base_query: str, top_k: int = 20) -> list:
    \"\"\"Search FAISS separately for each company and merge deduplicated results.
    
    When a user asks "삼성전자랑 SK하이닉스 비교해줘", a single FAISS query with
    both names produces poor semantic matches. Instead, search each company
    separately with a clean query and merge the results.
    \"\"\"
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
                all_results.append(
                    SearchResult(
                        report_id=rid,
                        score=round(float(distances[0][i]), 4),
                        종목코드=report_texts.get(rid, {}).get("종목코드"),
                        reason=report_texts.get(rid, {}).get("reason"),
                        keywords=report_texts.get(rid, {}).get("keywords"),
                        risks=report_texts.get(rid, {}).get("risks"),
                        year=report_texts.get(rid, {}).get("year"),
                        month=report_texts.get(rid, {}).get("month"),
                    )
                )
        except Exception as e:
            logger.warning("Multi-ticker search failed for %s: %s", company, e)
    
    # Sort by score descending
    all_results.sort(key=lambda r: r.score, reverse=True)
    logger.info("Multi-ticker search: %d companies → %d unique results", len(companies), len(all_results))
    return all_results


def _detect_analysis_request(message: str) -> Optional[str]:"""

if old_detect_fn in content:
    content = content.replace(old_detect_fn, new_search_fn)
    print("✓ Fix 2a applied: Multi-ticker FAISS search function added")
else:
    print("✗ Fix 2a FAILED: Could not find _detect_analysis_request")

# ── Fix 3: _convert_sources enrichment — parse 증권사 from reason field ──
old_convert = """def _convert_sources(sources: list) -> list:
    \"\"\"Convert SearchResult objects to dicts compatible with agent pipeline.\"\"\"
    converted = []
    for s in sources:
        if hasattr(s, '__dict__'):
            d = {}
            for k, v in s.__dict__.items():
                if not k.startswith('_'):
                    d[k] = v
            converted.append(d)
        elif isinstance(s, dict):
            converted.append(s)
    return converted"""

new_convert = """def _parse_brokerage_from_reason(reason: str) -> str:
    \"\"\"Extract 증권사 name from reason field. Format: '[한국투자증권] 실적 전망...' → '한국투자증권'\"\"\"
    if not reason:
        return ""
    m = re.match(r'\[([^\]]+)\]', str(reason))
    if m:
        return m.group(1)
    return ""


def _convert_sources(sources: list) -> list:
    \"\"\"Convert SearchResult objects to dicts compatible with agent pipeline.
    Enriches with parsed fields: 증권사 (from reason), 발행일 (from year/month).\"\"\"
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
    return converted"""

if old_convert in content:
    content = content.replace(old_convert, new_convert)
    print("✓ Fix 3 applied: _convert_sources enrichment (증권사/발행일 parsing)")
else:
    print("✗ Fix 3 FAILED: Could not find _convert_sources")

# ── Fix 2b: Update _run_analysis_with_data to use multi-ticker search for compare ──
old_run_analysis = """        try:
        import agent_integration
        converted = _convert_sources(sources)

        # Extract ticker info from intent or message
        companies = intent_info.get("companies", [])
        ticker_name = companies[0] if companies else _extract_ticker_from_message(req_message)

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
            if result:"""

new_run_analysis = """        try:
        import agent_integration
        converted = _convert_sources(sources)

        # Extract ticker info from intent or message
        companies = intent_info.get("companies", [])
        ticker_name = companies[0] if companies else _extract_ticker_from_message(req_message)

        # v5: For "compare" with multiple companies, search FAISS per-company
        if analysis_type == "compare" and len(companies) >= 2:
            logger.info("Compare: multi-ticker search for %d companies: %s", len(companies), companies)
            multi_results = _search_faiss_for_companies(companies, "증권사 리포트", top_k=20)
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
            # compare_reports expects List[dict] with 증권사/발행일/투자의견/목표주가/현재주가 fields
            result = agent_integration._analysis.compare_reports(converted, ticker_name)
            if result:"""

if old_run_analysis in content:
    content = content.replace(old_run_analysis, new_run_analysis)
    print("✓ Fix 2b applied: Multi-ticker search in _run_analysis_with_data for compare")
else:
    print("✗ Fix 2b FAILED: Could not find _run_analysis_with_data block")
    idx = content.find("Analysis: Stage 2 had 0 sources")
    if idx > 0:
        print(f"  Found at offset {idx}, snippet: ...{content[idx-100:idx+300]}...")

# ── Write back ──
if content != original:
    with open(SERVER_PATH, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"\n✓ All fixes applied. File size: {len(original)} → {len(content)} bytes")
else:
    print("\n⚠ No changes were made to the file!")
