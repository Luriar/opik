# OPIK Phase 2 — LangGraph 멀티에이전트 재설계

## 1. 왜 다시 설계하는가

Phase 2 원안은 Spark ETL 파이프라인 중심이었다. 데이터를 Parquet → Delta로 옮기고, SQL JOIN으로 composite score를 계산하고, 정해진 템플릿으로 텔레그램을 쏘는 구조. 이건 **데이터가 깔끔하게 정형화되어 있다는 가정** 위에서만 동작한다.

현실은 다르다. 리포트 텍스트는 길고 비정형이고, DART 공시는 복잡한 법적 언어로 쓰여 있으며, 애널리스트 의견은 서로 충돌한다. 사용자의 질문도 "삼성전자 목표주가 알려줘" 같은 단순 검색을 넘어 "왜 올랐지?", "이 공시가 무슨 의미야?", "경쟁사는 어때?" 같은 **추론과 해석을 요구하는 질문**으로 진화하고 있다.

단일 파이프라인이 아니라 **역할이 분리된 여러 에이전트가 협업하는 구조**가 필요하다.

## 2. 핵심 변경점

| 영역 | Phase 2 (원안) | Phase 2 Multi-Agent |
|------|---------------|---------------------|
| 아키텍처 | Spark ETL 파이프라인 | LangGraph 멀티에이전트 |
| 브리핑 | 상승여력 Top 8 나열 | ★(삼중 신호 일치) / !(공시 이벤트) 티어 구분 |
| 챗봇 | 단일 LLM 응답 | 4개 전문 에이전트 + Supervisor 라우팅 |
| 스코어링 | a+b+c composite 점수 | **폐지** — 삼중 consensus 이진 판단(★ 여부)으로 대체 |
| LLM 사용 | Haiku 단일 | Haiku(경량 태스크) + Sonnet(추론 태스크) 계층 |
| 상태 관리 | 없음 | LangGraph StateGraph로 대화 상태 유지 |

## 3. LangGraph 아키텍처

### 3.1 전체 에이전트 그래프

```
                          ┌─────────────────────┐
                          │   Supervisor Agent   │
                          │   (Routing + Final   │
                          │    Decision)         │
                          └──────────┬───────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    │                │                │
              ┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
              │  Safety   │   │  Intent   │   │  Context  │
              │  Agent    │   │  Parser   │   │  Manager  │
              │           │   │           │   │           │
              │ Haiku     │   │ Haiku     │   │ (in-mem)  │
              └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
                    │                │                │
                    └────────────────┼────────────────┘
                                     │
                          ┌──────────▼───────────┐
                          │    Router Decision    │
                          │  (which agents to     │
                          │   activate)           │
                          └──────────┬───────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                      │
        ┌─────▼─────┐          ┌─────▼─────┐          ┌─────▼─────┐
        │  Report   │          │   DART    │          │  Hybrid   │
        │  Agent    │          │  Agent    │          │  Agent    │
        │           │          │           │          │           │
        │ FAISS     │          │ S3 Parquet│          │ Report +  │
        │ Semantic  │          │ DART Gold │          │ DART Join │
        │ Search    │          │ Tables    │          │           │
        │ Haiku     │          │ Haiku     │          │ Haiku     │
        └─────┬─────┘          └─────┬─────┘          └─────┬─────┘
              │                      │                      │
              └──────────────────────┼──────────────────────┘
                                     │
                          ┌──────────▼───────────┐
                          │   Analysis Agent      │
                          │   (Cross-source       │
                          │    synthesis)          │
                          │   Sonnet              │
                          └──────────┬───────────┘
                                     │
                          ┌──────────▼───────────┐
                          │  Response Composer    │
                          │  (Format + Source     │
                          │   citations + Safety  │
                          │   check)              │
                          │  Sonnet / template    │
                          └──────────────────────┘
```

### 3.2 LangGraph StateGraph 정의

```python
from typing import TypedDict, Annotated, Sequence, Optional
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
import operator

class AgentState(TypedDict):
    # 사용자 입력
    messages: Annotated[Sequence[dict], add_messages]
    session_id: str
    user_id: str
    
    # Intent & Safety
    intent: str                        # report_search | dart_query | hybrid | general | investment_advice
    intent_params: dict                # tickers, brokerages, sectors, time_range, keywords
    is_safe: bool
    
    # 데이터 검색 결과
    report_results: list[dict]         # FAISS 검색 결과
    dart_results: list[dict]           # DART 쿼리 결과
    search_metadata: dict              # 검색 통계 (건수, 시간, 신뢰도)
    
    # 분석 결과
    analysis: dict                     # Analysis Agent의 추론 결과
    cross_references: list[dict]       # 연관 종목/이벤트
    
    # 응답
    response: Optional[str]
    sources: list[str]
    confidence: str                    # high | medium | low | none
    
    # 라우팅
    next_agent: str                    # 다음 실행할 에이전트
    error: Optional[str]
```

### 3.3 라우팅 로직

```python
def supervisor_router(state: AgentState) -> str:
    """어떤 에이전트를 활성화할지 결정"""
    
    if not state.get("is_safe", True):
        return "safety_refusal"  # 즉시 거절 응답
    
    intent = state["intent"]
    params = state.get("intent_params", {})
    
    # 하이브리드: 리포트 + 공시 모두 필요
    if intent == "hybrid":
        return "parallel_search"  # Report + DART 병렬 실행
    
    # 리포트 검색
    if intent == "report_search":
        if params.get("compare"):  # 비교 분석 요청
            return "report_agent"
        if params.get("cause_tracking"):  # 원인 추적
            return "report_agent"
        return "report_agent"
    
    # DART 쿼리
    if intent == "dart_query":
        if params.get("interpret"):  # 공시 해석 요청
            return "dart_with_analysis"
        return "dart_agent"
    
    # 투자 조언
    if intent == "investment_advice":
        return "safety_refusal"
    
    # 일반 질문
    return "general_response"
```

## 4. 에이전트 상세 설계

### 4.1 Safety Agent

**목적**: 모든 요청을 투자 조언 경계선에서 1차 필터링

**모델**: Haiku (저비용, 고속)

**입력**: 사용자 메시지
**출력**: `{is_safe: bool, violation_type: str | null, redirect_suggestion: str}`

**분류 체계**:
```
SAFE → 다음 단계 진행
  ├─ report_search     "삼성전자 리포트 보여줘"
  ├─ dart_query        "최근 공시 뭐 있어?"
  ├─ hybrid            "삼성전자 리포트랑 공시 같이"
  └─ general           "OPIK이 뭐야?", "고마워"

UNSAFE → 즉시 거절 + 대안 제시
  ├─ buy_recommend     "뭐 사는게 좋을까?"
  ├─ sell_recommend    "지금 팔아야 하나?"
  ├─ portfolio         "내 포트폴리오 어때?"
  ├─ timing            "언제 사는게 좋아?"
  ├─ guarantee         "확실한 종목 알려줘"
  └─ out_of_domain     "코딩 해줘", "요리 레시피 알려줘"
```

**거절 응답 패턴**:
```
"{violation_type}에 대한 조언은 제공하지 않습니다.
OPIK은 증권사 리포트와 공시 데이터를 검색·요약해드리는 금융 정보 챗봇입니다.
대신 {redirect_suggestion}을(를) 도와드릴까요?"
```

### 4.2 Intent Parser Agent

**목적**: Safety 통과 후 사용자 의도를 세분화하고 검색 파라미터 추출

**모델**: Haiku

**입력**: 사용자 메시지 + 대화 맥락
**출력**: `{intent: str, params: {tickers, brokerages, sectors, time_range, keywords, compare, cause_tracking, interpret}}`

**기존 대비 개선점**:
- `compare` 플래그: "A증권사랑 B증권사 의견 비교해줘" → Analysis Agent 트리거
- `cause_tracking` 플래그: "왜 올랐어?" → 리포트+공시+시계열 검색 후 원인 추론
- `interpret` 플래그: "이 공시가 무슨 의미야?" → DART 검색 + LLM 해석

### 4.3 Report Agent

**목적**: FAISS 시맨틱 검색 + 리포트 요약

**도구**:
- `search_faiss(query, top_k=20, filters)`: 384-dim multilingual-e5-small 임베딩 검색
- `browse_by_date(date, page, page_size=20)`: 날짜 기반 리포트 브라우징
- `get_report_detail(report_id)`: 특정 리포트 전문 조회

**데이터 소스**:
- FAISS Index (51,583 vectors, IndexIDMap + IndexFlatIP)
- S3 Gold Structured Parquet
- S3 Gold LLM Parquet (reason, risks, keywords)

**응답 형식**:
```
[신뢰도: {confidence}]
{종목명} ({증권사}, {날짜})
투자의견: {opinion} | 목표주가: {tp:,}원 | 현재주가: {cp:,}원 | 상승여력: {upside:+.1f}%

핵심 논리: {reason}
리스크: {risks}
키워드: {keywords}

[출처: {증권사} {title}, {날짜}]
```

### 4.4 DART Agent

**목적**: DART Gold 테이블 쿼리 + 공시 내용 요약

**도구**:
- `query_disclosure_events(ticker, date_range, event_type)`: 공시 이벤트 검색
- `query_financials(ticker, year, quarter)`: 재무제표 조회
- `query_insider_trades(ticker, date_range)`: 임원·주주 거래 조회
- `query_major_shareholders(ticker)`: 주요주주 현황 조회
- `summarize_disclosure(text, event_type)`: LLM으로 공시 텍스트 요약 (신규)

**데이터 소스** (DartCollector — 상용 Airflow가 S3에 출력):
- `gold/dart/facts/material_event/`: 주요사항보고(B) 이벤트 (★/! 1차 소스)
- `gold/dart/facts/regular_structured/`: 정기공시(A) structured data
- `gold/dart/facts/financial_statement/`: 재무제표 (연결/별도)
- `gold/dart/facts/ownership/`: 지분공시(D) — 임원·주주 지분 변동
- `gold/dart/facts/securities/`: 증권신고서(C) — CB/BW/유상증자 실행
- `gold/dart/rag/rag_document/`: 공시 원문 전문 (해석·맥락 확인용)
- `gold/dart/rag/rag_chunk/`: 청크 분할 텍스트 (RAG 검색용)
- `gold/dart/rag/embedding/`: 384-dim e5-small 임베딩 벡터

**공시 의미 해석 (신규 기능)**:
```
입력: 공시 원문 텍스트
처리: Haiku → 3문장 이내 요약 + 시장 영향도 분류 (긍정/중립/부정)
출력: "{이벤트 제목} — {한 줄 요약} [{영향도}]"
예: "자기주식 취득 신탁계약 체결 — 50억원 규모 자사주 매입 결정 [긍정적]"
```

### 4.5 Analysis Agent

**목적**: 여러 소스의 정보를 종합하여 고차원 추론 수행

**모델**: Sonnet (맥락 이해 + 추론 필요)

**하위 기능**:

#### A. 리포트 비교 분석 (`compare_reports`)
```
입력: 동일 종목에 대한 2개 이상의 리포트
처리: 
  1. 투자의견 분포 시각화
  2. 목표주가 차이와 근거 비교
  3. 리스크 요인의 공통점/차이점 추출
  4. 애널리스트별 강조 포인트 대조
출력: 비교 테이블 + 핵심 차이점 서술
```

#### B. 업계/경쟁사 분석 (`industry_analysis`)
```
입력: 특정 종목/섹터
처리:
  1. 해당 섹터의 최근 리포트 일괄 검색
  2. 섹터 내 종목별 투자의견/목표주가 추이
  3. 공통으로 언급되는 업황 키워드 추출
  4. 섹터 내 상대적 포지셔닝
출력: 섹터 개요 + 종목별 포지션 + 업황 consensus
```

#### C. 주가 움직임 원인 추적 (`trace_cause`)
```
입력: 종목명 + 날짜 범위
처리:
  1. 해당 기간 리포트 검색 (의견 변화, TP 변동)
  2. 해당 기간 DART 공시 검색 (주요 이벤트)
  3. 리포트-공시 시간선상 매핑
  4. LLM이 가능한 인과관계 가설 제시 (확정적 진술 금지)
출력: 타임라인 + 가능한 요인 (확률적 표현)
```

#### D. 삼중 신호 Consensus 체크 (브리핑용) (`check_triple_consensus`)
```
입력: 전체 종목 × 3개 신호 (24h 리포트 / 1개월 DART / 모델 예측)
처리:
  1. 각 종목별로 3개 신호의 방향성 평가 (긍정/중립/부정)
  2. 3개 모두 긍정 → ★ 후보
  3. DART 공시 이벤트만 유의미 → ! 후보
출력: ★ 리스트 + ! 리스트 (점수/근거 포함)
```

### 4.6 Response Composer

**목적**: 각 에이전트의 출력을 사용자에게 최적화된 형식으로 조합

**원칙**:
- factual 데이터는 원본 그대로 (수치, 날짜, 증권사명)
- LLM 해석은 항상 "분석 결과"임을 명시
- 모든 factual claim에 출처 첨부
- Zone B 응답에는 신뢰도 표시
- Zone C는 Safety Agent가 차단했으므로 여기서는 처리하지 않음

## 5. Daily Briefing 상세 설계

### 5.1 Briefing 형식

```
OPIK Daily Briefing / 2026.06.18

오늘의 리포트 및 공시 (n개 리포트 / m개 공시)

━━━━━━━━━━━━━━━━━━━━━━━━━━

오늘의 주목할 종목 (o개)

★ 삼성전자 (모델: 85,000원 / 리포트 TP 85,000원)

  [리포트] 한국투자증권 (BUY, TP 85,000원)
  아시아 중심의 강력한 코어 팬덤 문화는 머니타이즈 관점에서 
  MD&라이선싱 다각화 전략과 맞물려 뚜렷한 수익성 개선으로 
  이어질 공산이 큰 상황이다.
  → 주의할 점: 미/이스라엘 대이란 공습 여파로 발생한 주가 하락

  [공시] 2026년 1분기 실적: 매출 23.4조(+15.2% YoY), 
  영업이익 4.8조(+32.1% YoY)
  [공시] 자기주식 취득 신탁계약 체결 결정 (500억원 규모)

★ SK하이닉스 (모델: 280,000원 / 리포트 TP 280,000원)
  ...

! 카카오
  [공시] 카카오헬스케어 분할 결정 — 신설법인 설립 후 지분 100% 보유 예정
! 셀트리온
  [공시] 셀트리온헬스케어 흡수합병 증권신고서 제출 — 합병비율 1:2.3
! POSCO홀딩스
  [공시] 유상증자 결정 — 2조원 규모, 시설투자 목적
! 한화에어로스페이스
  [공시] 대규모 공급계약 체결 — 폴란드향 K9 자주포 추가 수출

━━━━━━━━━━━━━━━━━━━━━━━━━━

이외의 종목들에 대한 리포트나 공시가 궁금하시면 질문해주세요.

※ 본 브리핑은 증권사 리포트 및 DART 공시의 사실적 요약이며 
   투자 권유가 아닙니다.
```

### 5.2 ★ (Star) 선정 로직

```
★ 검색 공간: 아래 3개 데이터의 종목코드 INNER JOIN부터 시작한다.

  Step 1 — 종목코드 교집합 필터링
    - 오늘 리포트 발행 종목: ~30개 (30건 리포트, 종목 중복 제거 시 ~20종목)
    - 찬호 모델 prediction 존재 종목: ~348개 (KOSPI200 + KOSDAQ150)
    - 최근 1개월 DART 이벤트 존재 종목: 수백 개
    →
    INNER JOIN 결과: 3개 소스 모두에 존재하는 종목만 ★ 후보군.
    오늘 리포트가 없는 종목은 ★가 될 수 없다.
    (데이터 부재는 긍정 신호가 아니다 → 삼중 신호 중 하나라도
     없으면 consensus를 확인할 수 없으므로 제외)

  Step 2 — 개별 신호 검증 (교집합 내에서만)
    1. 주가예측모델 (Chanho): ranking_score > 0
       - LightGBM regression raw output으로, 양수이면 모델이 양의 수익을 전망한다는 뜻.
       - ranking_score < 0인 날은 시장 전체가 하락 예측인 날이므로 ★는 나오지 않는다 (정직한 결과).
       - pred_open_price / pred_close_price 등 절대가격 예측은 사용하지 않는다. 방향성(부호)만 사용.

    2. 최근 1개월 DART 공시: 긍정 이벤트 존재, 부정 이벤트 없음
       → DART 공시 긍정/부정 분류는 섹션 5.2.1 참조

    3. 오늘 리포트: 투자의견 BUY + 상승여력 > 0%
       - HOLD는 긍정 신호로 간주하지 않는다.
       - 한국 증권사 HOLD는 사실상 "비중축소" 의미를 내포하기 때문.
       - NOT_RATED / null → 데이터 부재와 동일하게 제외.

  Step 3 — 결과
    - 교집합이 먼저 걸러진 상태에서 개별 검증을 하므로 실제 계산량은 극히 작다.
    - 오늘 리포트 30건 × 교집합 종목 수 ~15개 × 3개 신호 체크 = 상수 시간.
    - ★가 나오는 날은 하루 0~3개가 일반적.
    - ★가 하나도 없는 날: "오늘은 삼중 신호 일치 종목이 없습니다"라고 정직하게 표기.
    - ★가 5개를 넘는 날: 상한 없이 전부 표시 (5개 넘는 날은 사실상 없음).
```

#### 5.2.1 DART 공시 긍정/부정 분류 — Agent 기반

```
DART 공시는 리포트와 달리 명시적인 sentiment label이 없다.
OPIK은 DART Sentiment Agent (Haiku, batch mode)로 분류한다.

키워드 매칭이 아닌 Agent를 쓰는 이유:
  - "유상증자" → 성장 투자용이면 긍정, 부실 방어용이면 부정
  - "회사분할" → 주주가치 제고 목적이면 긍정, 경영권 방어용이면 중립
  - 공시 제목만 보고 판단할 수 없고, 요약 텍스트의 맥락을 읽어야 함

처리 방식 (상세는 섹션 7.4 참조):
  - 1개월 DART 200~300건을 25건씩 배치로 나누어 Haiku 호출
  - asyncio 병렬로 12회 호출 → 1~2초 내 완료
  - 각 공시를 positive / negative / neutral로 분류 + 이유 한 줄
  - 입력 데이터: report_nm(제목) + text(원문 전문, OPIK disclosure_events 기준)
  - 공시 제목만으로는 부족 — 실제 텍스트를 읽고 판단해야 함

positive 예시:
  자기주식 취득 (주주환원), 신규 대규모 수주, 실적 가이던스 상향,
  신용등급 상향, 성장 목적 시설투자, 배당 확대

negative 예시:
  유상증자 (채무상환 목적), 소송·검찰 이슈, 감사의견 비적정,
  대규모 영업손실, 부도·회생절차, 최대주주 지분 매각,
  CB/BW 대규모 발행 (희석 우려)

neutral 예시:
  정기주총 소집, 이사 선임, 정관 변경, 일상적 소액 계약,
  정보 부족으로 판단 불가한 경우
```

### 5.3 ! (Exclamation) 선정 로직

```
!는 진짜 주가에 직결되는 주요사항보고서 이벤트.
공시를 아무거나 보여주는 게 아니라, 이 공시가 오늘 나왔다는 사실 자체가
투자자에게 의미 있는 정보인 것만.

! 대상 공시 유형 (명시적):
  - 부도발생 / 회생절차 개시신청 / 회생절차 종결
  - 유상증자 결정 / 무상증자 결정
  - 전환사채(CB) 발행 결정 / 교환사채(EB) 발행 결정
  - 신주인수권부사채(BW) 발행 결정
  - 영업양도 결정 / 주요자산 처분 결정
  - 타법인 출자·취득 결정 (대규모 M&A)
  - 합병 결정 / 분할 결정
  - 자기주식 취득 결정 (대규모)
  - 최대주주 변경 / 경영권 변동
  - 감자 결정 (자본감소)
  - 단일판매·공급계약 체결 (대규모)
  - 소송 제기 / 검찰·공정위 조사

! 개수 제한: 상한 없음.
위 공시 유형들은 누가 봐도 주가에 직결되는 이벤트이므로,
하루에 10건이면 10건, 15건이면 15건 모두 보여준다.
어차피 이런 major disclosure는 하루에 몇 건 나오지 않는다 (통상 3~8건).

! 이벤트 분류: 같은 공시 유형이라도 맥락에 따라 다르게 읽힌다.
DART Sentiment Agent가 분류하며, 긍정/부정/중립으로 나뉜다:
  - 조건부 호재(급등 가능): 제3자배정 유상증자(대기업 참여), 알짜사업부 아닌 매각
  - 조건부 악재(하락 가능): 주주배정·일반공모 유상증자(희석), 알짜사업부 매각
  - 절대적 악재(폭락): 부도발생, 회생절차, 대규모 영업손실
  - 중립/복합: EB 발행(CB보다 희석 적음), 빚의 목적(성장 vs 운영자금)에 따라 평가

표시 형식:
  ! 종목명 [impact: 긍정/부정/중립]
    [공시] {이벤트 제목} — {한 줄 요약 + 이유 (Haiku)}

  예: ! 카카오 [긍정]
        [공시] 분할 결정 — 비핵심 자회사 분할로 본업 집중도 상승, 주주가치 제고 기대
  예: ! 한화에어로스페이스 [긍정]
        [공시] 대규모 공급계약 체결 — 폴란드향 K9 자주포 추가 수출, 3조원 규모
  예: ! POSCO홀딩스 [부정]
        [공시] 유상증자 결정 — 주주배정 2조원, 채무상환 목적, 지분희석 우려

동일 종목에 여러 공시가 있으면 그 종목 아래 전부 표시.
종목 정렬 순서: 시가총액 큰 순서 (정보 가치가 큰 종목 우선).
```

### 5.4 전체 파이프라인 타임라인

Phase 2는 리포트·DART·모델 3개 데이터 소스가 각자 다른 시간에 완성된다.
브리핑은 모든 소스가 준비된 후 실행되어야 ★ 삼중 consensus가 의미 있다.

```
시간 (KST)  │  주체            │  작업
────────────┼──────────────────┼─────────────────────────────────────
00:00       │ OPIK Airflow     │ 리포트 Bronze → Silver → Gold Structured
            │ (Docker/EC2)     │ → Gold LLM (Haiku embedding)
            │ 6개 DAG          │ 모든 DAG schedule: 0 0 * * *
            │                  │ 완료 시각: 약 02:00~03:00
────────────┼──────────────────┼─────────────────────────────────────
05:30       │ DART Airflow     │ Bronze → Silver 일괄 변환
06:00       │ Chanho model     │ 주가 예측 daily run (미국장 마감 후)
            │                  │ → ranking_score (raw return 예측치), 348종목
07:00       │ DART Airflow     │ Gold Builder: Silver → Gold Parquet
            │                  │ (facts + rag_chunk + embedding)
07:00~18:59 │ DART Airflow     │ Incremental Discovery (5분 간격)
            │                  │ Detail Collector (10분 간격)
08:00       │ DART Airflow     │ Gold Compaction 완료
            │                  │ ★★★ DART GOLD 확정 — 쿼리 가능 ★★★
────────────┼──────────────────┼─────────────────────────────────────
07:00       │ OPIK Briefing    │ ★/! 브리핑 파이프라인 (별도 DAG)
            │ DAG (신설)       │ (장 시작 전 전송 — 아래 Step 1~9)
────────────┴──────────────────┴─────────────────────────────────────
```

**브리핑 DAG는 기존 리포트 Airflow(00:00)와 완전히 분리한다.**
이유: 00:00 시점에는 모델 예측(06:00 완료 후)이 존재하지 않아
★ 삼중 consensus를 확인할 수 없다. 07:00 별도 DAG로 분리해 장 시작 전(09:00) 모든 데이터가 준비된
상태에서 실행한다.

DART Gold는 당일 07:00 gold builder가 브리핑 시점에 미완료이므로, 전일 08:00 compaction 기준 데이터를 사용한다 (1영업일 lag). 공시 이벤트는 실시간성이 주가 대비 낮아 Phase 2a에서 허용 가능한 트레이드오프다.

### 5.5 Briefing Pipeline (07:00 DAG)

Spark는 scoring에 사용하지 않는다. composite score(a+b+c)는 폐지되었다.
★/! 선정은 전량 Pandas in-process로 처리하며, LangGraph Agent가 triple consensus를 계산한다.

```
07:00 KST — Briefing DAG 트리거
  │
  ├─ Step 1: Gold Structured 로드 (OPIK — 금일자 리포트)
  │   → S3: gold/structured/year={Y}/month={M}/data.parquet
  │   → 당일 발행일자 필터
  │
  ├─ Step 2: Gold LLM 로드 (OPIK — 금일자 reason/risks/keywords)
  │   → S3: gold/embeddings/year={Y}/month={M}/data.parquet
  │   → report_id 기준 LEFT JOIN
  │
  ├─ Step 3: DART Disclosure Events 로드 (OPIK Gold — 최근 1개월)
  │   → S3: gold/dart/disclosure_events/dt={YYYY-MM}/data.parquet
  │   → rcept_dt 기준 최근 1개월 필터
  │   → stock_code + event_category + report_nm + text (원문 전문)
  │   → ★ Phase 2 1차 소스: OPIK 자체 Gold (12K+건, text 컬럼 보유)
  │   → 추후 DartCollector facts/material_event/ 로 전환 예정
  │
  ├─ Step 4: DART Sentiment Agent 실행 (Haiku batch)
  │   ├─ 1개월 material events를 25건씩 배치로 Haiku 호출
  │   ├─ positive / negative / neutral 분류 + reason
  │   └─ asyncio 20 병렬 → 1~2초 내 완료
  │
  ├─ Step 5: Chanho 모델 prediction 로드
  │   → ticker + ticker_name + ranking_score + pred_close_price (브리핑 표시용)
  │
  ├─ Step 6: ★ Triple Consensus (LangGraph — Pandas in-process)
  │   ├─ Step 6a: 3개 소스 종목코드 INNER JOIN
  │   │   오늘 리포트 발행 종목 ∩ 모델 prediction 존재 종목 ∩
  │   │   1개월 DART positive 이벤트 존재 종목 → 교집합만 ★ 후보군
  │   │   (오늘 리포트 없는 종목은 ★에서 자동 제외)
  │   ├─ Step 6b: 교집합 내에서 개별 신호 검증
  │   │   - 모델: ranking_score > 0 (양수 = 상방 전망)
  │   │   - DART: positive 이벤트 ≥ 1건 AND negative 이벤트 = 0건
  │   │   - 리포트: BUY + 상승여력 > 0% (HOLD는 불인정)
  │   │   → 3개 모두 통과한 종목 = ★
  │   └─ 계산 비용: 교집합으로 후보군이 0~15개로 축소되어 O(1) 수준
  │
  ├─ Step 7: ! Major Disclosure 필터링 (별도 경로, 교집합 불필요)
  │   └─ Step 4에서 sentiment 분류 완료된 material events 중
  │       major 유형(B: 주요사항보고) 필터링 → ! 대상
  │
  ├─ Step 8: Briefing Composer (LangGraph)
  │   ├─ ★ 항목: 리포트 reason/risk 전문 + DART 공시 요약
  │   ├─ ! 항목: 이벤트 제목 + Haiku 한 줄 요약
  │   └─ 디스클레이머 추가
  │
  └─ Step 9: Telegram 전송
```

### 5.6 Briefing Agent (LangGraph)

```python
from langgraph.graph import StateGraph

class BriefingState(TypedDict):
    date: str
    # Step 1-5: 데이터 로드
    structured: list[dict]       # 오늘 Gold Structured (종목코드 중복 제거: ~20종목)
    llm_data: list[dict]         # 오늘 Gold LLM (reason/risks/keywords)
    dart_events: list[dict]      # 1개월 DART material_event (sentiment 분류 완료)
    model_preds: list[dict]      # 찬호 348종목 prediction (ranking_score + pred_close_price)
    # Step 6-7: Consensus (scoring 폐지 — 삼중 신호 이진 판단으로 대체)
    intersection_tickers: list[str]  # 3개 소스 INNER JOIN 결과
    star_candidates: list[dict]      # triple consensus 통과 종목
    exclamation_items: list[dict]    # major disclosure events (cap 없음)
    # Step 8-9: 출력
    final_briefing: str

def check_triple_consensus(state: BriefingState) -> BriefingState:
    """
    ★ 선정: 3개 소스 INNER JOIN → 개별 신호 검증.

    복잡도 분석:
      - structured: ~20개 고유 종목코드
      - model_preds: ~348개
      - dart_events: 수백 개지만 종목코드 기준 set으로 변환
      → set intersection(constant × constant × set lookup) = O(20) 수준

    절대 모든 리포트 × 모든 DART 이벤트를 중첩 루프로 비교하지 않는다.
    종목코드 기준 INNER JOIN으로 후보군을 먼저 축소한 뒤에만 신호를 검증한다.
    """
    report_tickers = {r["종목코드"] for r in state["structured"]}
    model_tickers = {m["ticker"] for m in state["model_preds"]
                     if m.get("ranking_score", 0) > 0}  # 양수 = 상방 전망
    dart_positive_tickers = {d["종목코드"] for d in state["dart_events"]
                             if d.get("sentiment") == "positive"}
    dart_negative_tickers = {d["종목코드"] for d in state["dart_events"]
                             if d.get("sentiment") == "negative"}

    # INNER JOIN: 3개 소스 모두에 존재하는 종목만
    intersection = report_tickers & model_tickers & dart_positive_tickers
    # 부정 DART 이벤트가 있는 종목은 제외
    intersection -= dart_negative_tickers

    # 교집합 내에서만 리포트 조건 확인 (BUY + 상승여력 > 0)
    stars = []
    for ticker in intersection:
        reports = [r for r in state["structured"] if r["종목코드"] == ticker
                   and r.get("투자의견") == "BUY"
                   and r.get("상승여력_pct", 0) > 0]
        if reports:
            stars.append({
                "ticker": ticker,
                "종목명": reports[0]["종목명"],
                "reports": reports,
                "dart_events": [d for d in state["dart_events"]
                                if d["종목코드"] == ticker
                                and d.get("sentiment") == "positive"],
            })

    state["intersection_tickers"] = list(intersection)
    state["star_candidates"] = stars
    state["exclamation_items"] = filter_major_disclosures(state["dart_events"])
    return state

def build_briefing_graph():
    g = StateGraph(BriefingState)
    
    g.add_node("load_structured", load_gold_structured)
    g.add_node("load_llm", load_gold_llm)
    g.add_node("load_dart", load_dart_events)
    g.add_node("load_model_preds", load_model_predictions)
    g.add_node("score_triple_consensus", check_triple_consensus)
    g.add_node("compose_briefing", compose_daily_briefing)
    g.add_node("send_telegram", send_to_telegram)
    
    g.add_edge("load_structured", "load_llm")
    g.add_edge("load_llm", "load_dart")
    g.add_edge("load_dart", "load_model_preds")
    g.add_edge("load_model_preds", "score_triple_consensus")
    g.add_edge("score_triple_consensus", "compose_briefing")
    g.add_edge("compose_briefing", "send_telegram")
    
    g.set_entry_point("load_structured")
    g.set_finish_point("send_telegram")
    
    return g.compile()
```

## 6. 챗봇 신규 기능 상세

### 6.1 공시 의미 해석

**트리거**: `intent.dart_query` + `params.interpret: True`

**파이프라인**:
```
User: "이 자사주 공시가 무슨 의미야?"
  → Safety Agent: safe
  → Intent Parser: intent=dart_query, interpret=true, ticker=삼성전자
  → DART Agent: disclosure_events 검색 → 공시 원문 확보
  → Analysis Agent: 
      1. Haiku로 공시 요약 (3문장)
      2. 과거 유사 공시 검색 (self-tender history)
      3. 시장 관행상 의미 해석 (자사주 매입 = 주주환원 정책 강화 신호)
      4. "확정적 사실이 아닌 해석"임을 명시
  → Response Composer: 요약 + 해석 + 과거 사례 + 한계점
```

### 6.2 리포트 비교 분석

**트리거**: `intent.report_search` + `params.compare: True`

**파이프라인**:
```
User: "삼성전자 한국투자증권이랑 미래에셋 의견 비교해줘"
  → Report Agent: 두 증권사 삼성전자 최신 리포트 검색
  → Analysis Agent:
      1. 투자의견 매트릭스
      2. 목표주가 차이 분석 (gap이 나는 이유 → 가정 비교)
      3. 공통 리스크 / 차별적 리스크
      4. 강조 포인트 비교
  → Response Composer: 비교 테이블 + 핵심 인사이트
```

### 6.3 업계/경쟁사 분석

**트리거**: `intent.report_search` + `params.sector` + `params.cross_reference`

**파이프라인**:
```
User: "반도체 섹터 전체적으로 어때?"
  → Report Agent: "반도체" 최근 1주일 리포트 검색
  → DART Agent: 관련 종목 주요 공시
  → Analysis Agent:
      1. 섹터 내 종목별 의견/TP 매트릭스
      2. 공통 업황 키워드 (수요 둔화, 재고 조정, HBM 등)
      3. 섹터 consensus 도출
      4. 종목별 상대 포지셔닝
  → Response Composer: 섹터 맵 + 종목별 포지션
```

### 6.4 주가 움직임 원인 추적

**트리거**: `intent.report_search` + `params.cause_tracking: True`

**파이프라인**:
```
User: "삼성전자 왜 올랐어?"
  → Report Agent: 최근 1주일 삼성전자 리포트 (TP 변동, 의견 변화)
  → DART Agent: 최근 1주일 삼성전자 공시
  → Analysis Agent:
      1. 리포트-공시 타임라인 매핑
      2. LLM이 "가능한 요인"을 확률적 언어로 제시
         ("TP 상향 리포트가 3건 있었고, 이 중 2건이 HBM 수요를 근거로 듭니다")
      3. 확정적 진술 금지 ("이것 때문입니다" → "다음 요인들이 복합적으로 작용한 것으로 보입니다")
  → Response Composer: 타임라인 + 가능 요인 + "이는 추론이며 확정적 원인이 아닙니다"
```

## 7. 데이터 & 모델 스펙

### 7.1 LLM 할당

| 에이전트 | 모델 | 근거 |
|----------|------|-------|
| Safety Agent | Haiku | 단순 분류, 저지연 필수 |
| Intent Parser | Haiku | 분류 + 파라미터 추출, 1초 이내 |
| Report Agent | Haiku | 검색 결과 나열, 템플릿 기반 |
| DART Agent | Haiku | 쿼리 결과 나열, 요약 |
| DART Sentiment Agent | Haiku (batch) | 25건씩 배치 분류, 저비용 |
| Analysis Agent | Sonnet | 추론, 비교, 맥락 이해 필요 |
| Response Composer | Sonnet/template | 복잡한 응답은 Sonnet, 단순 응답은 템플릿 |

### 7.2 모델 비용 추정 (일간)

| 호출 | 모델 | 입력 토큰 | 출력 토큰 | 건수 | 일 비용 |
|------|------|----------|----------|------|---------|
| Safety + Intent | Haiku | ~500 | ~100 | 100회/일 | ~$0.06 |
| Report 검색 요약 | Haiku | ~2,000 | ~300 | 30회/일 | ~$0.08 |
| DART 쿼리 요약 | Haiku | ~1,500 | ~250 | 20회/일 | ~$0.04 |
| DART Sentiment (batch) | Haiku | ~8,000 | ~2,000 | 12회/일 | ~$0.02 |
| Analysis (비교/추적) | Sonnet | ~5,000 | ~800 | 10회/일 | ~$0.15 |
| Briefing Composer | Sonnet | ~8,000 | ~1,500 | 1회/일 | ~$0.03 |
| LLM Gold (daily) | Haiku | ~4,000 | ~200 | 50건 | ~$0.26 |
| **합계** | | | | | **~$0.64/일** |

월간 약 $19.2 → 연간 약 $233

### 7.3 인프라

기존 EC2 r6g.large (2vCPU, 16GB) 그대로 사용.
Spark는 scoring에서 제거됨. LangGraph + Pandas로 in-process 처리.

```
메모리 할당:
  ├─ OS + 시스템: 2 GB
  ├─ FAISS Index (51,583 vectors): ~500 MB
  ├─ uvicorn (FastAPI): 2 GB
  ├─ LangGraph agents (in-process): 2 GB
  ├─ SentenceTransformer (384-dim): 2 GB
  ├─ Pandas DataFrame (일 50건 + 1개월 DART): ~1 GB
  ├─ 모델 prediction parquet 로드: ~50 MB
  └─ 여유: ~6.5 GB

Spark는 Delta Lake MERGE 용도로만 선택적 사용 (cold-start 15초).
Phase 2b에서 Delta Lake 도입 시에만 spark-submit으로 ETL job 1회 실행.
실시간 챗봇 응답에는 Spark가 전혀 관여하지 않음.
```

### 7.4 Delta Lake MERGE — ETL 설명 및 추진 근거

#### MERGE가 정확히 뭐 하는 건가

```
현재 상태: S3에 Parquet 파일이 날짜/월별로 흩어져 있음
  s3://s3-opik-bucket/gold/structured/year=2026/month=01/data.parquet
  s3://s3-opik-bucket/gold/structured/year=2026/month=02/data.parquet
  ... 84개월치
  s3://s3-opik-bucket/gold/llm/year=2026/month=01/data.parquet
  ...

  문제: "삼성전자 리포트 지난 3개월치 보여줘" → 3개 파일을 각각 열어서
        read_parquet + concat 해야 함. 84개월 전체 검색은 84개 파일을 스캔.

Delta Lake 도입 후:
  spark.read.format("delta").load("s3://.../delta/gold_db/structured/")
    .filter("종목명 == '삼성전자' AND 발행일 >= '2026-03-01'")

  → Delta Lake 하나의 테이블에서 SQL 한 줄로 조회.
    내부적으로는 Delta transaction log가 어떤 파일에 어떤 데이터가
    들어있는지 알고 있어서 필요한 파일만 읽는다 (file pruning).
```

MERGE는 매일 새로 생긴 Parquet 데이터를 Delta Lake 테이블에 "덧붙이는" 작업이다:
```
오늘 새 Parquet 30건 ──→ MERGE INTO delta_table
                            ├─ 같은 report_id가 이미 있으면 UPDATE (재처리 대응)
                            └─ 없으면 INSERT
```

#### 왜 진행해야 하는가

| 이유 | 현실적 근거 |
|------|-----------|
| SQL 한 줄로 날짜/종목 범위 검색 | 지금은 for 루프로 월별 Parquet 열고 concat — 84개월이 점점 느려짐 |
| 챗봇이 즉시 응답 가능 | DART Agent가 "삼성전자 최근 공시" 조회할 때 1개 테이블만 쿼리 |
| Time Travel | "어제랑 비교해줘" → `VERSION AS OF`로 이전 스냅샷을 복제 없이 조회 |
| 데이터 정합성 | MERGE는 ACID 트랜잭션 — Spark job 실패해도 partial write 없음 |
| Schema Evolution | LLM 필드 추가돼도 기존 데이터 그대로 + 새 컬럼만 ALTER TABLE |

#### 작업 범위 (Phase 2a에 포함)

```
1. EC2에서 Spark local mode로 spark-submit (15초 cold-start, 하루 1회)
   → JVM cold-start 15초는 07:00 새벽 배치에서 완전히 무시할 수 있는 수준.
   → 챗봇 실시간 응답 경로에는 Spark가 전혀 없음.
   → 06:00~07:00까지 1시간 데드라인에 15초는 전체 작업의 0.3%.
2. spark_silver_to_delta.py 실행:
   - gold/structured/ Parquet → Delta MERGE
   - gold/embeddings/ Parquet → Delta MERGE
   - gold/dart/facts/material_event/ Parquet → Delta MERGE
   - gold/dart/facts/regular_structured/ Parquet → Delta MERGE
3. 챗봇은 Delta 테이블을 읽도록 opik_server.py 수정
4. 기존 Parquet direct read 코드는 유지 (Delta가 없으면 fallback)

Delta Lake는 실시간 챗봇 응답용이 아니라
백엔드 데이터 레이어의 쿼리 효율성과 데이터 정합성을 위한 것이다.
챗봇 agent가 Delta를 읽어야 하는 게 아니라,
브리핑 파이프라인이 하루 한 번 Delta에 MERGE하고,
챗봇은 Delta 테이블을 SQL로 조회한다.
```

#### DART 공시 sentiment 분류: Agent 기반 (batch mode)

DART 공시의 긍정/부정은 키워드 매칭으로 판단하기 어렵다.
같은 "유상증자"라도 성장 투자용이면 긍정, 부실 방어용이면 부정이다.
기업 상황과 공시 문맥을 읽을 수 있는 Agent(LLM)에게 맡긴다.

**속도 걱정 해소 — 배치 처리**:
```
1개월 DART 공시는 보통 200~300건.
이걸 하나씩 Haiku로 보내면 200~300회 API 호출 → 순차 실행 시 3~5분.

→ 배치 처리: 한 번의 LLM 호출에 20~30건씩 묶어서 보낸다.
  각 공시 = {종목코드, 공시유형, 제목, 요약(첫 300자)}
  출력 = JSON 배열 [{종목코드, sentiment, reason}]

  300건 ÷ 25건/호출 = 12회 호출
  12회 × asyncio 20 병렬 → 사실상 동시 실행 → 1~2초 내 완료

  Haiku 1회 호출(입력 8K tokens + 출력 2K tokens) ≈ $0.002
  12회 × $0.002 = $0.024/일 → 월 $0.72
```

**DART Sentiment Agent 프롬프트**:
```
당신은 한국 DART 공시의 시장 영향을 평가하는 금융 AI입니다.
주어진 공시 목록을 분석하여 각각의 sentiment를 판단하세요.

출력 형식 (JSON 배열):
[{"ticker": "005930", "sentiment": "positive", "reason": "자사주 500억 매입, 주주환원 강화"},
 {"ticker": "000660", "sentiment": "negative", "reason": "유상증자 2조, 주가희석 우려"},
 {"ticker": "035720", "sentiment": "neutral",  "reason": "정기주총 소집공고, 일상적 공시"}]

판단 기준:
- positive: 주주가치 제고, 성장 모멘텀, 재무건전성 개선을 시사하는 공시
- negative: 주가 희석, 재무 리스크, 영업 악화, 법적 리스크를 시사하는 공시
- neutral: 일상적·의례적 공시, 영향 미미, 정보 부족으로 판단 불가

중요:
- 공시유형명만 보지 말고 제목과 요약의 구체적 내용을 읽고 판단할 것
- 같은 유상증자라도 목적(시설투자 vs 채무상환)에 따라 sentiment가 달라짐
- 판단이 모호하면 망설이지 말고 neutral로 분류할 것
- 각 판단의 근거를 reason에 한글로 15단어 이내로 작성할 것
```

**처리 시점**:
```
매일 07:00 DART Sentiment Agent 실행 (briefing pipeline의 일부):
  1. 최근 1개월 DART material_event 로드 (gold/dart/facts/material_event/)
     → DartCollector Gold는 전일 08:00 compaction 기준 사용 (당일 07:00 gold builder 미완료 → 1영업일 lag)
  2. 25건씩 배치로 나누어 Haiku 병렬 호출
  3. sentiment(positive/negative/neutral) + reason 결과 수집
  4. 브리핑 step에서 이 sentiment 사용 (Delta MERGE 불필요 — 인메모리 처리)

전체 Briefing DAG 타임라인 (07:00 KST):
  07:00 - spark_silver_to_delta.py (gold/structured + gold/embeddings Delta MERGE)
  07:00:02 - DART material_event S3 로드
  07:00:03 - dart_sentiment_agent (Haiku batch, 1~2초)
  07:00:05 - triple_consensus 체크 (Pandas in-process)
  07:00:06 - Briefing compose → Telegram 전송
```



### Phase 2a — Agent Framework + Delta Lake (2주)

```
Week 1: LangGraph 코어 + Delta Lake 기반
  □ LangGraph + LangChain 설치 및 기본 그래프 구성
  □ AgentState TypedDict 정의
  □ Safety Agent + Intent Parser Agent 구현
  □ Report Agent + DART Agent 구현 (기존 FAISS/S3 로직 래핑)
  □ Supervisor Router 구현
  □ spark_silver_to_delta.py EC2 배포 및 cron 등록 (매일 07:00)
    - gold/structured Parquet → Delta MERGE
    - gold/embeddings Parquet → Delta MERGE
    - gold/dart/facts/material_event/ Parquet → Delta MERGE
  □ DART Sentiment Agent 구현 (Haiku batch — 25건/호출, asyncio 병렬)
    - 최근 1개월 material_event S3 로드 후 sentiment(positive/negative/neutral) 분류
    - 키워드 매칭 아님 — LLM이 공시 제목+요약을 읽고 맥락 판단
  □ 기존 84개월 Parquet → Delta 초기 적재 (일회성 백필)

Week 2: Analysis Agent + 통합
  □ Analysis Agent - compare_reports 구현
  □ Analysis Agent - industry_analysis 구현
  □ Analysis Agent - trace_cause 구현
  □ Response Composer 구현
  □ /chat 엔드포인트에 LangGraph 연동
  □ 챗봇 DART/DART Agent가 Delta 테이블을 읽도록 opik_server.py 수정
    (Delta 우선, 없으면 Parquet fallback)
  □ E2E 테스트 (챗봇 4개 신규 기능 + Delta 쿼리 검증)
```

### Phase 2b — Briefing Redesign (1주)

```
Week 3: 브리핑 파이프라인
  □ Briefing DAG 생성 (schedule: 0 7 * * *, 기존 리포트 DAG와 분리)
    - 리포트 Airflow(00:00)는 현행 유지
    - 브리핑 DAG(07:00)는 독립 실행 — 모든 소스 준비 후 트리거
  □ BriefingState + briefing_graph 구현
  □ DART material_event 로더 구현 (gold/dart/facts/material_event/ S3 경로)
  □ triple_consensus 체크 로직 구현 (Pandas in-process)
  □ compose_daily_briefing (★/! 새 포맷) 구현
  □ telegram_briefing.py를 LangGraph Briefing Graph로 교체
  □ Chanho 모델 prediction 연동 인터페이스 (ranking_score + pred_close_price 로드)
  □ 3일간 dry-run 테스트 (★/! 품질 검증)
```

### Phase 2c — Production Hardening (1주)

```
Week 4: 안정화
  □ 에러 핸들링 (agent timeout, LLM failover)
  □ LangGraph tracing (LangSmith or custom)
  □ CloudWatch 메트릭 + 알람
  □ Elastic IP 할당
  □ deploy.sh SSM 기반으로 교체
  □ CHATBOT_RESPONSE_POLICY.md와 프롬프트 정합성 검토
```

## 9. 기존 코드 변경 범위

Spark는 scoring에서는 완전히 제거 (Pandas로 대체).
다만 Delta Lake MERGE 용도로 하루 1회 spark-submit 실행은 유지.
JVM cold-start 15초는 nightly batch에서는 무시할 수 있는 수준이므로 문제없다.

| 파일 | 변경 | 설명 |
|------|------|------|
| `opik_server.py` | 대폭 수정 | `/chat` 엔드포인트에 LangGraph 연동, Delta 우선 읽기 |
| `spark_silver_to_delta.py` | 수정 + 배포 | EC2 cron 등록 (매일 07:00), Delta MERGE 전용
| `intent_parser.py` | 수정 | Agent 호환 인터페이스 추가 |
| `telegram_briefing.py` | 재작성 | LangGraph Briefing Graph 기반, Spark 의존성 제거 |
| `prompts/system.md` | 수정 | Multi-agent 역할 체계 반영 |
| `prompts/intent_parser.md` | 수정 | compare/interpret/cause_tracking 플래그 추가 |
| `prompts/answer_generator.md` | 분할 | 각 Agent별 프롬프트로 분리 |
| `requirements.txt` | 추가 | langgraph, langchain, langchain-aws |
| `deploy.sh` | 재작성 | SSH → SSM 기반 |
| `spark_compute_scores.py` | **삭제** | scoring을 LangGraph Agent가 Pandas로 대체 |

신규 파일:
- `server/agents/__init__.py`
- `server/agents/safety_agent.py`
- `server/agents/intent_agent.py`
- `server/agents/report_agent.py`
- `server/agents/dart_agent.py`
- `server/agents/dart_sentiment_agent.py`    # DART 공시 배치 sentiment 분류 (Haiku batch)
- `server/agents/analysis_agent.py`
- `server/agents/response_composer.py`
- `server/agents/supervisor.py`
- `server/agents/briefing_graph.py`          # ★/! 브리핑 LangGraph (Pandas in-process)
- `server/spark_jobs/spark_silver_to_delta.py`  # Delta MERGE (하루 1회 Spark)
- `dags/briefing/daily_briefing.py`          # Briefing DAG (schedule: 0 7 * * *)

## 10. 한계점과 위험

1. **Sonnet latency**: Analysis Agent는 Sonnet 추론에 3~5초 소요. 사용자 경험을 위해 스트리밍 응답 검토 필요.
2. **LLM 비용 증가**: 월 $19 → 원안 $8 대비 2.4배. 채택률이 오르면 추가 최적화 필요.
3. **LangGraph 학습 곡선**: 팀원들이 LangGraph 개념(state, node, edge, conditional routing)에 익숙해지는 시간 필요.
4. **r6g.large 충분**: Spark scoring 제거로 메모리 여유 6.5GB 확보. LangGraph + FAISS + uvicorn 동시 구동 충분. Delta MERGE는 하루 1회 spark-submit(6GB 할당), 완료 후 JVM 종료하므로 메모리 경합 없음.
5. **Triple consensus false positive**: 3개 신호가 모두 긍정이어도 실제 주가 하락 가능. ★는 "신호 일치"이지 "수익 보장"이 아니라는 점을 브리핑에 명시.
6. **모델 신호**: 찬호 모델의 LightGBM regression output인 `ranking_score`는 raw return 예측치다. 부호(> 0)가 상승 전망을 의미한다. 모든 종목이 음수인 날(전체 하락 예측)에는 ★가 나오지 않는다. 브리핑에는 `pred_close_price`(모델 예측 종가)를 참고가로 표시한다.
7. **DART Sentiment 분류 정확도**: Haiku batch 분류의 정확도는 실제 DART 공시 100건 샘플로 측정 필요. 85% 미만이면 Sonnet으로 상향하거나 입력 데이터(공시 요약 텍스트)의 품질을 먼저 개선한다. 공시 제목만으로는 유상증자의 긍정/부정을 판단할 수 없으므로 DartCollector Gold의 `rag_document` 원문 텍스트를 입력에 포함할지 검토.
8. **Delta 초기 백필 시간**: 84개월치 Parquet를 Delta로 처음 적재할 때 spark-submit이 2~3분 소요 예상. 일회성 작업이므로 nightly pipeline 이전에 수동 실행.

## 11. 추가 고려사항 및 결정 대기 항목

### 11.1 DartCollector — OPIK EC2 공동 배포 (확정)

**결정**: 상용님 DartCollector Airflow를 OPIK EC2(r6g.large, 16GB)에 공동 배포한다.

배포 시 고려사항:
- PostgreSQL: OPIK Airflow(airflow DB) + DartCollector(dart_service DB) 별도 database로 공존 가능
- Redis: OPIK Celery broker(6379) + DartCollector 별도 port 또는 동일 Redis의 별도 DB 번호
- 메모리: r6g.large 16GB — OPIK Airflow(scheduler+worker+webserver 4GB) + DartCollector Airflow(4GB) + Spark JVM(6GB) + LangGraph+FAISS(2GB) = 16GB 내 수용
- DartCollector DAG schedule: 기존대로 07:00~18:59 영업시간 내 5분 간격 유지

### 11.2 Chanho 모델 prediction 연동 인터페이스 (확정)

S3 업로드 방식으로 구현 완료:
- 경로: `s3://s3-opik-bucket/gold/model/predictions/dt={YYYY-MM-DD}/predictions.parquet`
- 노출 컬럼: `prediction_date`, `ticker`, `ticker_name`, `ranking_score`, `pred_close_price`
- Briefing DAG가 Step 5에서 pandas.read_parquet(s3_key)로 직접 로드

### 11.3 DART Sentiment Agent — 입력 데이터 검증 완료

S3에 올라와 있는 DART Gold 데이터를 확인했다.

**Phase 2 1차 소스: OPIK 자체 Gold (`gold/dart/disclosure_events/`)**
```
경로: gold/dart/disclosure_events/dt={YYYY-MM}/data.parquet
규모: 10개 파일 (dt=2024-08 ~ dt=2026-03), 파일당 12K~16K행
스키마: corp_code, stock_code, corp_name, rcept_no, rcept_dt,
        report_nm(제목), event_category(17개 분류),
        text_len, text(원문 전문 — PyMuPDF 추출), extracted_amount

핵심: text 컬럼에 공시 원문 전문이 들어있다.
      → Sentiment Agent에 report_nm + text를 함께 입력하면 맥락 판단 가능```

**Phase 2 2차 소스(추후 전환): DartCollector Gold (`gold/dart/facts/material_event/`)**
```
경로: gold/dart/facts/material_event/event_type={type}/rcept_year={Y}/rcept_month={M}/part-*.parquet
현재 상태: 331개 파일, 2026년 데이터 41행 — 매우 희소
           counterparty 컬럼 전부 null, amount 5/41건만 존재
           normalized_text는 있지만 매우 짧은 요약(50~100자 수준)

전환 조건: DartCollector ETL 확정 + 데이터 충분히 축적된 후 전환. Phase 2a는 OPIK disclosure_events로 진행.
```

### 11.4 LangGraph → Airflow 실행 방식 (확정)

**결정**: 단일 PythonOperator로 `briefing_graph.py`를 통째로 실행한다.

이유: LangGraph StateGraph 자체가 이미 노드 간 실행 순서, 조건부 라우팅(state conditional edge), 상태 전이를 관리한다. Airflow가 다시 태스크로 쪼개면 이중 orchestration 문제가 생긴다:
- Airflow task 간 데이터 전달 = XCom (직렬화 필요, 큰 DataFrame에 부적합)
- LangGraph state 간 데이터 전달 = in-memory dict (zero copy)
- 전체 파이프라인 소요 시간 2~6초 → Airflow task 분할 오버헤드(스케줄링 지연 + XCom 직렬화)가 실제 작업 시간보다 큼
- 실패 처리: LangGraph 내에서 try/except + Supervisor fallback으로 충분

```python
# dags/briefing/daily_briefing.py
run_briefing = PythonOperator(
    task_id="run_briefing",
    python_callable=run_briefing_pipeline,  # briefing_graph.py의 main 함수
    op_kwargs={"date": "{{ ds_nodash }}"},
)
```

### 11.5 ! 티어 대상 공시유형 (확정)

**결정**: B-type(주요사항보고)만 대상으로 한다. 다른 타입은 제외 근거가 명확하다.

| 공시유형 | 포함 | 근거 |
|----------|------|------|
| **B (주요사항보고)** | ★ | 유상증자, 감자, 부도발생, 회생절차, 계약체결, 최대주주변경 등 — 당일 주가 impact가 가장 큰 공시. DART Sentiment Agent가 맥락 분류하는 대상도 이것. |
| A (정기공시) | X | 사업/분기/반기보고서 — 실적 서프라이즈는 중요하지만, 발표일 기준으로도 며칠 전 예상치가 이미 반영됨. 07:00 브리핑에서 실적을 실시간으로 분석하기엔 데이터 부족. Phase 2b에서 실적 분석 Agent 추가 시 검토. |
| C (발행공시) | X | 증권신고서 — IPO/유상증자 실행 단계. B-type 유상증자결정에서 이미 감지됨. 후속 절차 문서일 뿐. |
| D (지분공시) | X | 임원·주요주주 지분 변동 — insider signal로서 유의미하나, 신고일이 거래일보다 수일 늦어 이미 주가에 반영됨. Phase 2b에서 insider tracking Agent 추가 시 검토. |
| E (기타공시) | X | 정정공시 — 원본 공시(B/A)의 수정본. 독립적 신호가 아니라 원본의 부속물. |
| F~J | X | 외부감사/펀드/자산유동화/거래소/공정위 — 주식 종목 직접 영향 없음. |

**! 티어 동작 요약**:
- 매일 07:00, DART Sentiment Agent가 B-type 공시를 batch 분류(positive/negative/neutral + 1-line reason)
- negative/neutral = 제외 (★ 조건인 "DART positive"가 아니므로)
- positive + BUY 리포트 + model ranking_score > 0 → ★ (triple consensus)
- positive지만 BUY 리포트 없음 → ! (major disclosure alert) — 단독 이벤트로 브리핑 하단에 표시

※ !는 ★처럼 3-source consensus가 아니라 DART 단독 시그널이다. cap 없이 모든 positive B-type 공시를 보여주되, Sentiment Agent가 context-dependent 분류로 필터링한다.

### 11.6 Telegram 브리핑 레이아웃 (확정)

설계문서 Section 5.2/5.3에서 정의한 대로 유지:

```
[OPIK 브리핑] 2026-06-20 (금) 07:00

★ TRIPLE CONSENSUS (리포트 + DART + 모델)
━━━━━━━━━━━━━━━━━━━━━━━━━━━
삼성전자 (모델: 85,000원 / 리포트 TP 85,000원)
  ✓ BUY (한국투자증권)
  ✓ DART: 유상증자결정(제3자배정) → 호재 - 신규사업 투자
  ✓ 모델: ranking_score +0.032
  [...]

! MAJOR DISCLOSURES (B-type)
━━━━━━━━━━━━━━━━━━━━━━━━━━━
카카오 - 단일판매공급계약체결
  → 대규모 계약 수주, 연매출 15% 규모 - 긍정적

```

---

## 12. 설계 확정 — 구현 돌입

2026-06-19 기준 모든 설계 결정 완료. 미결 사항 없음.

**Phase 2a 구현 목록**:
1. `server/agents/` — 7개 LangGraph Agent + Supervisor + Briefing Graph
2. `dags/briefing/daily_briefing.py` — Airflow DAG (schedule: 0 7 * * *)
3. DartCollector EC2 공동 배포 (PostgreSQL dart_service DB, Redis)
4. DART Sentiment Agent — Haiku batch (25건/배치, asyncio 20 병렬, OPIK disclosure_events)
5. Delta Lake MERGE — `spark_silver_to_delta.py` (07:00 cron, 하루 1회)
