# 9. Portfolio Construction Engine

## AI Trading System Design Document v1.0

---

# 1. Purpose

Portfolio Construction Engine은 AI Model의 예측값을 실제 투자 가능한 Portfolio로 변환하는 모듈이다.

본 시스템의 목표는

```text
높은 예측 정확도

↓

안정적인 Portfolio

↓

높은 Risk-adjusted Return
```

을 달성하는 것이다.

Portfolio Engine은 Model과 Execution 사이의 독립적인 계층으로 설계한다.

---

# 2. Portfolio Architecture

```
Universe

↓

Feature Generation

↓

Ranking Model

↓

Gap Model

↓

Intraday Model

↓

Prediction Merge

↓

Portfolio Construction Engine

↓

Execution Engine

↓

Backtest / Live Trading
```

---

# 3. Input Specification

Portfolio Engine의 입력은 Prediction Dataset이다.

```
date

ticker

ranking_score

pred_gap

pred_intraday

expected_return

atr_percent

volatility_20d

sector

market_type

market_cap_group

trading_value_rank_pct

confidence_score(optional)
```

---

# 4. Portfolio Philosophy

## Rule 1

AI Score를 신뢰한다.

Rule-based Selection은 최소화한다.

---

## Rule 2

수익률보다 Risk-adjusted Return을 우선한다.

---

## Rule 3

Diversification을 항상 유지한다.

---

## Rule 4

Prediction은 매일 새롭게 생성한다.

Portfolio는 매일 재구성한다.

---

# 5. Candidate Selection

Universe

↓

Ranking Score 기준 정렬

↓

Top30 Candidate 생성

```
Candidate Size = 30
```

---

## Candidate Rule

```
ranking_score descending

Top30
```

---

# 6. Expected Return Estimation

Model2와 Model3 결과를 결합한다.

```
expected_return

=

(1+pred_gap)

×

(1+pred_intraday)

-

1
```

---

## Expected Return Ranking

Top30 Candidate

↓

expected_return descending

↓

우선순위 생성

---

# 7. Liquidity Filter

거래 불가능한 종목 제거

## Rule

```
trading_value_ma20

>=

50억원
```

또는

```
trading_value_rank_pct

>=

0.20
```

---

# 8. Risk Filter

다음 조건을 만족해야 한다.

```
atr_percent

<=

0.08
```

```
volatility_20d

<=

0.08
```

---

## Optional

Extreme Gap Filter

```
abs(pred_gap)

<=

10%
```

---

# 9. Diversification Engine

## Sector Limit

```
최대 3종목 / Sector
```

예

```
반도체

3

자동차

2

바이오

2

인터넷

2

은행

1
```

---

## Market Type Limit

```
KOSPI

30~70%

KOSDAQ

30~70%
```

---

## Market Cap Diversification

```
Top20

≤40%

Top50

≤70%

Others

≥10%
```

---

# 10. Portfolio Scoring

최종 점수는 다음과 같이 계산한다.

```
portfolio_score

=

0.50 × ranking_score

+

0.30 × expected_return_rank

+

0.10 × liquidity_rank

+

0.10 × momentum_rank
```

---

## v1.0

동일 Weight 사용

---

## v2.0

Dynamic Weight Learning 예정

---

# 11. Portfolio Selection

```
Candidate

30

↓

Risk Filter

↓

Diversification

↓

Portfolio Score

↓

Top10
```

---

# 12. Position Sizing

## v1.0

Equal Weight

```
weight

=

1 / N
```

Top10이면

```
10%
```

---

## Future Extension

```
Confidence Weight

Expected Return Weight

Risk Parity

Kelly Fraction

Volatility Weight
```

---

# 13. Cash Management

v1.0

```
Cash

=

0%
```

항상 Fully Invested

---

Future

```
Cash Allocation

Dynamic Cash

Risk-off Mode
```

---

# 14. Daily Rebalancing

매일 수행

```
T-1 Feature

↓

Prediction

↓

Portfolio Construction

↓

T Open Buy

↓

T Close Sell
```

---

# 15. Portfolio Constraints

## Max Position

```
10%
```

---

## Min Position

```
10%
```

(Equal Weight)

---

## Max Sector Exposure

```
30%
```

---

## Max Market Exposure

```
KOSDAQ

70%

KOSPI

70%
```

---

## Max Turnover

```
100%
```

(Reference Only)

---

# 16. Risk Control

Portfolio 생성 이후 다음 항목을 계산한다.

```
Average ATR

Average Volatility

Sector Exposure

Market Exposure

Expected Return

Expected Risk
```

---

## Risk Score

```
risk_score

=

0.5×ATR

+

0.5×Volatility
```

---

# 17. Execution Dataset

Portfolio Engine Output

```
date

ticker

weight

ranking_score

pred_gap

pred_intraday

expected_return

portfolio_score

sector

market_type

buy_order

sell_order
```

---

# 18. Performance Attribution

Portfolio 성과를 다음으로 분해한다.

```
AI Selection Effect

Sector Allocation Effect

Market Allocation Effect

Execution Effect

Transaction Cost

Slippage
```

---

# 19. Portfolio Metrics

매일 계산

```
Expected Return

Expected Volatility

Number of Positions

Sector Exposure

Market Exposure

Turnover

Average Holding Score
```

---

# 20. Portfolio Lifecycle

```
Daily Universe

↓

Prediction

↓

Candidate Selection

↓

Risk Filter

↓

Diversification

↓

Weight Calculation

↓

Portfolio

↓

Execution

↓

Performance

↓

Next Day
```

---

# 21. Output Structure

```
outputs/

portfolio/

daily_portfolio.parquet

daily_positions.parquet

daily_orders.parquet

daily_metrics.json

daily_attribution.json
```

---

# 22. configs/portfolio.yaml

```yaml
portfolio:

  candidate_size: 30

  portfolio_size: 10

  weighting: equal_weight

  liquidity:

    min_trading_value_ma20: 5000000000

  risk:

    max_atr_percent: 0.08

    max_volatility_20d: 0.08

  diversification:

    max_sector_names: 3

    max_sector_weight: 0.30

    max_market_weight: 0.70

  rebalance:

    frequency: daily

  execution:

    buy: open

    sell: close
```

---

# 23. Codex Required Functions

```
build_candidate_list()

apply_liquidity_filter()

apply_risk_filter()

apply_sector_limit()

calculate_portfolio_score()

select_final_portfolio()

calculate_equal_weight()

generate_orders()

calculate_portfolio_metrics()

run_portfolio_engine()
```

---

# 24. Portfolio Design Philosophy

본 시스템은

```
Prediction Accuracy
```

를 직접 최적화하지 않는다.

최적화 대상은

```
Portfolio Return

↓

Sharpe Ratio

↓

Maximum Drawdown

↓

Long-term Compound Return
```

이다.

---

# 25. Final Architecture

```
Universe

↓

Feature Library

↓

Ranking Model

↓

Gap Model

↓

Intraday Model

↓

Prediction Merge

↓

Portfolio Construction Engine

↓

Execution Engine

↓

Backtest

↓

Live Trading
```

---

# Final Principle

```
좋은 AI 모델

≠

좋은 투자 시스템

--------------------------------

좋은 AI 모델

+

좋은 Portfolio Construction

+

좋은 Risk Management

=

좋은 AI Trading System
```
