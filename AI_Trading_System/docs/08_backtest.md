# 8. Backtest

## Purpose

본 문서는 AI Trading System v1.0의 백테스트 방법을 정의한다.

백테스트의 목적은 단순히 높은 과거 수익률을 만드는 것이 아니라,

```text
실제 운용 가능한 조건에서
모델의 예측력과 포트폴리오 성과를 검증하는 것
```

이다.

---

# 8.1 Backtest Core Principle

## Rule 1. Prediction First

백테스트는 반드시 예측 결과를 먼저 생성한 후 실행한다.

```text
Feature(T-1)
↓
Prediction(T)
↓
Portfolio Selection(T)
↓
Trade Execution(T)
↓
Return Calculation(T)
```

---

## Rule 2. Actual Return으로 종목을 선택하면 안 된다.

금지:

```text
target_rank_return
target_gap
target_intraday
actual_return
close(T)
```

허용:

```text
ranking_score
pred_gap
pred_intraday
expected_return
risk_score
```

---

# 8.2 Backtest Input

백테스트 입력 데이터는 다음을 포함해야 한다.

```text
date
ticker
ranking_score
pred_gap
pred_intraday
pred_open
pred_close
expected_return
open
close
volume
trading_value
sector
market_type
atr_percent
volatility_20d
```

---

# 8.3 Expected Return

예상 수익률은 Model 2와 Model 3의 예측값을 결합하여 계산한다.

```python
expected_return = (1 + pred_gap) * (1 + pred_intraday) - 1
```

또는

```python
expected_return = pred_close / close_t_minus_1 - 1
```

---

# 8.4 Trading Assumption

v1.0 기본 매매 가정은 다음과 같다.

```text
매수 시점: T일 시가
매도 시점: T일 종가
보유 기간: 1거래일 intraday
리밸런싱: 매일
매매 대상: Top10 종목
매수 방식: 동일가중
공매도: 없음
레버리지: 없음
```

---

# 8.5 Trade Price

## Buy Price

기본 매수가는 T일 시가이다.

슬리피지를 반영한다.

```python
buy_price = open_T * (1 + slippage)
```

---

## Sell Price

기본 매도가는 T일 종가이다.

슬리피지를 반영한다.

```python
sell_price = close_T * (1 - slippage)
```

---

# 8.6 Transaction Cost

v1.0 기본 거래비용은 다음과 같다.

```yaml
transaction_cost:
  buy_cost: 0.0015
  sell_cost: 0.0015
```

즉,

```text
매수 비용 0.15%
매도 비용 0.15%
왕복 비용 0.30%
```

---

# 8.7 Slippage

v1.0 기본 슬리피지는 다음과 같다.

```yaml
slippage:
  buy_slippage: 0.001
  sell_slippage: 0.001
```

즉,

```text
매수 시 0.10% 불리한 가격
매도 시 0.10% 불리한 가격
```

---

# 8.8 Net Trade Return

단일 종목의 순수익률은 다음과 같이 계산한다.

```python
gross_return = sell_price / buy_price - 1

net_return = gross_return - buy_cost - sell_cost
```

또는 명시적으로:

```python
buy_price = open_T * (1 + buy_slippage)
sell_price = close_T * (1 - sell_slippage)

gross_return = sell_price / buy_price - 1

net_return = gross_return - buy_cost - sell_cost
```

---

# 8.9 Portfolio Selection

기본 후보군 선정 방식:

```text
1. Daily Universe 로드
2. ranking_score 기준 Top30 후보 선정
3. 유동성 필터 적용
4. 위험 필터 적용
5. expected_return 기준 정렬
6. 최종 Top10 선정
```

---

## v1.0 기본 선정 기준

```text
Ranking Model Top30
↓
expected_return 상위 Top10
```

---

# 8.10 Liquidity Filter

거래 가능성을 확보하기 위해 다음 필터를 적용한다.

```text
trading_value_ma20 >= 50억원
```

또는

```text
trading_value_rank_pct >= 0.2
```

---

# 8.11 Risk Filter

다음 조건을 적용할 수 있다.

```text
atr_percent <= max_atr_percent
volatility_20d <= max_volatility
```

v1.0 기본값:

```yaml
risk_filter:
  max_atr_percent: 0.08
  max_volatility_20d: 0.08
```

---

# 8.12 Sector Diversification

Top10 포트폴리오에서 특정 업종 쏠림을 제한한다.

v1.0 기본값:

```yaml
sector_limit:
  max_names_per_sector: 3
```

예:

```text
반도체 최대 3종목
바이오 최대 3종목
자동차 최대 3종목
```

---

# 8.13 Portfolio Weighting

v1.0 기본은 동일가중이다.

```python
weight = 1 / number_of_selected_stocks
```

Top10이면:

```text
종목당 10%
```

---

## v2.0 대안

```text
expected_return weighting
risk parity weighting
volatility adjusted weighting
confidence weighted weighting
```

---

# 8.14 Daily Portfolio Return

일별 포트폴리오 수익률은 선택 종목의 순수익률 가중합이다.

```python
daily_portfolio_return = sum(weight_i * net_return_i)
```

동일가중 Top10이면:

```python
daily_portfolio_return = mean(net_return_i for selected stocks)
```

---

# 8.15 Cumulative Return

누적수익률:

```python
cumulative_return = (1 + daily_return).cumprod() - 1
```

---

# 8.16 Performance Metrics

백테스트는 다음 지표를 계산해야 한다.

```text
Cumulative Return
Annual Return
Annual Volatility
Sharpe Ratio
Sortino Ratio
Maximum Drawdown
Calmar Ratio
Win Rate
Average Daily Return
Average Trade Return
Turnover
Hit Ratio
Profit Factor
```

---

## Annual Return

```python
annual_return = (1 + cumulative_return_final) ** (252 / num_trading_days) - 1
```

---

## Annual Volatility

```python
annual_volatility = daily_return.std() * sqrt(252)
```

---

## Sharpe Ratio

```python
sharpe_ratio = annual_return / annual_volatility
```

v1.0에서는 risk-free rate를 0으로 가정한다.

---

## Maximum Drawdown

```python
cum = (1 + daily_return).cumprod()
running_max = cum.cummax()
drawdown = cum / running_max - 1
mdd = drawdown.min()
```

---

## Win Rate

```python
win_rate = number_of_positive_days / total_days
```

---

# 8.17 Benchmark

성과 비교 기준은 다음을 사용한다.

```text
KOSPI200
KOSDAQ150
Equal-weight Universe
```

---

## Benchmark Return

```python
benchmark_return = benchmark_close / benchmark_close.shift(1) - 1
```

---

# 8.18 Turnover

일별 포트폴리오 교체율을 계산한다.

```python
turnover = 1 - overlap(previous_holdings, current_holdings) / portfolio_size
```

예:

```text
어제 보유 10개
오늘 보유 10개
겹치는 종목 6개

turnover = 1 - 6/10 = 40%
```

---

# 8.19 Backtest Output

백테스트 결과는 다음 구조로 저장한다.

```text
outputs/backtests/
  daily_portfolio.csv
  daily_positions.csv
  daily_trades.csv
  performance_metrics.json
  benchmark_comparison.csv
  drawdown.csv
```

---

## daily_positions.csv

```text
date
ticker
weight
ranking_score
pred_gap
pred_intraday
expected_return
buy_price
sell_price
gross_return
net_return
sector
```

---

## daily_portfolio.csv

```text
date
daily_return
cumulative_return
benchmark_return
benchmark_cumulative_return
drawdown
turnover
num_positions
```

---

# 8.20 Data Leakage Rules

백테스트는 다음을 절대 위반해서는 안 된다.

금지:

```text
실제 T일 수익률을 보고 Top10 선정
T일 종가를 보고 매수 여부 결정
T일 거래대금을 보고 유동성 필터 적용
T일 변동성을 보고 위험 필터 적용
```

허용:

```text
T-1일 Feature
예측값
T일 시가 매수
T일 종가 매도
```

---

# 8.21 Required Tests

`tests/test_backtest.py`는 다음을 검증해야 한다.

```text
1. Portfolio selection이 actual return을 사용하지 않음
2. Top10 종목 수가 정확함
3. Sector limit이 지켜짐
4. 거래비용이 반영됨
5. 슬리피지가 반영됨
6. Net return 계산이 올바름
7. Daily return이 position return의 가중합과 일치함
8. 누적수익률 계산이 올바름
9. MDD 계산이 올바름
10. Benchmark 비교가 생성됨
```

---

# 8.22 Codex Implementation Requirements

Codex는 다음 파일을 구현해야 한다.

```text
src/backtest/portfolio.py
src/backtest/simulator.py
src/backtest/metrics.py
tests/test_backtest.py
configs/backtest.yaml
```

---

## Required Functions

```python
select_daily_portfolio()

apply_liquidity_filter()

apply_risk_filter()

apply_sector_limit()

calculate_trade_return()

calculate_daily_portfolio_return()

calculate_performance_metrics()

run_backtest()
```

---

# 8.23 configs/backtest.yaml

예시:

```yaml
backtest:
  initial_capital: 100000000
  portfolio_size: 10
  candidate_size: 30

  execution:
    buy_price: "open"
    sell_price: "close"
    holding_period: "intraday"

  transaction_cost:
    buy_cost: 0.0015
    sell_cost: 0.0015

  slippage:
    buy_slippage: 0.001
    sell_slippage: 0.001

  filters:
    min_trading_value_ma20: 5000000000
    max_atr_percent: 0.08
    max_volatility_20d: 0.08

  diversification:
    max_names_per_sector: 3

  weighting:
    method: "equal_weight"
```

---

# 8.24 Forbidden Practices

다음은 명시적으로 금지한다.

```text
실제 수익률을 기준으로 종목 선정
T일 종가를 사용한 사전 필터링
T일 거래량을 사용한 유동성 필터
T일 변동성을 사용한 위험 필터
거래비용 미반영
슬리피지 미반영
Benchmark 없는 성능 평가
누적수익률만 보고 모델 선택
```

---

# 8.25 Final Principle

백테스트의 목적은 좋은 숫자를 만드는 것이 아니다.

목적은 다음이다.

```text
실제 운용 가능한 조건에서
모델이 수익성과 리스크를 개선하는지 검증하는 것
```

따라서 백테스트는 항상 보수적으로 설계한다.

```text
보수적인 백테스트
>
과장된 백테스트
```
