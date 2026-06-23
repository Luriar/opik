# AI Trading System Design Document v1.0

# 5. Feature Library

# Part 1

* Design Principles
* Naming Convention
* Price Feature
* Momentum Feature

---

# 5.1 Design Principles

## 목적

Feature Library는 AI Trading System에서 사용하는 모든 입력 Feature를 정의한다.

모든 모델(Ranking Model, Gap Model, Intraday Model)은 동일한 Feature Library를 공유한다.

---

## Design Philosophy

### Rule 1. Return First

가능하면 가격보다 수익률(Return)을 사용한다.

Good

```text
return_5d
close_ma20_ratio
```

Bad

```text
close
ma20
```

이유

```text
가격 수준이 다른 종목 간 일반화 성능 향상
```

---

### Rule 2. Ratio First

가능하면 절대값보다 Ratio를 사용한다.

Good

```text
atr_percent

bb_position

close_ma20_ratio
```

Bad

```text
atr14

close

ma20
```

---

### Rule 3. Relative Feature Preferred

가능하면 Relative Feature를 사용한다.

Example

```text
relative_return_5d_vs_market

relative_return_20d_vs_sector

momentum_rank_pct
```

---

### Rule 4. Cross-sectional Feature 적극 활용

동일 날짜 Universe 내 Ranking Feature를 적극 사용한다.

Example

```text
return_rank_pct

trading_value_rank_pct

momentum_rank_pct

volatility_rank_pct
```

---

### Rule 5. T-1 Rule

모든 Feature는 반드시 T-1일까지 생성한다.

사용 가능

```text
Close(T-1)

Volume(T-1)

NASDAQ(T-1)

SOX(T-1)
```

사용 금지

```text
Open(T)

Close(T)

High(T)

Low(T)

Volume(T)
```

---

### Rule 6. Feature는 사람이 이해 가능해야 한다.

Feature 이름만 보고 의미를 알 수 있어야 한다.

Good

```text
return_5d

bb_position

atr_percent

momentum_rank_pct
```

Bad

```text
feature001

factorA

x12
```

---

# 5.2 Naming Convention

## Return

```text
return_{period}
```

Example

```text
return_1d

return_3d

return_5d

return_20d

return_60d
```

---

## Ratio

```text
{feature}_ratio
```

Example

```text
close_ma5_ratio

close_ma20_ratio

atr_percent

macd_hist_ratio
```

---

## Rank

```text
{feature}_rank_pct
```

Example

```text
return_rank_pct

momentum_rank_pct

trading_value_rank_pct
```

---

## Relative

```text
relative_{feature}
```

Example

```text
relative_return_5d_vs_market

relative_return_20d_vs_sector

relative_trading_value
```

---

## Position

```text
{feature}_position
```

Example

```text
bb_position

close_position
```

---

## Change

```text
{feature}_change_{period}
```

Example

```text
rsi_change_5d

bb_position_change_5d
```

---

# 5.3 Price Feature

Price Feature는 가장 기본적인 Feature이다.

모든 모델에서 사용한다.

---

## return_1d

Definition

전일 수익률

Formula

```python
return_1d = close / close.shift(1) - 1
```

Range

```text
(-1, +∞)
```

Used Model

```text
Ranking

Gap

Intraday
```

Scaling

```text
None
```

Leakage

```text
close(T-1)까지만 사용
```

---

## return_3d

Formula

```python
return_3d = close / close.shift(3) - 1
```

Meaning

최근 3일 Momentum

---

## return_5d

Formula

```python
return_5d = close / close.shift(5) - 1
```

Meaning

최근 1주 Momentum

---

## return_20d

Formula

```python
return_20d = close / close.shift(20) - 1
```

Meaning

최근 1개월 Momentum

---

## return_60d

Formula

```python
return_60d = close / close.shift(60) - 1
```

Meaning

중기 Trend

---

## close_ma5_ratio

Formula

```python
ma5 = close.shift(1).rolling(5).mean()

close_ma5_ratio = close / ma5 - 1
```

Meaning

현재 가격과 MA5의 거리

---

## close_ma20_ratio

Formula

```python
ma20 = close.shift(1).rolling(20).mean()

close_ma20_ratio = close / ma20 - 1
```

Meaning

중기 Trend

---

## close_ma60_ratio

Formula

```python
ma60 = close.shift(1).rolling(60).mean()

close_ma60_ratio = close / ma60 - 1
```

Meaning

장기 Trend

---

## close_position

Definition

최근 20일 가격 범위 내 현재 위치

Formula

```python
high20 = high.shift(1).rolling(20).max()

low20 = low.shift(1).rolling(20).min()

close_position = (close - low20) / (high20 - low20)
```

Range

```text
0 ~ 1
```

Meaning

0

최근 20일 최저 수준

1

최근 20일 최고 수준

---

# Price Feature Summary

| Feature          | Recommended |
| ---------------- | ----------- |
| return_1d        | ★★★★★       |
| return_3d        | ★★★★☆       |
| return_5d        | ★★★★★       |
| return_20d       | ★★★★★       |
| return_60d       | ★★★★☆       |
| close_ma5_ratio  | ★★★★☆       |
| close_ma20_ratio | ★★★★★       |
| close_ma60_ratio | ★★★★☆       |
| close_position   | ★★★★★       |

---

# 5.4 Momentum Feature

Momentum Feature는 본 프로젝트의 핵심 Feature이다.

Ranking Model에서 가장 중요한 Feature 그룹으로 사용한다.

---

## momentum_5d

Formula

```python
momentum_5d = close / close.shift(5) - 1
```

---

## momentum_20d

Formula

```python
momentum_20d = close / close.shift(20) - 1
```

---

## momentum_diff

Definition

단기 Momentum과 중기 Momentum의 차이

Formula

```python
momentum_diff = momentum_5d - momentum_20d
```

Meaning

양수

최근 Momentum 강화

음수

Momentum 둔화

---

## momentum_accel

Formula

```python
momentum_accel = return_5d - return_20d
```

Meaning

Momentum Acceleration

---

## relative_return_5d_vs_market

Formula

```python
relative_return_5d_vs_market

=

return_5d

-

market_return_5d
```

Meaning

시장 대비 초과 수익률

---

## relative_return_20d_vs_sector

Formula

```python
relative_return_20d_vs_sector

=

return_20d

-

sector_return_20d
```

Meaning

업종 대비 초과 수익률

---

## momentum_rank_pct

Definition

Universe 내 Momentum Percentile

Formula

```python
momentum_rank_pct

=

momentum_5d.rank(pct=True)
```

Range

```text
0 ~ 1
```

Meaning

1

Universe 최고 Momentum

0

Universe 최저 Momentum

---

## return_rank_pct

Formula

```python
return_rank_pct

=

return_5d.rank(pct=True)
```

---

# Momentum Feature Summary

| Feature                       | Recommended |
| ----------------------------- | ----------- |
| momentum_5d                   | ★★★★★       |
| momentum_20d                  | ★★★★★       |
| momentum_diff                 | ★★★★★       |
| momentum_accel                | ★★★★☆       |
| relative_return_5d_vs_market  | ★★★★★       |
| relative_return_20d_vs_sector | ★★★★★       |
| momentum_rank_pct             | ★★★★★       |
| return_rank_pct               | ★★★★★       |

---

# Part 1 Recommended Feature Set

## Price

```text
return_1d
return_3d
return_5d
return_20d
return_60d

close_ma5_ratio
close_ma20_ratio
close_ma60_ratio

close_position
```

---

## Momentum

```text
momentum_5d
momentum_20d

momentum_diff
momentum_accel

relative_return_5d_vs_market
relative_return_20d_vs_sector

momentum_rank_pct
return_rank_pct
```

---

# Part 1 Implementation Rules

```text
1. 모든 Feature는 T-1까지 생성한다.

2. Return은 Ratio 형태를 사용한다.

3. Absolute Price는 직접 Feature로 사용하지 않는다.

4. Cross-sectional Rank는 Daily Universe 기준으로 계산한다.

5. Feature 생성 후 Missing Value를 검사한다.

6. Feature 생성 과정에서 Target 정보를 참조해서는 안 된다.

7. Feature 이름은 Naming Convention을 반드시 따른다.
```
# AI Trading System Design Document v1.0

# 5. Feature Library

# Part 2

* Volume Feature
* Volatility Feature
* Candlestick Feature
* Breakout Feature

---

# 5.5 Volume Feature

## 목적

Volume Feature는 시장 참여자의 관심도와 수급 강도를 표현한다.

단순 거래량보다 거래대금(Trading Value)을 우선 사용하며,
가능하면 절대값보다 Ratio와 Cross-sectional Rank를 사용한다.

---

# Design Principles

Priority

```text
Trading Value Ratio
>
Trading Value Rank
>
Volume Change
>
Absolute Volume
```

Absolute Volume은 종목별 차이가 매우 크므로 v1.0에서는 사용하지 않는다.

---

# volume_change_1d

## Definition

전일 대비 거래량 변화율

## Formula

```python
volume_change_1d = volume / volume.shift(1) - 1
```

## Meaning

```text
양수

거래량 증가

------------------

음수

거래량 감소
```

---

## Used Model

```text
Ranking

Intraday
```

---

# trading_value

## Definition

일별 거래대금

## Formula

```python
trading_value = close * volume
```

---

## Note

Absolute Trading Value는 직접 Feature로 사용하지 않는다.

Ratio 또는 Rank Feature 생성의 중간 변수로만 사용한다.

---

# trading_value_ma20

## Formula

```python
trading_value_ma20

=

trading_value.shift(1).rolling(20).mean()
```

---

# relative_trading_value

## Definition

현재 거래대금이 최근 평균 대비 얼마나 큰가

## Formula

```python
relative_trading_value

=

trading_value

/

trading_value_ma20
```

---

## Interpretation

```text
1

평균 수준

-------------------

2

평균 대비 2배 거래

-------------------

0.5

평균 대비 절반 거래
```

---

# trading_value_rank_pct

## Definition

Universe 내 거래대금 Percentile

## Formula

```python
trading_value_rank_pct

=

relative_trading_value.rank(pct=True)
```

---

## Range

```text
0 ~ 1
```

---

# Volume Feature Summary

| Feature                | Recommended |
| ---------------------- | ----------- |
| volume_change_1d       | ★★★★☆       |
| relative_trading_value | ★★★★★       |
| trading_value_rank_pct | ★★★★★       |

---

# 5.6 Volatility Feature

## 목적

Volatility Feature는 가격 움직임의 위험도와 변동성을 표현한다.

Ranking뿐 아니라 Portfolio Risk Control에도 사용된다.

---

# ret

## Formula

```python
ret = close.pct_change()
```

---

# volatility_5d

## Formula

```python
volatility_5d

=

ret.shift(1).rolling(5).std()
```

---

## Meaning

최근 5거래일 수익률 표준편차

---

# volatility_20d

## Formula

```python
volatility_20d

=

ret.shift(1).rolling(20).std()
```

---

## Meaning

최근 1개월 변동성

---

# intraday_range

## Formula

```python
intraday_range

=

high

/

low

-

1
```

---

# intraday_range_5d

## Formula

```python
intraday_range_5d

=

intraday_range.shift(1).rolling(5).mean()
```

---

## Meaning

최근 평균 장중 변동폭

---

# atr14

## Formula

```python
tr1 = high - low

tr2 = abs(high - close.shift(1))

tr3 = abs(low - close.shift(1))

tr = max(tr1, tr2, tr3)

atr14 = tr.shift(1).rolling(14).mean()
```

---

# atr_percent

## Definition

ATR를 가격으로 정규화

## Formula

```python
atr_percent

=

atr14

/

close
```

---

# volatility_rank_pct

## Formula

```python
volatility_rank_pct

=

volatility_20d.rank(pct=True)
```

---

# Volatility Feature Summary

| Feature             | Recommended |
| ------------------- | ----------- |
| volatility_5d       | ★★★★☆       |
| volatility_20d      | ★★★★★       |
| intraday_range_5d   | ★★★★★       |
| atr_percent         | ★★★★★       |
| volatility_rank_pct | ★★★★☆       |

---

# 5.7 Candlestick Feature

## 목적

Candlestick Feature는 하루 동안의 가격 움직임과 수급 구조를 표현한다.

절대 가격이 아니라 비율 형태를 사용한다.

---

# body

## Formula

```python
body

=

(close - open)

/

open
```

---

## Meaning

양수

양봉

음수

음봉

---

# upper_shadow

## Formula

```python
upper_shadow

=

(high - maximum(open, close))

/

close
```

---

## Meaning

윗꼬리 비율

---

# lower_shadow

## Formula

```python
lower_shadow

=

(minimum(open, close) - low)

/

close
```

---

## Meaning

아랫꼬리 비율

---

# body_ratio

## Formula

```python
body_ratio

=

abs(close-open)

/

(high-low+1e-8)
```

---

## Meaning

전체 캔들 대비 몸통 비율

---

# close_position

## Formula

```python
close_position

=

(close-low)

/

(high-low+1e-8)
```

---

## Range

```text
0 ~ 1
```

---

## Interpretation

```text
1

고가 근처 마감

-----------------

0

저가 근처 마감
```

---

# Candlestick Feature Summary

| Feature        | Recommended |
| -------------- | ----------- |
| body           | ★★★★★       |
| upper_shadow   | ★★★★☆       |
| lower_shadow   | ★★★★☆       |
| body_ratio     | ★★★★☆       |
| close_position | ★★★★★       |

---

# 5.8 Breakout Feature

## 목적

Breakout Feature는 신고가/신저가 접근 정도와 추세 지속 가능성을 표현한다.

---

# high_20d

## Formula

```python
high_20d

=

high.shift(1).rolling(20).max()
```

---

## Meaning

최근 20거래일 최고가

오늘 데이터는 포함하지 않는다.

---

# low_20d

## Formula

```python
low_20d

=

low.shift(1).rolling(20).min()
```

---

# close_to_20d_high

## Formula

```python
close_to_20d_high

=

close

/

high_20d

-

1
```

---

## Meaning

현재 가격이 신고가에 얼마나 가까운가

```text
0

신고가

----------------

-0.02

신고가 대비 2% 아래
```

---

# close_to_20d_low

## Formula

```python
close_to_20d_low

=

close

/

low_20d

-

1
```

---

## Meaning

현재 가격이 신저가 대비 얼마나 위에 있는가

---

# breakout_strength

## Formula

```python
breakout_strength

=

close_to_20d_high

-

close_to_20d_low
```

---

## Meaning

신고가 접근 강도

---

# breakout_rank_pct

## Formula

```python
breakout_rank_pct

=

close_to_20d_high.rank(pct=True)
```

---

# Breakout Feature Summary

| Feature           | Recommended |
| ----------------- | ----------- |
| high_20d          | Internal    |
| low_20d           | Internal    |
| close_to_20d_high | ★★★★★       |
| close_to_20d_low  | ★★★★★       |
| breakout_strength | ★★★★☆       |
| breakout_rank_pct | ★★★★☆       |

---

# Part 2 Recommended Feature Set

## Volume

```text
volume_change_1d

relative_trading_value

trading_value_rank_pct
```

---

## Volatility

```text
volatility_5d

volatility_20d

intraday_range_5d

atr_percent

volatility_rank_pct
```

---

## Candlestick

```text
body

upper_shadow

lower_shadow

body_ratio

close_position
```

---

## Breakout

```text
close_to_20d_high

close_to_20d_low

breakout_strength

breakout_rank_pct
```

---

# Part 2 Implementation Rules

```text
1. 거래량은 Absolute Volume보다 Trading Value를 우선 사용한다.

2. ATR은 반드시 atr_percent(ATR/Close) 형태로 사용한다.

3. Candlestick Feature는 모두 Ratio 형태를 사용한다.

4. high_20d, low_20d는 반드시 shift(1) 후 rolling()을 적용한다.

5. Breakout Feature 생성 시 오늘(T) 고가/저가를 사용해서는 안 된다.

6. Cross-sectional Rank는 Daily Universe 기준으로 계산한다.

7. 모든 Rolling 계산은 반드시 T-1 데이터까지만 포함한다.

8. Feature 생성 과정에서 미래 가격(Open(T), High(T), Low(T), Close(T))을 참조하는 것은 금지한다.
```
# AI Trading System Design Document v1.0

# 5. Feature Library

# Part 3-1A

* Technical Feature
* RSI
* MACD
* Bollinger Band
* ATR
* Technical Feature Rules

---

# 5.9 Technical Feature

## 목적

Technical Feature는 가격과 거래량으로부터 계산되는 기술적 지표를 의미한다.

본 프로젝트에서는 전통적인 매매신호(RSI 30 이하 매수 등)를 사용하지 않고,

**Machine Learning Feature로만 사용한다.**

---

# Design Principles

## Rule 1

Technical Indicator는 절대적인 매매신호가 아니다.

모든 Indicator는 다른 Feature와 함께 모델이 학습하도록 한다.

---

## Rule 2

Absolute Value보다 Ratio 또는 Position 형태를 우선 사용한다.

Good

```text
bb_position

atr_percent

macd_hist_ratio
```

Bad

```text
atr14

macd

signal
```

---

## Rule 3

모든 Rolling 계산은 반드시 shift(1) 이후 수행한다.

Good

```python
close.shift(1).rolling(20).mean()
```

Bad

```python
close.rolling(20).mean()
```

---

## Rule 4

Technical Feature는 Ranking Model과 Intraday Model에서 주로 사용한다.

Gap Model에서는 보조 Feature로 사용한다.

---

# 5.9.1 RSI Feature

---

## rsi14

### Definition

14일 Relative Strength Index

최근 상승폭과 하락폭의 상대적인 강도를 나타낸다.

---

### Formula

```python
delta = close.diff()

gain = delta.clip(lower=0)

loss = (-delta).clip(lower=0)

avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()

avg_loss = loss.ewm(alpha=1/14, adjust=False).mean()

rs = avg_gain / (avg_loss + 1e-8)

rsi14 = 100 - 100 / (1 + rs)
```

---

### Range

```text
0 ~ 100
```

---

### Interpretation

```text
70 이상

강한 Momentum

--------------------

50

중립

--------------------

30 이하

약한 Momentum
```

---

### Used Model

```text
Ranking

Intraday
```

---

## rsi_change_5d

### Formula

```python
rsi_change_5d

=

rsi14

-

rsi14.shift(5)
```

---

### Meaning

최근 5거래일 동안 RSI 변화

---

## rsi_rank_pct

### Formula

```python
rsi_rank_pct

=

rsi14.rank(pct=True)
```

---

### Range

```text
0 ~ 1
```

---

### Recommended

```text
★★★★★
```

---

# RSI Feature Summary

| Feature       | Recommended |
| ------------- | ----------- |
| rsi14         | ★★★★★       |
| rsi_change_5d | ★★★★☆       |
| rsi_rank_pct  | ★★★★☆       |

---

# 5.9.2 MACD Feature

---

## ema12

```python
ema12

=

close.shift(1).ewm(span=12,adjust=False).mean()
```

---

## ema26

```python
ema26

=

close.shift(1).ewm(span=26,adjust=False).mean()
```

---

## macd

```python
macd

=

ema12

-

ema26
```

---

## signal

```python
signal

=

macd.ewm(span=9,adjust=False).mean()
```

---

## macd_histogram

```python
macd_histogram

=

macd

-

signal
```

---

## macd_hist_ratio

### Definition

MACD Histogram의 가격 정규화 값

---

### Formula

```python
macd_hist_ratio

=

macd_histogram

/

close
```

---

### Interpretation

```text
양수

단기 Momentum 강화

------------------

음수

Momentum 약화
```

---

## macd_rank_pct

```python
macd_rank_pct

=

macd_hist_ratio.rank(pct=True)
```

---

# MACD Feature Summary

| Feature         | Recommended |
| --------------- | ----------- |
| macd_hist_ratio | ★★★★★       |
| macd_rank_pct   | ★★★★☆       |

---

# 5.9.3 Bollinger Band Feature

---

## ma20

```python
ma20

=

close.shift(1).rolling(20).mean()
```

---

## std20

```python
std20

=

close.shift(1).rolling(20).std()
```

---

## upper_band

```python
upper_band

=

ma20

+

2*std20
```

---

## lower_band

```python
lower_band

=

ma20

-

2*std20
```

---

## bb_position

### Definition

현재 가격의 Bollinger Band 내 위치

---

### Formula

```python
bb_position

=

(close-lower_band)

/

(upper_band-lower_band+1e-8)
```

---

### Range

```text
0 ~ 1
```

---

### Interpretation

```text
1

Upper Band 근처

----------------

0.5

Middle

----------------

0

Lower Band 근처
```

---

## bb_width

### Formula

```python
bb_width

=

(upper_band-lower_band)

/

ma20
```

---

### Meaning

Band Width

=

시장 변동성

---

## bb_position_change_5d

```python
bb_position_change_5d

=

bb_position

-

bb_position.shift(5)
```

---

## bb_position_rank_pct

```python
bb_position_rank_pct

=

bb_position.rank(pct=True)
```

---

# Bollinger Feature Summary

| Feature               | Recommended |
| --------------------- | ----------- |
| bb_position           | ★★★★★       |
| bb_width              | ★★★★★       |
| bb_position_change_5d | ★★★★☆       |
| bb_position_rank_pct  | ★★★★☆       |

---

# 5.9.4 ATR Feature

---

## True Range

```python
tr1

=

high-low

tr2

=

abs(high-close.shift(1))

tr3

=

abs(low-close.shift(1))

tr

=

maximum(tr1,tr2,tr3)
```

---

## atr14

```python
atr14

=

tr.shift(1).rolling(14).mean()
```

---

## atr_percent

### Definition

ATR를 가격으로 정규화

---

### Formula

```python
atr_percent

=

atr14

/

close
```

---

### Interpretation

```text
값이 클수록

변동성이 큰 종목
```

---

## atr_rank_pct

```python
atr_rank_pct

=

atr_percent.rank(pct=True)
```

---

# ATR Feature Summary

| Feature      | Recommended |
| ------------ | ----------- |
| atr_percent  | ★★★★★       |
| atr_rank_pct | ★★★★☆       |

---

# 5.9.5 Technical Composite Feature

---

## momentum_strength

```python
momentum_strength

=

0.4*momentum_rank_pct

+

0.3*macd_rank_pct

+

0.3*rsi_rank_pct
```

---

## trend_strength

```python
trend_strength

=

0.5*bb_position

+

0.5*close_position
```

---

## Note

Composite Feature는 v1.0에서는 생성하지 않는다.

v2.0 Experimental Feature로 관리한다.

---

# Technical Feature Summary

## RSI

```text
rsi14

rsi_change_5d

rsi_rank_pct
```

---

## MACD

```text
macd_hist_ratio

macd_rank_pct
```

---

## Bollinger

```text
bb_position

bb_width

bb_position_change_5d

bb_position_rank_pct
```

---

## ATR

```text
atr_percent

atr_rank_pct
```

---

# Recommended Technical Feature Set (v1.0)

```text
rsi14

rsi_change_5d

macd_hist_ratio

bb_position

bb_width

atr_percent
```

---

# Technical Feature Scaling Rule

```text
RSI

0~100 그대로 사용

--------------------

MACD

가격으로 나누어 Ratio 사용

--------------------

ATR

가격으로 나누어 Ratio 사용

--------------------

BB

Position 또는 Width 사용

--------------------

Absolute Indicator 사용 금지
```

---

# Missing Value Rule

```text
Rolling 기간 부족

↓

NaN

↓

Train Dataset에서 제거

또는

Feature Generation Minimum Window 적용
```

---

# Leakage Rule

모든 Technical Feature는 반드시

```python
shift(1)
```

이후 Rolling 계산을 수행한다.

허용

```python
close.shift(1).rolling(20).mean()
```

금지

```python
close.rolling(20).mean()
```

---

# Part 3-1A Final Feature List

```text
RSI

rsi14
rsi_change_5d
rsi_rank_pct

-------------------------

MACD

macd_hist_ratio
macd_rank_pct

-------------------------

Bollinger

bb_position
bb_width
bb_position_change_5d
bb_position_rank_pct

-------------------------

ATR

atr_percent
atr_rank_pct
```

---

# Implementation Rules

```text
1. Technical Feature는 Feature Engineering 용도로만 사용한다.

2. RSI 30/70 같은 Rule-based Signal은 사용하지 않는다.

3. MACD는 Histogram Ratio만 사용한다.

4. ATR은 반드시 ATR/Close 형태를 사용한다.

5. Bollinger는 Position과 Width를 우선 사용한다.

6. 모든 Rolling 계산은 shift(1) 이후 수행한다.

7. 모든 Technical Feature는 T-1일까지 생성한다.

8. Absolute Indicator보다 Ratio와 Rank Feature를 우선 사용한다.
```

# AI Trading System Design Document v1.0

# 5. Feature Library

# Part 3-1B

* Cross-sectional Feature
* Return Rank
* Momentum Rank
* Trading Value Rank
* Volatility Rank
* Breakout Rank
* Relative Feature
* Cross-sectional Rule

---

# 5.10 Cross-sectional Feature

## 목적

Cross-sectional Feature는 같은 날짜에 Universe 내 모든 종목을 서로 비교하여 생성하는 Feature이다.

본 프로젝트의 핵심은 개별 종목의 절대 가격을 예측하는 것이 아니라,

```text
350개 종목 중 T일에 상대적으로 강할 종목을 찾는 것
```

이다.

따라서 Cross-sectional Feature는 Ranking Model에서 가장 중요한 Feature 그룹 중 하나이다.

---

# 5.10.1 Cross-sectional Feature 기본 원칙

## 기준 날짜

모든 Cross-sectional Feature는 동일한 날짜 기준으로 계산한다.

```text
Feature 기준일: T-1
Prediction 대상일: T
```

즉, T일을 예측하기 위해 T-1일 장 종료 후 Universe 내 모든 종목을 비교한다.

---

## 계산 단위

Cross-sectional Feature는 반드시 Daily Universe 단위로 계산한다.

```python
df.groupby("date")[feature].rank(pct=True)
```

---

## Percentile Range

모든 Rank Percentile Feature는 다음 범위를 가진다.

```text
0 ~ 1
```

해석:

```text
1.00 = Universe 내 최상위
0.50 = 중간
0.00 = Universe 내 최하위
```

---

# 5.10.2 Return Rank Feature

## return_1d_rank_pct

### Definition

T-1일 기준 Universe 내 1일 수익률 순위

### Formula

```python
return_1d_rank_pct = (
    df.groupby("date")["return_1d"]
      .rank(pct=True)
)
```

### Meaning

```text
오늘 이 종목이 Universe 내에서 얼마나 강했는가?
```

---

## return_5d_rank_pct

### Formula

```python
return_5d_rank_pct = (
    df.groupby("date")["return_5d"]
      .rank(pct=True)
)
```

### Meaning

최근 5거래일 수익률의 시장 내 상대 순위

---

## return_20d_rank_pct

### Formula

```python
return_20d_rank_pct = (
    df.groupby("date")["return_20d"]
      .rank(pct=True)
)
```

### Meaning

최근 1개월 수익률의 시장 내 상대 순위

---

# Return Rank Summary

| Feature             | Recommended |
| ------------------- | ----------- |
| return_1d_rank_pct  | ★★★★☆       |
| return_5d_rank_pct  | ★★★★★       |
| return_20d_rank_pct | ★★★★★       |

---

# 5.10.3 Momentum Rank Feature

## momentum_rank_pct

### Definition

Momentum Feature의 Universe 내 백분위 순위

### Formula

```python
momentum_rank_pct = (
    df.groupby("date")["momentum_5d"]
      .rank(pct=True)
)
```

---

## momentum_20d_rank_pct

```python
momentum_20d_rank_pct = (
    df.groupby("date")["momentum_20d"]
      .rank(pct=True)
)
```

---

## momentum_diff_rank_pct

```python
momentum_diff_rank_pct = (
    df.groupby("date")["momentum_diff"]
      .rank(pct=True)
)
```

---

# Momentum Rank Summary

| Feature                | Recommended |
| ---------------------- | ----------- |
| momentum_rank_pct      | ★★★★★       |
| momentum_20d_rank_pct  | ★★★★★       |
| momentum_diff_rank_pct | ★★★★☆       |

---

# 5.10.4 Trading Value Rank Feature

## trading_value_rank_pct

### Definition

거래대금 강도의 Universe 내 백분위 순위

v1.0에서는 Raw Trading Value보다 `relative_trading_value`를 기준으로 Rank를 계산한다.

---

### Formula

```python
trading_value_rank_pct = (
    df.groupby("date")["relative_trading_value"]
      .rank(pct=True)
)
```

---

### Meaning

```text
오늘 이 종목의 거래대금이 평소 대비 얼마나 강하게 증가했는가?
그리고 그 강도가 Universe 내에서 어느 정도인가?
```

---

## volume_change_rank_pct

```python
volume_change_rank_pct = (
    df.groupby("date")["volume_change_1d"]
      .rank(pct=True)
)
```

---

# Trading Value Rank Summary

| Feature                | Recommended |
| ---------------------- | ----------- |
| trading_value_rank_pct | ★★★★★       |
| volume_change_rank_pct | ★★★★☆       |

---

# 5.10.5 Volatility Rank Feature

## volatility_rank_pct

### Formula

```python
volatility_rank_pct = (
    df.groupby("date")["volatility_20d"]
      .rank(pct=True)
)
```

---

### Meaning

Universe 내 변동성 순위

---

## atr_rank_pct

```python
atr_rank_pct = (
    df.groupby("date")["atr_percent"]
      .rank(pct=True)
)
```

---

## bb_width_rank_pct

```python
bb_width_rank_pct = (
    df.groupby("date")["bb_width"]
      .rank(pct=True)
)
```

---

# Volatility Rank Summary

| Feature             | Recommended |
| ------------------- | ----------- |
| volatility_rank_pct | ★★★★☆       |
| atr_rank_pct        | ★★★★★       |
| bb_width_rank_pct   | ★★★★★       |

---

# 5.10.6 Breakout Rank Feature

## breakout_rank_pct

### Definition

20일 고점 근접도의 Universe 내 순위

---

### Formula

```python
breakout_rank_pct = (
    df.groupby("date")["close_to_20d_high"]
      .rank(pct=True)
)
```

---

### Meaning

```text
1.00 = Universe 내 신고가에 가장 가까운 종목
0.00 = Universe 내 신고가와 가장 먼 종목
```

---

## low_rebound_rank_pct

```python
low_rebound_rank_pct = (
    df.groupby("date")["close_to_20d_low"]
      .rank(pct=True)
)
```

---

# Breakout Rank Summary

| Feature              | Recommended |
| -------------------- | ----------- |
| breakout_rank_pct    | ★★★★★       |
| low_rebound_rank_pct | ★★★★☆       |

---

# 5.10.7 Relative Feature

Relative Feature는 특정 종목의 움직임을 시장, 업종, 또는 외부 지수와 비교한 Feature이다.

Ranking Model에서 매우 중요하다.

---

## relative_return_5d_vs_market

### Formula

```python
relative_return_5d_vs_market = (
    return_5d - market_return_5d
)
```

---

### Meaning

최근 5일 동안 시장 대비 얼마나 강했는가

---

## relative_return_20d_vs_market

```python
relative_return_20d_vs_market = (
    return_20d - market_return_20d
)
```

---

## relative_return_20d_vs_sector

```python
relative_return_20d_vs_sector = (
    return_20d - sector_return_20d
)
```

---

## relative_return_5d_rank_pct

```python
relative_return_5d_rank_pct = (
    df.groupby("date")["relative_return_5d_vs_market"]
      .rank(pct=True)
)
```

---

## sector_relative_rank_pct

```python
sector_relative_rank_pct = (
    df.groupby(["date", "sector"])["return_20d"]
      .rank(pct=True)
)
```

---

# Relative Feature Summary

| Feature                       | Recommended |
| ----------------------------- | ----------- |
| relative_return_5d_vs_market  | ★★★★★       |
| relative_return_20d_vs_market | ★★★★★       |
| relative_return_20d_vs_sector | ★★★★★       |
| relative_return_5d_rank_pct   | ★★★★☆       |
| sector_relative_rank_pct      | ★★★★★       |

---

# 5.10.8 Z-score Feature

v1.0에서는 Rank Percentile을 기본으로 사용한다.

Z-score Feature는 v2.0 확장 항목으로 둔다.

---

## Formula

```python
feature_zscore = (
    feature - df.groupby("date")[feature].transform("mean")
) / (
    df.groupby("date")[feature].transform("std") + 1e-8
)
```

---

## Candidate Features

```text
return_5d_zscore

momentum_zscore

relative_trading_value_zscore

atr_zscore
```

---

# 5.10.9 Recommended Cross-sectional Feature Set v1.0

```text
return_5d_rank_pct

return_20d_rank_pct

momentum_rank_pct

momentum_20d_rank_pct

trading_value_rank_pct

atr_rank_pct

bb_width_rank_pct

breakout_rank_pct

relative_return_5d_vs_market

relative_return_20d_vs_market

relative_return_20d_vs_sector

sector_relative_rank_pct
```

---

# 5.10.10 Cross-sectional Rule

## Rule 1. Rank는 반드시 날짜별로 계산한다.

Good

```python
df.groupby("date")["return_5d"].rank(pct=True)
```

Bad

```python
df["return_5d"].rank(pct=True)
```

---

## Rule 2. Rank는 Daily Universe 기준으로 계산한다.

Rank 계산 대상은 그날 실제 투자 가능한 Universe여야 한다.

```text
KOSPI200 + KOSDAQ150
- 거래정지 제외
- 거래대금 부족 제외
- 가격 필터 통과
```

---

## Rule 3. T일 Target Rank를 Feature로 사용하면 안 된다.

금지:

```text
T일 수익률 Rank
T일 거래대금 Rank
T일 변동성 Rank
```

허용:

```text
T-1일 수익률 Rank
T-1일 거래대금 Rank
T-1일 변동성 Rank
```

---

## Rule 4. Sector Rank는 sector 단위로 계산한다.

```python
df.groupby(["date", "sector"])[feature].rank(pct=True)
```

---

## Rule 5. Rank 방향을 명확히 한다.

기본은 값이 클수록 높은 Rank가 되도록 한다.

```python
rank(pct=True, ascending=True)
```

단, 위험도가 높은 Feature의 경우 해석에 주의한다.

예:

```text
atr_rank_pct 높음 = 변동성 높음
```

이는 좋은 신호가 아니라 Risk Feature일 수 있다.

---

# Final Cross-sectional Feature List

```text
Return Rank

return_5d_rank_pct
return_20d_rank_pct

----------------------------

Momentum Rank

momentum_rank_pct
momentum_20d_rank_pct
momentum_diff_rank_pct

----------------------------

Trading Value Rank

trading_value_rank_pct
volume_change_rank_pct

----------------------------

Volatility Rank

atr_rank_pct
bb_width_rank_pct
volatility_rank_pct

----------------------------

Breakout Rank

breakout_rank_pct
low_rebound_rank_pct

----------------------------

Relative Feature

relative_return_5d_vs_market
relative_return_20d_vs_market
relative_return_20d_vs_sector
relative_return_5d_rank_pct
sector_relative_rank_pct
```

---

# Implementation Rules

```text
1. 모든 Cross-sectional Feature는 date 기준 groupby로 계산한다.

2. Rank Feature는 pct=True를 사용한다.

3. Rank Feature는 0~1 범위를 가진다.

4. Cross-sectional Feature는 T-1일 Daily Universe 기준으로만 계산한다.

5. T일 실제 수익률, T일 거래대금, T일 변동성은 Feature로 사용하지 않는다.

6. Sector Rank는 반드시 date + sector 기준으로 계산한다.

7. Rank Feature는 Ranking Model에서 우선 사용한다.

8. Gap Model과 Intraday Model에서도 보조 Feature로 사용할 수 있다.
```

# AI Trading System Design Document v1.0

# 5. Feature Library

# Part 3-2

* Macro Feature
* Identity Feature

---

# 5.11 Macro Feature

## 목적

Macro Feature는 개별 종목이 아닌

```text
미국시장
환율
변동성
원자재
```

등 시장 전체 환경을 표현하는 Feature이다.

Macro Feature는 Daily Universe의 모든 종목에 동일하게 적용된다.

---

# Design Principles

## Rule 1

Macro Feature는 반드시

```text
T-1일까지 공개된 데이터
```

만 사용한다.

---

## Rule 2

Absolute Level보다 Return(Change)을 우선 사용한다.

Good

```text
nasdaq_return_1d

usdkrw_return_1d

vix_change_1d
```

Bad

```text
NASDAQ Index

USDKRW Level

VIX Level
```

---

## Rule 3

Macro Feature는 Ranking Model에서 가장 중요하게 사용한다.

Gap Model에서는 매우 중요하며,

Intraday Model에서는 보조 Feature로 사용한다.

---

# 5.11.1 US Equity Feature

---

## nasdaq_return_1d

### Formula

```python
nasdaq_return_1d

=

nasdaq_close

/

nasdaq_close.shift(1)

-

1
```

---

### Meaning

미국 성장주의 하루 수익률

---

### Used Model

```text
Ranking

Gap

Intraday
```

---

### Priority

★★★★★

---

## sp500_return_1d

### Formula

```python
sp500_return_1d

=

sp500_close

/

sp500_close.shift(1)

-

1
```

---

### Meaning

미국 전체 시장 분위기

---

### Priority

★★★★☆

---

## sox_return_1d

### Formula

```python
sox_return_1d

=

sox_close

/

sox_close.shift(1)

-

1
```

---

### Meaning

미국 반도체 업종 수익률

---

### Priority

★★★★★

---

# 5.11.2 Risk Feature

---

## vix_change_1d

### Formula

```python
vix_change_1d

=

vix_close

/

vix_close.shift(1)

-

1
```

---

### Meaning

미국 공포지수 변화율

---

### Interpretation

```text
양수

Risk Off

-------------------

음수

Risk On
```

---

### Priority

★★★★☆

---

# 5.11.3 FX Feature

---

## usdkrw_return_1d

### Formula

```python
usdkrw_return_1d

=

usdkrw

/

usdkrw.shift(1)

-

1
```

---

### Meaning

원/달러 환율 변화율

양수

↓

원화 약세

↓

외국인 자금 유출 가능성 증가

---

### Priority

★★★★★

---

# 5.11.4 Commodity Feature

---

## wti_return_1d

### Formula

```python
wti_return_1d

=

wti_close

/

wti_close.shift(1)

-

1
```

---

### Meaning

WTI 유가 변화율

---

### 영향 업종

```text
정유

석유화학

항공

조선
```

---

### Priority

★★★★☆

---

# 5.11.5 Bond Feature

---

## us10y_change_1d

### Formula

```python
us10y_change_1d

=

us10y

-

us10y.shift(1)
```

---

### Unit

```text
% Point
```

---

### Meaning

미국 장기금리 변화

---

### Priority

★★★☆☆

---

# Macro Feature Summary

| Feature          | Priority |
| ---------------- | -------- |
| nasdaq_return_1d | ★★★★★    |
| sox_return_1d    | ★★★★★    |
| usdkrw_return_1d | ★★★★★    |
| sp500_return_1d  | ★★★★☆    |
| vix_change_1d    | ★★★★☆    |
| wti_return_1d    | ★★★★☆    |
| us10y_change_1d  | ★★★☆☆    |

---

# Recommended Macro Feature Set (v1.0)

```text
nasdaq_return_1d

sox_return_1d

sp500_return_1d

vix_change_1d

usdkrw_return_1d

wti_return_1d
```

---

# 5.12 Identity Feature

## 목적

Identity Feature는 종목의 구조적 특성을 표현한다.

가격과 무관한 정보를 제공하며,

LightGBM에서는 categorical feature로 처리한다.

---

# Design Principles

## Rule 1

Ticker 자체는 Feature로 사용하지 않는다.

Good

```text
sector

market_type

market_cap_group
```

Bad

```text
005930

000660
```

---

## Rule 2

Identity Feature는 변하지 않는 구조적 정보만 사용한다.

---

# 5.12.1 sector

## Definition

업종 정보

---

### Example

```text
반도체

자동차

인터넷

은행

정유

조선

바이오
```

---

### Type

```text
Categorical
```

---

### Priority

★★★★★

---

# 5.12.2 market_type

## Definition

시장 구분

---

### Values

```text
KOSPI

KOSDAQ
```

---

### Type

Categorical

---

### Priority

★★★★☆

---

# 5.12.3 market_cap_group

## Definition

시가총액 그룹

---

### Values

```text
Top20

Top50

Top100

Top200

Others
```

---

### Example

```python
if market_cap_rank <=20:
    Top20

elif market_cap_rank<=50:
    Top50

...
```

---

### Type

Categorical

---

### Priority

★★★★★

---

# 5.12.4 Optional Feature (v2)

```text
industry

listing_age_group

ownership_group

index_membership
```

---

v1.0에서는 사용하지 않는다.

---

# Identity Feature Summary

| Feature          | Type        | Priority |
| ---------------- | ----------- | -------- |
| sector           | Categorical | ★★★★★    |
| market_type      | Categorical | ★★★★☆    |
| market_cap_group | Categorical | ★★★★★    |

---

# Feature Group Summary

```text
Price

Momentum

Volume

Volatility

Candlestick

Breakout

Technical

Cross-sectional

Macro

Identity
```

---

# Recommended Feature Count

| Group           | Count |
| --------------- | ----- |
| Price           | 9     |
| Momentum        | 8     |
| Volume          | 3     |
| Volatility      | 5     |
| Candlestick     | 5     |
| Breakout        | 4     |
| Technical       | 8     |
| Cross-sectional | 14    |
| Macro           | 6     |
| Identity        | 3     |

```text
Total

약 65 ~ 70 Feature
```

---

# Feature Priority

## Core Feature

```text
return_5d

close_ma20_ratio

momentum_rank_pct

relative_return_5d_vs_market

relative_trading_value

atr_percent

bb_position

breakout_rank_pct

nasdaq_return_1d

sox_return_1d

usdkrw_return_1d

sector

market_cap_group
```

---

# Implementation Rules

```text
1. Macro Feature는 Daily Universe 전체에 동일하게 적용한다.

2. Macro Feature는 반드시 T-1일까지의 데이터를 사용한다.

3. Macro Feature는 Return(Change) 형태를 우선 사용한다.

4. Identity Feature는 Categorical Feature로 처리한다.

5. Ticker 자체는 Feature로 사용하지 않는다.

6. Sector는 반드시 모든 모델에 입력한다.

7. Market Cap은 Absolute Value 대신 Group으로 사용한다.

8. Optional Feature는 v1.0에서 사용하지 않는다.
```

---

# Feature Library v1.0 Final Summary

```text
Feature Group

01 Price

02 Momentum

03 Volume

04 Volatility

05 Candlestick

06 Breakout

07 Technical

08 Cross-sectional

09 Macro

10 Identity

--------------------------------

Total Feature

약 70개

--------------------------------

Model

Ranking Model

Gap Model

Intraday Model

공통 Feature Library 사용

--------------------------------

Feature Design Principle

Return First

Ratio First

Relative First

Cross-sectional First

T-1 Only

Categorical Identity

No Data Leakage
```
