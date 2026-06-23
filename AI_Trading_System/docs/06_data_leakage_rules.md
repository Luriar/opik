# 6. Data Leakage Rules

## Purpose

본 문서는 AI Trading System v1.0에서 데이터 누수를 방지하기 위한 필수 규칙을 정의한다.

데이터 누수는 모델이 실제 예측 시점에는 알 수 없는 정보를 학습 또는 예측에 사용하는 현상이다.

본 프로젝트에서는 다음 원칙을 최우선으로 한다.

```text
Prediction 대상일: T
Feature 기준일: T-1
```

즉, T일을 예측할 때 모든 Feature는 반드시 T-1일까지의 정보만 사용해야 한다.

---

# 6.1 Core Principle

## Rule 1. T-1 Rule

모든 Feature는 반드시 T-1일까지의 데이터만 사용해야 한다.

사용 가능:

```text
Open(T-1)
High(T-1)
Low(T-1)
Close(T-1)
Volume(T-1)
US Market(T-1)
FX(T-1)
Macro(T-1)
```

사용 금지:

```text
Open(T)
High(T)
Low(T)
Close(T)
Volume(T)
Trading Value(T)
Intraday Data(T)
```

---

## Rule 2. Target과 Feature는 분리한다.

Target은 T일 데이터를 사용하여 생성할 수 있다.

하지만 Target 또는 Target 생성에 사용된 T일 데이터는 Feature에 포함되면 안 된다.

Target 예:

```python
target_rank_return = close_T / close_T_minus_1 - 1

target_gap = open_T / close_T_minus_1 - 1

target_intraday = close_T / open_T - 1
```

금지:

```text
target_gap을 Feature로 사용
target_intraday를 Feature로 사용
Open(T)을 Feature로 사용
Close(T)을 Feature로 사용
```

---

# 6.2 Feature Generation Rules

## Rule 3. Raw OHLCV는 Feature 생성 전에 shift한다.

T일을 예측하는 Feature를 만들 때는 원시 OHLCV를 먼저 1일 shift한 후 Feature를 생성하는 것을 기본 원칙으로 한다.

권장 방식:

```python
open_lag1 = open.shift(1)
high_lag1 = high.shift(1)
low_lag1 = low.shift(1)
close_lag1 = close.shift(1)
volume_lag1 = volume.shift(1)
```

이후 모든 rolling, ratio, technical indicator는 lagged series를 기반으로 계산한다.

---

## Rule 4. T일 OHLCV 직접 사용 금지

다음과 같은 코드는 Feature 생성에서 금지한다.

```python
body = close / open - 1
```

T일 row에서 위 코드를 그대로 사용하면 T일 open과 close를 사용하게 된다.

권장:

```python
body_lag1 = close.shift(1) / open.shift(1) - 1
```

---

# 6.3 Rolling Calculation Rules

## Rule 5. Rolling은 반드시 shift 후 계산한다.

잘못된 예:

```python
ma20 = close.rolling(20).mean()
```

올바른 예:

```python
ma20 = close.shift(1).rolling(20).mean()
```

---

## Rule 6. High / Low 기반 rolling도 shift 후 계산한다.

잘못된 예:

```python
high_20d = high.rolling(20).max()
low_20d = low.rolling(20).min()
```

올바른 예:

```python
high_20d = high.shift(1).rolling(20).max()
low_20d = low.shift(1).rolling(20).min()
```

---

## Rule 7. Technical Indicator도 T-1 기준으로 계산한다.

잘못된 예:

```python
rsi14 = compute_rsi(close)
macd = compute_macd(close)
bb_position = compute_bollinger(close)
```

올바른 예:

```python
rsi14 = compute_rsi(close.shift(1))
macd = compute_macd(close.shift(1))
bb_position = compute_bollinger(close.shift(1))
```

---

# 6.4 Target Generation Rules

## Rule 8. Target은 T일 데이터를 사용한다.

Target은 미래 결과이므로 T일 데이터를 사용한다.

```python
target_gap = open.shift(-1) / close - 1

target_intraday = close.shift(-1) / open.shift(-1) - 1

target_rank_return = close.shift(-1) / close - 1
```

이때 현재 row는 T-1을 의미하고, `shift(-1)`은 T일 데이터를 의미한다.

---

## Rule 9. Target column은 Feature list에 포함 금지

다음 컬럼은 모델 input feature에 포함해서는 안 된다.

```text
target_gap
target_intraday
target_rank_return
future_return
future_open
future_close
next_open
next_close
```

---

# 6.5 Cross-sectional Rules

## Rule 10. Cross-sectional rank는 날짜별로 계산한다.

올바른 예:

```python
df["return_5d_rank_pct"] = (
    df.groupby("date")["return_5d"].rank(pct=True)
)
```

잘못된 예:

```python
df["return_5d_rank_pct"] = df["return_5d"].rank(pct=True)
```

---

## Rule 11. Rank 계산은 T-1 Feature에 대해서만 수행한다.

허용:

```text
T-1일 return_5d_rank_pct
T-1일 trading_value_rank_pct
T-1일 volatility_rank_pct
```

금지:

```text
T일 return rank
T일 trading value rank
T일 volatility rank
T일 target rank
```

---

# 6.6 Universe Rules

## Rule 12. Daily Universe는 T-1 기준으로 생성한다.

T일 예측에 사용할 Universe는 T-1일 장 종료 후 알 수 있는 정보로만 구성한다.

허용:

```text
T-1일 기준 KOSPI200/KOSDAQ150 구성
T-1일 거래정지 여부
T-1일까지의 거래대금 필터
T-1일 종가 기준 가격 필터
```

금지:

```text
T일 거래대금 기준 필터
T일 종가 기준 필터
T일 이후 지수 편입 정보 사용
```

---

## Rule 13. 현재 Universe를 과거 전체 기간에 적용하면 생존편향을 명시한다.

v1.0에서는 현재 기준 KOSPI200 + KOSDAQ150 종목을 사용할 수 있다.

단, 백테스트 리포트에는 다음 문구를 반드시 포함한다.

```text
This backtest may contain survivorship bias because the current index constituents are applied to historical periods.
```

---

# 6.7 Scaling and Encoding Rules

## Rule 14. Scaler는 Train 데이터에만 fit한다.

잘못된 예:

```python
scaler.fit(full_dataset)
```

올바른 예:

```python
scaler.fit(train_dataset)

X_train = scaler.transform(train_dataset)
X_valid = scaler.transform(valid_dataset)
X_test = scaler.transform(test_dataset)
```

---

## Rule 15. Encoder도 Train 데이터에만 fit한다.

Categorical Encoding이 필요한 경우, encoder는 반드시 Train 데이터로만 fit한다.

단, LightGBM categorical feature를 직접 사용하는 경우 별도 encoding을 하지 않는다.

---

## Rule 16. Feature Selection도 Train 데이터 기준으로 수행한다.

잘못된 예:

```text
전체 기간 SHAP importance로 Feature 선택 후 과거부터 재학습
```

올바른 예:

```text
각 Walk-forward fold의 Train 기간에서 Feature Selection 수행
```

---

# 6.8 Walk-forward Rules

## Rule 17. Random Split 금지

주식 시계열 데이터에서는 `shuffle=True`를 사용한 random split을 금지한다.

금지:

```python
train_test_split(X, y, shuffle=True)
```

허용:

```text
Train: 과거 기간
Validation: 이후 기간
Test: 그 다음 미래 기간
```

---

## Rule 18. Hyperparameter tuning은 Validation 기간에서만 수행한다.

Test 기간을 사용하여 hyperparameter를 조정해서는 안 된다.

---

# 6.9 Backtest Rules

## Rule 19. Portfolio는 예측값만으로 구성한다.

허용:

```text
ranking_score
pred_gap
pred_intraday
expected_return
risk_score
```

금지:

```text
actual_return
target_gap
target_intraday
T일 종가
T일 거래대금
```

---

## Rule 20. 거래비용과 슬리피지를 반영한다.

Backtest는 반드시 다음을 반영한다.

```text
거래비용
슬리피지
매수/매도 가격 기준
유동성 필터
```

---

# 6.10 Forbidden Feature Examples

다음 Feature는 금지한다.

```python
today_return = close / open - 1
```

T일 row에서 사용하면 T일 종가를 사용하므로 금지.

```python
future_return = close.shift(-1) / close - 1
```

Target으로는 허용되지만 Feature로는 금지.

```python
next_open_gap = open.shift(-1) / close - 1
```

Target으로는 허용되지만 Feature로는 금지.

```python
volume_rank_today = df.groupby("date")["volume"].rank(pct=True)
```

T일 volume으로 계산하면 금지.

---

# 6.11 Allowed Feature Examples

허용 Feature 예:

```python
return_5d = close.shift(1) / close.shift(6) - 1
```

```python
ma20 = close.shift(1).rolling(20).mean()
```

```python
high_20d = high.shift(1).rolling(20).max()
```

```python
gap_mean_5d = gap_1d.shift(1).rolling(5).mean()
```

```python
return_5d_rank_pct = (
    df.groupby("date")["return_5d"].rank(pct=True)
)
```

---

# 6.12 Codex Implementation Requirements

Codex는 다음 원칙을 반드시 구현해야 한다.

```text
1. 모든 Feature 생성 함수는 prediction_date 기준으로 T-1 데이터만 사용해야 한다.

2. Feature 생성 함수와 Target 생성 함수는 분리해야 한다.

3. Target column은 feature_list에 포함되지 않아야 한다.

4. Rolling feature는 shift(1) 이후 계산되어야 한다.

5. Cross-sectional rank는 date 기준 groupby로 계산되어야 한다.

6. Train/Validation/Test split은 시간 순서를 유지해야 한다.

7. Random shuffle split은 금지해야 한다.

8. Scaler, Encoder, Feature Selector는 Train 데이터에만 fit해야 한다.

9. Backtest portfolio selection은 예측값만 사용해야 한다.

10. Data leakage test를 반드시 통과해야 한다.
```

---

# 6.13 Unit Test Specification

`tests/test_data_leakage.py`는 다음 항목을 검증해야 한다.

## Test 1. Target columns are not in feature list

```text
target_gap
target_intraday
target_rank_return
```

이 Feature list에 없어야 한다.

---

## Test 2. No future columns in feature list

금지 패턴:

```text
future
next
target
actual
T_open
T_close
```

---

## Test 3. Rolling features use lagged data

Feature generation code는 rolling 계산 전에 shift를 적용해야 한다.

---

## Test 4. Cross-sectional ranks are grouped by date

Rank feature는 반드시 `groupby("date")`로 계산되어야 한다.

---

## Test 5. Train/Test split is time ordered

Train max date는 Validation min date보다 작아야 한다.

Validation max date는 Test min date보다 작아야 한다.

---

## Test 6. Scaler is fit on train only

Scaler가 full dataset에 fit되지 않았는지 확인한다.

---

## Test 7. Backtest does not use actual returns for selection

Portfolio selection 단계에서 target 또는 actual return을 사용하면 실패해야 한다.

---

# 6.14 Code Review Checklist

Codex 또는 개발자는 PR 제출 전 다음 체크리스트를 확인해야 한다.

```text
[ ] 모든 Feature는 T-1 기준인가?

[ ] Target 생성 함수와 Feature 생성 함수가 분리되어 있는가?

[ ] Target column이 Feature list에 포함되어 있지 않은가?

[ ] Rolling feature는 shift(1) 이후 계산되는가?

[ ] Cross-sectional rank는 date 기준으로 계산되는가?

[ ] Universe filter는 T-1 기준으로 적용되는가?

[ ] Train/Validation/Test는 시간 순서대로 분리되는가?

[ ] Random shuffle split을 사용하지 않았는가?

[ ] Scaler/Encoder는 Train 데이터에만 fit하는가?

[ ] Backtest portfolio selection은 예측값만 사용하는가?

[ ] 거래비용과 슬리피지를 반영했는가?

[ ] `tests/test_data_leakage.py`를 통과했는가?
```

---

# 6.15 Final Rule

본 프로젝트에서 성능보다 중요한 것은 데이터 누수 방지이다.

```text
낮은 성능의 누수 없는 모델
>
높은 성능의 누수 있는 모델
```

백테스트 성과가 비정상적으로 높을 경우, 가장 먼저 데이터 누수를 의심한다.
