# 4. Models

## 목적

본 문서는 AI Trading System v1.0에서 사용하는 3개 모델의 역할, 입력값, 출력값, 학습 방식, 평가 지표, 저장 형식을 정의한다.

본 시스템은 단일 가격 예측 모델이 아니라 다음 3개 모델을 조합하여 운용한다.

```text
Model 1: Ranking Model
Model 2: Gap / Open Model
Model 3: Intraday Model
```

---

# 4.1 전체 모델 구조

```text
T-1일까지의 Feature Dataset
        |
        |-----------------------------|
        |                             |
        v                             v
 Model 1: Ranking Model        Model 2: Gap Model
        |                             |
        v                             v
 ranking_score                  pred_gap
        |
        v
 Top 후보군 선정
        |
        v
 Model 3: Intraday Model
        |
        v
 pred_intraday
        |
        v
 pred_open / pred_close 계산
        |
        v
 Portfolio Optimizer
        |
        v
 최종 Top10 종목 선정
```

---

# 4.2 공통 모델 원칙

## 기본 알고리즘

v1.0의 기본 알고리즘은 LightGBM이다.

```text
Primary Model: LightGBM
Backup / Comparison Model: XGBoost 또는 CatBoost
```

v1.0에서는 LightGBM만 구현하고, XGBoost/CatBoost는 v2.0에서 비교 실험 대상으로 추가한다.

---

## 입력 데이터 기준

모든 모델은 동일한 Feature Dataset을 기반으로 한다.

```text
Feature 기준일: T-1
Prediction 대상일: T
```

모델 입력에는 T일의 정보가 포함되어서는 안 된다.

사용 가능:

```text
T-1일까지의 한국 종목 데이터
T-1일까지의 한국 시장 데이터
T-1일까지의 미국 시장 데이터
T-1일까지의 환율, VIX, WTI 등 Macro 데이터
```

사용 불가:

```text
Open(T)
High(T)
Low(T)
Close(T)
Volume(T)
T일 장중 수급
T일 거래대금
T일 고가/저가
```

---

## 공통 입력 컬럼

모든 모델은 다음 공통 키를 가진다.

```text
date
ticker
market_type
sector
market_cap_group
```

모델 학습 시 `date`, `ticker`는 식별자로만 사용하고 Feature로 사용하지 않는다.

v1.0에서는 `ticker` 자체를 categorical feature로 사용하지 않는다.

---

## Categorical Feature

v1.0에서 categorical feature로 사용하는 항목은 다음과 같다.

```text
sector
market_type
market_cap_group
```

LightGBM 학습 시 categorical feature로 명시한다.

---

## 공통 출력 저장 형식

모든 예측 결과는 다음 구조로 저장한다.

```text
date
ticker
ranking_score
pred_gap
pred_intraday
pred_open
pred_close
expected_return
selected_flag
model_version
```

---

# 4.3 Model 1: Ranking Model

## 목적

Ranking Model은 KOSPI200 + KOSDAQ150 Universe 내 종목들을 대상으로 T일에 상대적으로 강할 가능성이 높은 종목을 순위화한다.

본 시스템에서 가장 중요한 모델이다.

---

## 입력

```text
T-1일까지 생성된 Feature
```

주요 Feature 그룹:

```text
Price / Return
Relative Return
Momentum
Volume / Trading Value
Volatility
Gap
Breakout
Technical
Cross-sectional Rank
Macro
Identity
```

---

## Target

기본 Target은 다음 중 하나로 정의한다.

### v1.0 기본 Target

```python
target_rank_return = close(T) / close(T-1) - 1
```

의미:

```text
전일 종가 대비 T일 종가 수익률
```

---

### 대안 Target

실제 매매가 T일 시가 매수, T일 종가 매도라면 다음 Target도 검토한다.

```python
target_intraday = close(T) / open(T) - 1
```

v1.0에서는 `target_rank_return`을 기본으로 사용하고, `target_intraday` 기반 Ranking은 실험 옵션으로 둔다.

---

## 출력

```text
ranking_score
```

`ranking_score`는 종목별 상대 강도 점수이다.

점수가 높을수록 T일에 강할 가능성이 높다고 해석한다.

---

## 모델 타입

v1.0에서는 회귀 모델로 구현한다.

```text
LightGBM Regressor
```

입력 Feature로 미래 수익률을 예측하고, 예측값을 기준으로 종목을 Ranking한다.

---

## 후보군 선정

Ranking Model 출력 후 다음 순서로 후보군을 만든다.

```text
1. Daily Universe 전체 종목에 대해 ranking_score 계산
2. ranking_score 기준 내림차순 정렬
3. Top20 또는 Top30 후보군 선정
4. 거래대금, 변동성, 업종 분산 필터 적용
5. 최종 포트폴리오 후보로 전달
```

v1.0 기본값:

```text
Ranking 후보군: Top30
최종 포트폴리오: Top10
```

---

## 평가 지표

Ranking Model은 일반적인 RMSE보다 Ranking 품질을 우선 평가한다.

```text
IC
RankIC
Top10 평균 수익률
Top20 평균 수익률
Top10 Hit Ratio
Long-short Spread
NDCG@10
```

### IC

예측 점수와 실제 수익률의 상관관계.

### RankIC

예측 순위와 실제 수익률 순위의 Spearman 상관관계.

### Top10 평균 수익률

모델이 선택한 상위 10개 종목의 실제 T일 수익률 평균.

### Hit Ratio

Top10 종목 중 실제 수익률이 0보다 큰 종목 비율.

---

# 4.4 Model 2: Gap / Open Model

## 목적

Gap Model은 T일 시가 수익률을 예측한다.

즉, 전일 종가에서 T일 시가까지의 Overnight Return을 예측한다.

---

## Target

```python
target_gap = open(T) / close(T-1) - 1
```

---

## 입력

```text
T-1일까지 생성된 Feature
```

특히 중요한 Feature 그룹:

```text
US Market Feature
Macro Feature
Gap History
Relative Momentum
Trading Value
Volatility
Sector
Market Type
```

예:

```text
nasdaq_return_1d
sox_return_1d
sp500_return_1d
vix_change_1d
usdkrw_return_1d
wti_return_1d
gap_1d
gap_mean_5d
gap_std_20d
relative_return_5d_vs_market
trading_value_ratio_20d
atr_percent
sector
market_type
```

---

## 출력

```text
pred_gap
```

예상 시가 계산:

```python
pred_open = close(T-1) * (1 + pred_gap)
```

---

## 모델 타입

```text
LightGBM Regressor
```

---

## 평가 지표

```text
MAE
RMSE
Directional Accuracy
Gap Sign Accuracy
Correlation
```

### Directional Accuracy

예측 Gap 방향과 실제 Gap 방향이 일치하는 비율.

```text
pred_gap > 0 and target_gap > 0
pred_gap < 0 and target_gap < 0
```

---

# 4.5 Model 3: Intraday Model

## 목적

Intraday Model은 T일 시가에서 종가까지의 장중 수익률을 예측한다.

v1.0에서는 T일 장 시작 전 예측을 기준으로 하므로, T일 시가나 장중 데이터는 사용하지 않는다.

---

## Target

```python
target_intraday = close(T) / open(T) - 1
```

---

## 입력

```text
T-1일까지 생성된 Feature
```

특히 중요한 Feature 그룹:

```text
Momentum
Relative Return
Trading Value
Cross-sectional Rank
Candlestick
Breakout
Technical
Volatility
Gap History
Sector
Market Type
```

예:

```text
return_1d
return_5d
relative_return_5d_vs_market
relative_return_20d_vs_sector
momentum_diff
trading_value_ratio_20d
trading_value_rank_pct
body
upper_shadow
lower_shadow
close_position
close_to_20d_high
close_to_20d_low
macd_hist_ratio
rsi14
bb_position
bb_width
atr_percent
sector
market_type
```

---

## 출력

```text
pred_intraday
```

예상 종가 계산:

```python
pred_close = pred_open * (1 + pred_intraday)
```

여기서 `pred_open`은 Gap Model의 출력으로 계산한 값이다.

---

## 모델 타입

```text
LightGBM Regressor
```

---

## 평가 지표

```text
MAE
RMSE
Directional Accuracy
Correlation
Top10 Intraday Return
```

---

# 4.6 모델 간 관계

## 기본 관계

```text
Ranking Model
  |
  v
Top 후보군 선정

Gap Model
  |
  v
pred_open 계산

Intraday Model
  |
  v
pred_close 계산
```

---

## 최종 기대수익률

최종 기대수익률은 다음과 같이 계산한다.

```python
expected_return = (1 + pred_gap) * (1 + pred_intraday) - 1
```

또는

```python
expected_return = pred_close / close(T-1) - 1
```

---

## Portfolio Optimizer 입력

Portfolio Optimizer는 다음 값을 사용한다.

```text
ranking_score
pred_gap
pred_intraday
expected_return
atr_percent
volatility_20d
trading_value_ratio_20d
sector
market_type
```

---

# 4.7 학습 방식

## Walk-forward Training

모든 모델은 Walk-forward 방식으로 학습한다.

예:

```text
Train: 2018-2021
Validation: 2022
Test: 2023

Train: 2018-2022
Validation: 2023
Test: 2024

Train: 2018-2023
Validation: 2024
Test: 2025
```

---

## Retraining 주기

v1.0 기본값:

```text
월 1회 재학습
```

대안:

```text
분기 1회 재학습
주 1회 재학습
```

---

## 데이터 분할 원칙

```text
과거 데이터로 학습하고 미래 데이터로 평가한다.
Random Shuffle 금지.
Train/Test Split에서 시간 순서 유지.
```

---

# 4.8 모델 저장

각 모델은 다음 형식으로 저장한다.

```text
outputs/models/
  ranking_model/
    ranking_model_YYYYMMDD.txt
    ranking_model_metadata_YYYYMMDD.json

  gap_model/
    gap_model_YYYYMMDD.txt
    gap_model_metadata_YYYYMMDD.json

  intraday_model/
    intraday_model_YYYYMMDD.txt
    intraday_model_metadata_YYYYMMDD.json
```

---

## Metadata 포함 항목

```text
model_name
model_version
train_start_date
train_end_date
validation_start_date
validation_end_date
feature_list
categorical_features
target_name
hyperparameters
metrics
created_at
```

---

# 4.9 Hyperparameter 기본값

v1.0 기본값:

```yaml
lightgbm:
  objective: regression
  metric: rmse
  learning_rate: 0.03
  num_leaves: 31
  max_depth: -1
  min_data_in_leaf: 100
  feature_fraction: 0.8
  bagging_fraction: 0.8
  bagging_freq: 1
  lambda_l1: 0.0
  lambda_l2: 1.0
  n_estimators: 2000
  early_stopping_rounds: 100
```

---

# 4.10 금지 사항

다음은 명시적으로 금지한다.

```text
1. T일 Open/High/Low/Close/Volume을 Feature로 사용
2. Random train_test_split 사용
3. 전체 데이터로 scaler 또는 encoder fit
4. Target 값을 Feature에 포함
5. T일 수익률 Rank를 Feature로 사용
6. Test 기간으로 Hyperparameter 튜닝
7. 현재 Universe 구성 종목을 과거 전체 기간에 무비판적으로 적용
```

---

# 4.11 v1.0 구현 범위

v1.0에서 구현할 모델:

```text
1. LightGBM Ranking Regressor
2. LightGBM Gap Regressor
3. LightGBM Intraday Regressor
```

v1.0에서 구현하지 않는 항목:

```text
1. Transformer
2. LSTM
3. Reinforcement Learning
4. Real-time intraday update model
5. 종목별 개별 모델 350개
6. 자동 주문 실행
```

---

# 4.12 향후 v2.0 개선 방향

```text
1. XGBoost / CatBoost 비교
2. LambdaRank 기반 Ranking Model
3. 실제 T일 시가 확인 후 Intraday Model 재예측
4. Model Ensemble
5. Prediction Confidence 추정
6. SHAP 기반 Feature Selection
7. 업종별 별도 모델 실험
8. Risk Model 분리
```
