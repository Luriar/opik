# 7. Walk-forward Validation

## Purpose

본 문서는 AI Trading System v1.0에서 사용하는 Walk-forward Validation 방식을 정의한다.

Walk-forward Validation은 주식 시계열 모델에서 데이터 누수와 과적합을 줄이기 위한 핵심 검증 방식이다.

본 프로젝트에서는 모든 모델을 다음 방식으로 검증한다.

```text
과거 데이터로 학습
↓
이후 기간으로 검증
↓
그 다음 미래 기간으로 테스트
↓
기간을 앞으로 이동하며 반복
```

---

# 7.1 Core Principle

## Rule 1. 시간 순서를 반드시 유지한다.

모든 데이터 분할은 시간 순서를 따라야 한다.

허용:

```text
Train < Validation < Test
```

금지:

```text
Random Shuffle
Random Train/Test Split
미래 데이터로 과거 예측
```

---

## Rule 2. Test 기간은 모델 선택에 사용하지 않는다.

Test 기간은 최종 성능 평가에만 사용한다.

금지:

```text
Test 성과를 보고 Feature 수정
Test 성과를 보고 Hyperparameter 수정
Test 성과를 보고 Portfolio Rule 수정
```

---

# 7.2 Validation 대상 모델

Walk-forward Validation은 다음 3개 모델에 모두 적용한다.

```text
Model 1: Ranking Model
Model 2: Gap / Open Model
Model 3: Intraday Model
```

각 모델은 동일한 Walk-forward split을 사용한다.

---

# 7.3 기본 Split 구조

v1.0에서는 연도 단위 Walk-forward Split을 기본으로 한다.

예:

```text
Fold 1
Train:      2018-01-01 ~ 2021-12-31
Validation: 2022-01-01 ~ 2022-12-31
Test:       2023-01-01 ~ 2023-12-31

Fold 2
Train:      2018-01-01 ~ 2022-12-31
Validation: 2023-01-01 ~ 2023-12-31
Test:       2024-01-01 ~ 2024-12-31

Fold 3
Train:      2018-01-01 ~ 2023-12-31
Validation: 2024-01-01 ~ 2024-12-31
Test:       2025-01-01 ~ 2025-12-31
```

---

# 7.4 Expanding Window

v1.0의 기본 방식은 Expanding Window이다.

```text
Fold 1:
Train = 2018~2021

Fold 2:
Train = 2018~2022

Fold 3:
Train = 2018~2023
```

즉, 시간이 지날수록 학습 데이터가 증가한다.

---

## 장점

```text
학습 데이터가 점점 많아진다.
장기 패턴을 활용할 수 있다.
구현이 단순하다.
```

---

## 단점

```text
오래된 시장 패턴이 계속 남는다.
시장 regime 변화에 둔감할 수 있다.
```

---

# 7.5 Rolling Window

v2.0에서는 Rolling Window도 실험한다.

예:

```text
Fold 1:
Train: 2018~2021
Test: 2022

Fold 2:
Train: 2019~2022
Test: 2023

Fold 3:
Train: 2020~2023
Test: 2024
```

---

## 장점

```text
최근 시장 환경에 더 민감하다.
Regime 변화에 적응하기 쉽다.
```

---

## 단점

```text
학습 데이터가 적어질 수 있다.
장기 패턴을 버릴 수 있다.
```

---

# 7.6 Retraining 주기

v1.0 기본 retraining 주기는 월 1회이다.

```text
매월 마지막 거래일
↓
그 시점까지의 Train 데이터로 모델 재학습
↓
다음 1개월 동안 예측
```

---

## 기본값

```yaml
retraining:
  frequency: monthly
  train_window: expanding
```

---

## 대안

```text
weekly retraining
quarterly retraining
event-driven retraining
```

v1.0에서는 구현하지 않는다.

---

# 7.7 Fold 생성 규칙

Walk-forward fold는 다음 필드를 가져야 한다.

```text
fold_id
train_start_date
train_end_date
valid_start_date
valid_end_date
test_start_date
test_end_date
```

예:

```yaml
fold_id: 1
train_start_date: 2018-01-01
train_end_date: 2021-12-31
valid_start_date: 2022-01-01
valid_end_date: 2022-12-31
test_start_date: 2023-01-01
test_end_date: 2023-12-31
```

---

# 7.8 Model Training per Fold

각 Fold에서 다음 순서로 진행한다.

```text
1. Train 기간 Feature 로드
2. Train 기간 Target 로드
3. Validation 기간 Feature 로드
4. Validation 기간 Target 로드
5. LightGBM 학습
6. Validation 성능 평가
7. Best iteration 저장
8. Train + Validation으로 재학습
9. Test 기간 예측
10. Test 성능 저장
```

---

# 7.9 Hyperparameter Tuning

Hyperparameter tuning은 Validation 기간에서만 수행한다.

Test 기간은 절대 사용하지 않는다.

---

## Allowed

```text
Train으로 학습
Validation으로 hyperparameter 선택
Test로 최종 평가
```

---

## Forbidden

```text
Test 성과가 좋아지는 hyperparameter 선택
Test 성과를 보고 feature 제거/추가
```

---

# 7.10 Feature Selection

Feature Selection은 각 Fold의 Train/Validation 기간에서만 수행한다.

금지:

```text
전체 기간 SHAP importance로 Feature 선택
전체 기간 correlation으로 Feature 제거
Test 기간 feature importance 사용
```

허용:

```text
Fold별 Train 기간에서 Feature Selection
Validation 기간으로 검증
Test 기간은 최종 평가만
```

---

# 7.11 Scaling and Encoding

Scaler 또는 Encoder가 필요한 경우 반드시 Fold별 Train 기간에만 fit한다.

```python
scaler.fit(X_train)

X_train = scaler.transform(X_train)
X_valid = scaler.transform(X_valid)
X_test = scaler.transform(X_test)
```

금지:

```python
scaler.fit(X_all)
```

LightGBM categorical feature를 사용하는 경우에는 별도 scaling/encoding 없이 categorical feature로 전달한다.

---

# 7.12 Prediction Dataset

각 Fold의 Test 예측 결과는 다음 형식으로 저장한다.

```text
date
ticker
fold_id
model_version
ranking_score
pred_gap
pred_intraday
pred_open
pred_close
expected_return
target_rank_return
target_gap
target_intraday
```

---

# 7.13 Evaluation Metrics

## Ranking Model

```text
IC
RankIC
Top10 Return
Top20 Return
Top10 Hit Ratio
NDCG@10
Long-short Spread
```

---

## Gap Model

```text
MAE
RMSE
Directional Accuracy
Correlation
```

---

## Intraday Model

```text
MAE
RMSE
Directional Accuracy
Correlation
```

---

## Portfolio

```text
Cumulative Return
Annual Return
MDD
Sharpe Ratio
Sortino Ratio
Win Rate
Turnover
Average Holding Return
```

---

# 7.14 Aggregating Fold Results

Fold별 결과는 다음 방식으로 합산한다.

```text
1. 각 Fold의 Test prediction을 연결한다.
2. 날짜순으로 정렬한다.
3. 중복 날짜/종목을 제거한다.
4. 전체 Test 기간 기준으로 성능을 계산한다.
```

---

# 7.15 Walk-forward Output Structure

```text
outputs/
  walk_forward/
    folds.csv
    predictions/
      fold_001_predictions.parquet
      fold_002_predictions.parquet
      fold_003_predictions.parquet

    metrics/
      fold_001_metrics.json
      fold_002_metrics.json
      fold_003_metrics.json
      all_folds_metrics.json
```

---

# 7.16 Required Tests

`tests/test_walk_forward.py`는 다음을 검증해야 한다.

```text
1. Train < Validation < Test 순서 유지
2. Fold 간 날짜 중복 없음
3. Test 기간이 Train 기간에 포함되지 않음
4. Validation 기간이 Train 기간에 포함되지 않음
5. Random shuffle split 사용 금지
6. Scaler는 Train에만 fit
7. Feature Selection은 Test 기간을 사용하지 않음
8. 각 Fold prediction에는 fold_id가 존재
```

---

# 7.17 Codex Implementation Requirements

Codex는 다음 파일을 구현해야 한다.

```text
src/validation/walk_forward.py
tests/test_walk_forward.py
configs/validation.yaml
```

---

## src/validation/walk_forward.py

필수 함수:

```python
generate_walk_forward_folds()

validate_fold_order()

run_walk_forward_training()

aggregate_fold_predictions()
```

---

## configs/validation.yaml

예시:

```yaml
walk_forward:
  start_date: "2018-01-01"
  end_date: "2025-12-31"

  train_start_date: "2018-01-01"

  validation_years: 1
  test_years: 1

  train_window_type: "expanding"

  retraining_frequency: "monthly"

  folds:
    - fold_id: 1
      train_start_date: "2018-01-01"
      train_end_date: "2021-12-31"
      valid_start_date: "2022-01-01"
      valid_end_date: "2022-12-31"
      test_start_date: "2023-01-01"
      test_end_date: "2023-12-31"
```

---

# 7.18 Forbidden Practices

다음은 명시적으로 금지한다.

```text
Random train_test_split 사용
shuffle=True 사용
전체 기간으로 scaler fit
전체 기간으로 feature selection
Test 결과를 보고 hyperparameter 수정
Test 결과를 보고 feature 추가/삭제
Test 결과를 보고 portfolio rule 수정
Validation과 Test 기간 중복
Train과 Test 기간 중복
```

---

# 7.19 Final Principle

Walk-forward Validation의 목적은 높은 백테스트 수익률을 만드는 것이 아니다.

목적은 다음이다.

```text
실제 운용 시점에서 가능한 방식으로
모델 성능을 보수적으로 평가하는 것
```

따라서 다음 원칙을 따른다.

```text
낮지만 재현 가능한 성능
>
높지만 데이터 누수가 의심되는 성능
```
