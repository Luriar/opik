# 1. 전체 아키텍처

## 프로젝트 목적

KOSPI200 + KOSDAQ150 종목을 대상으로, 
T-1일까지의 데이터만 사용하여 T일에 상대적으로 강할 가능성이 높은 종목을 선별하고, 
해당 종목의 T일 시가 수익률과 장중 수익률을 예측하여 
실제 운용 가능한 AI 기반 퀀트 시스템을 구축한다.

---

## 전체 구조

```text
Raw Data
  |
  |-- 한국 종목 OHLCV
  |-- 한국 지수 데이터
  |-- 미국 시장 데이터
  |-- 환율/유가/변동성 데이터
  |-- 종목 메타데이터
  |
  v
Data Processing
  |
  |-- 날짜 정렬
  |-- 종목 universe 구성
  |-- 결측치 처리
  |-- T-1 기준 데이터 정렬
  |
  v
Feature Engineering
  |
  |-- Price Feature
  |-- Momentum Feature
  |-- Volume / Trading Value Feature
  |-- Volatility Feature
  |-- Gap Feature
  |-- Breakout Feature
  |-- Technical Feature
  |-- Cross-sectional Feature
  |-- Macro Feature
  |-- Identity Feature
  |
  v
Target Generation
  |
  |-- target_rank_return
  |-- target_gap
  |-- target_intraday
  |
  v
Model Training
  |
  |-- Model 1: Ranking Model
  |-- Model 2: Gap / Open Model
  |-- Model 3: Intraday Model
  |
  v
Prediction
  |
  |-- 350개 종목 Ranking Score 생성
  |-- Top 후보군 선정
  |-- Predicted Gap 계산
  |-- Predicted Intraday Return 계산
  |-- Predicted Open / Close 계산
  |
  v
Portfolio Construction
  |
  |-- Top10 종목 선정
  |-- 업종 분산
  |-- 변동성 필터
  |-- 거래대금 필터
  |-- 동일가중 또는 리스크 조정 가중
  |
  v
Backtest
  |
  |-- Walk-forward Validation
  |-- 거래비용 미반영
  |-- 슬리피지 미반영
  |-- 일별 수익률 계산
  |-- 누적수익률, MDD, Sharpe, Hit Ratio 미평가
  |
  v
Report
  |
  |-- 성능 리포트
  |-- 종목별 예측 결과
  |-- Feature Importance / SHAP
  |-- 백테스트 결과 저장
```

---

## 핵심 설계 철학

이 프로젝트는 단순히 개별 종목의 내일 종가를 맞히는 모델이 아니다.

핵심 목적은 다음과 같다.

```text
350개 종목 중
T일에 상대적으로 강할 가능성이 높은 종목을 찾고,
그 후보군에 대해 시가와 장중 움직임을 예측하여
실제 매매 가능한 포트폴리오를 구성한다.
```

따라서 시스템의 중심은 가격 예측 모델이 아니라 **Ranking Model**이다.

---

## 3단계 모델 구조

### Model 1. Ranking Model

목적:

```text
KOSPI200 + KOSDAQ150 약 350개 종목 중
T일에 가장 강할 가능성이 높은 종목을 순위화한다.
```

출력:

```text
ranking_score
```

이 점수를 기준으로 Top20 후보군을 만든다.

---

### Model 2. Gap / Open Model

목적:

```text
T일 시가 수익률을 예측한다.
```

Target:

```text
target_gap = open(T) / close(T-1) - 1
```

예측값:

```text
pred_gap
```

예상 시가:

```text
pred_open = close(T-1) * (1 + pred_gap)
```

---

### Model 3. Intraday Model

목적:

```text
T일 시가에서 종가까지의 장중 수익률을 예측한다.
```

Target:

```text
target_intraday = close(T) / open(T) - 1
```

예측값:

```text
pred_intraday
```

예상 종가:

```text
pred_close = pred_open * (1 + pred_intraday)
```

---

## 운용 흐름

실제 운용 시점은 T-1일 장 종료 후로 가정한다.

```text
T-1일 장 종료
  |
  v
T-1일까지의 모든 Feature 생성
  |
  v
Ranking Model 실행
  |
  v
350개 종목 Ranking Score 산출
  |
  v
Top20 후보군 선정
  |
  v
Gap Model 실행
  |
  v
Intraday Model 실행
  |
  v
예상 시가 / 예상 종가 / 예상 수익률 계산
  |
  v
Portfolio Optimizer 실행
  |
  v
최종 Top10 종목 선정
  |
  v
T일 시가 또는 지정된 매매 규칙에 따라 매수
  |
  v
T일 종가 또는 지정된 매매 규칙에 따라 청산
```

---

## 데이터 기준 시점

모든 Feature는 반드시 **T-1일까지의 데이터만 사용**한다.

```text
Feature 기준일: T-1
Prediction 대상일: T
```

예:

```text
2026-06-11 장 종료 후 Feature 생성
2026-06-12 시가 / 종가 / 장중수익률 예측
```

사용 가능:

```text
2026-06-11까지의 한국 종목 데이터
2026-06-11 미국 장 마감 데이터
2026-06-11 환율, VIX, WTI 등 외부 데이터
```

사용 불가:

```text
2026-06-12 시가
2026-06-12 고가
2026-06-12 저가
2026-06-12 종가
2026-06-12 거래량
2026-06-12 장중 수급
```

---

## 최종 산출물

프로젝트는 다음 산출물을 생성해야 한다.

```text
1. 일별 Feature Dataset
2. 일별 Target Dataset
3. Ranking Model 학습 결과
4. Gap Model 학습 결과
5. Intraday Model 학습 결과
6. 일별 예측 결과
7. Top10 포트폴리오 결과
8. Walk-forward Backtest 결과
9. 성능 리포트
10. Feature Importance / SHAP 리포트
```

---

## 권장 프로젝트 구조

```text
korea-ai-quant/
  README.md
  configs/
    universe.yaml
    features.yaml
    model.yaml
    backtest.yaml

  data/
    raw/
      kr_stock/
      kr_index/
      us_market/
      fx/
      commodity/
      metadata/
    processed/
      features/
      targets/
      predictions/

  src/
    data/
      collect_kr_stock.py
      collect_us_market.py
      collect_macro.py
      build_universe.py

    features/
      price_features.py
      momentum_features.py
      volume_features.py
      volatility_features.py
      gap_features.py
      breakout_features.py
      technical_features.py
      cross_section_features.py
      macro_features.py
      identity_features.py
      build_features.py

    targets/
      build_targets.py

    models/
      train_ranking_model.py
      train_gap_model.py
      train_intraday_model.py
      predict.py

    validation/
      walk_forward.py

    backtest/
      portfolio.py
      simulator.py
      metrics.py

    reports/
      generate_report.py

  notebooks/
    01_data_check.ipynb
    02_feature_check.ipynb
    03_model_experiment.ipynb
    04_backtest_review.ipynb

  outputs/
    models/
    predictions/
    backtests/
    reports/

  tests/
    test_no_data_leakage.py
    test_feature_generation.py
    test_targets.py
```

---

## 핵심 원칙

```text
1. Ranking Model이 시스템의 중심이다.
2. Gap Model과 Intraday Model은 Ranking Model을 보조한다.
3. 모든 Feature는 T-1일까지의 데이터만 사용한다.
4. Target은 T일 데이터를 사용하되 Feature에 절대 포함하지 않는다.
5. 모든 성능 평가는 Walk-forward Validation으로 수행한다.
6. Backtest에는 거래비용과 슬리피지를 반영하지 않는다. 추후 필요하다고 판단되면 반영할 예정이다.
7. 최종 목표는 예측 정확도가 아니라 실제 포트폴리오 수익률과 리스크 개선이다.
```
