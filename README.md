# AI Trading System v1.0

## Production-grade Quant AI Trading Framework

---

# Overview

AI Trading System v1.0은

**한국 주식(KOSPI200 + KOSDAQ150)을 대상으로 하는 Machine Learning 기반 Daily Trading Framework**이다.

시스템은

```text
Feature Engineering

↓

AI Prediction

↓

Portfolio Construction

↓

Execution

↓

Backtest

↓

Evaluation
```

전체 Pipeline을 하나의 Framework로 구현한다.

---

# Current Production Status

현재 v1.0 production pipeline은 다음 범위를 지원한다.

- KOSPI200 + KOSDAQ150 기반 약 350개 종목 universe
- 일별 데이터 수집부터 예측과 보관까지 이어지는 자동 업데이트 pipeline
- 최근 350 trading days를 사용하는 rolling training window
- Ranking / Gap / Intraday LightGBM 모델의 독립 학습과 예측
- `ranking_score`, `pred_gap`, `pred_intraday` 기반 Top10 portfolio 생성
- 모델, 예측, Top10, 학습 데이터, 상태 및 metadata를 보존하는 archive system
- 시간 순서를 유지하고 미래 데이터를 차단하는 walk-forward evaluation framework

Production entry point:

```bat
run_daily_update.bat
```

---

# Project Philosophy

본 프로젝트는

```text
높은 Backtest 수익률
```

보다

```text
Leakage-Free

Reproducible

Production Ready

Maintainable
```

System 구축을 목표로 한다.

---

# Core Principles

모든 구현은 다음 원칙을 따른다.

```
Correctness

>

Reproducibility

>

Risk Control

>

Performance
```

---

# System Architecture

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

Portfolio Construction

↓

Execution Engine

↓

Backtest

↓

Performance Evaluation
```

---

# Daily Production Pipeline

```text
run_daily_update.bat

↓

Config Load

↓

Target Update Date Selection

↓

KRX OHLCV Download

↓

Macro Data Download

↓

Feature Generation

↓

Feature Completeness Check

↓

Training Dataset Update

↓

350-Day Rolling Train

↓

Ranking / Gap / Intraday Training

↓

Prediction

↓

Top10 Selection

↓

Archive
```

각 단계는 config 기반으로 실행되며, 필수 데이터 검증에 실패하면 production pipeline을 중단한다. 성공한 실행은 재현에 필요한 모델, 예측, portfolio, training window, status, hash 및 metadata를 archive에 기록한다.

---

# Models

본 시스템은 서로 다른 target을 담당하는 3개의 독립 LightGBM 모델을 사용한다. 세 모델을 하나로 합치지 않으며, 모든 입력은 prediction date 기준 T-1까지 확정된 feature만 사용한다.

## Model 1: Ranking Model

- Target: `target_ranking`
- Formula: `Close(T) / Close(T-1) - 1`
- Purpose: 약 350개 종목의 next-day close return을 예측하고 상대 순위를 생성
- Output: `ranking_score`
- Usage: Top10 후보 선정의 기본 점수

## Model 2: Gap Model

- Target: `target_gap`
- Formula: `Open(T) / Close(T-1) - 1`
- Purpose: 전일 종가 대비 다음 거래일 시가 gap 예측
- Output: `pred_gap`

## Model 3: Intraday Model

- Target: `target_intraday`
- Formula: `Close(T) / Open(T) - 1`
- Purpose: 다음 거래일 시가부터 종가까지의 intraday return 예측
- Output: `pred_intraday`

Prediction merge 단계는 `pred_gap`과 `pred_intraday`를 결합해 expected return과 예상 가격 수준을 계산하고, Ranking Model의 상대 순위와 함께 portfolio construction에 전달한다.

---

# Universe

```
KOSPI200

+

KOSDAQ150

≈350 Stocks
```

Daily Universe를 사용한다.

---

# Feature Library

Production model은 약 55개의 feature를 사용한다. Feature library의 주요 그룹은 다음과 같다.

- **Momentum**: 1일, 3일, 5일, 20일, 60일 수익률과 momentum acceleration
- **Trend**: 이동평균 대비 가격 비율과 중장기 추세 위치
- **Relative Strength**: 동일 거래일 universe 내 cross-sectional percentile rank
- **Liquidity**: 거래량, 거래대금, 상대 거래대금 및 liquidity rank
- **Volatility**: rolling volatility, ATR, intraday range 및 volatility rank
- **Candlestick**: body, upper/lower shadow와 전일 candle 구조
- **Breakout**: 과거 high/low 범위 대비 위치와 breakout strength
- **Technical Indicators**: RSI, MACD, Bollinger Band 계열 지표
- **Macro Features**: NASDAQ, S&P500, VIX, WTI, USD/KRW 및 SOX 기반 변화율

모든 Feature는

```
T-1
```

기준으로 생성된다.

---

# Data Leakage Policy

가장 중요한 원칙

```
Feature Date

<

Target Date
```

항상

```
T-1 Feature

↓

Predict T
```

를 따른다.

---

# Walk-forward Validation

Random Split은 금지한다.

항상

```
Train

↓

Validation

↓

Test
```

순서를 유지한다.

기본 구조

```
Expanding Window

Monthly Retraining
```

---

# Evaluation Results

최신 production-aligned 350-day window walk-forward replay 결과는 다음과 같다.

| Metric | Result |
|---|---:|
| Evaluation window | 90 trading days |
| Evaluation period | 2026-01-30 to 2026-06-17 |
| Rolling train window | 350 trading days |
| Feature count | 55 |
| Top10 average daily return | 0.98% |
| Sharpe ratio | 3.64 |
| CAGR | 822.84% |
| Maximum drawdown | -23.14% |

평가는 각 fold에서 evaluation date 이전의 데이터만 학습에 사용한 walk-forward replay다. Top10은 `ranking_score` 내림차순, equal weight, 1 trading day holding 조건으로 계산했다.

상세 결과는 [`reports/window_comparison/window_350/evaluation_report.md`](reports/window_comparison/window_350/evaluation_report.md)와 [`metrics_summary.json`](reports/window_comparison/window_350/metrics_summary.json)에서 확인할 수 있다.

> 과거 walk-forward 성과는 미래 수익을 보장하지 않는다. 특히 CAGR은 90 trading day 표본을 연율화한 값이므로 장기 기대수익으로 해석해서는 안 된다.

---

# Portfolio Construction

```
Ranking Score

↓

Top30 Candidate

↓

Expected Return

↓

Risk Filter

↓

Diversification

↓

Equal Weight

↓

Top10 Portfolio
```

---

# Execution

기본 Execution

```
Prediction

↓

Order Plan

↓

Paper Trading

↓

Performance Tracking
```

실거래 API는 v2.0에서 지원한다.

---

# Repository Structure

```text
AI-Trading-System/

├── configs/   # Feature, model, validation, portfolio, execution, daily configs
├── scripts/   # Dataset, training, evaluation, and daily pipeline entry points
├── src/       # Production data, feature, model, pipeline, validation modules
├── tests/     # Leakage, pipeline, model, prediction, and integration tests
├── docs/      # Architecture and system specifications
├── reports/   # Curated audits and walk-forward evaluation reports
├── data/      # Local runtime market/feature/training data (Git ignored)
└── outputs/   # Local models, predictions, status, and archives (Git ignored)
```

대용량 market data와 generated runtime artifacts는 repository history에 포함하지 않는다. 필요한 directory는 pipeline 실행 시 생성된다.

---

# Documentation

```
docs/

01_system_architecture.md

02_universe.md

03_targets.md

04_models.md

05_feature_library.md

06_data_leakage_rules.md

07_walk_forward_validation.md

08_backtest.md

09_portfolio.md

10_execution.md
```

---

# Configuration

```
configs/

feature.yaml

model.yaml

validation.yaml

backtest.yaml

portfolio.yaml

execution.yaml
```

모든 Parameter는 Config 기반으로 관리한다.

Hard Coding은 금지한다.

---

# Tests

모든 구현은 Test를 포함해야 한다.

```
tests/

test_feature_generation.py

test_data_leakage.py

test_walk_forward.py

test_backtest.py

test_portfolio.py

test_execution.py
```

모든 Test는 통과해야 한다.

```
pytest

↓

100% PASS

↓

Merge
```

---

# Development Workflow

모든 개발은 다음 순서를 따른다.

```
Specification

↓

Test

↓

Implementation

↓

Validation

↓

Backtest

↓

Review
```

Test 없이 구현하지 않는다.

---

# Coding Rules

```
Simple

Readable

Typed

Config Driven

Unit Tested

Leakage Free
```

를 기본 원칙으로 한다.

---

# Forbidden

절대 금지

```
Random Shuffle

Future Data

Target as Feature

Actual Return Selection

Transaction Cost Ignore

Slippage Ignore

Hard Coding

Silent Exception
```

---

# Output

Pipeline은 항상 다음 결과를 생성한다.

```
Prediction

Portfolio

Execution

Metrics

Logs
```

모든 실행은 재현 가능해야 한다.

---

# Technology Stack

```
Python 3.12+

pandas

numpy

lightgbm

pyyaml

pytest

pyarrow
```

---

# Future Roadmap

v1.0

```
Daily Prediction

Walk-forward

Portfolio

Paper Trading
```

v1.1

```
Dashboard

SHAP Analysis

Model Monitoring
```

v2.0

```
Broker API

Automatic Execution

Real-time Prediction

Risk Monitoring
```

---

# Final Philosophy

본 프로젝트는

```
Machine Learning Project
```

가 아니라

```
Production Quant Trading System
```

이다.

최종 목표는

```
Good Model

+

Good Portfolio

+

Good Execution

+

Good Risk Control

=

Good Trading System
```

이다.

---

# One Rule

언제든 판단이 어려울 경우 다음 원칙을 따른다.

```
Leakage-Free

>

Higher Backtest Return

Simple

>

Complex

Reproducible

>

Optimized
```

이 원칙은 모든 구현보다 우선한다.

---

# README Changelog

## 2026-06-22

- Current Production Status와 daily automated pipeline 추가
- Ranking / Gap / Intraday 모델 target 및 formula 명시
- 약 55개 production feature group 정리
- 350-day rolling window 기반 90-day walk-forward 평가 결과 추가
- 현재 GitHub repository structure와 runtime data 정책 반영


## 최근 운영 환경 변경 사항 (2026-06)

### 학습 기간(Training Window) 변경

* 운영 환경의 Rolling Training Window를 **250영업일 → 350영업일**로 변경하였습니다.
* 150D / 200D / 250D / 300D / 350D / 500D 비교 실험 결과, 본 프로젝트에서는 **350영업일 학습 구간이 가장 우수한 포트폴리오 성과(CAGR, Sharpe Ratio)를 기록**하였습니다.
* 이에 따라 운영 기본 설정(Production Default)을 350영업일로 변경하였습니다.

### 시장별 휴일을 고려한 Macro 데이터 검증 정책 도입

* 한국(KRX)과 미국 시장의 휴일 차이로 인해 발생할 수 있는 Macro 데이터 불일치 문제를 해결하였습니다.
* KRX 데이터는 기존과 동일하게 **Target Update Date와 정확히 일치**해야 합니다.
* 미국 Macro 데이터(NASDAQ, SOX, S&P500, VIX, WTI, USD/KRW)는 **최근 유효 거래일 기준 허용 범위 내 데이터 사용**이 가능하도록 정책을 개선하였습니다.
* 이를 통해 미국 휴장일에도 안정적인 운영이 가능하도록 하였습니다.

### SOX Feature 운영 계약(Production Contract) 추가

* 반도체 업종 지표인 **SOX (^SOX)** 를 공식 운영 Feature로 추가하였습니다.
* SOX 데이터 다운로드, 검증, 상태 추적(Status Tracking)을 운영 파이프라인에 포함하였습니다.
* SOX 데이터가 누락되거나 오래된 경우 Prediction 생성이 차단되도록 개선하였습니다.
* Feature Source Completeness Check에 SOX 검증을 추가하여 데이터 무결성을 강화하였습니다.

### SOX 데이터 복구 및 백필(Backfill)

* 운영 과정에서 발견된 SOX 데이터 누락 문제를 분석하고 원인을 확인하였습니다.

* 누락된 SOX 이력을 복구하였으며, 다음 날짜에 대해 Backfill을 수행하였습니다.

  * 2026-06-17
  * 2026-06-18
  * 2026-06-19

* 영향을 받은 Feature Snapshot을 재생성하여 정상화하였습니다.

* 모든 운영 Feature에 대해 SOX 기반 수익률(`sox_return_1d`)이 정상 계산되는 것을 확인하였습니다.

### 현재 운영 환경

* Universe

  * KOSPI200 + KOSDAQ150
  * 약 350개 종목

* Rolling Training Window

  * 350 영업일

* Feature 개수

  * 55개

* 모델 구성

  * Ranking Model
  * Gap Model
  * Intraday Model

### 검증 결과

* Daily Update Pipeline 테스트 통과
* Model Training 테스트 통과
* Prediction 테스트 통과
* 최신 검증 결과: **178개 테스트 통과**

### 운영 원칙

본 시스템은 높은 백테스트 수익률보다 다음 원칙을 우선합니다.

* Data Leakage 방지
* 재현 가능성(Reproducibility)
* 데이터 무결성(Data Integrity)
* 운영 안정성(Production Stability)
* 유지보수 용이성(Maintainability)

모든 Feature는 반드시 Target Date 이전 시점의 정보만 사용하며, 운영 환경에서는 Feature Source Completeness 검증을 통과한 경우에만 Prediction 및 Top10 생성이 허용됩니다.
