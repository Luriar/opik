# PROJECT_TASKS.md

# AI Trading System v1.0

## Codex Implementation Roadmap

---

# Overview

This document defines the implementation roadmap for AI Trading System v1.0.

Implementation order is mandatory.

Every phase must satisfy

```text
Specification

↓

Unit Test

↓

Implementation

↓

Integration Test

↓

Review
```

before moving to the next phase.

---

# Progress

```text
Phase 0  Project Setup          □

Phase 1  Data Layer             □

Phase 2  Feature Engine         □

Phase 3  Model Layer            □

Phase 4  Walk-forward           □

Phase 5  Portfolio Engine       □

Phase 6  Backtest Engine        □

Phase 7  Execution Engine       □

Phase 8  Integration            □

Phase 9  Production Ready       □
```

---

# Phase 0

## Project Setup

### Directory Structure

```text
□ Create src/

□ Create configs/

□ Create docs/

□ Create tests/

□ Create outputs/

□ Create notebooks/
```

---

### Configuration

```text
□ Load feature.yaml

□ Load model.yaml

□ Load validation.yaml

□ Load portfolio.yaml

□ Load backtest.yaml

□ Load execution.yaml
```

---

### Infrastructure

```text
□ Logger

□ Config Loader

□ Path Manager

□ Random Seed Manager

□ Version Manager
```

---

# Exit Criteria

```text
✓ Project builds

✓ Config loads

✓ Logger works

✓ pytest passes
```

---

# Phase 1

# Data Layer

---

## Data Loader

```text
□ Korean OHLCV Loader

□ US Index Loader

□ Macro Loader

□ FX Loader
```

---

## Data Validation

```text
□ Missing Data Check

□ Duplicate Check

□ Date Alignment

□ Trading Calendar Validation
```

---

## Daily Universe

```text
□ KOSPI200

□ KOSDAQ150

□ Universe Generator
```

---

## Tests

```text
□ test_data_loader.py

□ test_universe.py
```

---

# Exit Criteria

```text
✓ All datasets aligned

✓ Universe generated

✓ Tests pass
```

---

# Phase 2

# Feature Engine

---

## Price Features

```text
□ Return

□ MA Ratio

□ Close Position
```

---

## Momentum

```text
□ Momentum

□ Relative Return

□ Momentum Rank
```

---

## Volume

```text
□ Trading Value

□ Relative Trading Value

□ Volume Rank
```

---

## Volatility

```text
□ ATR

□ Volatility

□ Range
```

---

## Technical

```text
□ RSI

□ MACD

□ Bollinger

□ ATR
```

---

## Cross-sectional

```text
□ Return Rank

□ Momentum Rank

□ Breakout Rank

□ Trading Value Rank
```

---

## Macro

```text
□ NASDAQ

□ SOX

□ SP500

□ VIX

□ USDKRW

□ WTI
```

---

## Tests

```text
□ test_feature_generation.py

□ test_data_leakage.py
```

---

# Exit Criteria

```text
✓ Leakage Free

✓ Feature Metadata Generated

✓ Tests Pass
```

---

# Phase 3

# Model Layer

---

## Ranking Model

```text
□ Dataset

□ Train

□ Predict

□ Save Model
```

---

## Gap Model

```text
□ Dataset

□ Train

□ Predict
```

---

## Intraday Model

```text
□ Dataset

□ Train

□ Predict
```

---

## Prediction Merge

```text
□ ranking_score

□ pred_gap

□ pred_intraday

□ expected_return
```

---

## Tests

```text
□ test_model_training.py

□ test_prediction.py
```

---

# Exit Criteria

```text
✓ Three Models Train

✓ Prediction Dataset Created

✓ Metrics Generated
```

---

# Phase 4

# Walk-forward Validation

---

```text
□ Fold Generator

□ Expanding Window

□ Validation

□ Test Prediction

□ Fold Aggregation
```

---

## Tests

```text
□ test_walk_forward.py
```

---

# Exit Criteria

```text
✓ Walk-forward Complete

✓ No Leakage

✓ Metrics Saved
```

---

# Phase 5

# Portfolio Engine

---

```text
□ Candidate Selection

□ Liquidity Filter

□ Risk Filter

□ Diversification

□ Portfolio Score

□ Equal Weight

□ Final Top10
```

---

## Tests

```text
□ test_portfolio.py
```

---

# Exit Criteria

```text
✓ Daily Portfolio Generated

✓ Risk Constraints Satisfied
```

---

# Phase 6

# Backtest Engine

---

```text
□ Buy Simulation

□ Sell Simulation

□ Cost

□ Slippage

□ Metrics

□ Benchmark Comparison
```

---

## Tests

```text
□ test_backtest.py
```

---

# Exit Criteria

```text
✓ Backtest Complete

✓ Performance Report Generated
```

---

# Phase 7

# Execution Engine

---

```text
□ Order Plan

□ Risk Check

□ Manual Review CSV

□ Paper Trading

□ Execution Report

□ Execution Log
```

---

## Tests

```text
□ test_execution.py
```

---

# Exit Criteria

```text
✓ Execution Plan Generated

✓ Logs Saved

✓ Reports Saved
```

---

# Phase 8

# System Integration

---

```text
□ End-to-End Pipeline

□ Config Validation

□ Output Validation

□ Runtime Validation

□ Error Handling
```

---

## Tests

```text
□ test_pipeline.py

□ test_integration.py
```

---

# Exit Criteria

```text
✓ Full Pipeline Runs

✓ Outputs Verified

✓ Logs Generated
```

---

# Phase 9

# Production Ready

---

## Performance

```text
□ Profile Runtime

□ Optimize Memory

□ Optimize Feature Generation
```

---

## Reliability

```text
□ Retry Logic

□ Failure Recovery

□ Versioning

□ Audit Log
```

---

## Documentation

```text
□ Update README

□ Update AGENTS

□ Update docs/

□ Generate API Docs
```

---

# Release Checklist

```text
□ All Tests Pass

□ No Data Leakage

□ Walk-forward Valid

□ Backtest Complete

□ Portfolio Valid

□ Execution Valid

□ Config Valid

□ Logging Valid

□ Documentation Complete
```

---

# Development Rules

Never skip

```text
Specification

↓

Test

↓

Implementation
```

Never implement

```text
Future Feature

Random Split

Target Leakage

Hard Coding
```

---

# Success Criteria

The project is considered complete only when

```text
✓ All phases completed

✓ All unit tests passed

✓ All integration tests passed

✓ No leakage detected

✓ Walk-forward validated

✓ Backtest reproducible

✓ Portfolio reproducible

✓ Execution reproducible

✓ Configuration driven

✓ Documentation complete
```

---

# Final Mission

This project is NOT intended to maximize historical returns.

The mission is

```text
Reliable

Leakage-Free

Explainable

Config-Driven

Production-Ready

AI Trading System
```

Every implementation decision must support this mission.
