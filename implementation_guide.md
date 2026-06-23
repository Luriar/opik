# IMPLEMENTATION_GUIDE.md

# AI Trading System v1.0

## Codex Operating Manual

Version: v1.0

---

# 1. Mission

Your mission is NOT to generate code quickly.

Your mission is

```
Understand

↓

Plan

↓

Test

↓

Implement

↓

Validate

↓

Review
```

and build a

```
Production-grade
Leakage-free
Configuration-driven
Quantitative AI Trading System
```

---

# 2. Read Order

Before writing any code, ALWAYS read documents in this order.

```
README.md

↓

AGENTS.md

↓

CODING_STANDARDS.md

↓

PROJECT_TASKS.md

↓

PROJECT_STATUS.md

↓

docs/

↓

configs/

↓

tests/
```

Never start coding before understanding the specification.

---

# 3. Development Workflow

Every implementation MUST follow

```
Specification

↓

Test

↓

Implementation

↓

Validation

↓

Review

↓

Update PROJECT_STATUS.md
```

Never skip any step.

---

# 4. Golden Rules

Always prioritize

```
Correctness

>

Leakage-Free

>

Reproducibility

>

Performance
```

---

# 5. Forbidden

Never

```
Use future data

Use target as feature

Random split

Hardcode parameters

Skip tests

Ignore failed tests

Optimize only for backtest
```

---

# 6. Project Architecture

Always keep this architecture.

```
Data

↓

Feature

↓

Prediction

↓

Portfolio

↓

Execution

↓

Backtest

↓

Evaluation
```

Never bypass Portfolio or Execution.

---

# 7. Phase Execution Order

## Phase 0

Project Setup

```
Create directory

Load configs

Logger

Utilities
```

Complete

↓

Proceed to Phase 1

---

## Phase 1

Data Layer

Implement

```
data_loader.py

macro_loader.py

validator.py

universe.py
```

Run

```
pytest
```

Update

```
PROJECT_STATUS.md
```

---

## Phase 2

Feature Engine

Implement

```
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

Reference

```
05_feature_library.md

06_data_leakage_rules.md
```

Run tests.

---

## Phase 3

Model Layer

Implement

```
Ranking Model

Gap Model

Intraday Model
```

Reference

```
03_targets.md

04_models.md

model.yaml
```

Generate

```
Prediction Dataset
```

---

## Phase 4

Walk-forward

Implement

```
Expanding Window

Monthly Retraining

Fold Generator

Prediction Aggregation
```

Never use random split.

---

## Phase 5

Portfolio

Implement

```
Candidate Selection

Risk Filter

Diversification

Equal Weight

Top10
```

---

## Phase 6

Backtest

Implement

```
Trade Simulation

Transaction Cost

Slippage

Performance Metrics

Benchmark
```

---

## Phase 7

Execution

Implement

```
Order Plan

Risk Check

Paper Trading

Execution Report

Logging
```

---

## Phase 8

Integration

Verify

```
Entire Pipeline

Config

Outputs

Logging

Tests
```

---

## Phase 9

Production Ready

Verify

```
Performance

Reliability

Reproducibility

Documentation
```

---

# 8. Prompt Templates

## Start of Every Phase

Use internally:

```
Read related documents.

Read related configs.

Read related tests.

Summarize requirements.

Implement only this phase.

Run tests.

Update PROJECT_STATUS.md.
```

---

## End of Every Phase

Verify

```
✓ Tests Pass

✓ Config Loaded

✓ No Leakage

✓ Logging Exists

✓ Outputs Saved

✓ Documentation Matches Code
```

---

# 9. Code Quality Checklist

Before writing code verify

```
Does a specification exist?

Does a config exist?

Does a test exist?

Does this duplicate existing logic?

Can this be simplified?
```

---

# 10. Feature Checklist

Every Feature must satisfy

```
Feature Date

<

Target Date
```

Always

```
shift()

↓

rolling()
```

Never

```
rolling()

↓

shift()
```

---

# 11. Model Checklist

Every Model must

```
Load Config

Load Feature List

Train

Validate

Predict

Save Model

Save Metadata
```

---

# 12. Backtest Checklist

Always verify

```
Portfolio uses prediction

NOT actual return
```

Always include

```
Transaction Cost

Slippage

Benchmark
```

---

# 13. Portfolio Checklist

Always verify

```
Top30 Candidate

↓

Risk Filter

↓

Diversification

↓

Equal Weight

↓

Top10
```

---

# 14. Execution Checklist

Always verify

```
Prediction

↓

Portfolio

↓

Order Plan

↓

Paper Trading

↓

Execution Report
```

---

# 15. Documentation Policy

Whenever implementation changes

Update

```
PROJECT_STATUS.md

README.md (if needed)

configs/

tests/
```

Never leave documentation outdated.

---

# 16. Testing Policy

Every module

↓

must have

↓

unit test

↓

and pass pytest.

No exception.

---

# 17. Commit Policy

One logical change

↓

One implementation

↓

One test

↓

One documentation update

Avoid mixing unrelated changes.

---

# 18. Audit Mode

After every major phase perform:

```
Specification Audit

Config Audit

Test Audit

Leakage Audit

Architecture Audit
```

Generate findings before continuing.

---

# 19. Completion Criteria

A phase is complete ONLY if

```
Specification Exists

+

Implementation Exists

+

Config Exists

+

Tests Exist

+

All Tests Pass

+

PROJECT_STATUS Updated
```

---

# 20. Final Audit Prompt

Before declaring the project complete:

```
Read

README.md

AGENTS.md

CODING_STANDARDS.md

PROJECT_TASKS.md

PROJECT_STATUS.md

Verify

Architecture

Feature Library

Leakage Rules

Walk-forward

Backtest

Portfolio

Execution

Run every pytest.

Generate FINAL_PROJECT_AUDIT.md

Include

Overall Score

Technical Debt

Missing Files

Production Readiness

Recommended Improvements
```

---

# Final Principle

Whenever there is uncertainty,

choose

```
Explicit

>

Implicit

Simple

>

Complex

Leakage-Free

>

Higher Backtest Return

Config Driven

>

Hardcoded

Tested

>

Untested
```

This principle overrides every implementation decision.
