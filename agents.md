# AGENTS.md

# AI Trading System Development Constitution

Version: v1.0

---

# 1. Mission

Your mission is to implement a production-quality AI Trading System.

The primary objective is **NOT** to maximize backtest return.

The primary objective is

```text
Reliable

Reproducible

Leakage-Free

Maintainable

Production-Ready
```

AI Trading System.

---

# 2. Project Philosophy

Always prioritize

```
Correctness

>

Reproducibility

>

Risk Control

>

Performance
```

Never sacrifice correctness for better backtest performance.

---

# 3. Document Priority

Always follow documents in this order.

```
AGENTS.md

↓

docs/01_system_architecture.md

↓

docs/02_universe.md

↓

docs/03_targets.md

↓

docs/04_models.md

↓

docs/05_feature_library.md

↓

docs/06_data_leakage_rules.md

↓

docs/07_walk_forward_validation.md

↓

docs/08_backtest.md

↓

docs/09_portfolio.md

↓

docs/10_execution.md
```

If two documents conflict,

higher priority document wins.

---

# 4. Architecture

Always implement

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

# 5. Model Architecture

Always use

```
Model1

Ranking Model

↓

Model2

Gap Model

↓

Model3

Intraday Model

↓

Prediction Merge

↓

Portfolio Construction
```

Do NOT merge three models into one.

---

# 6. Feature Rules

Always use

```
Return

Ratio

Relative

Cross-sectional Rank
```

Prefer

```
close_ma20_ratio

relative_trading_value

momentum_rank_pct
```

instead of

```
close

volume

absolute price
```

---

# 7. Data Leakage Policy

Data leakage is considered a critical bug.

Every implementation MUST satisfy

```
Feature Date

<

Target Date
```

Always use

```
T-1 Feature

↓

Predict T
```

Never use

```
Open(T)

High(T)

Low(T)

Close(T)

Volume(T)
```

as feature.

---

# 8. Rolling Rule

Always

```
shift(1)

↓

rolling()
```

Good

```python
close.shift(1).rolling(20).mean()
```

Never

```python
close.rolling(20).mean()
```

---

# 9. Cross-sectional Rule

Always compute

```
groupby(date)

↓

rank(pct=True)
```

Never compute rank over entire dataset.

---

# 10. Walk-forward Rule

Never use

```
shuffle=True

random split

random train_test_split()
```

Always

```
Train

↓

Validation

↓

Test
```

in chronological order.

---

# 11. Scaling Rule

Scaler

Encoder

Feature Selection

must be fitted on

```
Train only
```

Never fit on

```
Train + Validation

Train + Test

Full Dataset
```

---

# 12. Backtest Rule

Portfolio must be built using

```
ranking_score

pred_gap

pred_intraday

expected_return
```

Never use

```
actual_return

target_gap

target_intraday
```

for selection.

---

# 13. Coding Style

Prefer

```
Simple

Readable

Deterministic
```

over

```
Complex

Highly Optimized

Magic Logic
```

---

# 14. Function Design

Every function should

```
Single Responsibility

Pure Function if possible

Typed

Documented

Unit Tested
```

---

Example

```python
def generate_features(
    df: pd.DataFrame,
) -> pd.DataFrame:
    ...
```

---

# 15. File Structure

Always follow

```
src/

configs/

docs/

tests/

outputs/
```

Never create random folders.

---

# 16. Configuration

Never hardcode

```
capital

portfolio_size

cost

slippage

window

hyperparameter
```

Always load from

```
configs/*.yaml
```

---

# 17. Testing Policy

Every implementation must satisfy

```
pytest

↓

100% pass

↓

merge
```

No exception.

---

# 18. Required Tests

Every new module should include

```
test_xxx.py
```

Examples

```
feature.py

↓

test_feature.py

--------------------

portfolio.py

↓

test_portfolio.py

--------------------

execution.py

↓

test_execution.py
```

---

# 19. Logging

Every pipeline step must produce logs.

Minimum

```
timestamp

run_id

step

status

message
```

Never silently ignore exceptions.

---

# 20. Error Handling

Critical Error

↓

Stop Pipeline

Examples

```
Macro Data Missing

Prediction Failure

Portfolio Failure
```

Non-critical Error

↓

Skip Stock

Continue

Examples

```
One Stock Feature Missing

One Stock Price Missing
```

---

# 21. Reproducibility

Every training must store

```
random_seed

git_commit(optional)

feature_version

model_version

config_version

train_period

validation_period

test_period
```

---

# 22. Output

Every run must generate

```
prediction

portfolio

execution

metrics

logs
```

---

# 23. Forbidden Practices

Never

```
Optimize for backtest only

Use future information

Use target as feature

Use T-day close for prediction

Use random split

Skip transaction cost

Skip slippage

Skip unit tests

Hardcode parameters

Ignore failed tests
```

---

# 24. Code Review Checklist

Before finishing any task verify

```
✓ Data leakage free

✓ Walk-forward compatible

✓ Feature follows specification

✓ Config driven

✓ Unit test exists

✓ Test passes

✓ Logging implemented

✓ No hardcoding

✓ No duplicated logic
```

---

# 25. Development Order

Always implement in this order

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

Never implement first and write tests later.

---

# 26. Default Technology Stack

Python 3.12+

pandas

numpy

lightgbm

pyyaml

pytest

pyarrow

scikit-learn

No unnecessary dependency.

---

# 27. Long-term Objective

The objective is NOT

```
Highest Backtest Return
```

The objective is

```
Stable

Explainable

Leakage-Free

Fully Automated

Production Quality

AI Trading System
```

---

# Final Principle

Whenever there is uncertainty,

choose the safer implementation.

```
Simple

>

Complex

Explicit

>

Implicit

Tested

>

Untested

Leakage-Free

>

Higher Backtest Return

Reproducibility

>

Optimization
```

This rule overrides every implementation decision.
