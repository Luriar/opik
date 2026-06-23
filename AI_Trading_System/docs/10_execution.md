# 10. Execution

## Purpose

The Execution Engine converts the daily portfolio into an auditable order plan.
For v1.0, execution is limited to manual review and paper trading. Broker API
integration is out of scope.

## Input

Execution input must come from the Portfolio Engine.

```text
date
ticker
weight
ranking_score
pred_gap
pred_intraday
expected_return
portfolio_score
sector
market_type
```

## Output

Execution must generate:

```text
orders
execution_report
logs
```

Recommended output paths:

```text
outputs/execution/orders.parquet
outputs/execution/execution_report.json
outputs/execution/daily_log.txt
```

## Required Rules

```text
1. Use portfolio output only.
2. Do not use target or actual return columns.
3. Run risk checks before creating final orders.
4. Support paper trading mode for v1.0.
5. Log every execution step.
6. Stop on critical execution failures.
```

## Required Components

```text
order_builder
risk_checker
paper_trader
execution_report
```

## Required Tests

```text
tests/test_execution.py
```

## Final Principle

Execution must be conservative, auditable, and reproducible. It must never hide
failed risk checks or silently ignore invalid orders.
