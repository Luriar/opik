# ==========================================================
# Phase 7 : Execution
# ==========================================================

Implement Phase 7 ONLY.

Read

README.md
AGENTS.md
IMPLEMENTATION_GUIDE.md

Reference

docs/10_execution.md
configs/execution.yaml
tests/test_execution.py
------------------------------------------------------------

Implement

src/execution/

    order_builder.py
    risk_checker.py
    paper_trader.py
    execution_report.py
------------------------------------------------------------

Requirements

Prediction
↓
Portfolio
↓
Order Plan
↓
Risk Check
↓
Paper Trading
↓
Execution Report
↓
Logging

------------------------------------------------------------

Output
orders.parquet
execution_report.json
daily_log.txt

------------------------------------------------------------

Run

pytest \
tests/test_config.py \
tests/test_project_structure.py \
tests/test_project_health.py \
tests/test_data_loader.py \
tests/test_universe.py \
tests/test_feature_generation.py \
tests/test_data_leakage.py \
tests/test_model_training.py \
tests/test_prediction.py \
tests/test_walk_forward.py \
tests/test_portfolio.py \
tests/test_backtest.py \
tests/test_execution.py

Fix every failure.

Update PROJECT_STATUS.md.

Do NOT implement future phases.
