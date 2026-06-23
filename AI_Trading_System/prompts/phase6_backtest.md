# ==========================================================
# Phase 6 : Backtest
# ==========================================================

Implement Phase 6 ONLY.

Read

README.md
AGENTS.md
IMPLEMENTATION_GUIDE.md

Reference

docs/08_backtest.md
configs/backtest.yaml
tests/test_backtest.py
------------------------------------------------------------

Implement

src/backtest/

    trade_simulator.py
    performance.py
    benchmark.py
    report.py
------------------------------------------------------------

Requirements

Trade Simulation
Transaction Cost
Slippage
Benchmark
Daily Return
Cumulative Return
Annual Return
Sharpe
Sortino
Maximum Drawdown
Calmar
Turnover
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
tests/test_backtest.py

Fix every failure.

Update PROJECT_STATUS.md.

Do NOT implement future phases.
