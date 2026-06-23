# ==========================================================
# Phase 5 : Portfolio Construction
# ==========================================================

Implement Phase 5 ONLY.

Read

README.md
AGENTS.md
IMPLEMENTATION_GUIDE.md

Reference

docs/09_portfolio.md
configs/portfolio.yaml
tests/test_portfolio.py

------------------------------------------------------------

Implement

src/portfolio/

    candidate_selector.py
    expected_return.py
    risk_filter.py
    diversification.py
    position_sizer.py
    portfolio_builder.py
-----------------------------------------------------------

Requirements

Top30 Candidate
↓
Liquidity Filter
↓
Risk Filter
↓
Sector Diversification
↓
Equal Weight
↓
Top10 Portfolio

Never use

target_gap

target_intraday

actual_return

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
tests/test_portfolio.py

Fix every failure.

Update PROJECT_STATUS.md.

Do NOT implement future phases.
