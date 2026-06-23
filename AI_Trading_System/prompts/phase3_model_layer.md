# ==========================================================
# AI Trading System v1.0
#
# Phase 3 : Model Layer
# ==========================================================

Implement Phase 3 ONLY.
Before coding, read:

README.md
AGENTS.md
IMPLEMENTATION_GUIDE.md
CODING_STANDARDS.md
PROJECT_TASKS.md
PROJECT_STATUS.md

Reference:

docs/03_targets.md
docs/04_models.md

configs/model.yaml

tests/
    test_model_training.py
    test_prediction.py

------------------------------------------------------------

Implement

src/models/

    ranking_model.py
    gap_model.py
    intraday_model.py
    trainer.py
    predictor.py
    model_factory.py
------------------------------------------------------------

Requirements

Ranking Model

    target_rank_return

Gap Model

    target_gap

Intraday Model

    target_intraday

Use LightGBM only.

Config driven.

Type hints.

Docstrings.

Logging.

Metadata saving.

Prediction output:

date
ticker
ranking_score
pred_gap
pred_intraday
pred_open
pred_close
expected_return
model_version
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
tests/test_prediction.py

Fix every failure.

Update PROJECT_STATUS.md.

Do NOT implement future phases.
