# ==========================================================
# Phase 4 : Walk-forward Validation
# ==========================================================

Implement Phase 4 ONLY.

Read

README.md
AGENTS.md
IMPLEMENTATION_GUIDE.md

Reference

docs/07_walk_forward_validation.md

configs/validation.yaml

tests/test_walk_forward.py

------------------------------------------------------------

Implement

src/validation/

    fold_generator.py

    walk_forward_runner.py

    retrainer.py

    prediction_merger.py

------------------------------------------------------------

Requirements

Expanding Window

Monthly Retraining

No Random Split

No Shuffle

No Data Leakage

Prediction aggregation

Fold metadata saving

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
tests/test_walk_forward.py

Fix every failure.

Update PROJECT_STATUS.md.

Do NOT implement future phases.
