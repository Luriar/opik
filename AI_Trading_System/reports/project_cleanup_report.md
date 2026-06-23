# Project Cleanup Report

Generated: 2026-06-18

Scope: safe cleanup based on `reports/project_directory_guide.md` and `reports/delete_candidates.md`.

No production-critical files were deleted. Historical experiment outputs were moved, not permanently deleted.

## Deleted Cache Directories

The cleanup removed clear cache directories only.

| Item | Action | Notes |
|---|---|---|
| `.cache/` | Deleted | Local tool/library cache |
| `.pytest_cache/` | Deleted | Pytest cache; pytest recreated it during the test run, then it was removed again |
| `data/raw/.mplconfig/` | Deleted | Matplotlib cache/config accidentally stored under data root |
| `__pycache__/` directories | Deleted | Python bytecode cache directories across the workspace, including virtualenv package caches |

Cache cleanup counts:

| Pass | Removed directories |
|---|---:|
| Initial cleanup | 285 |
| Final post-test cache sweep | 111 |

The final sweep was needed because running `pytest` recreated bytecode and pytest cache directories.

## Moved Legacy Directories

The following non-canonical generated outputs were moved into `outputs/legacy/`.

| Original directory | New location | Reason |
|---|---|---|
| `outputs/models` | `outputs/legacy/outputs__models` | Historical one-off model outputs, not daily production canonical output |
| `outputs/predictions` | `outputs/legacy/outputs__predictions` | Historical one-off prediction outputs, not daily production canonical output |
| `outputs/metrics` | `outputs/legacy/outputs__metrics` | Historical one-off metrics output |
| `data/features/model_training` | `outputs/legacy/data__features__model_training` | Historical/sample training feature export |
| `data/features/model_training_real` | `outputs/legacy/data__features__model_training_real` | Historical real model training export |

These directories were moved only after confirming they are not configured production daily pipeline paths.

## Skipped Candidates

| Candidate | Action | Reason |
|---|---|---|
| `outputs/archive/latest` | Skipped | Rebuildable, but useful current production shortcut; not requested for movement |
| `outputs/walk_forward_full_universe_rolling` | Skipped | Validation evidence; listed as conditional archive candidate, not an obvious cleanup target |
| `outputs/walk_forward_real_rolling` | Skipped | Validation evidence; listed as conditional archive candidate, not an obvious cleanup target |
| `reports/validation_predictions_*` | Skipped | Human-readable validation reports; not requested for cleanup |
| `reports/full_universe_validation_*` | Skipped | Human-readable validation reports; not requested for cleanup |
| `reports/feature_*` | Skipped | Feature EDA/optimization reports; not requested for cleanup |
| `data/daily/*` | Skipped | Daily pipeline audit snapshots; requires a retention policy before cleanup |
| `outputs/daily_models/*` | Skipped | Current daily pipeline outputs; archive is canonical but retention policy was not requested |
| `outputs/daily_predictions/*` | Skipped | Current daily pipeline outputs; archive is canonical but retention policy was not requested |
| `logs/*.log` | Skipped | Logs were not requested for deletion; directory is production operational output |

## Production Safety Check

Required daily pipeline core paths were verified after cleanup.

| Path | Exists after cleanup |
|---|---|
| `src/` | YES |
| `scripts/` | YES |
| `configs/` | YES |
| `data/metadata/` | YES |
| `data/raw/kr_stock/` | YES |
| `data/processed/` | YES |
| `data/features/` | YES |
| `outputs/archive/` | YES |
| `reports/daily/` | YES |

Production safety result:

```text
PASS
```

## Test Result

Command run:

```bash
python -m pytest tests/test_daily_update_pipeline.py
```

Result:

```text
122 passed, 2 warnings
```

Warnings observed:

1. `joblib` could not detect physical core count and fell back to logical cores.
2. A pytest thread warning from subprocess output decoding occurred during a daily rolling retrain test.

Neither warning caused a test failure.

## Final State

| Check | Result |
|---|---|
| Cache directories removed | YES |
| Historical experiment outputs preserved under `outputs/legacy/` | YES |
| Production-critical paths preserved | YES |
| Daily pipeline tests passed | YES |

