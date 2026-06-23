# Delete Candidates

Generated: 2026-06-18

No deletion was performed. This file only lists candidates for review.

## Summary

| Directory | Reason | Last detected usage | Estimated impact |
|---|---|---|---|
| `.cache/` | Local tool/library cache only | Matplotlib/tool cache; no project code reference | Safe to delete; may be regenerated |
| `.pytest_cache/` | Pytest cache only | Pytest internals | Safe to delete; pytest will recreate |
| `src/**/__pycache__/` | Python bytecode cache | Python runtime cache | Safe to delete; Python will recreate |
| `scripts/__pycache__/` | Python bytecode cache | Python runtime cache | Safe to delete; Python will recreate |
| `tests/__pycache__/` | Python bytecode cache | Python runtime cache | Safe to delete; Python will recreate |
| `data/raw/.mplconfig/` | Matplotlib cache/config stored under data root | No project reference found | Safe to delete; cache may be recreated elsewhere |
| `data/features/model_training/` | One-off/sample model training feature export | `scripts/export_model_training_features.py` output path | Safe if audit exports are no longer needed |
| `data/features/model_training/fold_001/` | Sample fold export | No active production reference | Safe if sample fold export is no longer needed |
| `data/features/model_training_real/` | One-off real model training export | `scripts/train_real_lightgbm_models.py` output path | Safe if superseded by daily archive |
| `outputs/models/` | One-off real model files outside daily archive | Historical `train_real_lightgbm_models.py` output | Safe after confirming archive/daily models are canonical |
| `outputs/predictions/` | One-off validation prediction files outside daily archive | Historical model/prediction scripts | Safe after confirming validation evidence is archived |
| `outputs/metrics/` | One-off real model metrics | Historical training script | Safe after archiving or if no longer needed |
| `outputs/archive/latest/` | Rebuildable mirror of latest dated archive | `src/pipeline/archive.py` regenerates it | Safe to delete only if a dated archive exists; next successful run recreates |
| `notebooks/` contents | Empty development workspace | `src/utils/paths.py` references directory, but contents absent | Do not remove directory unless path expectations change; empty contents are safe |

## Conditional Archive Candidates

These are not immediate delete candidates because they contain validation evidence, but they can be moved to external archive storage if the project treats `outputs/archive/` as canonical for production runs.

| Directory | Reason | Last detected usage | Estimated impact |
|---|---|---|---|
| `outputs/walk_forward_full_universe_rolling/` | Generated validation run artifacts | `scripts/run_full_universe_rolling_walk_forward.py` | Removing loses local validation result files |
| `outputs/walk_forward_real_rolling/` | Generated validation run artifacts | `scripts/run_real_rolling_walk_forward.py` | Removing loses local validation result files |
| `reports/validation_predictions_*` | Generated validation readable reports | `scripts/create_validation_predictions_readable_report.py` | Removing loses local inspection reports |
| `reports/full_universe_validation_*` | Generated full-universe validation readable reports | `scripts/create_full_universe_validation_readable_report.py` | Removing loses local inspection reports |
| `reports/feature_*` | Generated feature EDA/optimization reports | EDA/optimization scripts | Removing loses local analysis artifacts |
| `data/daily/*` older snapshots | Daily intermediate snapshots | Daily pipeline writes them | Remove only with retention policy and after archive verification |
| `outputs/daily_models/*` older runs | Pre-archive daily model outputs | Daily model pipeline writes them | Remove only after matching `outputs/archive/YYYYMMDD/models/` exists |
| `outputs/daily_predictions/*` older runs | Pre-archive daily prediction outputs | Daily prediction pipeline writes them | Remove only after matching `outputs/archive/YYYYMMDD/predictions/` exists |

## Not Delete Candidates

| Directory | Reason |
|---|---|
| `src/` | Production source code |
| `configs/` | Configuration source of truth |
| `scripts/` | Production and diagnostic entrypoints |
| `data/metadata/` | Universe and ticker-name source |
| `data/raw/kr_stock/` | Raw OHLCV master data |
| `data/processed/kr_stock/` | Clean OHLCV production source |
| `data/processed/macro/` | Clean/latest macro production source |
| `data/features/full_universe_features_optimized.*` | Production optimized feature store |
| `data/processed/full_universe_training_dataset.*` | Production training dataset |
| `outputs/archive/YYYYMMDD/` | Immutable production audit artifact |
| `outputs/daily_status/` | Operational status history |
| `reports/daily/` | Production daily reports and audits |
| `docs/` | Required documentation |
| `tests/` | Required quality gate |
| `logs/` directory | Required log target; rotate contents instead |

