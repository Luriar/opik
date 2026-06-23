# SOX Backfill Report

## Backfill Scope

- Backfill download window: `2026-06-07` through `2026-06-22` inclusive (`end=2026-06-23` for yfinance).
- Missing SOX close date range before repair: `2026-06-17` to `2026-06-19`.
- Feature dates rebuilt: `2026-06-17, 2026-06-18, 2026-06-19, 2026-06-22`.
- Models were not retrained and predictions were not rerun.
- Archives and Top10 outputs were not modified.

## YFinance SOX Rows Used

| target date | source date | sox_close | note |
|---|---|---:|---|
| `2026-06-08` | `2026-06-08` | 12906.690430 | exact date |
| `2026-06-09` | `2026-06-09` | 12657.809570 | exact date |
| `2026-06-10` | `2026-06-10` | 12206.459961 | exact date |
| `2026-06-11` | `2026-06-11` | 13171.440430 | exact date |
| `2026-06-12` | `2026-06-12` | 13371.469727 | exact date |
| `2026-06-16` | `2026-06-16` | 13294.219727 | exact date |
| `2026-06-17` | `2026-06-17` | 13477.070312 | exact date |
| `2026-06-18` | `2026-06-18` | 14341.780273 | exact date |
| `2026-06-19` | `2026-06-18` | 14341.780273 | prior valid trading day used |
| `2026-06-22` | `2026-06-22` | 14634.722656 | exact date |

## Files Modified

- `data\processed\macro\macro_clean_latest.parquet`
- `data\daily\processed\macro_clean_20260617.csv`
- `data\daily\processed\macro_clean_20260618.csv`
- `data\daily\processed\macro_clean_20260619.csv`
- `data\daily\features\features_20260617.csv`
- `data\daily\features\features_20260618.csv`
- `data\daily\features\features_20260619.csv`
- `data\daily\features\features_20260622.csv`
- `data\features\full_universe_features_optimized.parquet`
- `src\pipeline\macro_download.py`
- `scripts\run_daily_update_pipeline.py`
- `tests\test_daily_update_pipeline.py`

The code change was limited to dry-run validation wiring: dry-run macro download now exposes `macro_clean_latest.parquet`, and the pipeline uses that path for feature-source validation even in dry-run mode. This was required because `run_daily_update_dry_run.bat` uses `--skip-download`; before the fix, the dry-run falsely validated against the legacy macro file ending `2026-06-16`.

## Backup Files

- `data\backups\sox_backfill_20260623_100333\data\processed\macro\macro_clean_latest.parquet`
- `data\backups\sox_backfill_20260623_100333\data\daily\processed\macro_clean_20260617.csv`
- `data\backups\sox_backfill_20260623_100333\data\daily\processed\macro_clean_20260618.csv`
- `data\backups\sox_backfill_20260623_100333\data\daily\processed\macro_clean_20260619.csv`
- `data\backups\sox_backfill_20260623_100333\data\daily\features\features_20260617.csv`
- `data\backups\sox_backfill_20260623_100333\data\daily\features\features_20260618.csv`
- `data\backups\sox_backfill_20260623_100333\data\daily\features\features_20260619.csv`
- `data\backups\sox_backfill_20260623_100333\data\daily\features\features_20260622.csv`
- `data\backups\sox_backfill_20260623_100333\data\features\full_universe_features_optimized.parquet`

## Rebuilt Feature Validation

| feature date | rows | sox_return_1d non-null |
|---|---:|---:|
| `2026-06-17` | 348 | 348 |
| `2026-06-18` | 348 | 348 |
| `2026-06-19` | 348 | 348 |
| `2026-06-22` | 348 | 348 |

## Latest SOX Feature Source Check

- Expected date: `2026-06-22`
- SOX available: `True`
- SOX actual date: `2026-06-22`
- SOX close present: `True`
- SOX return present: `True`
- SOX return non-null count: `348`
- Feature source completeness passed: `True`
- SOX failure reason: `None`

## Dry-Run Validation

- Command: `cmd /c "echo. | run_daily_update_dry_run.bat"`
- Exit code: `0`
- Console result: `SOX......... PASS`
- Status preview `feature_column_count`: `55`
- Status preview `feature_missing_count`: `0`
- Status preview `feature_source_completeness_passed`: `true`

## Tests

- Command: `python -m pytest tests/test_daily_update_pipeline.py tests/test_model_training.py tests/test_prediction.py`
- Result: `178 passed, 2 warnings`
- Warnings: existing joblib CPU detection warning and CP949 subprocess reader warning.

## Remaining SOX Issues

- None found for repaired feature dates and latest target update date.
