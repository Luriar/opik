# SOX Production Data Contract Fix

## Summary

The production macro and feature contracts now treat SOX as a required source. The daily downloader requests `^SOX`, persists `sox_close`, validates its freshness, records its actual date, reports its console outcome, and stops a non-dry-run feature update before storage when `sox_return_1d` is absent or entirely null.

No model was retrained, no prediction was rerun, and no archive was modified.

## Changes

### Macro Download

- Added `sox` to `MACRO_TICKERS`:
  - ticker: `^SOX`
  - output column: `sox_close`
  - display label: `SOX`
- Added `sox_close` to `MACRO_CLOSE_COLUMNS`.
- SOX uses the existing latest-valid-row policy:
  - source date must be on or before the target date;
  - `Close` must be numeric, non-null, and greater than zero;
  - a prior valid row may be used only within the configured tolerance;
  - no valid row within tolerance raises `MacroDataUnavailableError` and stops production.

### Source Completeness

- Added SOX to `SOURCE_COLUMN_CANDIDATES` using `sox_close` / `sox`.
- Added SOX to `REQUIRED_SOURCES`.
- Added SOX to `US_MACRO_SOURCES`.
- Missing, null, future-dated, or stale SOX source data now makes `all_available = false` and includes `sox` in `failed_sources`.

### Status and Console

- Added status fields:
  - `actual_sox_date`
  - `sox_check_enabled`
- Added SOX to `macro_source_actual_dates` and `macro_source_expected_dates`.
- Added SOX to the ordered macro validation console output.
- Successful macro download prints `SOX ......... PASS`.
- A SOX download failure prints `SOX ......... FAIL` and the existing detailed failure reason.

### Feature Contract

- Added `sox_return_1d` to the required production feature set.
- A non-dry-run feature update raises a clear error when the feature column is absent, non-numeric for all rows, or null for all rows.
- Validation occurs before feature-store append and daily snapshot writes, preventing an invalid feature date from entering production storage.
- Dry-run remains non-blocking and write-free; strict production updates remain fail-closed.

## Production Stop Conditions

Production now stops through two independent controls:

1. **Source gate:** `sox_close` is unavailable or older than the configured US macro tolerance.
2. **Feature gate:** `sox_return_1d` is absent or entirely missing across the daily universe.

The source gate runs before feature generation. The feature gate protects against downstream calculation or schema failures even when a source close passed freshness validation.

## Tests Added or Extended

- Production ticker mapping requests `^SOX` and emits `sox_close` with actual/expected date metadata.
- Missing SOX is a required-source completeness failure.
- Valid SOX exposes `actual_sox_date` and `sox_check_enabled = true`.
- Status JSON includes the new SOX fields.
- Console output contains explicit SOX PASS and FAIL results.
- Entirely missing `sox_return_1d` is rejected.
- Existing market-date, stale-source, macro download, and feature fixtures now implement the six-source macro contract.

## Verification

Command:

```text
python -m pytest tests/test_daily_update_pipeline.py tests/test_model_training.py tests/test_prediction.py -q
```

Result:

```text
172 passed, 2 warnings in 26.77s
```

The warnings are unrelated environment warnings from joblib CPU detection and a CP949 subprocess reader. No test failed.

## Changed Files

- `src/pipeline/macro_download.py`
- `src/pipeline/feature_source_completeness.py`
- `src/pipeline/feature_update.py`
- `src/pipeline/status.py`
- `scripts/run_daily_update_pipeline.py`
- `tests/test_daily_update_pipeline.py`
- `reports/sox_contract_fix.md`

## Operational Note

The existing production macro store created before this fix lacks `sox_close`. One new valid close does not supply the two prior observations required by the leakage-safe lagged return formula. A future real production run will therefore fail closed until sufficient valid SOX history has been restored through a controlled data repair. This task intentionally did not repair historical data, retrain models, rerun predictions, or alter archives.
