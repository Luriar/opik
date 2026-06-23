# SOX Feature Source Check Fix

## Summary

The contradictory production message was caused by stale validation state, not a date comparison defect.

The repaired SOX download for `2026-06-22` produced a valid daily CSV row, but `macro_clean_latest.parquet` already contained that date under the old five-source schema. With `force=False`, the append logic retained the existing row and did not add the newly downloaded SOX fields. The completeness checker therefore saw no SOX in the Parquet file and marked SOX failed. The pipeline subsequently overwrote only the displayed `actual_sox_date` with the successful download result, without recomputing `sox_available`, `all_available`, `failed_sources`, or `failure_reason`. This produced:

```text
SOX FAIL
actual: 2026-06-22
expected: 2026-06-22
```

The fix keeps the source and feature checks fail-closed while reporting the real reason.

## Artifact Verification

### Macro Snapshot

`data/daily/processed/macro_clean_20260622.csv`:

| Check | Result |
|---|---:|
| Rows | 1 |
| `sox_close` exists | Yes |
| `sox_close` non-null | 1 |
| `sox_close` null | 0 |
| `sox_close` value | 14634.72265625 |
| `actual_sox_date` | `2026-06-22` |
| `expected_sox_date` | `2026-06-22` |

The SOX download itself succeeded.

### Daily Feature Snapshot

`data/daily/features/features_20260622.csv`:

| Check | Result |
|---|---:|
| Rows | 348 |
| Columns | 56 including `date` and `ticker` |
| `sox_return_1d` exists | No |
| SOX non-null count | 0 |
| SOX null/unavailable rows | 348 |

The long-lived production feature store retains `sox_return_1d` through schema alignment, but its `2026-06-22` slice has zero non-null values. Thus the effective prediction feature is entirely missing even though the daily CSV omits the column rather than storing 348 explicit null cells.

## Root Cause

### 1. Same-Date Macro Row Was Not Enriched

`append_latest_macro()` treated an existing date with `force=False` as fully verified and kept the old row unchanged. This behavior was correct for preventing accidental replacement of existing values, but it did not account for a newly required column such as `sox_close`.

The snapshot was written from the newly downloaded row, while completeness read the unchanged Parquet dataset. The two artifacts therefore described different schemas for the same date.

### 2. Download Provenance Overwrote Display State Only

After completeness ran, the pipeline copied downloaded actual dates into `source_check`. It did not recompute availability or clear the earlier failed-source state. The displayed date became current while the failure boolean remained stale.

### 3. SOX Return Is Still Not Computable

Even after adding the current `sox_close`, the production macro store lacks sufficient earlier SOX observations for the leakage-safe lagged return formula. The source close can therefore pass while `sox_return_1d` remains unavailable.

This is a real feature-contract failure and must not be silently passed.

The explicit error is now:

```text
SOX close exists but sox_return_1d cannot be computed yet because prior SOX history is missing.
```

## Fixes

### Macro Store Enrichment

For an already-existing target date with `force=False`, the macro append logic now:

- preserves all existing non-null values;
- fills only missing columns or null cells from the verified download;
- records mode `existing_download_enriched` when enrichment occurs;
- continues to use `existing_download_verified` when no enrichment is needed.

This allows the repaired June 22 SOX fields to enter `macro_clean_latest.parquet` without overwriting other macro values.

### Completeness Diagnostics

`FeatureSourceCompletenessChecker` now records:

- `sox_close_present`
- `sox_return_present`
- `sox_return_non_null_count`
- `sox_failure_reason`
- existing `actual_sox_date`
- existing `sox_check_enabled`

When an expected-date feature slice already exists, SOX passes only when:

- the SOX close column exists;
- its selected source date is within tolerance;
- `sox_return_1d` exists;
- `sox_return_1d` has at least one numeric value.

If no expected-date feature slice exists yet, source validation proceeds and the feature-update contract performs the same all-null protection before any write.

### Clear Console Output

The console now reports the feature-specific reason:

```text
SOX......... PASS
actual: 2026-06-22
```

or:

```text
SOX......... FAIL
actual: 2026-06-22
reason: SOX close exists but sox_return_1d cannot be computed yet because prior SOX history is missing.
```

It no longer emits only a contradictory same-date expected/actual message.

### Casing Safety

Macro source and provenance columns are resolved case-insensitively. External forms such as `SOX_CLOSE` and `ACTUAL_SOX_DATE` map to the canonical lowercase SOX contract and do not create false failures.

## Required Data Remediation

Code now diagnoses the condition correctly, but it intentionally does not fabricate SOX returns.

Recommended primary action:

**A. Backfill valid SOX closes for `2026-06-17` through `2026-06-22`.**

Alternative:

**B. Rebuild `macro_clean_latest` from the legacy macro dataset plus validated new SOX rows.**

After either action, rebuild the affected feature dates under controlled validation. Do not bypass the all-null gate. This change does not perform that data repair, retrain models, rerun predictions, or modify archives.

## Tests

Added or extended coverage for:

- matching SOX date plus valid `sox_return_1d` passes;
- matching SOX date plus all-null `sox_return_1d` fails;
- failure includes the explicit lag-history reason rather than only equal expected/actual dates;
- uppercase SOX source/provenance keys do not cause false failure;
- same-date verified macro download enriches missing SOX fields without overwriting existing values;
- console and status expose the SOX feature failure reason;
- status JSON contains all requested SOX diagnostics.

Command:

```text
python -m pytest tests/test_daily_update_pipeline.py tests/test_model_training.py tests/test_prediction.py
```

Result:

```text
177 passed, 2 warnings in 27.83s
```

The warnings are unrelated environment warnings from joblib CPU detection and a CP949 subprocess reader.

## Files Changed

- `src/pipeline/macro_download.py`
- `src/pipeline/feature_source_completeness.py`
- `src/pipeline/feature_update.py`
- `src/pipeline/status.py`
- `scripts/run_daily_update_pipeline.py`
- `tests/test_daily_update_pipeline.py`
- `reports/sox_feature_source_check_fix.md`

## Constraints Preserved

- Model logic unchanged.
- Feature formulas unchanged.
- Target formulas unchanged.
- Top10 logic unchanged.
- Rolling window unchanged.
- Archive logic and archive artifacts unchanged.
- No model retraining or prediction rerun performed.
