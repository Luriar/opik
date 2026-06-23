# Macro Date Policy Change

## Old Policy

All required macro sources had to contain a row whose date exactly matched
`target_update_date`. This incorrectly stopped a KRX production run when the
US market was closed on a day that KRX was open.

## New Policy

- KRX data must exactly match `target_update_date` (zero-day tolerance).
- NASDAQ, S&P500, VIX, WTI, and USD/KRW use the latest available source row on
  or before `target_update_date`.
- US macro rows may be at most five calendar days old. The tolerance is set by
  `us_macro_max_age_calendar_days` in `configs/daily_update.yaml`.
- Each downloaded US source stores `actual_<source>_date` and
  `expected_<source>_date` provenance metadata.
- Missing data, empty downloads, missing Close values, future-only rows, and
  rows older than the tolerance stop the production pipeline.

## Examples

- Normal day: KRX `2026-06-18`, US sources `2026-06-18` - PASS.
- Juneteenth: KRX `2026-06-19`, US sources `2026-06-18` - PASS.
- Stale source: target `2026-06-19`, NASDAQ `2026-06-12` - FAIL.

## Holiday Handling

When one or more required US sources use a valid prior trading day, status JSON
sets `us_market_holiday_detected`, records the reason and affected sources, and
the daily summary includes the holiday date and sources using prior data.

## Files Modified

- `configs/daily_update.yaml`
- `scripts/run_daily_update_pipeline.py`
- `src/pipeline/config.py`
- `src/pipeline/daily_report.py`
- `src/pipeline/feature_source_completeness.py`
- `src/pipeline/feature_update.py`
- `src/pipeline/macro_download.py`
- `src/pipeline/status.py`
- `tests/test_daily_update_pipeline.py`

## Tests Passed

`python -m pytest tests/test_daily_update_pipeline.py tests/test_prediction.py tests/test_model_training.py`

Result: 161 passed.
