# SOX Missing History Audit

## Summary

- First date with an existing production macro row but missing SOX close: `2026-06-17`
- Last date with an existing production macro row but missing SOX close: `2026-06-19`
- Missing SOX close dates before repair: `2026-06-17, 2026-06-18, 2026-06-19`
- Supplemental lag-history rows absent from `macro_clean_latest` before repair: `2026-06-08, 2026-06-09, 2026-06-10, 2026-06-11, 2026-06-12, 2026-06-16`
- Dates where `sox_return_1d` was absent or all-null before repair: `2026-06-17, 2026-06-18, 2026-06-19, 2026-06-22`
- Missing SOX close pattern: `contiguous production trading-date outage`

The supplemental lag-history rows were not failed daily production rows; they were added to `macro_clean_latest` so the existing `shift(1) / shift(2) - 1` macro formula could compute repaired SOX returns for the first affected feature date.

## Detail

| date | macro_row_exists | sox_close_exists | sox_close_non_null | sox_return_1d_exists | sox_return_1d_non_null_count | affected_prediction_date | status_file | reason |
|---|---:|---:|---:|---:|---:|---|---|---|
| `2026-06-08` | False | False | False | True | 348 | `` | `` | sox_close column absent |
| `2026-06-09` | False | False | False | True | 348 | `` | `` | sox_close column absent |
| `2026-06-10` | False | False | False | True | 348 | `` | `` | sox_close column absent |
| `2026-06-11` | False | False | False | True | 348 | `` | `` | sox_close column absent |
| `2026-06-12` | True | True | True | True | 348 | `` | `outputs\daily_status\daily_update_status_20990203.json` | OK |
| `2026-06-16` | True | True | True | True | 348 | `2026-06-17` | `outputs\daily_status\daily_update_status_20260617.json` | OK |
| `2026-06-17` | True | True | False | True | 0 | `2026-06-18` | `outputs\daily_status\daily_update_status_20260618.json` | sox_close null/missing; sox_return_1d all-null |
| `2026-06-18` | True | True | False | True | 0 | `2026-06-19` | `outputs\daily_status\daily_update_status_20260619.json` | sox_close null/missing; sox_return_1d all-null |
| `2026-06-19` | True | True | False | True | 0 | `2026-06-22` | `outputs\daily_status\daily_update_status_20260622.json` | sox_close null/missing; sox_return_1d all-null |
| `2026-06-22` | True | True | True | True | 0 | `2026-06-23` | `outputs\daily_status\daily_update_status_20260623.json` | sox_return_1d all-null |
