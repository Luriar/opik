# Production Daily Macro Download Architecture

Generated: 2026-06-17

## Old Architecture

The previous daily macro update policy was file-based:

```text
data/processed/macro/macro_clean_20230615_20260614.parquet
  -> use existing update_date row if present
  -> otherwise create update_date row by prior-only forward fill
```

This was leakage-safe, but it could allow stale macro data to be carried forward into a production run.

## New Architecture

The production daily pipeline now downloads required macro sources from yfinance after `target_update_date` is determined.

Required macro sources:

| Feature Source | yfinance Ticker | Stored Close Column |
|---|---|---|
| NASDAQ | `^IXIC` | `nasdaq_close` |
| S&P500 | `^GSPC` | `sp500_close` |
| VIX | `^VIX` | `vix_close` |
| WTI | `CL=F` | `wti_close` |
| USD/KRW | `KRW=X` | `usdkrw` |

The production macro dataset is:

```text
data/processed/macro/macro_clean_latest.parquet
```

The daily pipeline updates downstream macro reads to use this latest production macro dataset after a successful download.

## Download Flow

```text
scripts/run_daily_update_pipeline.py
  -> run_production_macro_download()
     src/pipeline/macro_download.py
       -> yfinance.download(ticker, start=target_update_date, end=target_update_date + 1 day)
       -> validate exactly one target-date row
       -> validate Close is present and positive
       -> append/update macro_clean_latest.parquet
       -> write data/daily/processed/macro_clean_YYYYMMDD.csv
```

## Target Update Date Validation

For each required source:

```text
row count for target date == 1
date == target_update_date
Close is not null
Close > 0
```

If any validation fails, `MacroDataUnavailableError` is raised.

## Strict Production Policy

If any required macro source fails:

```text
Feature Update: NOT EXECUTED
Training Update: NOT EXECUTED
Rolling Train: NOT EXECUTED
Model Retrain: NOT EXECUTED
Prediction: NOT EXECUTED
Top10: NOT GENERATED
Archive: NOT CREATED
Exit code: 1
```

The pipeline writes status JSON and a failure summary markdown, then returns immediately.

## Status Fields

The daily status JSON records:

```text
macro_download_method
macro_download_passed
macro_downloaded_date
macro_download_failed_sources
macro_download_error
macro_rows_downloaded
```

## Notes

- Feature formulas are unchanged.
- Target formulas are unchanged.
- Model logic is unchanged.
- Rolling 250-day policy is unchanged.
- Archive logic is unchanged.
- Portfolio/backtest/execution logic is untouched.
