# Macro Source Audit

Generated: 2026-06-17

## Scope

This audit inspected the production macro update path starting at:

- `src/pipeline/macro_update.py`

It also traced imported project modules used by that path and searched the repository for hidden macro download paths using:

- `yfinance`
- `pandas_datareader`
- `requests.get`
- `fred` / `FRED`
- `stooq`
- `investing`
- `AlphaVantage`
- `pykrx`
- `csv`
- `read_csv`
- `download`

No code or config was changed.

## Project Macro Architecture

The current production daily macro update does **not** download macro data from the internet.

Production daily flow:

```text
scripts/run_daily_update_pipeline.py
  -> run_macro_update()
     src/pipeline/macro_update.py
       -> read_macro_file(config.macro_file)
       -> build_macro_update_row()
       -> safe_append_macro()
       -> write_macro_file()
       -> write_daily_macro_snapshot()
```

The production macro source is the configured processed macro parquet:

```text
data/processed/macro/macro_clean_20230615_20260614.parquet
```

Daily update policy:

- If `update_date` already exists in `macro_file`, use that row.
- If `update_date` is missing, create one row by forward-filling from the latest prior macro row.
- If no prior macro row exists, raise `ValueError`.
- In dry-run, preview only and do not write.
- In normal mode, append or replace the one macro row and write a daily CSV snapshot.

Historical macro dataset creation is separate from the production daily update. The detected historical download scripts use `yfinance`.

## Current Processed Macro File

Configured file:

```text
data/processed/macro/macro_clean_20230615_20260614.parquet
```

Detected columns:

```text
date
nasdaq_close
sox_close
sp500_close
vix_close
usdkrw
wti_close
```

Detected dtype summary:

```text
date            datetime64[ms]
nasdaq_close           float64
sox_close              float64
sp500_close            float64
vix_close              float64
usdkrw                 float64
wti_close              float64
```

Observed date range in the file:

```text
2023-06-15 to 2026-06-16
```

No `us10y`, `gold`, or `dxy` column is present in the current macro file.

## Source Dependency Diagram

```text
configs/daily_update.yaml
  macro_file: data/processed/macro/macro_clean_20230615_20260614.parquet
        |
        v
src/pipeline/macro_update.py
  read_macro_file()
        |
        v
  build_macro_update_row()
     |-- existing row for update_date
     `-- forward_fill_prior from latest prior row
        |
        v
  safe_append_macro()
        |
        v
  write_macro_file()
  write_daily_macro_snapshot()

src/pipeline/feature_update.py
  read_macro()
        |
        v
  merge clean OHLCV with macro on date
        |
        v
src/features/macro_features.py
  add_macro_features()
```

Historical source creation path:

```text
scripts/download_real_market_data.py
  download_macro_data()
    -> yfinance.download(...)
    -> extract Close
    -> data/raw/macro/macro_20230615_20260614.*

scripts/build_full_universe_real_dataset.py
  ensure_macro()
    -> use existing raw macro parquet if present
    -> otherwise download_macro()
       -> yfinance.download(...)
       -> extract Close
```

## Summary Table

| Feature | Source Library | Actual Source | Ticker/API | Update Mode | Failure Policy |
|---|---|---|---|---|---|
| NASDAQ / `nasdaq_close` / `nasdaq_return_1d` | Production: pandas parquet. Historical build: yfinance | Production reads `macro_file`; historical source is Yahoo Finance via yfinance | `^IXIC` | Production: existing row or prior-only forward fill; historical scripts: download Close and save parquet/csv | Production: missing file/date without prior row raises; otherwise fallback forward-fill. Historical: empty yfinance result raises `RuntimeError` |
| SOX / `sox_close` / `sox_return_1d` | Production: pandas parquet. Historical build: yfinance | Production reads `macro_file`; historical source is Yahoo Finance via yfinance | `^SOX` | Production: existing row or prior-only forward fill; historical scripts: download Close and save parquet/csv | Production: missing file/date without prior row raises; otherwise fallback forward-fill. Historical: empty yfinance result raises `RuntimeError` |
| S&P500 / `sp500_close` / `sp500_return_1d` | Production: pandas parquet. Historical build: yfinance | Production reads `macro_file`; historical source is Yahoo Finance via yfinance | `^GSPC` | Production: existing row or prior-only forward fill; historical scripts: download Close and save parquet/csv | Production: missing file/date without prior row raises; otherwise fallback forward-fill. Historical: empty yfinance result raises `RuntimeError` |
| VIX / `vix_close` / `vix_change_1d` | Production: pandas parquet. Historical build: yfinance | Production reads `macro_file`; historical source is Yahoo Finance via yfinance | `^VIX` | Production: existing row or prior-only forward fill; historical scripts: download Close and save parquet/csv | Production: missing file/date without prior row raises; otherwise fallback forward-fill. Historical: empty yfinance result raises `RuntimeError` |
| USD/KRW / `usdkrw` / `usdkrw_return_1d` | Production: pandas parquet. Historical build: yfinance | Production reads `macro_file`; historical source is Yahoo Finance via yfinance | `KRW=X` | Production: existing row or prior-only forward fill; historical scripts: download Close and save parquet/csv | Production: missing file/date without prior row raises; otherwise fallback forward-fill. Historical: empty yfinance result raises `RuntimeError` |
| WTI / `wti_close` / `wti_return_1d` | Production: pandas parquet. Historical build: yfinance | Production reads `macro_file`; historical source is Yahoo Finance via yfinance | `CL=F` | Production: existing row or prior-only forward fill; historical scripts: download Close and save parquet/csv | Production: missing file/date without prior row raises; otherwise fallback forward-fill. Historical: empty yfinance result raises `RuntimeError` |

## Per-Feature Details

### NASDAQ

- Feature/source column: `nasdaq_close`
- Generated model feature: `nasdaq_return_1d`
- Production function: `run_macro_update()` -> `read_macro_file()` -> `build_macro_update_row()`
- Production source file: `src/pipeline/macro_update.py`
- Production source library: `pandas`
- Production actual source: `data/processed/macro/macro_clean_20230615_20260614.parquet`
- Historical downloader function: `download_macro_data()` / `download_macro()`
- Historical source files:
  - `scripts/download_real_market_data.py`
  - `scripts/build_full_universe_real_dataset.py`
- Historical source library: `yfinance`
- Historical ticker/API: `^IXIC`
- Returned fields from yfinance: Yahoo Finance OHLCV-style frame; project extracts `Close` only.
- Stored fields: `date`, `nasdaq_close`
- Feature formula: `nasdaq_return_1d = nasdaq_close.shift(1) / nasdaq_close.shift(2) - 1` per ticker when merged into stock rows.
- Update policy: production existing row if present; otherwise prior-only forward fill from latest available `nasdaq_close`.
- Failure behavior: missing macro file raises `FileNotFoundError`; missing date with no prior row raises `ValueError`; historical yfinance empty response raises `RuntimeError`.

### SOX

- Feature/source column: `sox_close`
- Generated model feature: `sox_return_1d`
- Production function: `run_macro_update()` -> `read_macro_file()` -> `build_macro_update_row()`
- Production source file: `src/pipeline/macro_update.py`
- Production source library: `pandas`
- Production actual source: `data/processed/macro/macro_clean_20230615_20260614.parquet`
- Historical downloader function: `download_macro_data()` / `download_macro()`
- Historical source files:
  - `scripts/download_real_market_data.py`
  - `scripts/build_full_universe_real_dataset.py`
- Historical source library: `yfinance`
- Historical ticker/API: `^SOX`
- Returned fields from yfinance: Yahoo Finance OHLCV-style frame; project extracts `Close` only.
- Stored fields: `date`, `sox_close`
- Feature formula: `sox_return_1d = sox_close.shift(1) / sox_close.shift(2) - 1` per ticker when merged into stock rows.
- Update policy: production existing row if present; otherwise prior-only forward fill from latest available `sox_close`.
- Failure behavior: missing macro file raises `FileNotFoundError`; missing date with no prior row raises `ValueError`; historical yfinance empty response raises `RuntimeError`.

### S&P500

- Feature/source column: `sp500_close`
- Generated model feature: `sp500_return_1d`
- Production function: `run_macro_update()` -> `read_macro_file()` -> `build_macro_update_row()`
- Production source file: `src/pipeline/macro_update.py`
- Production source library: `pandas`
- Production actual source: `data/processed/macro/macro_clean_20230615_20260614.parquet`
- Historical downloader function: `download_macro_data()` / `download_macro()`
- Historical source files:
  - `scripts/download_real_market_data.py`
  - `scripts/build_full_universe_real_dataset.py`
- Historical source library: `yfinance`
- Historical ticker/API: `^GSPC`
- Returned fields from yfinance: Yahoo Finance OHLCV-style frame; project extracts `Close` only.
- Stored fields: `date`, `sp500_close`
- Feature formula: `sp500_return_1d = sp500_close.shift(1) / sp500_close.shift(2) - 1` per ticker when merged into stock rows.
- Update policy: production existing row if present; otherwise prior-only forward fill from latest available `sp500_close`.
- Failure behavior: missing macro file raises `FileNotFoundError`; missing date with no prior row raises `ValueError`; historical yfinance empty response raises `RuntimeError`.

### VIX

- Feature/source column: `vix_close`
- Generated model feature: `vix_change_1d`
- Production function: `run_macro_update()` -> `read_macro_file()` -> `build_macro_update_row()`
- Production source file: `src/pipeline/macro_update.py`
- Production source library: `pandas`
- Production actual source: `data/processed/macro/macro_clean_20230615_20260614.parquet`
- Historical downloader function: `download_macro_data()` / `download_macro()`
- Historical source files:
  - `scripts/download_real_market_data.py`
  - `scripts/build_full_universe_real_dataset.py`
- Historical source library: `yfinance`
- Historical ticker/API: `^VIX`
- Returned fields from yfinance: Yahoo Finance OHLCV-style frame; project extracts `Close` only.
- Stored fields: `date`, `vix_close`
- Feature formula: `vix_change_1d = vix_close.shift(1) / vix_close.shift(2) - 1` per ticker when merged into stock rows.
- Update policy: production existing row if present; otherwise prior-only forward fill from latest available `vix_close`.
- Failure behavior: missing macro file raises `FileNotFoundError`; missing date with no prior row raises `ValueError`; historical yfinance empty response raises `RuntimeError`.

### USD/KRW

- Feature/source column: `usdkrw`
- Generated model feature: `usdkrw_return_1d`
- Production function: `run_macro_update()` -> `read_macro_file()` -> `build_macro_update_row()`
- Production source file: `src/pipeline/macro_update.py`
- Production source library: `pandas`
- Production actual source: `data/processed/macro/macro_clean_20230615_20260614.parquet`
- Historical downloader function: `download_macro_data()` / `download_macro()`
- Historical source files:
  - `scripts/download_real_market_data.py`
  - `scripts/build_full_universe_real_dataset.py`
- Historical source library: `yfinance`
- Historical ticker/API: `KRW=X`
- Returned fields from yfinance: Yahoo Finance OHLCV-style frame; project extracts `Close` only.
- Stored fields: `date`, `usdkrw`
- Feature formula: `usdkrw_return_1d = usdkrw.shift(1) / usdkrw.shift(2) - 1` per ticker when merged into stock rows.
- Update policy: production existing row if present; otherwise prior-only forward fill from latest available `usdkrw`.
- Failure behavior: missing macro file raises `FileNotFoundError`; missing date with no prior row raises `ValueError`; historical yfinance empty response raises `RuntimeError`.

### WTI

- Feature/source column: `wti_close`
- Generated model feature: `wti_return_1d`
- Production function: `run_macro_update()` -> `read_macro_file()` -> `build_macro_update_row()`
- Production source file: `src/pipeline/macro_update.py`
- Production source library: `pandas`
- Production actual source: `data/processed/macro/macro_clean_20230615_20260614.parquet`
- Historical downloader function: `download_macro_data()` / `download_macro()`
- Historical source files:
  - `scripts/download_real_market_data.py`
  - `scripts/build_full_universe_real_dataset.py`
- Historical source library: `yfinance`
- Historical ticker/API: `CL=F`
- Returned fields from yfinance: Yahoo Finance OHLCV-style frame; project extracts `Close` only.
- Stored fields: `date`, `wti_close`
- Feature formula: `wti_return_1d = wti_close.shift(1) / wti_close.shift(2) - 1` per ticker when merged into stock rows.
- Update policy: production existing row if present; otherwise prior-only forward fill from latest available `wti_close`.
- Failure behavior: missing macro file raises `FileNotFoundError`; missing date with no prior row raises `ValueError`; historical yfinance empty response raises `RuntimeError`.

## Config Inspection

File inspected:

```text
configs/daily_update.yaml
```

Detected macro-related settings:

| Setting | Value |
|---|---|
| `macro_file` | `data/processed/macro/macro_clean_20230615_20260614.parquet` |
| `daily_processed_dir` | `data/daily/processed` |
| `production_mode` | `true` |
| `strict_feature_source_check` | `true` |
| `enable_us10y_check` | `false` |
| `enable_gold_check` | `false` |
| `enable_dxy_check` | `false` |

Settings searched but not found in `configs/daily_update.yaml`:

- `macro_source`
- `macro_cache`
- `macro_download`
- `macro_update_mode`

Related feature config:

```text
configs/feature.yaml
  feature.macro.enabled: true
  feature.macro.features:
    - nasdaq_return_1d
    - sox_return_1d
    - sp500_return_1d
    - vix_change_1d
    - usdkrw_return_1d
    - wti_return_1d
  feature.missing_value.fill_macro_missing: "forward_fill"
```

## Hidden Macro Download Paths Found

### `scripts/download_real_market_data.py`

Function:

```text
download_macro_data()
```

Library:

```text
yfinance
```

Ticker map:

```text
nasdaq_close -> ^IXIC
sox_close    -> ^SOX
sp500_close  -> ^GSPC
vix_close    -> ^VIX
usdkrw       -> KRW=X
wti_close    -> CL=F
```

Download call:

```text
yf.download(ticker, start=START_DATE, end="2026-06-15", progress=False, auto_adjust=False, threads=False)
```

Returned yfinance fields:

```text
Open
High
Low
Close
Adj Close
Volume
```

Project stores only:

```text
date
<output_column from Close>
```

Failure behavior:

```text
if data.empty: raise RuntimeError("No macro data returned for ...")
```

### `scripts/build_full_universe_real_dataset.py`

Function:

```text
ensure_macro()
```

Behavior:

```text
if raw macro parquet exists:
    read existing data/raw/macro/macro_20230615_20260614.parquet
else:
    download_macro()
```

Function:

```text
download_macro()
```

Library:

```text
yfinance
```

Ticker map:

```text
nasdaq_close -> ^IXIC
sox_close    -> ^SOX
sp500_close  -> ^GSPC
vix_close    -> ^VIX
usdkrw       -> KRW=X
wti_close    -> CL=F
```

Download call:

```text
yf.download(ticker, start=START_DATE, end="2026-06-15", progress=False, auto_adjust=False, threads=False)
```

Project stores only:

```text
date
<output_column from Close>
```

Cleaning:

```text
clean_macro()
  sort by date
  forward-fill all macro columns using prior observations only
  write data/processed/macro/macro_clean_20230615_20260614.parquet
```

Failure behavior:

```text
if data.empty: raise RuntimeError("No macro data returned for ...")
```

## Imported Module Trace From `macro_update.py`

Direct imports:

| Import | Purpose | Macro download behavior |
|---|---|---|
| `pandas` | Read/write parquet and transform dates | No download |
| `DailyUpdateConfig` from `src.pipeline.config` | Resolve configured file paths | No download |
| `DailyRunContext` from `src.pipeline.daily_context` | Provides `update_date`, flags | No download |

Recursive project modules from this path do not call yfinance, requests, FRED, pykrx, or any macro API. The production macro update is file-based.

## Feature Source Completeness Interaction

`src/pipeline/feature_source_completeness.py` checks macro freshness by reading the configured `macro_file` and finding the latest non-null date per column.

Required production sources:

```text
KRX
NASDAQ
S&P500
VIX
WTI
USD/KRW
```

Macro source column mapping:

```text
nasdaq -> nasdaq_close or nasdaq
sp500  -> sp500_close or sp500
vix    -> vix_close or vix
wti    -> wti_close or wti
usdkrw -> usdkrw or usdkrw_close
```

Optional disabled by config:

```text
US10Y
Gold
DXY
```

Important: completeness checks freshness of the stored macro file. It does not download missing macro observations.

## Final Recommendation

The production daily macro update currently relies on prior-only forward-fill from the processed macro parquet, not fresh daily macro downloads. This is reproducible and leakage-safe, but it means the feature source completeness check can pass only if `macro_file` already contains all required macro source values for the expected date.

Recommendation:

1. Keep `src/pipeline/macro_update.py` as the safe file-based append/forward-fill layer.
2. Add a separate explicit production macro downloader step before `FeatureSourceCompletenessChecker` if fresh NASDAQ/S&P500/VIX/WTI/USD/KRW values are required every morning.
3. Make that downloader config-driven and timeout-bounded, similar to the KRX per-ticker downloader.
4. Store raw downloaded macro observations separately before cleaning.
5. Preserve the current prior-only forward-fill policy for missing dates, but mark forward-filled macro values in status so production can decide whether to block or continue.

## Console Summary

```text
========================================
Macro Source Audit

NASDAQ ............ production macro_file; historical yfinance (^IXIC)
SOX ............... production macro_file; historical yfinance (^SOX)
S&P500 ............ production macro_file; historical yfinance (^GSPC)
VIX ............... production macro_file; historical yfinance (^VIX)
WTI ............... production macro_file; historical yfinance (CL=F)
USD/KRW ........... production macro_file; historical yfinance (KRW=X)

Production daily macro download: NONE
Production update mode: existing row or prior-only forward fill
========================================
```
