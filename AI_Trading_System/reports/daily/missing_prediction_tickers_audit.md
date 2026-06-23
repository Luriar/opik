# Missing Prediction Tickers Audit

## Scope

This audit compares the configured full production universe against the latest archived production prediction universe. It does not modify production code, data, models, or reports other than this audit file.

## Latest Production Context

- status_path: `outputs\archive\latest\status\daily_update_status_20260618.json`
- archive_metadata_path: `outputs\archive\latest\metadata\archive_metadata.json`
- prediction_path: `outputs\archive\latest\predictions\predictions_20260618.parquet`
- top10_path: `outputs\archive\latest\top10\top10_20260618.csv`
- raw_snapshot_path: `data\daily\raw\ohlcv_20260617.csv`
- clean_snapshot_path: `data\daily\processed\ohlcv_clean_20260617.csv`
- feature_snapshot_path: `data\daily\features\features_20260617.csv`
- target_update_date: `2026-06-17`
- prediction_date: `2026-06-18`
- status_universe_count: `350`
- status_pykrx_tickers_requested: `350`
- status_pykrx_tickers_downloaded: `348`
- status_pykrx_tickers_failed: `2`
- status_pykrx_failed_tickers_sample: `['000009', '000126']`
- status_raw_rows_downloaded_or_found: `348`
- status_cleaned_rows_added: `348`
- status_feature_ticker_count: `348`
- status_prediction_rows: `348`

## Counts

- Universe count: 350
- Latest raw OHLCV tickers for target update date: 348
- Latest clean OHLCV tickers for target update date: 348
- Latest feature tickers for target update date: 348
- Prediction count: 348
- Missing count: 2
- Extra prediction tickers not in universe: 0

## Missing Tickers

|   ticker | ticker_name      | market   | source_index   | in_universe   | in_raw   | in_clean   | in_features   | has_nan_feature_values   | in_predictions   | reason                                             |
|---------:|:-----------------|:---------|:---------------|:--------------|:---------|:-----------|:--------------|:-------------------------|:-----------------|:---------------------------------------------------|
|   000009 | 에임드바이오     | KOSDAQ   | KOSDAQ150      | True          | False    | False      | False         | False                    | False            | missing OHLCV row in latest raw daily KRX snapshot |
|   000126 | 삼성에피스홀딩스 | KOSPI    | KOSPI200       | True          | False    | False      | False         | False                    | False            | missing OHLCV row in latest raw daily KRX snapshot |

## Stage Trace

### raw_missing_from_universe

Count: 2
- 000009 에임드바이오
- 000126 삼성에피스홀딩스

### clean_missing_from_universe

Count: 2
- 000009 에임드바이오
- 000126 삼성에피스홀딩스

### feature_missing_from_universe

Count: 2
- 000009 에임드바이오
- 000126 삼성에피스홀딩스

### prediction_missing_from_universe

Count: 2
- 000009 에임드바이오
- 000126 삼성에피스홀딩스

## Detailed Missing Ticker Evidence

|   ticker | ticker_name      | latest_clean_master_dates   | latest_feature_master_dates   | in_training_dataset_for_update_date   | in_top10   | nan_feature_count   | reason                                             |
|---------:|:-----------------|:----------------------------|:------------------------------|:--------------------------------------|:-----------|:--------------------|:---------------------------------------------------|
|   000009 | 에임드바이오     |                             |                               | False                                 | False      |                     | missing OHLCV row in latest raw daily KRX snapshot |
|   000126 | 삼성에피스홀딩스 |                             |                               | False                                 | False      |                     | missing OHLCV row in latest raw daily KRX snapshot |

## Interpretation

Both missing tickers are present in `full_universe_260616.csv` but absent from the latest raw daily KRX OHLCV snapshot for the target update date. Because they never enter the raw daily OHLCV snapshot, they also do not enter the clean OHLCV snapshot, feature rows, or final prediction universe. The exclusion occurs before feature generation and before model prediction.

## Caveats

- The audit uses saved local production artifacts only.
- If pykrx did not explicitly return failed ticker IDs in status, the strongest local evidence is the absence from `data/daily/raw/ohlcv_YYYYMMDD.csv`.
- The exact market reason, such as suspension, listing issue, or no-trading day for a specific ticker, would require external KRX lookup; no network lookup was performed for this audit.