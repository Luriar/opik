# Full Universe Rolling Walk-Forward Report

- Created at: `2026-06-16T00:35:17.336483+00:00`
- Train start policy: `2024-07-01`
- Validation policy: `2026-03-01` to `2026-06-12`
- Actual prediction period: `2026-03-03` to `2026-06-11`
- Daily retrains: `69`
- Prediction rows: `24012`
- Unique tickers: `348`
- 005930 exists: `True`

## Metrics
- Ranking Rank IC: `0.27637410`
- Expected Return Rank IC: `0.26437093`
- Gap RMSE: `0.02765864`
- Gap MAE: `0.01882253`
- Gap directional accuracy: `0.69552724`
- Intraday RMSE: `0.04481114`
- Intraday MAE: `0.03158277`
- Intraday directional accuracy: `0.50837081`

## Leakage Checks
- max(train feature_date) < prediction_date: `True`
- no duplicate date/ticker: `True`
- no forbidden model features: `True`
