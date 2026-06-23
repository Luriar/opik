# Real Model Training Report

Created at: 2026-06-15T11:01:15.063248+00:00

## Dataset
- Train date range: 2023-06-16 to 2025-11-05
- Validation date range: 2025-11-06 to 2026-06-12
- Train rows: 58752
- Validation rows: 14669
- Train unique tickers: 102
- Validation unique tickers: 101
- Feature count: 58
- 005930 included: True

## Metrics
- Ranking Rank IC/Spearman: 0.15590549
- Gap RMSE: 0.02415455
- Gap MAE: 0.01523392
- Gap directional accuracy: 0.64919217
- Intraday RMSE: 0.03803547
- Intraday MAE: 0.02636899
- Intraday directional accuracy: 0.49710273
- Prediction row count: 14669

## Model Files
- `outputs\models\real\ranking_model.txt`
- `outputs\models\real\gap_model.txt`
- `outputs\models\real\intraday_model.txt`

## Audit
- Chronological split only; no random split and no shuffle.
- date, ticker, feature_date, target_date, prediction_horizon, and target columns are excluded from X_train.
- Prediction pred_open and pred_close are normalized to previous close = 1.0 because the optimized training dataset does not store raw previous close prices.
