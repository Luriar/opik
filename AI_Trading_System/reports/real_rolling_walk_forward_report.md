# Real Rolling Walk-Forward Report

Created at: 2026-06-15T13:00:59.207081+00:00

## Date Policy
- Rolling train window start: 2024-07-01
- Rolling train window end for first prediction: last available feature_date before first prediction
- Validation policy start: 2026-03-01
- Actual first prediction date: 2026-03-03
- Actual last prediction date: 2026-06-11
- Daily retrains: 69
- Runtime LightGBM n_estimators cap: 300

## Output Shape
- Prediction rows: 6900
- Unique tickers: 100
- 005930 exists: True

## Overall Metrics
- Ranking Rank IC / Spearman: 0.28970181
- Gap RMSE: 0.02842654
- Gap MAE: 0.01891190
- Gap directional accuracy: 0.69347826
- Intraday RMSE: 0.04256947
- Intraday MAE: 0.02979606
- Intraday directional accuracy: 0.51115942
- Expected return Rank IC / Spearman: 0.26733529

## Leakage Checks
- max(train feature_date) < prediction_date: True
- no duplicated date/ticker predictions: True
- prev_close not in model features: True
- target columns not in model features: True

## Files
- `outputs/walk_forward_real_rolling/predictions.csv`
- `outputs/walk_forward_real_rolling/predictions.parquet`
- `outputs/walk_forward_real_rolling/metrics.json`
- `outputs/walk_forward_real_rolling/daily_metrics.csv`
- `outputs/walk_forward_real_rolling/fold_metadata.csv`

## Fold Metadata Preview

|   fold_id | prediction_date   | target_date_min   | target_date_max   | train_start_date   | train_end_date   |   train_rows |   prediction_rows |   unique_tickers | max_train_feature_date_lt_prediction_date   | max_train_feature_date   |
|----------:|:------------------|:------------------|:------------------|:-------------------|:-----------------|-------------:|------------------:|-----------------:|:--------------------------------------------|:-------------------------|
|         1 | 2026-03-03        | 2026-03-04        | 2026-03-04        | 2024-07-01         | 2026-02-27       |        40680 |               100 |              100 | True                                        | 2026-02-27               |
|         2 | 2026-03-04        | 2026-03-05        | 2026-03-05        | 2024-07-01         | 2026-03-03       |        40780 |               100 |              100 | True                                        | 2026-03-03               |
|         3 | 2026-03-05        | 2026-03-06        | 2026-03-06        | 2024-07-01         | 2026-03-04       |        40880 |               100 |              100 | True                                        | 2026-03-04               |
|         4 | 2026-03-06        | 2026-03-09        | 2026-03-09        | 2024-07-01         | 2026-03-05       |        40980 |               100 |              100 | True                                        | 2026-03-05               |
|         5 | 2026-03-09        | 2026-03-10        | 2026-03-10        | 2024-07-01         | 2026-03-06       |        41080 |               100 |              100 | True                                        | 2026-03-06               |
|         6 | 2026-03-10        | 2026-03-11        | 2026-03-11        | 2024-07-01         | 2026-03-09       |        41180 |               100 |              100 | True                                        | 2026-03-09               |
|         7 | 2026-03-11        | 2026-03-12        | 2026-03-12        | 2024-07-01         | 2026-03-10       |        41280 |               100 |              100 | True                                        | 2026-03-10               |
|         8 | 2026-03-12        | 2026-03-13        | 2026-03-13        | 2024-07-01         | 2026-03-11       |        41380 |               100 |              100 | True                                        | 2026-03-11               |
|         9 | 2026-03-13        | 2026-03-16        | 2026-03-16        | 2024-07-01         | 2026-03-12       |        41480 |               100 |              100 | True                                        | 2026-03-12               |
|        10 | 2026-03-16        | 2026-03-17        | 2026-03-17        | 2024-07-01         | 2026-03-13       |        41580 |               100 |              100 | True                                        | 2026-03-13               |
