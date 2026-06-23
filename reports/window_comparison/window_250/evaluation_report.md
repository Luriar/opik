# Window 250 Walk-Forward Evaluation

- Evaluation window: 2026-01-30 to 2026-06-17
- Evaluation days: 90
- Rolling train days: 250
- Prediction rows: 31319
- Top10 rows: 900
- Feature count: 55

## Ranking

- rmse: 0.05326020
- mae: 0.03720363
- r2: -0.04338410
- pearson_correlation: 0.09824437
- spearman_rank_correlation_ic: 0.05798812
- daily_mean_spearman_ic: 0.00419463
- top10_hit_rate: 0.09555556
- top20_hit_rate: 0.11222222
- ndcg_at_10: 0.47841119

## Gap

- rmse: 0.03036283
- direction_accuracy: 0.54331237

## Intraday

- rmse: 0.04329516
- direction_accuracy: 0.48848942

## Portfolio

- cumulative_return: 0.59077170
- average_return: 0.00615186
- win_rate: 0.55555556
- volatility: 0.04436705
- sharpe_ratio: 2.20113233
- maximum_drawdown: -0.24927334
- cagr: 2.66861382

## Timing and Size

- training_time_total_seconds: 347.1432
- training_time_mean_seconds: 3.8571
- prediction_time_total_seconds: 1.2915
- prediction_time_mean_seconds: 0.0144
- train_rows_mean: 86835.5222
- feature_count: 55
- model_size_mean_bytes: 1926388.3222
- ranking_model_size_mean_bytes: 640510.9111
- gap_model_size_mean_bytes: 645340.1667
- intraday_model_size_mean_bytes: 640537.2444