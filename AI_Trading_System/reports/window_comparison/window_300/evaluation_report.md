# Window 300 Walk-Forward Evaluation

- Evaluation window: 2026-01-30 to 2026-06-17
- Evaluation days: 90
- Rolling train days: 300
- Prediction rows: 31319
- Top10 rows: 900
- Feature count: 55

## Ranking

- rmse: 0.05310202
- mae: 0.03709845
- r2: -0.03719562
- pearson_correlation: 0.10857434
- spearman_rank_correlation_ic: 0.07705101
- daily_mean_spearman_ic: 0.00331745
- top10_hit_rate: 0.08111111
- top20_hit_rate: 0.11388889
- ndcg_at_10: 0.48550562

## Gap

- rmse: 0.03064158
- direction_accuracy: 0.53532999

## Intraday

- rmse: 0.04342137
- direction_accuracy: 0.48037932

## Portfolio

- cumulative_return: 0.96133173
- average_return: 0.00835267
- win_rate: 0.58888889
- volatility: 0.04104341
- sharpe_ratio: 3.23059214
- maximum_drawdown: -0.22690984
- cagr: 5.59390907

## Timing and Size

- training_time_total_seconds: 374.1892
- training_time_mean_seconds: 4.1577
- prediction_time_total_seconds: 1.2925
- prediction_time_mean_seconds: 0.0144
- train_rows_mean: 104084.7222
- feature_count: 55
- model_size_mean_bytes: 1928989.7444
- ranking_model_size_mean_bytes: 641412.0889
- gap_model_size_mean_bytes: 646172.3333
- intraday_model_size_mean_bytes: 641405.3222