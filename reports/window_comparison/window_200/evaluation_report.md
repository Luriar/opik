# Window 200 Walk-Forward Evaluation

- Evaluation window: 2026-01-30 to 2026-06-17
- Evaluation days: 90
- Rolling train days: 200
- Prediction rows: 31319
- Top10 rows: 900
- Feature count: 55

## Ranking

- rmse: 0.05380835
- mae: 0.03764291
- r2: -0.06497155
- pearson_correlation: 0.09032985
- spearman_rank_correlation_ic: 0.07743967
- daily_mean_spearman_ic: 0.02595799
- top10_hit_rate: 0.08666667
- top20_hit_rate: 0.10833333
- ndcg_at_10: 0.47715678

## Gap

- rmse: 0.03101403
- direction_accuracy: 0.52029120

## Intraday

- rmse: 0.04349075
- direction_accuracy: 0.48986238

## Portfolio

- cumulative_return: 0.50151844
- average_return: 0.00550358
- win_rate: 0.61111111
- volatility: 0.04412433
- sharpe_ratio: 1.98001106
- maximum_drawdown: -0.26456075
- cagr: 2.12094330

## Timing and Size

- training_time_total_seconds: 315.6219
- training_time_mean_seconds: 3.5069
- prediction_time_total_seconds: 1.3023
- prediction_time_mean_seconds: 0.0145
- train_rows_mean: 69526.3889
- feature_count: 55
- model_size_mean_bytes: 1923917.0778
- ranking_model_size_mean_bytes: 639672.9111
- gap_model_size_mean_bytes: 644492.6000
- intraday_model_size_mean_bytes: 639751.5667