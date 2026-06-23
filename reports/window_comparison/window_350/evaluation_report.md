# Window 350 Walk-Forward Evaluation

- Evaluation window: 2026-01-30 to 2026-06-17
- Evaluation days: 90
- Rolling train days: 350
- Prediction rows: 31319
- Top10 rows: 900
- Feature count: 55

## Ranking

- rmse: 0.05379840
- mae: 0.03758634
- r2: -0.06457771
- pearson_correlation: 0.05554254
- spearman_rank_correlation_ic: 0.04526828
- daily_mean_spearman_ic: 0.01064943
- top10_hit_rate: 0.08222222
- top20_hit_rate: 0.11333333
- ndcg_at_10: 0.48564997

## Gap

- rmse: 0.03059607
- direction_accuracy: 0.54890003

## Intraday

- rmse: 0.04321010
- direction_accuracy: 0.46664964

## Portfolio

- cumulative_return: 1.21150299
- average_return: 0.00976774
- win_rate: 0.61111111
- volatility: 0.04258299
- sharpe_ratio: 3.64131455
- maximum_drawdown: -0.23137250
- cagr: 8.22837182

## Timing and Size

- training_time_total_seconds: 415.2705
- training_time_mean_seconds: 4.6141
- prediction_time_total_seconds: 1.2827
- prediction_time_mean_seconds: 0.0143
- train_rows_mean: 121259.8222
- feature_count: 55
- model_size_mean_bytes: 1931646.4444
- ranking_model_size_mean_bytes: 642434.1222
- gap_model_size_mean_bytes: 646983.7444
- intraday_model_size_mean_bytes: 642228.5778