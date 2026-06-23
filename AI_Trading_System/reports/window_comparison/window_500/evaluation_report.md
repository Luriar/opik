# Window 500 Walk-Forward Evaluation

- Evaluation window: 2026-01-30 to 2026-06-17
- Evaluation days: 90
- Rolling train days: 500
- Prediction rows: 31319
- Top10 rows: 900
- Feature count: 55

## Ranking

- rmse: 0.05485199
- mae: 0.03804056
- r2: -0.10668327
- pearson_correlation: 0.00465030
- spearman_rank_correlation_ic: 0.01529829
- daily_mean_spearman_ic: -0.00621849
- top10_hit_rate: 0.08222222
- top20_hit_rate: 0.11333333
- ndcg_at_10: 0.46403091

## Gap

- rmse: 0.03234135
- direction_accuracy: 0.53654331

## Intraday

- rmse: 0.04326940
- direction_accuracy: 0.47686708

## Portfolio

- cumulative_return: 0.32960363
- average_return: 0.00423778
- win_rate: 0.55555556
- volatility: 0.04629713
- sharpe_ratio: 1.45306461
- maximum_drawdown: -0.23394192
- cagr: 1.22035357

## Timing and Size

- training_time_total_seconds: 523.4382
- training_time_mean_seconds: 5.8160
- prediction_time_total_seconds: 1.2537
- prediction_time_mean_seconds: 0.0139
- train_rows_mean: 172398.5000
- feature_count: 55
- model_size_mean_bytes: 1941723.8333
- ranking_model_size_mean_bytes: 645775.2889
- gap_model_size_mean_bytes: 650869.6556
- intraday_model_size_mean_bytes: 645078.8889