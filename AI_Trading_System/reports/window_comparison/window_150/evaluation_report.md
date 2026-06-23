# Window 150 Walk-Forward Evaluation

- Evaluation window: 2026-01-30 to 2026-06-17
- Evaluation days: 90
- Rolling train days: 150
- Prediction rows: 31319
- Top10 rows: 900
- Feature count: 55

## Ranking

- rmse: 0.05692292
- mae: 0.04015514
- r2: -0.19182620
- pearson_correlation: -0.00241264
- spearman_rank_correlation_ic: -0.00033841
- daily_mean_spearman_ic: 0.00144636
- top10_hit_rate: 0.08111111
- top20_hit_rate: 0.11166667
- ndcg_at_10: 0.46432538

## Gap

- rmse: 0.03301471
- direction_accuracy: 0.52546378

## Intraday

- rmse: 0.04483029
- direction_accuracy: 0.48542418

## Portfolio

- cumulative_return: 0.11571526
- average_return: 0.00219320
- win_rate: 0.50000000
- volatility: 0.04424747
- sharpe_ratio: 0.78684650
- maximum_drawdown: -0.22531325
- cagr: 0.35878094

## Timing and Size

- training_time_total_seconds: 267.2137
- training_time_mean_seconds: 2.9690
- prediction_time_total_seconds: 1.2381
- prediction_time_mean_seconds: 0.0138
- train_rows_mean: 52171.9778
- feature_count: 55
- model_size_mean_bytes: 1913069.2333
- ranking_model_size_mean_bytes: 635975.3889
- gap_model_size_mean_bytes: 640833.9667
- intraday_model_size_mean_bytes: 636259.8778