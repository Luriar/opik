# 90-Trading-Day Walk-Forward Production Evaluation

This evaluation replays the latest 90 target trading days using existing production model helpers without modifying production code, feature formulas, target formulas, pipeline logic, archive logic, production policy, Top10 selection, or configuration.

## Procedure

- For each evaluation target date, rows with `target_date < evaluation_date` were treated as known historical training data.
- From that known set, the most recent 250 unique `feature_date` values were selected with the existing rolling-window helper.
- Existing production model specifications and training helper were used for Ranking, Gap, and Intraday LightGBM models.
- Predictions were generated only for the evaluation target date.
- Top10 was selected by the production rule: `ranking_score` descending.

## Evaluation Window

- Evaluation days: 90
- Start: 2026-01-30
- End: 2026-06-17
- Prediction rows: 31319
- Top10 rows: 900
- Feature count: 55

## Ranking Model Metrics

- rmse: 0.05326020
- mae: 0.03720363
- r2: -0.04338410
- pearson_correlation: 0.09824437
- spearman_rank_correlation_ic: 0.05798812
- daily_mean_spearman_ic: 0.00419463
- top10_hit_rate: 0.09555556
- top20_hit_rate: 0.11222222
- top50_hit_rate: 0.18377778
- ndcg_at_10: 0.47841119
- ndcg_at_20: 0.52140622
- average_top10_actual_return: 0.00615186

## Gap Model Metrics

- rmse: 0.03036283
- mae: 0.02080680
- r2: 0.02202260
- direction_accuracy: 0.54331237
- correlation: 0.21309049

## Intraday Model Metrics

- rmse: 0.04329516
- mae: 0.03023029
- r2: -0.04668299
- direction_accuracy: 0.48848942
- correlation: -0.01563693

## Top10 Portfolio

- cumulative_return: 0.59077170
- average_return: 0.00615186
- win_rate: 0.55555556
- volatility: 0.04436705
- sharpe_ratio: 2.20113233
- maximum_drawdown: -0.24927334

## KOSPI200 Equal-Weight Benchmark Proxy

- cumulative_return: 0.16895803
- average_return: 0.00212926
- win_rate: 0.60000000
- volatility: 0.02793490
- sharpe_ratio: 1.20998746
- maximum_drawdown: -0.17051734

## Benchmark Note

No standalone KOSPI index return series was present in the production training dataset. `kospi_benchmark_return` is an equal-weight KOSPI200 constituent proxy from `data/metadata/full_universe_260616.csv`.

## Output Files

- `reports/model_evaluation_90d/daily_predictions.csv`
- `reports/model_evaluation_90d/daily_top10.csv`
- `reports/model_evaluation_90d/portfolio_returns.csv`
- `reports/model_evaluation_90d/feature_importance_average.csv`
- `reports/model_evaluation_90d/fold_metadata.csv`
- `reports/model_evaluation_90d/metrics_summary.json`

## Leakage Check

PASS. Each fold filtered training rows to `target_date < evaluation_date` before selecting the rolling 250 feature dates. Evaluation-date target rows were never included in the corresponding training fold.