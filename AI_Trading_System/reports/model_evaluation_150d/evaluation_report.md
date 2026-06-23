# 150-Trading-Day Rolling Window Production Evaluation

This evaluation replays the latest 90 target trading days using existing production model helpers. The only evaluation change versus the 250-day replay is `rolling_train_days = 150`.

## Procedure

- For each evaluation target date, rows with `target_date < evaluation_date` were treated as known historical training data.
- From that known set, the most recent 150 unique `feature_date` values were selected with the existing rolling-window helper.
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
- Rolling train days: 150

## Ranking Model Metrics

- rmse: 0.05692292
- mae: 0.04015514
- r2: -0.19182620
- pearson_correlation: -0.00241264
- spearman_rank_correlation_ic: -0.00033841
- daily_mean_spearman_ic: 0.00144636
- top10_hit_rate: 0.08111111
- top20_hit_rate: 0.11166667
- ndcg_at_10: 0.46432538
- ndcg_at_20: 0.50789926

## Gap Model Metrics

- rmse: 0.03301471
- mae: 0.02221083
- direction_accuracy: 0.52546378
- correlation: 0.03768815

## Intraday Model Metrics

- rmse: 0.04483029
- mae: 0.03159551
- direction_accuracy: 0.48542418
- correlation: -0.06947746

## Top10 Portfolio

- cumulative_return: 0.11571526
- average_return: 0.00219320
- win_rate: 0.50000000
- volatility: 0.04424747
- sharpe_ratio: 0.78684650
- maximum_drawdown: -0.22531325

## KOSPI200 Equal-Weight Benchmark Proxy

- cumulative_return: 0.16895803
- average_return: 0.00212926
- win_rate: 0.60000000
- volatility: 0.02793490
- sharpe_ratio: 1.20998746
- maximum_drawdown: -0.17051734

## Leakage Check

PASS. Each fold filtered training rows to `target_date < evaluation_date` before selecting the rolling 150 feature dates. Evaluation-date target rows were never included in the corresponding training fold.