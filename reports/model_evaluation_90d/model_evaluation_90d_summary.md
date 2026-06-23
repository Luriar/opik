# 90-Day Walk-Forward Evaluation Summary

## Evaluation Window

- Latest 90 Trading Days: 2026-01-30 to 2026-06-17
- Evaluation Days: 90
- Prediction Rows: 31319

## Rolling Train

- Rolling Train: 250 Trading Days
- Average train rows: 86836
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

## Top10 Strategy

- Selection rule: production Top10 by `ranking_score` descending.
- Weighting: equal weight.
- Holding period: 1 trading day.
- Average Return: 0.00615186
- Win Rate: 0.55555556
- Sharpe Ratio: 2.20113233
- Maximum Drawdown: -0.24927334
- Portfolio CAGR: 2.66861382

## Benchmark Comparison

- Benchmark: equal-weight KOSPI200 constituent proxy, because no standalone KOSPI index series is present in the production training dataset.
- Portfolio cumulative return: 0.59077170
- Benchmark cumulative return: 0.16895803
- Portfolio CAGR: 2.66861382
- Benchmark CAGR: 0.54823532

## Top20 Stable Features

- usdkrw_return_1d: 566.1704
- sox_return_1d: 561.4148
- wti_return_1d: 488.5889
- nasdaq_return_1d: 481.4074
- vix_change_1d: 454.0037
- sp500_return_1d: 357.5296
- atr_percent: 139.5333
- trading_value: 124.4074
- return_60d: 113.5296
- volatility_20d: 111.9741
- intraday_range_5d: 105.9593
- return_3d: 104.0926
- atr_rank_pct: 103.4481
- return_1d: 102.6296
- upper_shadow: 94.4037
- momentum_accel: 94.1667
- lower_shadow: 90.7222
- volatility_rank_pct: 89.4222
- close_ma60_ratio: 84.9889
- body: 83.9778

## Recommendations

- Keep this as out-of-sample production replay evidence, but do not treat the high Top10 return as sufficient proof by itself.
- Ranking daily mean IC is low, so continue monitoring rank stability before increasing risk.
- Gap model direction accuracy is above random in this window; intraday direction accuracy is weaker and should be monitored.
- Keep the strict production source completeness policy; stale macro or OHLCV data would make this evaluation non-comparable to live operation.
- Add a true KOSPI index benchmark series in a future reporting pass for cleaner benchmark comparison.