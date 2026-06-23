# Performance Audit

========================================
Performance Audit

Ranking IC

Top10 Logic

Portfolio Logic

CAGR Formula

Sharpe Formula

Random Baseline

Leakage Check

Consistency Check

Potential Issues

Conclusions

========================================

## Executive Conclusion

No arithmetic or Top10-selection implementation issue was found in the saved evaluation artifacts. The portfolio outperformance is mathematically present in the 90-day replay and exceeds the 1000-run random baseline, but it should not be treated as fully proven model skill because daily mean IC is near zero, the window is short, transaction costs are absent, and the benchmark is a KOSPI200 equal-weight proxy rather than a true KOSPI index.

## Ranking IC

- Daily Spearman IC: per-day Spearman correlation between `ranking_score` and `target_ranking`.
- Overall Spearman IC: Spearman correlation over all saved prediction rows.
- Weighted IC: sample-count weighted average of daily Spearman IC.
- Daily sample count: 347 to 348.
- Mean daily IC: 0.00419463
- Weighted daily IC: 0.00420184
- Overall Spearman IC: 0.05798812
- Overall Pearson correlation: 0.09824437

## Top10 Logic

- Verified selection key: `ranking_score`.
- Sorting direction: descending.
- Extraction: first 10 rows after sorting.
- Not selected by `expected_return`, `pred_gap`, or `pred_intraday`.
- All daily Top10 rows match ranking-score descending selection: True

## Top10 Actual Return

- Average Top10 actual return: 0.00615186
- Median daily Top10 actual return: 0.00338376
- Best daily Top10 actual return: 0.14900919
- Worst daily Top10 actual return: -0.16876891

## Portfolio Logic

- Daily return: equal-weight arithmetic average of selected Top10 `target_ranking`.
- Holding period: 1 trading day.
- Rebalancing: daily.
- Transaction cost: not included.
- Compound return: cumulative product of `(1 + daily_return) - 1`.
- Recalculation verified: True

## CAGR Formula

- Formula: `(1 + cumulative_return) ** (252 / evaluation_days) - 1`.
- Evaluation days: 90
- AI Top10 cumulative return: 0.59077170
- AI Top10 CAGR: 2.66861382

## Sharpe Formula

- Formula: `mean(daily_return) / std(daily_return, ddof=1) * sqrt(252)`.
- Risk-free rate: 0.
- AI Top10 Sharpe: 2.20113233
- Maximum drawdown: -0.24927334

## Consistency Check

- Correlation between daily IC and Top10 actual return: 0.34915987
- Ranking score vs actual next-day return, Pearson: 0.09824437
- Ranking score vs actual next-day return, Spearman: 0.05798812

## Prediction Distribution

- Ranking score histogram and boxplot are in `audit_charts/`.
- Overall min: -0.05134180
- Overall median: 0.00304942
- Overall max: 0.15182414
- Mean daily Top10 threshold percentile: 0.97413710

## Portfolio Robustness

| K | Average Return | CAGR | Sharpe | MDD |
|---:|---:|---:|---:|---:|
| 10 | 0.00615186 | 2.66861382 | 2.20113233 | -0.24927334 |
| 20 | 0.00445100 | 1.48922319 | 1.73225880 | -0.22329335 |
| 30 | 0.00439928 | 1.50219361 | 1.79540548 | -0.21844604 |
| 50 | 0.00384584 | 1.20931603 | 1.63216456 | -0.20290049 |

## Random Baseline

- 1000 Monte Carlo simulations.
- Each day randomly selected 10 stocks.
- Equal weight, one-day holding.
- AI CAGR percentile vs random: 0.9960
- AI Sharpe percentile vs random: 0.9870
- Random median CAGR: 0.19082906
- Random median Sharpe: 0.59505779
- AI exceeds random 95th percentile on CAGR and Sharpe: True

## Leakage Check

- `evaluation_date > train_end_date` for every day: True
- Prediction target date equals evaluation date for every day: True
- Exact future target in training rows: False
- Note: one sparse-trading ticker had prediction `feature_date == train_end_date`, but its exact `(ticker, feature_date, target_date)` row was not trainable before the evaluation date. This is not target leakage.
- Leakage found: False

## Potential Issues

- No transaction costs, slippage, or liquidity capacity constraints are included.
- KOSPI comparison is an equal-weight KOSPI200 proxy, not a true index benchmark.
- 90 trading days is short; annualized CAGR is mathematically valid but unstable.
- Ranking IC near zero means broad cross-sectional ranking power is weak even though the selected Top10 tail performed well in this window.
- Current-universe evaluation may retain survivorship-bias risk depending on historical constituent accuracy.

## Conclusions

- Ranking IC verified: YES
- Portfolio calculation verified: YES
- Leakage found: NO
- Random baseline exceeded: YES
- CAGR trustworthy as arithmetic from saved outputs: YES
- Sharpe trustworthy as arithmetic from saved outputs: YES

Final judgment: the high portfolio CAGR is not explained by an obvious saved-output calculation, Top10 selection bug, or detected target leakage. It is a real result in this 90-day replay, but it is not conclusive evidence of durable model skill because ranking IC is weak, the window is short, and the evaluation omits trading costs.