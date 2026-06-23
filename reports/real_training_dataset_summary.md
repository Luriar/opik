# Real Training Dataset Summary

- Rows: `73421`
- Columns: `66`
- Feature count: `58`
- Target count: `3`
- Removed rows: `102`
- Unique tickers: `102`
- Unique trading dates: `727`
- Min feature date: `2023-06-15`
- Max feature date: `2026-06-11`
- Min target date: `2023-06-16`
- Max target date: `2026-06-12`
- Ticker 005930 exists: `True`
- Date is model feature: `False`
- Ticker is model feature: `False`
- Target columns as model features: `[]`
- Leakage check passed: `True`
- Leakage violation count: `0`

## NaN Summary
| Column | Missing Count |
| --- | ---: |
| return_60d | 6120 |
| close_ma60_ratio | 6018 |
| bb_position_change_5d | 2448 |
| relative_trading_value | 2378 |
| trading_value_rank_pct | 2378 |
| volatility_rank_pct | 2040 |
| volatility_20d | 2040 |
| momentum_diff | 2040 |
| relative_return_20d_vs_sector | 2040 |
| relative_return_20d_vs_market | 2040 |
| momentum_20d_rank_pct | 2040 |
| momentum_diff_rank_pct | 2040 |
| return_20d_rank_pct | 2040 |
| sector_relative_rank_pct | 2040 |
| return_20d | 2040 |
| bb_position | 1938 |
| breakout_rank_pct | 1938 |
| high_20d | 1938 |
| close_to_20d_high | 1938 |
| close_to_20d_low | 1938 |
| bb_position_rank_pct | 1938 |
| low_20d | 1938 |
| breakout_strength | 1938 |
| bb_width | 1938 |
| low_rebound_rank_pct | 1938 |
| bb_width_rank_pct | 1938 |
| close_ma20_ratio | 1938 |
| atr_percent | 1326 |
| atr_rank_pct | 1326 |
| momentum_accel | 612 |
| rsi_change_5d | 612 |
| volume_change_1d | 576 |
| volume_change_rank_pct | 576 |
| volatility_5d | 510 |
| relative_return_5d_rank_pct | 510 |
| return_5d_rank_pct | 510 |
| return_5d | 510 |
| relative_return_5d_vs_market | 510 |
| close_ma5_ratio | 408 |
| intraday_range_5d | 408 |
| return_3d | 306 |
| usdkrw_return_1d | 302 |
| nasdaq_return_1d | 302 |
| sox_return_1d | 302 |
| sp500_return_1d | 302 |
| wti_return_1d | 302 |
| vix_change_1d | 302 |
| rsi14 | 102 |
| rsi_rank_pct | 102 |
| return_1d | 102 |

## Target Definitions
- `target_ranking = close(T) / close(T-1) - 1`
- `target_gap = open(T) / close(T-1) - 1`
- `target_intraday = close(T) / open(T) - 1`

No models were trained and no feature formulas were modified.
