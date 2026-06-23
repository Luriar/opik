# Feature Optimization Report

- Input: `data/features/real_features_20230615_20260614.parquet`
- Optimized parquet: `data/features/real_features_optimized.parquet`
- Optimized CSV: `data/features/real_features_optimized.csv`

## Counts
- Original column count: 66
- Original feature count: 64
- Optimized column count: 60
- Optimized feature count: 58

## Preserved / Excluded
- Preserved audit columns: `['date', 'ticker']` -> `True`
- Identity features excluded: `['sector', 'market_type', 'market_cap_group']` -> `True`
- Leakage sanity flags: `[]`

## Removed Features
| Feature | Reason | Detail |
| --- | --- | --- |
| momentum_5d | explicit_duplicate | identical to return_5d/return_20d |
| momentum_20d | explicit_duplicate | identical to return_5d/return_20d |
| sector | identity_excluded | excluded from model feature store for now |
| market_type | identity_excluded | excluded from model feature store for now |
| market_cap_group | identity_excluded | excluded from model feature store for now |
| momentum_rank_pct | rank_duplicate | exact duplicate of preferred rank feature |

## Rank Duplicate Pairs Detected
| Feature A | Feature B | Exact Equal | Correlation | Removed? |
| --- | --- | --- | ---: | --- |
| momentum_rank_pct | return_20d_rank_pct | True | 0.9999999999999998 | `['momentum_rank_pct']` |
| momentum_rank_pct | momentum_20d_rank_pct | True | 0.9999999999999998 | `['momentum_rank_pct']` |
| momentum_rank_pct | sector_relative_rank_pct | True | 0.9999999999999998 | `['momentum_rank_pct']` |
| return_5d_rank_pct | relative_return_5d_rank_pct | True | 1.0 | `[]` |
| return_20d_rank_pct | momentum_20d_rank_pct | True | 0.9999999999999998 | `[]` |
| return_20d_rank_pct | sector_relative_rank_pct | True | 0.9999999999999998 | `[]` |
| momentum_20d_rank_pct | sector_relative_rank_pct | True | 0.9999999999999998 | `[]` |

## All Numeric Duplicate / Near-Duplicate Pairs
| Feature A | Feature B | Exact Equal | Correlation | Rank Pair |
| --- | --- | --- | ---: | --- |
| return_5d | momentum_5d | True | 1.0 | False |
| return_20d | momentum_20d | True | 0.9999999999999998 | False |
| relative_return_20d_vs_market | relative_return_20d_vs_sector | True | 0.9999999999999998 | False |
| momentum_rank_pct | return_20d_rank_pct | True | 0.9999999999999998 | True |
| momentum_rank_pct | momentum_20d_rank_pct | True | 0.9999999999999998 | True |
| momentum_rank_pct | sector_relative_rank_pct | True | 0.9999999999999998 | True |
| return_5d_rank_pct | relative_return_5d_rank_pct | True | 1.0 | True |
| return_20d_rank_pct | momentum_20d_rank_pct | True | 0.9999999999999998 | True |
| return_20d_rank_pct | sector_relative_rank_pct | True | 0.9999999999999998 | True |
| momentum_20d_rank_pct | sector_relative_rank_pct | True | 0.9999999999999998 | True |

## NaN Ratio Summary After Optimization
| Feature | NaN Ratio | NaN Count |
| --- | ---: | ---: |
| return_60d | 0.084627 | 6222 |
| close_ma60_ratio | 0.083239 | 6120 |
| bb_position_change_5d | 0.034683 | 2550 |
| relative_trading_value | 0.033731 | 2480 |
| trading_value_rank_pct | 0.033731 | 2480 |
| momentum_diff | 0.029134 | 2142 |
| relative_return_20d_vs_market | 0.029134 | 2142 |
| relative_return_20d_vs_sector | 0.029134 | 2142 |
| volatility_20d | 0.029134 | 2142 |
| return_20d | 0.029134 | 2142 |
| momentum_20d_rank_pct | 0.029134 | 2142 |
| return_20d_rank_pct | 0.029134 | 2142 |
| momentum_diff_rank_pct | 0.029134 | 2142 |
| sector_relative_rank_pct | 0.029134 | 2142 |
| volatility_rank_pct | 0.029134 | 2142 |
| close_ma20_ratio | 0.027746 | 2040 |
| breakout_strength | 0.027746 | 2040 |
| bb_width | 0.027746 | 2040 |
| low_20d | 0.027746 | 2040 |
| close_to_20d_high | 0.027746 | 2040 |
| high_20d | 0.027746 | 2040 |
| breakout_rank_pct | 0.027746 | 2040 |
| low_rebound_rank_pct | 0.027746 | 2040 |
| bb_width_rank_pct | 0.027746 | 2040 |
| bb_position | 0.027746 | 2040 |
| bb_position_rank_pct | 0.027746 | 2040 |
| close_to_20d_low | 0.027746 | 2040 |
| atr_rank_pct | 0.019422 | 1428 |
| atr_percent | 0.019422 | 1428 |
| rsi_change_5d | 0.009711 | 714 |
