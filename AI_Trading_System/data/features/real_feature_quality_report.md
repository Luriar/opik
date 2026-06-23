# Real Feature Store Quality Report

- Shape: `(73523, 66)`
- Feature count: `64`
- Unique ticker count: `102`
- Date range: `2023-06-15` to `2026-06-12`
- Ticker 005930 exists: `True`
- Target columns excluded: `True`
- First 10 columns: `['date', 'ticker', 'trading_value', 'return_1d', 'return_3d', 'return_5d', 'return_20d', 'return_60d', 'close_ma5_ratio', 'close_ma20_ratio']`

## NaN Ratio By Column
| Column | NaN Ratio |
| --- | ---: |
| return_60d | 0.084627 |
| close_ma60_ratio | 0.083239 |
| bb_position_change_5d | 0.034683 |
| relative_trading_value | 0.033731 |
| trading_value_rank_pct | 0.033731 |
| momentum_rank_pct | 0.029134 |
| sector_relative_rank_pct | 0.029134 |
| relative_return_20d_vs_sector | 0.029134 |
| momentum_20d | 0.029134 |
| momentum_diff | 0.029134 |
| volatility_20d | 0.029134 |
| momentum_diff_rank_pct | 0.029134 |
| relative_return_20d_vs_market | 0.029134 |
| volatility_rank_pct | 0.029134 |
| momentum_20d_rank_pct | 0.029134 |
| return_20d | 0.029134 |
| return_20d_rank_pct | 0.029134 |
| close_to_20d_low | 0.027746 |
| low_20d | 0.027746 |
| high_20d | 0.027746 |
| breakout_rank_pct | 0.027746 |
| close_to_20d_high | 0.027746 |
| low_rebound_rank_pct | 0.027746 |
| breakout_strength | 0.027746 |
| bb_position_rank_pct | 0.027746 |
| bb_width | 0.027746 |
| close_ma20_ratio | 0.027746 |
| bb_width_rank_pct | 0.027746 |
| bb_position | 0.027746 |
| atr_rank_pct | 0.019422 |
| atr_percent | 0.019422 |
| momentum_accel | 0.009711 |
| rsi_change_5d | 0.009711 |
| volume_change_rank_pct | 0.009222 |
| volume_change_1d | 0.009222 |
| return_5d | 0.008324 |
| momentum_5d | 0.008324 |
| return_5d_rank_pct | 0.008324 |
| volatility_5d | 0.008324 |
| relative_return_5d_vs_market | 0.008324 |
| relative_return_5d_rank_pct | 0.008324 |
| intraday_range_5d | 0.006937 |
| close_ma5_ratio | 0.006937 |
| return_3d | 0.005549 |
| wti_return_1d | 0.005495 |
| nasdaq_return_1d | 0.005495 |
| sox_return_1d | 0.005495 |
| sp500_return_1d | 0.005495 |
| vix_change_1d | 0.005495 |
| usdkrw_return_1d | 0.005495 |
| return_1d | 0.002775 |
| rsi_rank_pct | 0.002775 |
| rsi14 | 0.002775 |
| body_ratio | 0.001387 |
| macd_rank_pct | 0.001387 |
| lower_shadow | 0.001387 |
| body | 0.001387 |
| close_position | 0.001387 |
| upper_shadow | 0.001387 |
| macd_hist_ratio | 0.001387 |
| ticker | 0.000000 |
| date | 0.000000 |
| trading_value | 0.000000 |
| sector | 0.000000 |
| market_type | 0.000000 |
| market_cap_group | 0.000000 |

## Leakage Note
Features were generated only through the existing Phase 2 FeatureBuilder.
No feature formulas were changed in this script.
