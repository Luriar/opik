# Samsung Rank #1 Post-SOX Repair Audit

## Scope

This audit uses saved artifacts only. It does not retrain models, rerun the production prediction pipeline, mutate prediction outputs, or modify archives. For the pre-repair comparison, it uses the timestamped pre-backfill feature-store backup and computes ranking scores in memory with the saved ranking model.

| Item | Value |
|---|---|
| Prediction date | `2026-06-23` |
| Feature date | `2026-06-22` |
| Prediction artifact | `outputs\daily_predictions\predictions_20260623.parquet` |
| Top10 artifact | `reports\daily\top10_20260623.csv` |
| Ranking model | `outputs\daily_models\20260623\ranking_model.txt` |
| Feature source check | `PASS` |
| SOX actual date | `2026-06-22` |
| SOX return non-null count | `348` |
| Prediction SHA256 after repair | `f76a6deb5e2e0cf10deca4eed37ab602b7624b6aa4a36c6dfbe177be21b34031` |
| Previous audit prediction SHA256 before repair | `eec64a4950ea17e6b8cd2f89f816b8741bbaf7bb1ea8404c245ad80839289cca` |

## 1. Latest Production Prediction

| Metric | Samsung Electro-Mechanics |
|---|---|
| Ticker | `009150` |
| AI Rank | `1 / 348` |
| AI Score | 100.0 |
| ranking_score | 0.0302867802 |
| Expected return | 2.70% |
| Predicted gap | 0.27% |
| Predicted intraday | 2.42% |
| Previous close | 2,228,000 |

Samsung remains rank #1 in the post-repair production prediction. The post-repair `ranking_score` is lower than before repair, but the rank is unchanged.

## 2. Before vs After SOX Repair

| Metric | Pre-SOX repair | Post-SOX repair | Change |
|---|---|---|---|
| Samsung AI Rank | `1 / 348` | `1 / 348` | +0 |
| Samsung AI Score | 100.0 | 100.0 | +0.0 |
| Samsung ranking_score | 0.0396728535 | 0.0302867802 | -0.0093860733 |
| Top10 overlap | 4/10 | 4/10 | 40% |

### Top10 Rank Movement

| Ticker | Pre rank | Post rank | Movement | Pre score | Post score |
|---|---|---|---|---|---|
| `009150` | 1 | 1 | +0 | 0.039673 | 0.030287 |
| `347850` | 2 | 3 | -1 | 0.023711 | 0.015802 |
| `402340` | 3 | 2 | +1 | 0.022007 | 0.017799 |
| `084370` | 4 | 4 | +0 | 0.020342 | 0.013673 |
| `080220` | 20 | 5 | +15 | 0.011879 | 0.012897 |
| `319660` | 5 | 36 | -31 | 0.019799 | 0.010291 |
| `290650` | 6 | 30 | -24 | 0.019429 | 0.010555 |
| `000660` | 12 | 6 | +6 | 0.016007 | 0.012821 |
| `069960` | 7 | 16 | -9 | 0.018166 | 0.011171 |
| `348210` | 44 | 7 | +37 | 0.008545 | 0.012491 |
| `089970` | 8 | 40 | -32 | 0.017997 | 0.009981 |
| `032820` | 24 | 8 | +16 | 0.010924 | 0.012332 |
| `131970` | 52 | 9 | +43 | 0.007989 | 0.011971 |
| `095610` | 9 | 43 | -34 | 0.017808 | 0.009885 |
| `240810` | 10 | 48 | -38 | 0.017389 | 0.009556 |
| `310210` | 22 | 10 | +12 | 0.011213 | 0.011850 |

The SOX repair materially changed scores and some Top10 ordering: rank 2 and rank 3 swapped, and several names entered or exited the Top10. It did not materially change Samsung Electro-Mechanics rank: Samsung stayed #1 with a large gap to rank 2.

## 3. Feature Dominance Analysis

Cells show `raw value / universe percentile` using the post-repair feature row. Percentiles are descriptive cross-sectional ranks across the 348-stock prediction universe; they are not signed model contributions.

### Momentum

| Feature | R1 `009150` | R2 `402340` | R3 `347850` | R5 `080220` | R10 `310210` | R20 `131290` |
|---|---|---|---|---|---|---|
| `return_1d` | 0.031818 / 95.4% | 0.047059 / 96.8% | 0.026777 / 94.3% | -0.028887 / 46.3% | -0.037531 / 33.6% | -0.020080 / 58.0% |
| `return_3d` | 0.108398 / 98.3% | 0.185876 / 99.4% | 0.213974 / 99.7% | 0.129447 / 98.6% | -0.025013 / 80.5% | -0.027888 / 78.7% |
| `return_5d` | 0.257618 / 98.3% | 0.449511 / 100.0% | 0.177966 / 96.3% | 0.178351 / 96.6% | -0.058454 / 22.7% | -0.061538 / 21.3% |
| `return_20d` | 1.299899 / 100.0% | 0.745098 / 99.1% | 0.822951 / 99.7% | 0.191867 / 92.0% | -0.231164 / 6.6% | 0.161905 / 91.4% |
| `return_60d` | 3.886975 / 100.0% | 1.927632 / 99.4% | 0.423816 / 91.1% | 1.580135 / 98.6% | -0.399384 / 4.9% | 1.003284 / 96.6% |
| `momentum_diff` | -1.042281 / 0.3% | -0.295587 / 5.5% | -0.644985 / 1.4% | -0.013516 / 21.6% | 0.172710 / 87.6% | -0.223443 / 8.3% |
| `momentum_accel` | 0.038781 / 96.6% | 0.012487 / 91.4% | -0.068294 / 34.5% | -0.080473 / 27.0% | -0.081181 / 26.4% | -0.261538 / 0.9% |
| `return_5d_rank_pct` | 0.982759 / 98.3% | 1.000000 / 100.0% | 0.962644 / 96.3% | 0.965517 / 96.6% | 0.227011 / 22.7% | 0.212644 / 21.3% |
| `return_20d_rank_pct` | 1.000000 / 100.0% | 0.991379 / 99.1% | 0.997126 / 99.7% | 0.919540 / 92.0% | 0.066092 / 6.6% | 0.913793 / 91.4% |
| `momentum_diff_rank_pct` | 0.002874 / 0.3% | 0.054598 / 5.5% | 0.014368 / 1.4% | 0.215517 / 21.6% | 0.876437 / 87.6% | 0.083333 / 8.3% |

### Trend

| Feature | R1 `009150` | R2 `402340` | R3 `347850` | R5 `080220` | R10 `310210` | R20 `131290` |
|---|---|---|---|---|---|---|
| `close_ma5_ratio` | 0.105807 / 99.1% | 0.121613 / 99.7% | 0.097296 / 98.6% | 0.044408 / 96.8% | -0.031120 / 59.8% | -0.037096 / 47.4% |
| `close_ma20_ratio` | 0.275926 / 99.1% | 0.354952 / 100.0% | 0.252393 / 98.9% | 0.090285 / 94.5% | -0.153216 / 6.6% | 0.081297 / 94.0% |
| `close_ma60_ratio` | 1.152951 / 100.0% | 0.889028 / 99.7% | 0.413559 / 96.6% | 0.670483 / 98.9% | -0.281731 / 5.2% | 0.400220 / 96.0% |
| `bb_position` | 0.888440 / 98.9% | 1.105174 / 100.0% | 0.876859 / 98.3% | 0.763144 / 95.4% | 0.260252 / 38.2% | 0.690849 / 90.5% |
| `bb_position_change_5d` | 0.206228 / 84.8% | 0.516658 / 98.9% | 0.129089 / 73.9% | 0.276034 / 89.7% | 0.041496 / 55.5% | -0.402586 / 8.9% |
| `bb_position_rank_pct` | 0.988506 / 98.9% | 1.000000 / 100.0% | 0.982759 / 98.3% | 0.954023 / 95.4% | 0.382184 / 38.2% | 0.905172 / 90.5% |

### Relative Strength

| Feature | R1 `009150` | R2 `402340` | R3 `347850` | R5 `080220` | R10 `310210` | R20 `131290` |
|---|---|---|---|---|---|---|
| `relative_return_5d_vs_market` | 0.266521 / 98.3% | 0.458415 / 100.0% | 0.186869 / 96.3% | 0.187254 / 96.6% | -0.049551 / 22.7% | -0.052635 / 21.3% |
| `relative_return_20d_vs_market` | 1.344658 / 100.0% | 0.789858 / 99.1% | 0.867710 / 99.7% | 0.236626 / 92.0% | -0.186404 / 6.6% | 0.206664 / 91.4% |
| `relative_return_20d_vs_sector` | 1.344658 / 100.0% | 0.789858 / 99.1% | 0.867710 / 99.7% | 0.236626 / 92.0% | -0.186404 / 6.6% | 0.206664 / 91.4% |

### Liquidity

| Feature | R1 `009150` | R2 `402340` | R3 `347850` | R5 `080220` | R10 `310210` | R20 `131290` |
|---|---|---|---|---|---|---|
| `trading_value` | 1925.063B / 99.1% | 2790.820B / 99.4% | 139.586B / 90.8% | 1467.715B / 98.9% | 20.738B / 58.6% | 30.343B / 66.7% |
| `relative_trading_value` | 1.346202 / 86.8% | 1.951738 / 98.3% | 1.317164 / 85.6% | 2.037874 / 98.9% | 0.770327 / 37.1% | 0.866620 / 47.4% |
| `trading_value_rank_pct` | 0.867816 / 86.8% | 0.982759 / 98.3% | 0.856322 / 85.6% | 0.988506 / 98.9% | 0.370690 / 37.1% | 0.474138 / 47.4% |
| `volume_change_1d` | 0.066192 / 24.7% | 0.288074 / 44.8% | 0.357308 / 50.3% | 0.026235 / 21.3% | 0.877963 / 80.2% | 0.656716 / 71.0% |
| `volume_change_rank_pct` | 0.247126 / 24.7% | 0.448276 / 44.8% | 0.502874 / 50.3% | 0.212644 / 21.3% | 0.801724 / 80.2% | 0.709770 / 71.0% |

### Volatility

| Feature | R1 `009150` | R2 `402340` | R3 `347850` | R5 `080220` | R10 `310210` | R20 `131290` |
|---|---|---|---|---|---|---|
| `volatility_5d` | 0.094617 / 92.2% | 0.026730 / 18.7% | 0.092254 / 92.0% | 0.054104 / 68.7% | 0.061837 / 77.3% | 0.041069 / 47.1% |
| `volatility_20d` | 0.095411 / 92.5% | 0.069195 / 78.2% | 0.111988 / 98.0% | 0.083140 / 87.1% | 0.066291 / 76.7% | 0.089650 / 90.8% |
| `intraday_range_5d` | 0.100068 / 88.2% | 0.076025 / 66.1% | 0.132242 / 98.6% | 0.131169 / 98.0% | 0.112092 / 93.1% | 0.078301 / 68.1% |
| `atr_percent` | 0.105381 / 76.7% | 0.077287 / 50.6% | 0.119925 / 87.1% | 0.124734 / 88.8% | 0.129993 / 91.4% | 0.120960 / 87.4% |
| `volatility_rank_pct` | 0.925287 / 92.5% | 0.781609 / 78.2% | 0.979885 / 98.0% | 0.870690 / 87.1% | 0.767241 / 76.7% | 0.908046 / 90.8% |
| `atr_rank_pct` | 0.767241 / 76.7% | 0.505747 / 50.6% | 0.870690 / 87.1% | 0.887931 / 88.8% | 0.913793 / 91.4% | 0.873563 / 87.4% |
| `bb_width` | 0.710344 / 96.6% | 0.586528 / 92.8% | 0.669729 / 95.7% | 0.343100 / 61.8% | 0.639071 / 94.5% | 0.425973 / 80.7% |
| `bb_width_rank_pct` | 0.965517 / 96.6% | 0.928161 / 92.8% | 0.956897 / 95.7% | 0.617816 / 61.8% | 0.945402 / 94.5% | 0.807471 / 80.7% |

### Candlestick

| Feature | R1 `009150` | R2 `402340` | R3 `347850` | R5 `080220` | R10 `310210` | R20 `131290` |
|---|---|---|---|---|---|---|
| `body` | -0.004386 / 83.0% | -0.021440 / 61.5% | 0.043152 / 98.3% | -0.066176 / 10.1% | -0.025500 / 55.7% | -0.043137 / 28.7% |
| `upper_shadow` | 0.060088 / 97.4% | 0.039582 / 91.4% | 0.043152 / 92.8% | 0.054739 / 97.1% | 0.037500 / 90.2% | 0.013725 / 63.2% |
| `lower_shadow` | 0.044298 / 96.0% | 0.030786 / 74.4% | 0.031895 / 77.3% | 0.027778 / 65.2% | 0.035000 / 85.1% | 0.027451 / 64.7% |
| `body_ratio` | 0.040323 / 4.9% | 0.233533 / 20.4% | 0.365079 / 37.9% | 0.445055 / 48.3% | 0.260204 / 23.9% | 0.511628 / 60.1% |
| `close_position` | 0.407258 / 60.3% | 0.335329 / 42.8% | 0.634921 / 88.8% | 0.186813 / 12.6% | 0.357143 / 48.3% | 0.325581 / 40.8% |

### Breakout

| Feature | R1 `009150` | R2 `402340` | R3 `347850` | R5 `080220` | R10 `310210` | R20 `131290` |
|---|---|---|---|---|---|---|
| `high_20d` | 2,417,000 / 99.1% | 1,891,000 / 98.9% | 116,600.000 / 60.3% | 137,100.000 / 64.7% | 299,000.000 / 84.5% | 282,500.000 / 83.3% |
| `low_20d` | 924,000.000 / 97.7% | 991,000.000 / 98.3% | 57,100.000 / 50.0% | 81,800.000 / 60.2% | 169,500.000 / 79.6% | 173,700.000 / 81.0% |
| `close_to_20d_high` | -0.060819 / 96.6% | -0.058699 / 97.7% | -0.046312 / 99.1% | -0.166302 / 56.0% | -0.348161 / 7.8% | -0.136283 / 67.5% |
| `close_to_20d_low` | 1.456710 / 100.0% | 0.796165 / 98.9% | 0.947461 / 99.7% | 0.397311 / 94.3% | 0.149853 / 71.6% | 0.404721 / 94.5% |
| `breakout_strength` | 0.901541 / 99.4% | 0.876667 / 98.9% | 0.909244 / 99.7% | 0.587703 / 88.2% | 0.196139 / 33.9% | 0.646140 / 92.5% |
| `breakout_rank_pct` | 0.994253 / 99.4% | 0.988506 / 98.9% | 0.997126 / 99.7% | 0.882184 / 88.2% | 0.339080 / 33.9% | 0.925287 / 92.5% |
| `low_rebound_rank_pct` | 1.000000 / 100.0% | 0.988506 / 98.9% | 0.997126 / 99.7% | 0.942529 / 94.3% | 0.715517 / 71.6% | 0.945402 / 94.5% |

### Technical Indicators

| Feature | R1 `009150` | R2 `402340` | R3 `347850` | R5 `080220` | R10 `310210` | R20 `131290` |
|---|---|---|---|---|---|---|
| `rsi14` | 71.759736 / 99.1% | 75.894994 / 100.0% | 64.484026 / 97.7% | 64.031950 / 97.1% | 37.448979 / 25.9% | 58.510406 / 95.4% |
| `rsi_change_5d` | 8.122694 / 87.4% | 19.383301 / 99.7% | 6.706149 / 81.3% | 7.484610 / 85.3% | 0.338072 / 52.9% | -7.162074 / 15.2% |
| `macd_hist_ratio` | 0.007104 / 71.8% | 0.022363 / 97.7% | 0.021031 / 97.4% | -0.002627 / 29.6% | -0.004935 / 23.6% | 0.006050 / 66.1% |
| `rsi_rank_pct` | 0.991379 / 99.1% | 1.000000 / 100.0% | 0.977011 / 97.7% | 0.971264 / 97.1% | 0.258621 / 25.9% | 0.954023 / 95.4% |
| `macd_rank_pct` | 0.718391 / 71.8% | 0.977011 / 97.7% | 0.974138 / 97.4% | 0.295977 / 29.6% | 0.235632 / 23.6% | 0.660920 / 66.1% |

### Macro

| Feature | R1 `009150` | R2 `402340` | R3 `347850` | R5 `080220` | R10 `310210` | R20 `131290` |
|---|---|---|---|---|---|---|
| `nasdaq_return_1d` | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% |
| `sox_return_1d` | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% |
| `sp500_return_1d` | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% |
| `vix_change_1d` | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% |
| `usdkrw_return_1d` | 0.007958 / 50.0% | 0.007958 / 50.0% | 0.007958 / 50.0% | 0.007958 / 50.0% | 0.007958 / 50.0% | 0.007958 / 50.0% |
| `wti_return_1d` | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% | 0.000000 / 50.0% |

## 4. Extreme-Value Investigation

### Samsung Features At Or Above 99th Percentile

| Feature | Value | Percentile |
|---|---|---|
| `close_ma60_ratio` | 1.152951 | 100.0% |
| `close_to_20d_low` | 1.456710 | 100.0% |
| `low_rebound_rank_pct` | 1.000000 | 100.0% |
| `relative_return_20d_vs_market` | 1.344658 | 100.0% |
| `relative_return_20d_vs_sector` | 1.344658 | 100.0% |
| `return_20d` | 1.299899 | 100.0% |
| `return_20d_rank_pct` | 1.000000 | 100.0% |
| `return_60d` | 3.886975 | 100.0% |
| `breakout_rank_pct` | 0.994253 | 99.4% |
| `breakout_strength` | 0.901541 | 99.4% |
| `close_ma20_ratio` | 0.275926 | 99.1% |
| `close_ma5_ratio` | 0.105807 | 99.1% |
| `high_20d` | 2,417,000 | 99.1% |
| `rsi14` | 71.759736 | 99.1% |
| `rsi_rank_pct` | 0.991379 | 99.1% |
| `trading_value` | 1925.063B | 99.1% |

### Samsung Features At Or Above 95th Percentile

| Feature | Value | Percentile |
|---|---|---|
| `close_ma60_ratio` | 1.152951 | 100.0% |
| `close_to_20d_low` | 1.456710 | 100.0% |
| `low_rebound_rank_pct` | 1.000000 | 100.0% |
| `relative_return_20d_vs_market` | 1.344658 | 100.0% |
| `relative_return_20d_vs_sector` | 1.344658 | 100.0% |
| `return_20d` | 1.299899 | 100.0% |
| `return_20d_rank_pct` | 1.000000 | 100.0% |
| `return_60d` | 3.886975 | 100.0% |
| `breakout_rank_pct` | 0.994253 | 99.4% |
| `breakout_strength` | 0.901541 | 99.4% |
| `close_ma20_ratio` | 0.275926 | 99.1% |
| `close_ma5_ratio` | 0.105807 | 99.1% |
| `high_20d` | 2,417,000 | 99.1% |
| `rsi14` | 71.759736 | 99.1% |
| `rsi_rank_pct` | 0.991379 | 99.1% |
| `trading_value` | 1925.063B | 99.1% |
| `bb_position` | 0.888440 | 98.9% |
| `bb_position_rank_pct` | 0.988506 | 98.9% |
| `relative_return_5d_vs_market` | 0.266521 | 98.3% |
| `return_3d` | 0.108398 | 98.3% |
| `return_5d` | 0.257618 | 98.3% |
| `return_5d_rank_pct` | 0.982759 | 98.3% |
| `low_20d` | 924,000.000 | 97.7% |
| `upper_shadow` | 0.060088 | 97.4% |
| `bb_width` | 0.710344 | 96.6% |
| `bb_width_rank_pct` | 0.965517 | 96.6% |
| `close_to_20d_high` | -0.060819 | 96.6% |
| `momentum_accel` | 0.038781 | 96.6% |
| `lower_shadow` | 0.044298 | 96.0% |
| `return_1d` | 0.031818 | 95.4% |

### Samsung Features At Or Below 5th Percentile

| Feature | Value | Percentile |
|---|---|---|
| `momentum_diff` | -1.042281 | 0.3% |
| `momentum_diff_rank_pct` | 0.002874 | 0.3% |
| `body_ratio` | 0.040323 | 4.9% |

Extreme momentum, trend, breakout, and liquidity features explain most of the rank. The most important caution is that the same row is also extremely extended: `momentum_diff` is bottom-tail because the 60-day move is far larger than the 20-day move, and RSI/volatility are elevated.

## 5. Model Importance Alignment

### Top 20 By Split Importance

| Rank | Feature | Splits | Split % | Samsung percentile |
|---|---|---|---|---|
| 1 | `sox_return_1d` | 738 | 12.30% | 50.0% |
| 2 | `usdkrw_return_1d` | 685 | 11.42% | 50.0% |
| 3 | `vix_change_1d` | 505 | 8.42% | 50.0% |
| 4 | `wti_return_1d` | 498 | 8.30% | 50.0% |
| 5 | `nasdaq_return_1d` | 490 | 8.17% | 50.0% |
| 6 | `sp500_return_1d` | 448 | 7.47% | 50.0% |
| 7 | `atr_percent` | 194 | 3.23% | 76.7% |
| 8 | `intraday_range_5d` | 155 | 2.58% | 88.2% |
| 9 | `volatility_20d` | 119 | 1.98% | 92.5% |
| 10 | `atr_rank_pct` | 108 | 1.80% | 76.7% |
| 10 | `trading_value` | 108 | 1.80% | 99.1% |
| 12 | `lower_shadow` | 93 | 1.55% | 96.0% |
| 13 | `return_1d` | 90 | 1.50% | 95.4% |
| 14 | `volatility_rank_pct` | 88 | 1.47% | 92.5% |
| 15 | `return_60d` | 85 | 1.42% | 100.0% |
| 16 | `upper_shadow` | 81 | 1.35% | 97.4% |
| 17 | `close_ma5_ratio` | 77 | 1.28% | 99.1% |
| 17 | `return_3d` | 77 | 1.28% | 98.3% |
| 19 | `macd_hist_ratio` | 72 | 1.20% | 71.8% |
| 20 | `volatility_5d` | 69 | 1.15% | 92.2% |

### Top 20 By Gain Importance

| Rank | Feature | Gain | Gain % | Samsung percentile |
|---|---|---|---|---|
| 1 | `sox_return_1d` | 125.3637 | 17.97% | 50.0% |
| 2 | `usdkrw_return_1d` | 110.7699 | 15.88% | 50.0% |
| 3 | `nasdaq_return_1d` | 90.2633 | 12.94% | 50.0% |
| 4 | `wti_return_1d` | 89.5527 | 12.84% | 50.0% |
| 5 | `sp500_return_1d` | 77.9454 | 11.17% | 50.0% |
| 6 | `vix_change_1d` | 62.7041 | 8.99% | 50.0% |
| 7 | `atr_percent` | 18.6890 | 2.68% | 76.7% |
| 8 | `intraday_range_5d` | 13.7196 | 1.97% | 88.2% |
| 9 | `volatility_20d` | 8.6177 | 1.24% | 92.5% |
| 10 | `atr_rank_pct` | 6.1067 | 0.88% | 76.7% |
| 11 | `close_ma5_ratio` | 6.0204 | 0.86% | 99.1% |
| 12 | `return_3d` | 5.0454 | 0.72% | 98.3% |
| 13 | `return_1d` | 4.9814 | 0.71% | 95.4% |
| 14 | `upper_shadow` | 4.6928 | 0.67% | 97.4% |
| 15 | `volatility_rank_pct` | 4.2549 | 0.61% | 92.5% |
| 16 | `return_60d` | 4.2485 | 0.61% | 100.0% |
| 17 | `trading_value` | 4.1713 | 0.60% | 99.1% |
| 18 | `macd_hist_ratio` | 4.1614 | 0.60% | 71.8% |
| 19 | `lower_shadow` | 3.7371 | 0.54% | 96.0% |
| 20 | `volatility_5d` | 3.3013 | 0.47% | 92.2% |

### Samsung Alignment With Important Features

| Feature | Split rank | Gain rank | Samsung value | Samsung percentile |
|---|---|---|---|---|
| `sox_return_1d` | 1 | 1 | 0.000000 | 50.0% |
| `usdkrw_return_1d` | 2 | 2 | 0.007958 | 50.0% |
| `vix_change_1d` | 3 | 6 | 0.000000 | 50.0% |
| `wti_return_1d` | 4 | 4 | 0.000000 | 50.0% |
| `nasdaq_return_1d` | 5 | 3 | 0.000000 | 50.0% |
| `sp500_return_1d` | 6 | 5 | 0.000000 | 50.0% |
| `atr_percent` | 7 | 7 | 0.105381 | 76.7% |
| `intraday_range_5d` | 8 | 8 | 0.100068 | 88.2% |
| `volatility_20d` | 9 | 9 | 0.095411 | 92.5% |
| `atr_rank_pct` | 10 | 10 | 0.767241 | 76.7% |
| `trading_value` | 10 | 17 | 1925.063B | 99.1% |
| `lower_shadow` | 12 | 19 | 0.044298 | 96.0% |
| `return_1d` | 13 | 13 | 0.031818 | 95.4% |
| `volatility_rank_pct` | 14 | 15 | 0.925287 | 92.5% |
| `return_60d` | 15 | 16 | 3.886975 | 100.0% |
| `upper_shadow` | 16 | 14 | 0.060088 | 97.4% |
| `close_ma5_ratio` | 17 | 11 | 0.105807 | 99.1% |
| `return_3d` | 17 | 12 | 0.108398 | 98.3% |
| `macd_hist_ratio` | 19 | 18 | 0.007104 | 71.8% |
| `volatility_5d` | 20 | 20 | 0.094617 | 92.2% |

Samsung is strong on several of the highest-importance stock-varying features: `trading_value`, `return_60d`, `return_3d`, `close_ma5_ratio`, `return_1d`, `upper_shadow`, `lower_shadow`, and volatility rank. Macro features are important globally but are identical across the universe after repair, so they mainly define the daily market-regime baseline rather than Samsung-specific dominance.

## 6. SHAP-Style Contribution Audit

LightGBM `pred_contrib=True` was available, so this section uses model-native per-feature contribution values for Samsung. Contributions are in model score space and sum with the bias term to the predicted `ranking_score`.

### Top Positive Contributors

| Feature | Contribution | Samsung percentile | Split rank | Gain rank |
|---|---|---|---|---|
| `trading_value` | 0.00491811 | 99.1% | 10 | 17 |
| `close_ma60_ratio` | 0.00441966 | 100.0% | 28 | 29 |
| `relative_return_20d_vs_market` | 0.00405577 | 100.0% | 49 | 47 |
| `usdkrw_return_1d` | 0.00372348 | 50.0% | 2 | 2 |
| `momentum_diff_rank_pct` | 0.00223959 | 0.3% | 40 | 35 |
| `return_20d_rank_pct` | 0.00156740 | 100.0% | 30 | 32 |
| `atr_percent` | 0.00130088 | 76.7% | 7 | 7 |
| `lower_shadow` | 0.00123559 | 96.0% | 12 | 19 |
| `bb_position_change_5d` | 0.00101434 | 84.8% | 27 | 30 |
| `intraday_range_5d` | 0.00082773 | 88.2% | 8 | 8 |
| `rsi_rank_pct` | 0.00080256 | 99.1% | 37 | 40 |
| `return_5d` | 0.00078412 | 98.3% | 22 | 22 |
| `close_to_20d_low` | 0.00065327 | 100.0% | 41 | 38 |
| `momentum_accel` | 0.00050773 | 96.6% | 23 | 26 |
| `wti_return_1d` | 0.00046370 | 50.0% | 4 | 4 |

### Top Negative Contributors

| Feature | Contribution | Samsung percentile | Split rank | Gain rank |
|---|---|---|---|---|
| `sp500_return_1d` | -0.00165455 | 50.0% | 6 | 5 |
| `high_20d` | -0.00118848 | 99.1% | 26 | 27 |
| `sox_return_1d` | -0.00113435 | 50.0% | 1 | 1 |
| `return_3d` | -0.00071837 | 98.3% | 17 | 12 |
| `return_20d` | -0.00060140 | 100.0% | 35 | 31 |
| `volume_change_1d` | -0.00033331 | 24.7% | 37 | 39 |
| `close_ma5_ratio` | -0.00006997 | 99.1% | 17 | 11 |
| `trading_value_rank_pct` | -0.00003724 | 86.8% | 44 | 44 |
| `body_ratio` | -0.00002734 | 4.9% | 49 | 50 |
| `breakout_rank_pct` | -0.00000142 | 99.4% | 51 | 48 |
| `vix_change_1d` | 0.00000424 | 50.0% | 3 | 6 |
| `momentum_diff` | 0.00000572 | 0.3% | 20 | 21 |
| `close_ma20_ratio` | 0.00000605 | 99.1% | 29 | 25 |
| `return_5d_rank_pct` | 0.00000792 | 98.3% | 47 | 51 |
| `bb_position` | 0.00002038 | 98.9% | 54 | 53 |

The contribution audit supports the feature-percentile story: the largest positive stock-specific contributors are the extreme return/trend/breakout/liquidity profile. Negative contributors mostly come from overextension or weaker sub-signals such as MACD/risk-profile branches.

## 7. Corporate-Action Sanity Check

| Check | Observation |
|---|---|
| Local price continuity | No single daily close return above 30% was found in the inspected February-June 2026 local OHLCV slice. |
| Selected closes | `2026-03-31: 407,500`, `2026-04-21: 772,000`, `2026-05-12: 958,000`, `2026-05-29: 2,127,000`, `2026-06-16: 2,048,000`, `2026-06-22: 2,228,000` |
| return_20d | `1.299899` |
| return_60d | `3.886975` |
| close_ma20_ratio | `0.275926` |
| close_ma60_ratio | `1.152951` |
| breakout_strength | `0.901541` |

The local series is internally continuous, so this audit did not find a clear split/reverse-split jump, merger discontinuity, or one-day adjusted-price break inside the inspected window. However, the economic magnitude is very unusual: the stock roughly quintupled from late March to June in the local data. That makes the rank signal real relative to the stored dataset, but sensitive to whether the OHLCV vendor series itself is economically correct. The audit therefore classifies the corporate-action check as `no local discontinuity found, but external corporate-action verification still warranted`.

## 8. Rank Robustness

| Conceptual exclusion | Would Samsung likely remain Top10? | Reason |
|---|---|---|
| Macro | Almost certainly | Post-repair macro features are common across all stocks on the prediction date, so removing them would mostly alter a market-regime baseline rather than Samsung-specific cross-sectional strength. |
| Breakout | Likely | Samsung has enough independent momentum, trend, relative strength, and liquidity strength to remain highly ranked, although its #1 separation would likely shrink. |
| Relative Strength | Likely | The strongest relative-strength variables overlap with raw momentum/trend. Removing them would reduce support, but Samsung still has universe-leading 20/60-day return, high trading value, and strong breakout/trend features. |

This is a conceptual robustness assessment, not a counterfactual production prediction. No model outputs were regenerated or saved.

## 9. Plain-Language Explanation

Samsung Electro-Mechanics ranked #1 after the SOX repair because it simultaneously shows extreme 20-day and 60-day momentum, universe-leading relative strength, top-percentile trend distance from moving averages, near-maximum breakout/rebound characteristics, and very high trading value. The SOX repair lowered its model score but did not remove the core stock-specific signals that made the row stand out.

The uncomfortable part is that those same signals are extremely extended. The rank is not caused by SOX still being missing; SOX is now present and the feature-source check passed. But the rank depends heavily on an unusually explosive local OHLCV price path, so the right interpretation is "dominant according to the repaired production dataset," not "guaranteed investment-quality signal."

## 10. Final Conclusion

### A. Is Samsung Electro-Mechanics Rank #1 still justified after SOX repair?

**Yes, model-relative justification remains strong.** Samsung is still rank #1 after SOX is present, non-null, and validated. The post-repair ranking score is clearly above rank 2, and the strongest stock-specific feature families remain dominant.

### B. Did SOX repair materially affect its ranking?

**It materially affected scores, but not Samsung's rank.** Samsung stayed rank #1. Its in-memory comparable score moved from `0.0396728535` pre-repair to `0.0302867802` post-repair, while the Top10 overlap was `4/10`.

### C. Which 5 features most strongly explain Rank #1?

| Feature | Samsung value | Samsung percentile | Why it matters |
|---|---|---|---|
| `return_60d` | 3.886975 | 100.0% | Captures the extreme medium-term price move. |
| `return_20d` | 1.299899 | 100.0% | Shows universe-leading recent momentum. |
| `relative_return_20d_vs_market` | 1.344658 | 100.0% | Confirms the move is strong relative to the market. |
| `close_ma60_ratio` | 1.152951 | 100.0% | Measures the stock trading far above its 60-day trend. |
| `breakout_strength` | 0.901541 | 99.4% | Captures near-high breakout/rebound behavior. |

### D. Is Rank #1 driven by genuine signals or data artifacts?

**Primarily genuine signals within the repaired production dataset, with a meaningful data-quality caveat.** The SOX issue is repaired and no longer explains the rank. The feature row contains broad, internally consistent dominance across momentum, trend, relative strength, breakout, and liquidity. The caveat is that the stored OHLCV path is exceptionally large in economic terms; no local discontinuity was found, but external corporate-action/source validation would be prudent before treating this as a high-confidence trading conclusion.

### E. Confidence Level

**Medium.** Confidence is high that the repaired model and stored features rank Samsung #1 for transparent, stock-specific reasons. Confidence is reduced by the extreme price path, high volatility/overbought profile, and the broader known weakness of ranking-model statistical validation from prior walk-forward reports.
