# Samsung Electro-Mechanics Rank #1 Audit

## Scope

This audit uses only the latest completed daily prediction run:

- Prediction date: `2026-06-23`
- Feature/update date: `2026-06-22`
- Universe predicted: 348 stocks
- Rolling training window: 350 trading days
- Training period: `2025-01-08` to `2026-06-19`
- Ranking model: `outputs/daily_models/20260623/ranking_model.txt`
- Prediction output: `outputs/daily_predictions/predictions_20260623.parquet`
- Top10 output: `reports/daily/top10_20260623.csv`
- Exact prediction feature store: `data/features/full_universe_features_optimized.parquet`, date `2026-06-22`

The dated archive metadata records prediction SHA256 `eec64a4950ea17e6b8cd2f89f816b8741bbaf7bb1ea8404c245ad80839289cca` and ranking-model SHA256 `237e559c046f1e28a82a2e95dd31e71ef1cbe2f49ee9fb295bf6d0d7f641b106`. No model was retrained and prediction was not rerun for this audit.

## Verified Result

| Item | Verified value |
|---|---:|
| Company | Samsung Electro-Mechanics (삼성전기) |
| Ticker | `009150` |
| AI Rank | `1 / 348` |
| AI Percentile | Top 1% |
| AI Score | 100.0 |
| `ranking_score` | 0.0396728535 |
| Expected return | 1.7236% |
| Predicted gap | 0.0673% |
| Predicted intraday return | 1.6563% |
| Previous close | 2,228,000 |

`AI Score` is not a return forecast. It is the daily min-max normalization of `ranking_score`; the maximum score therefore receives 100.0.

## Score Separation

| Rank | Ticker | Company | `ranking_score` | Gap from #1 | Gap as % of #1 score |
|---:|---|---|---:|---:|---:|
| 1 | `009150` | 삼성전기 | 0.039673 | - | - |
| 2 | `347850` | 디앤디파마텍 | 0.023711 | 0.015962 | 40.2% |
| 3 | `402340` | SK스퀘어 | 0.022007 | 0.017666 | 44.5% |
| 4 | `084370` | 유진테크 | 0.020342 | 0.019330 | 48.7% |
| 5 | `319660` | 피에스케이 | 0.019799 | 0.019874 | 50.1% |
| 10 | `240810` | 원익IPS | 0.017389 | 0.022284 | 56.2% |
| 20 | `080220` | 제주반도체 | 0.011879 | 0.027794 | 70.1% |

The universe mean score was 0.004415 with standard deviation 0.004448. Samsung Electro-Mechanics was about 7.9 standard deviations above that daily mean. The score was also well above the 99th-percentile cutoff of 0.020087. The model preference was therefore not a near tie.

## Complete Model Input Row

The saved ranking model declares 55 input features. The table below is the complete row supplied from the production feature store. “Universe percentile” ranks the raw value across the 348 prediction rows; it is descriptive and is not a signed model contribution.

| Feature | Value | Universe percentile |
|---|---:|---:|
| `trading_value` | 1,925,063,296,000 | 99.1% |
| `return_1d` | 0.031818 | 95.4% |
| `return_3d` | 0.108398 | 98.3% |
| `return_5d` | 0.257618 | 98.3% |
| `return_20d` | 1.299899 | 100.0% |
| `return_60d` | 3.886975 | 100.0% |
| `close_ma5_ratio` | 0.105807 | 99.1% |
| `close_ma20_ratio` | 0.275926 | 99.1% |
| `close_ma60_ratio` | 1.152951 | 100.0% |
| `momentum_diff` | -1.042281 | 0.3% |
| `momentum_accel` | 0.038781 | 96.6% |
| `relative_return_5d_vs_market` | 0.266521 | 98.3% |
| `relative_return_20d_vs_market` | 1.344658 | 100.0% |
| `relative_return_20d_vs_sector` | 1.344658 | 100.0% |
| `volume_change_1d` | 0.066192 | 24.7% |
| `relative_trading_value` | 1.346202 | 86.8% |
| `trading_value_rank_pct` | 0.867816 | 86.8% |
| `volatility_5d` | 0.094617 | 92.2% |
| `volatility_20d` | 0.095411 | 92.5% |
| `intraday_range_5d` | 0.100068 | 88.2% |
| `atr_percent` | 0.105381 | 76.7% |
| `volatility_rank_pct` | 0.925287 | 92.5% |
| `body` | -0.004386 | 83.0% |
| `upper_shadow` | 0.060088 | 97.4% |
| `lower_shadow` | 0.044298 | 96.0% |
| `body_ratio` | 0.040323 | 4.9% |
| `close_position` | 0.407258 | 60.3% |
| `high_20d` | 2,417,000 | 99.1% |
| `low_20d` | 924,000 | 97.7% |
| `close_to_20d_high` | -0.060819 | 96.6% |
| `close_to_20d_low` | 1.456710 | 100.0% |
| `breakout_strength` | 0.901541 | 99.4% |
| `breakout_rank_pct` | 0.994253 | 99.4% |
| `rsi14` | 71.759736 | 99.1% |
| `rsi_change_5d` | 8.122694 | 87.4% |
| `macd_hist_ratio` | 0.007104 | 71.8% |
| `bb_position` | 0.888440 | 98.9% |
| `bb_width` | 0.710344 | 96.6% |
| `bb_position_change_5d` | 0.206228 | 84.8% |
| `rsi_rank_pct` | 0.991379 | 99.1% |
| `macd_rank_pct` | 0.718391 | 71.8% |
| `bb_position_rank_pct` | 0.988506 | 98.9% |
| `atr_rank_pct` | 0.767241 | 76.7% |
| `return_5d_rank_pct` | 0.982759 | 98.3% |
| `return_20d_rank_pct` | 1.000000 | 100.0% |
| `momentum_diff_rank_pct` | 0.002874 | 0.3% |
| `volume_change_rank_pct` | 0.247126 | 24.7% |
| `bb_width_rank_pct` | 0.965517 | 96.6% |
| `low_rebound_rank_pct` | 1.000000 | 100.0% |
| `nasdaq_return_1d` | 0.000000 | Same for all rows |
| `sox_return_1d` | missing (`NaN`) | Missing for all rows |
| `sp500_return_1d` | 0.000000 | Same for all rows |
| `vix_change_1d` | 0.000000 | Same for all rows |
| `usdkrw_return_1d` | 0.007958 | Same for all rows |
| `wti_return_1d` | 0.000000 | Same for all rows |

The daily CSV snapshot contains 54 features because it omits `sox_return_1d`. The production Parquet feature store used by prediction contains the model’s 55-column schema with `sox_return_1d = NaN` for all 348 rows. LightGBM accepts this missing value through its learned missing-value branches.

## Peer Comparisons

### Momentum Features

| Feature | R1 삼성전기 | R2 디앤디파마텍 | R3 SK스퀘어 | R4 유진테크 | R5 피에스케이 | R10 원익IPS | R20 제주반도체 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `return_1d` | 0.0318 | 0.0268 | 0.0471 | -0.0515 | -0.0407 | -0.0441 | -0.0289 |
| `return_3d` | 0.1084 | 0.2140 | 0.1859 | -0.0515 | 0.0179 | -0.0006 | 0.1294 |
| `return_5d` | 0.2576 | 0.1780 | 0.4495 | -0.0600 | 0.1015 | 0.1064 | 0.1784 |
| `return_20d` | 1.2999 | 0.8230 | 0.7451 | 0.4587 | 0.5927 | 0.3805 | 0.1919 |
| `return_60d` | 3.8870 | 0.4238 | 1.9276 | 0.2952 | 0.9097 | 0.2480 | 1.5801 |
| `close_ma5_ratio` | 0.1058 | 0.0973 | 0.1216 | -0.0607 | -0.0077 | -0.0491 | 0.0444 |
| `close_ma20_ratio` | 0.2759 | 0.2524 | 0.3550 | 0.1271 | 0.2893 | 0.2140 | 0.0903 |
| `close_ma60_ratio` | 1.1530 | 0.4136 | 0.8890 | 0.2332 | 0.6048 | 0.2627 | 0.6705 |
| `momentum_diff` | -1.0423 | -0.6450 | -0.2956 | -0.5187 | -0.4912 | -0.2741 | -0.0135 |
| `momentum_accel` | 0.0388 | -0.0683 | 0.0125 | -0.2007 | -0.0782 | -0.2921 | -0.0805 |

Samsung was strongest over 20 and 60 days and had positive short-term acceleration. Rank 3 was stronger over five days, so the decision was not simply “highest recent return.”

### Relative Strength Features

| Feature | R1 삼성전기 | R2 디앤디파마텍 | R3 SK스퀘어 | R4 유진테크 | R5 피에스케이 | R10 원익IPS | R20 제주반도체 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `relative_return_5d_vs_market` | 0.2665 | 0.1869 | 0.4584 | -0.0511 | 0.1104 | 0.1153 | 0.1873 |
| `relative_return_20d_vs_market` | 1.3447 | 0.8677 | 0.7899 | 0.5034 | 0.6374 | 0.4253 | 0.2366 |
| `relative_return_20d_vs_sector` | 1.3447 | 0.8677 | 0.7899 | 0.5034 | 0.6374 | 0.4253 | 0.2366 |
| `return_5d_rank_pct` | 0.9828 | 0.9626 | 1.0000 | 0.2241 | 0.9282 | 0.9368 | 0.9655 |
| `return_20d_rank_pct` | 1.0000 | 0.9971 | 0.9914 | 0.9713 | 0.9828 | 0.9540 | 0.9195 |
| `momentum_diff_rank_pct` | 0.0029 | 0.0144 | 0.0546 | 0.0201 | 0.0259 | 0.0603 | 0.2155 |

The strongest differentiator was 20-day relative strength: Samsung was first in the universe. Its `momentum_diff` percentile was simultaneously last, reflecting how much larger the 60-day return was than the 20-day return rather than uniformly weak momentum.

### Liquidity Features

| Feature | R1 삼성전기 | R2 디앤디파마텍 | R3 SK스퀘어 | R4 유진테크 | R5 피에스케이 | R10 원익IPS | R20 제주반도체 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `trading_value` | 1.925T | 0.140T | 2.791T | 0.055T | 0.175T | 0.350T | 1.468T |
| `volume_change_1d` | 0.0662 | 0.3573 | 0.2881 | 0.1567 | -0.2693 | -0.0196 | 0.0262 |
| `relative_trading_value` | 1.3462 | 1.3172 | 1.9517 | 0.7660 | 0.9246 | 1.1487 | 2.0379 |
| `trading_value_rank_pct` | 0.8678 | 0.8563 | 0.9828 | 0.3621 | 0.5345 | 0.7615 | 0.9885 |
| `volume_change_rank_pct` | 0.2471 | 0.5029 | 0.4483 | 0.3190 | 0.0546 | 0.1983 | 0.2126 |

Samsung had extremely high absolute trading value (99.1st raw percentile) and above-normal relative trading value, but it was not the liquidity leader among the compared names. One-day volume change was modest.

### Volatility Features

| Feature | R1 삼성전기 | R2 디앤디파마텍 | R3 SK스퀘어 | R4 유진테크 | R5 피에스케이 | R10 원익IPS | R20 제주반도체 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `volatility_5d` | 0.0946 | 0.0923 | 0.0267 | 0.0743 | 0.0757 | 0.1663 | 0.0541 |
| `volatility_20d` | 0.0954 | 0.1120 | 0.0692 | 0.1014 | 0.0988 | 0.1313 | 0.0831 |
| `intraday_range_5d` | 0.1001 | 0.1322 | 0.0760 | 0.0985 | 0.1115 | 0.1314 | 0.1312 |
| `atr_percent` | 0.1054 | 0.1199 | 0.0773 | 0.1295 | 0.1181 | 0.1349 | 0.1247 |
| `volatility_rank_pct` | 0.9253 | 0.9799 | 0.7816 | 0.9511 | 0.9483 | 0.9943 | 0.8707 |
| `atr_rank_pct` | 0.7672 | 0.8707 | 0.5057 | 0.9080 | 0.8563 | 0.9282 | 0.8879 |

Samsung was high-volatility, but generally less extreme than ranks 2, 4, 5, and 10 on ATR. Volatility was therefore a material regime/profile input, not a unique maximum.

### Breakout Features

| Feature | R1 삼성전기 | R2 디앤디파마텍 | R3 SK스퀘어 | R4 유진테크 | R5 피에스케이 | R10 원익IPS | R20 제주반도체 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `close_to_20d_high` | -0.0608 | -0.0463 | -0.0587 | -0.1682 | -0.1153 | -0.1489 | -0.1663 |
| `close_to_20d_low` | 1.4567 | 0.9475 | 0.7962 | 0.4689 | 0.9389 | 0.6250 | 0.3973 |
| `breakout_strength` | 0.9015 | 0.9092 | 0.8767 | 0.6121 | 0.7880 | 0.6873 | 0.5877 |
| `breakout_rank_pct` | 0.9943 | 0.9971 | 0.9885 | 0.8966 | 0.9770 | 0.9483 | 0.8822 |
| `low_rebound_rank_pct` | 1.0000 | 0.9971 | 0.9885 | 0.9569 | 0.9943 | 0.9684 | 0.9425 |

Samsung was near its 20-day high, ranked second in breakout strength, and ranked first in rebound from its 20-day low. This was one of the clearest multi-feature advantages.

### Technical Indicators

| Feature | R1 삼성전기 | R2 디앤디파마텍 | R3 SK스퀘어 | R4 유진테크 | R5 피에스케이 | R10 원익IPS | R20 제주반도체 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `rsi14` | 71.7597 | 64.4840 | 75.8950 | 56.5534 | 67.1683 | 58.0129 | 64.0320 |
| `rsi_change_5d` | 8.1227 | 6.7061 | 19.3833 | -8.4804 | -1.2677 | -1.1193 | 7.4846 |
| `macd_hist_ratio` | 0.0071 | 0.0210 | 0.0224 | 0.0188 | 0.0284 | 0.0321 | -0.0026 |
| `bb_position` | 0.8884 | 0.8769 | 1.1052 | 0.7038 | 0.8373 | 0.7865 | 0.7631 |
| `bb_width` | 0.7103 | 0.6697 | 0.5865 | 0.6233 | 0.8575 | 0.7470 | 0.3431 |
| `bb_position_change_5d` | 0.2062 | 0.1291 | 0.5167 | -0.4402 | -0.2411 | -0.2414 | 0.2760 |
| `rsi_rank_pct` | 0.9914 | 0.9770 | 1.0000 | 0.9310 | 0.9828 | 0.9454 | 0.9713 |
| `macd_rank_pct` | 0.7184 | 0.9741 | 0.9770 | 0.9655 | 0.9856 | 0.9943 | 0.2960 |
| `bb_position_rank_pct` | 0.9885 | 0.9828 | 1.0000 | 0.9109 | 0.9770 | 0.9598 | 0.9540 |
| `bb_width_rank_pct` | 0.9655 | 0.9569 | 0.9282 | 0.9425 | 0.9885 | 0.9770 | 0.6178 |

Samsung combined a top-1% RSI and Bollinger position with expanding momentum. Its MACD percentile was only 71.8%, weaker than most nearby ranks. RSI above 70 also signals an overbought/risk condition rather than an unambiguously positive forecast.

### Macro Features

| Feature | R1 | R2 | R3 | R4 | R5 | R10 | R20 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `nasdaq_return_1d` | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| `sox_return_1d` | NaN | NaN | NaN | NaN | NaN | NaN | NaN |
| `sp500_return_1d` | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| `vix_change_1d` | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| `usdkrw_return_1d` | 0.007958 | 0.007958 | 0.007958 | 0.007958 | 0.007958 | 0.007958 | 0.007958 |
| `wti_return_1d` | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

Macro features were identical across all 348 stocks on this prediction date. They may change the model’s common daily baseline or tree path, but they cannot explain Samsung’s cross-sectional advantage over ranks 2–20. The missing SOX value is operationally unusual because it is the model’s most frequently split feature.

## Latest Ranking Model Importance

The saved LightGBM file contains split-count importance. This measures how often a feature appears in tree splits; it is global, unsigned, and does not prove a positive per-row contribution.

| Rank | Feature | Split count | Samsung value | Samsung raw percentile |
|---:|---|---:|---:|---:|
| 1 | `sox_return_1d` | 738 | NaN | NA |
| 2 | `usdkrw_return_1d` | 685 | 0.007958 | Same for all |
| 3 | `vix_change_1d` | 505 | 0 | Same for all |
| 4 | `wti_return_1d` | 498 | 0 | Same for all |
| 5 | `nasdaq_return_1d` | 490 | 0 | Same for all |
| 6 | `sp500_return_1d` | 448 | 0 | Same for all |
| 7 | `atr_percent` | 194 | 0.105381 | 76.7% |
| 8 | `intraday_range_5d` | 155 | 0.100068 | 88.2% |
| 9 | `volatility_20d` | 119 | 0.095411 | 92.5% |
| 10 | `trading_value` | 108 | 1.925T | 99.1% |
| 11 | `atr_rank_pct` | 108 | 0.767241 | 76.7% |
| 12 | `lower_shadow` | 93 | 0.044298 | 96.0% |
| 13 | `return_1d` | 90 | 0.031818 | 95.4% |
| 14 | `volatility_rank_pct` | 88 | 0.925287 | 92.5% |
| 15 | `return_60d` | 85 | 3.886975 | 100.0% |
| 16 | `upper_shadow` | 81 | 0.060088 | 97.4% |
| 17 | `return_3d` | 77 | 0.108398 | 98.3% |
| 18 | `close_ma5_ratio` | 77 | 0.105807 | 99.1% |
| 19 | `macd_hist_ratio` | 72 | 0.007104 | 71.8% |
| 20 | `momentum_diff` | 69 | -1.042281 | 0.3% |

Among the highest-importance stock-varying features, Samsung was exceptionally strong on trading value, short- and long-horizon returns, moving-average position, shadows, and volatility rank. It was only moderate on ATR and MACD, and exceptionally low on `momentum_diff`. The six higher-ranked macro importance entries did not differentiate stocks that day.

## Signal Assessment

### Strongest supportive signals

- First in the universe on 20-day return, 60-day return, 20-day return percentile, market-relative 20-day return, sector-relative 20-day return, rebound from 20-day low, and 60-day moving-average distance.
- 99.4th percentile breakout strength and breakout rank.
- 98.3rd percentile five-day return and five-day relative return.
- 99.1st percentile trading value, RSI, five-day moving-average ratio, and 20-day moving-average ratio.
- Positive `momentum_accel` at the 96.6th percentile.
- High values on several important volatility/candlestick features used frequently by the model.

### Strongest adverse or cautionary signals

- `momentum_diff` and `momentum_diff_rank_pct` were at the 0.3rd percentile, the most extreme negative-side values in the row.
- One-day volume change was only at the 24.7th percentile; the move was not accompanied by an exceptional daily volume acceleration.
- MACD rank was 71.8%, materially below ranks 2, 3, 4, 5, and 10.
- RSI was above 70 and volatility was above the 92nd percentile, indicating overbought and high-risk conditions.
- `sox_return_1d`, the model’s most-used split feature, was missing for the entire daily universe.

### Unusual values

- `return_20d = 129.99%` and `return_60d = 388.70%` are extreme cross-sectional outliers.
- `close_to_20d_low = 145.67%` and `close_ma60_ratio = 115.30%` describe a very extended move.
- `trading_value = 1.925 trillion` and the high volatility/candlestick ranges are also exceptional.

These values explain why tree thresholds could route Samsung into rare high-score leaves, but they also make the result sensitive to outliers, corporate actions, and price-data quality. This audit confirms the stored row; it does not independently certify that every extreme input reflects an economically continuous price series.

## Justification Classification

**Model-relative classification: clearly justified.**

Samsung’s score was 40.2% above rank 2, about 7.9 daily cross-sectional standard deviations above the mean, and supported by simultaneous top-percentile readings across momentum, relative strength, breakout, liquidity, and technical groups. It was not promoted by a tiny score difference or a single isolated feature.

**Statistical-confidence caveat: weak.**

The latest matching 350-day/90-day walk-forward evaluation reports daily mean ranking IC of only 0.01065, negative ranking R², and Top10 hit rate of 8.22%. Therefore, “clearly justified” means the saved model consistently preferred this row, not that the realized next-day return is statistically certain. The missing SOX feature and extreme return values further reduce confidence in interpreting the score causally.

## Final Conclusion

Samsung Electro-Mechanics ranked #1 because the latest ranking model saw an unusually broad and extreme trend profile: universe-leading 20- and 60-day returns, strongest 20-day relative strength, near-maximum breakout/rebound measures, top-percentile moving-average and Bollinger positions, very high liquidity, and strong short-term acceleration. Those stock-specific signals aligned across multiple feature families and produced a `ranking_score` far above nearby ranks.

The rank was not caused by macro differences, because macro values were constant across the universe, and it was not uniformly positive: bottom-percentile `momentum_diff`, moderate MACD, high volatility, overbought RSI, and missing SOX data are meaningful cautions. The most accurate summary is: **a clearly separated #1 according to this trained model and stored feature row, but not a statistically high-confidence investment conclusion.**
