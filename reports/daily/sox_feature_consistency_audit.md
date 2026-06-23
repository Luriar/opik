# SOX Feature Consistency Audit

## Scope and Method

This audit uses only saved artifacts from the latest completed production run. It does not retrain a model, rerun prediction, or modify production artifacts.

- Prediction date: `2026-06-23`
- Feature/update date: `2026-06-22`
- Ranking model: `outputs/daily_models/20260623/ranking_model.txt`
- Prediction feature store: `data/features/full_universe_features_optimized.parquet`, filtered to `2026-06-22`
- Rolling training dataset: `outputs/archive/20260623/training/rolling_train_20260623.parquet`
- Prediction output: `outputs/daily_predictions/predictions_20260623.parquet`
- Archive metadata: `outputs/archive/20260623/metadata/archive_metadata.json`
- Training period: `2025-01-08` to `2026-06-19`
- Rolling training dates: 350
- Training rows: 121,492
- Prediction rows: 348

The model and prediction hashes recorded by the archive are:

- Ranking model SHA256: `237e559c046f1e28a82a2e95dd31e71ef1cbe2f49ee9fb295bf6d0d7f641b106`
- Prediction SHA256: `eec64a4950ea17e6b8cd2f89f816b8741bbaf7bb1ea8404c245ad80839289cca`

## Executive Finding

The latest ranking model was trained mostly on valid SOX returns but predicted with `sox_return_1d` missing for all 348 stocks. Training missingness was 1.43%; prediction missingness was 100%. SOX is the model’s most-used and highest-gain feature, representing 12.30% of all splits and 17.97% of total gain.

LightGBM can technically process the input because every SOX split has an explicit NaN route and NaNs occurred during training. However, moving from 1.43% missing training rows to 100% missing prediction rows is a material train/predict distribution inconsistency. This audit classifies it as **Severe**.

## 1. Latest Production Run Verification

| Item | Verified value |
|---|---|
| Prediction date | `2026-06-23` |
| Feature date | `2026-06-22` |
| Ranking model | `outputs/daily_models/20260623/ranking_model.txt` |
| Prediction feature dataset | `data/features/full_universe_features_optimized.parquet` |
| Feature rows used | Date `2026-06-22`, 348 rows |
| Model feature count | 55 |
| Rolling train window | 350 trading dates |
| Pipeline result | Success, exit code 0 |

The daily feature CSV snapshot has 54 feature columns because it omits SOX. Prediction loads the production Parquet feature store, which retains the 55-column model schema but contains `NaN` for SOX on all latest-date rows.

## 2. Ranking Model Feature Schema

`sox_return_1d` is included in the saved LightGBM model at zero-based feature index **50** (the 51st feature). The complete saved order is:

```text
 0 trading_value
 1 return_1d
 2 return_3d
 3 return_5d
 4 return_20d
 5 return_60d
 6 close_ma5_ratio
 7 close_ma20_ratio
 8 close_ma60_ratio
 9 momentum_diff
10 momentum_accel
11 relative_return_5d_vs_market
12 relative_return_20d_vs_market
13 relative_return_20d_vs_sector
14 volume_change_1d
15 relative_trading_value
16 trading_value_rank_pct
17 volatility_5d
18 volatility_20d
19 intraday_range_5d
20 atr_percent
21 volatility_rank_pct
22 body
23 upper_shadow
24 lower_shadow
25 body_ratio
26 close_position
27 high_20d
28 low_20d
29 close_to_20d_high
30 close_to_20d_low
31 breakout_strength
32 breakout_rank_pct
33 rsi14
34 rsi_change_5d
35 macd_hist_ratio
36 bb_position
37 bb_width
38 bb_position_change_5d
39 rsi_rank_pct
40 macd_rank_pct
41 bb_position_rank_pct
42 atr_rank_pct
43 return_5d_rank_pct
44 return_20d_rank_pct
45 momentum_diff_rank_pct
46 volume_change_rank_pct
47 bb_width_rank_pct
48 low_rebound_rank_pct
49 nasdaq_return_1d
50 sox_return_1d
51 sp500_return_1d
52 vix_change_1d
53 usdkrw_return_1d
54 wti_return_1d
```

The prediction store column exists, so this was not a feature-name/order mismatch. It was a value-distribution mismatch: the expected column was present but entirely null.

## 3. Latest Prediction SOX Distribution

| Metric | Value |
|---|---:|
| Total rows | 348 |
| Non-null rows | 0 |
| Null rows | 348 |
| Null percentage | 100.00% |
| Minimum | NA |
| Maximum | NA |
| Mean | NA |
| Standard deviation | NA |

Classification: **all rows are NaN**. There were no valid SOX values in the latest prediction universe.

## 4. Rolling Training SOX Distribution

| Metric | Value |
|---|---:|
| Total training rows | 121,492 |
| Non-null rows | 119,750 |
| Null rows | 1,742 |
| Missing percentage | 1.4338% |
| Minimum | -0.106019 |
| Maximum | 0.187348 |
| Mean | 0.003096 |
| Standard deviation | 0.026453 |
| Distinct valid values | 345 |

Training contained valid SOX observations in 98.57% of rows. It also contained five feature dates where SOX was missing for the entire cross-section:

- `2025-04-21`
- `2025-04-22`
- `2026-06-17`
- `2026-06-18`
- `2026-06-19`

Eight other feature dates had one missing stock row each. The last three training dates were all-missing SOX dates, so the model did observe this missing regime recently, but only five of 350 training dates were entirely missing.

Training valid-value quantiles provide context:

| Quantile | SOX return |
|---:|---:|
| 1% | -0.076028 |
| 5% | -0.039252 |
| 25% | -0.007313 |
| 50% | 0.002874 |
| 75% | 0.016842 |
| 95% | 0.039631 |
| 99% | 0.070397 |

## 5. Training vs Prediction

| Metric | Training | Prediction |
|---|---:|---:|
| Rows | 121,492 | 348 |
| Null % | 1.4338% | 100.0000% |
| Mean | 0.003096 | NA |
| Standard deviation | 0.026453 | NA |
| Minimum | -0.106019 | NA |
| Maximum | 0.187348 | NA |

The model was trained predominantly on numeric SOX returns but received only missing SOX values at prediction. The prediction distribution has no numeric support and therefore cannot be treated as a normal sample from the training distribution.

## 6. Consistency Classification

**Classification: Severe issue.**

Reasons:

1. Missingness changed from 1.43% in training to 100% in prediction.
2. SOX is #1 by both split count and gain.
3. All 348 predictions followed missing-value branches at every SOX node they encountered.
4. No valid prediction value existed with which to distinguish a normal market regime.
5. Although training included some all-missing dates, they were only 5 of 350 dates, not the dominant training regime.

This is not a schema or runtime-crash issue. It is a high-impact feature availability and distribution-consistency issue.

## 7. Macro Feature Importance

Importance was extracted from the saved latest ranking model. Split importance counts tree nodes; gain importance sums the training objective improvement attributed to those nodes.

| Feature | Feature index | Split count | Split rank | Split % | Gain | Gain rank | Gain % |
|---|---:|---:|---:|---:|---:|---:|---:|
| `sox_return_1d` | 50 | 738 | 1 | 12.3000% | 125.3637 | 1 | 17.9682% |
| `usdkrw_return_1d` | 53 | 685 | 2 | 11.4167% | 110.7699 | 2 | 15.8765% |
| `vix_change_1d` | 52 | 505 | 3 | 8.4167% | 62.7041 | 6 | 8.9873% |
| `wti_return_1d` | 54 | 498 | 4 | 8.3000% | 89.5527 | 4 | 12.8355% |
| `nasdaq_return_1d` | 49 | 490 | 5 | 8.1667% | 90.2633 | 3 | 12.9373% |
| `sp500_return_1d` | 51 | 448 | 6 | 7.4667% | 77.9454 | 5 | 11.1718% |

Model totals:

- Total splits: 6,000
- Total gain: 697.6970
- All six macro features combined: 56.07% of splits and 79.78% of total gain

Macro features repeat the same daily value across every stock, so their high importance largely represents market-regime partitioning. They can still affect cross-sectional ranking through interactions: stocks reach different SOX nodes after earlier stock-specific splits and then enter different downstream subtrees.

## 8. Model Dependence on SOX

| Dependence measure | Result |
|---|---:|
| Rank among all features by split count | #1 of 55 |
| Rank among all features by gain | #1 of 55 |
| SOX split nodes | 738 |
| Percentage of all splits | 12.30% |
| SOX gain | 125.3637 |
| Percentage of total gain | 17.97% |
| SOX root splits | 7 |
| Mean SOX split depth | 5.52 |
| Maximum SOX split depth | 17 |

SOX dependence is substantial. It is not a rarely used auxiliary feature.

## 9. Estimated Ranking Impact

Question: If SOX had been present with a normal numeric value instead of NaN, would Samsung Electro-Mechanics likely remain rank #1?

**Classification: Uncertain.**

Evidence favoring rank stability:

- Samsung’s observed `ranking_score` was 0.039673, 40.2% above rank 2.
- Its stock-specific row was extreme across momentum, relative strength, breakout, liquidity, and technical features.
- A daily SOX value would be common to all stocks, so some SOX effects would shift a broad market baseline rather than uniquely target Samsung.

Evidence against claiming stability:

- SOX accounts for 17.97% of total gain and 12.30% of all splits.
- Seven trees split on SOX at the root, and hundreds of other SOX splits occur after stock-specific decisions.
- Changing NaN to a numeric value can send observations into different downstream subtrees. Because stocks arrive at different nodes, the effect need not be equal across stocks.
- The actual missing SOX value cannot be reconstructed from the saved prediction artifacts.

Without performing a counterfactual model prediction—which this audit explicitly does not do—the saved score gap is insufficient to prove rank invariance. “Almost certainly” or “likely” would overstate the evidence.

## 10. LightGBM Missing-Value Handling

The saved model contains 738 SOX split nodes. Tree inspection found:

| Property | Result |
|---|---:|
| SOX nodes with `missing_type = NaN` | 738 / 738 |
| SOX nodes defaulting missing left | 583 |
| SOX nodes defaulting missing right | 155 |
| Training rows with SOX NaN | 1,742 |
| Training dates entirely SOX NaN | 5 |

Conclusions:

- NaN routes exist and are explicit in every trained SOX split.
- NaN was observed during training, so the model did not encounter an entirely novel data type at inference.
- Prediction-time NaN is mechanically supported and consistent with learned missing-value behavior.
- Prediction-time prevalence is not consistent with training prevalence: 100% versus 1.43%.

Thus LightGBM handled the input as designed, but that handling does not make the feature distribution operationally acceptable.

## 11. Final Answers

### A. Was the model trained using valid SOX values?

**Yes.** 119,750 of 121,492 training rows (98.57%) contained numeric SOX returns. The model also learned from 1,742 missing rows.

### B. Was the latest prediction run performed with SOX missing?

**Yes.** All 348 prediction rows had `sox_return_1d = NaN`.

### C. Does this represent train/predict feature inconsistency?

**Yes.** Missingness increased from 1.43% to 100%. The column/schema matched, but the value distribution did not.

### D. Could the missing SOX materially affect ranking results?

**Yes.** SOX is #1 by split count and gain, with 12.30% of splits and 17.97% of gain. LightGBM interactions mean a common SOX value can still alter stocks differently. Whether Samsung would remain #1 is **uncertain** without a prohibited counterfactual prediction.

### E. Should production be blocked when SOX is entirely missing?

**Yes.** For this model version, an entirely missing SOX cross-section should be treated as a critical feature-availability failure, or production should use a separately validated and explicitly documented fallback model/policy. Continuing silently is not appropriate because SOX is the model’s highest-dependence feature and the observed missingness shift is severe.

## Final Conclusion

The latest production run succeeded technically because LightGBM had learned NaN routing, including from several recent all-missing dates. Nevertheless, the model was trained overwhelmingly on valid SOX observations and then applied to an all-missing SOX universe. Given SOX’s #1 split/gain importance, this is a severe train/predict feature consistency failure that could materially affect ranking. Production should not silently publish rankings under this condition.
