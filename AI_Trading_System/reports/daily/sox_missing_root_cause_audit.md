# SOX Missingness Root-Cause Audit

## Scope and Method

This audit uses only existing production artifacts, logs, status JSON files, archive metadata, and source code. No model was retrained, no prediction was rerun, and no production artifact was changed.

- Latest prediction date: `2026-06-23`
- Latest feature/update date: `2026-06-22`
- Ranking model: `outputs/daily_models/20260623/ranking_model.txt`
- Prediction feature store: `data/features/full_universe_features_optimized.parquet`
- Rolling training artifact: `outputs/archive/20260623/training/rolling_train_20260623.parquet`
- Latest prediction: `outputs/daily_predictions/predictions_20260623.parquet`

## Executive Finding

The root cause is not a transient yfinance response, MultiIndex parsing error, merge error, or failed column rename. The daily production downloader never requests SOX.

`src/pipeline/macro_download.py` defines five sources only: NASDAQ, S&P 500, VIX, WTI, and USD/KRW. It does not contain ticker `^SOX` or output column `sox_close`. The new production macro store begins on `2026-06-17` without SOX, while the legacy cleaned macro dataset ends on `2026-06-16` with SOX. Feature generation silently skips `sox_return_1d` when `sox_close` is absent. Appending the 54-column daily frame to the existing 55-feature Parquet store then represents SOX as `NaN` for every new row.

Production continued because SOX is also absent from `SOURCE_COLUMN_CANDIDATES`, `REQUIRED_SOURCES`, and `US_MACRO_SOURCES` in `src/pipeline/feature_source_completeness.py`. The completeness check therefore passed by design. `feature_missing_count` counts null cells only in columns that exist in the daily frame, so an omitted feature column can produce a count of zero. The `2026-06-23` status demonstrates this exact failure: `feature_column_count = 54`, `feature_missing_count = 0`, and `feature_source_completeness_passed = true`.

This is a **Critical** production data-contract failure. SOX is the latest ranking model's #1 feature by split count and gain.

## 1. SOX Data Lineage

| Stage | Artifact or code path | Rows / date range | SOX availability | First invalid date | Finding |
|---|---|---|---|---|---|
| 1. yfinance download | `src/pipeline/macro_download.py` | No production SOX request; 0 SOX rows | `^SOX` is absent from `MACRO_TICKERS` | `2026-06-17` production handoff | No SOX frame exists to normalize or parse |
| 2. Raw macro snapshot | No production raw yfinance snapshot is persisted | Not available | Not observable | `2026-06-17` | The production path writes selected clean rows directly; this is an observability gap |
| Legacy raw macro | `data/raw/macro/macro_20230615_20260614.parquet` | 778 rows; `2023-06-15` to `2026-06-12` | `sox_close` present | None in saved range | Historical builder did request `^SOX` |
| 3. Legacy clean macro | `data/processed/macro/macro_clean_20230615_20260614.parquet` | 779 rows; `2023-06-15` to `2026-06-16` | 779/779 non-null | None | Final `2026-06-16` SOX close is `13371.469727` |
| 3. Production clean macro | `data/processed/macro/macro_clean_latest.parquet` | 4 rows; `2026-06-17` to `2026-06-22` | `sox_close` column absent | `2026-06-17` | Contains only the five registered macro sources |
| 4. Daily feature generation | `src/features/macro_features.py`; `data/daily/features/features_YYYYMMDD.csv` | 348 rows per inspected date | 55 features with SOX on `2026-06-16`; 54 features without SOX thereafter | `2026-06-17` | Missing source column is silently skipped |
| 5. Feature store | `data/features/full_universe_features_optimized.parquet` | 251,105 rows; `2023-06-15` to `2026-06-22` | SOX retained in union schema; new rows become null | Current continuous outage starts `2026-06-17` | 348/348 null on each of `06-17`, `06-18`, `06-19`, `06-22` |
| 6. Rolling training dataset | `outputs/archive/20260623/training/rolling_train_20260623.parquet` | 121,492 rows; 350 dates; `2025-01-09` to `2026-06-22` | 119,750 valid; 1,742 null | First unexplained date in this window: `2025-04-21` | Null rate 1.4338%; five dates are all-null |
| 7. Latest prediction features | Feature store filtered to `2026-06-22` | 348 rows | 0 valid; 348 null | `2026-06-22` feature date | All latest predictions used the learned SOX missing-value branches |

The earliest invalid point in the current production lineage is the **macro download/source-selection stage**, not feature calculation: the clean input reaching feature generation already lacks `sox_close`.

## 2. Macro Download Output Around the Failure

### Saved Daily Macro Snapshots

| Target date | Snapshot | Rows | SOX column | SOX downloaded rows | SOX actual source date | SOX Close | Null / duplicate rows | Other source result |
|---|---|---:|---|---:|---|---:|---|---|
| `2026-06-16` | `data/daily/processed/macro_clean_20260616.csv` | 1 | Present | 1 legacy clean row | `2026-06-16` | 13371.469727 | 0 / 0 | Five other closes present |
| `2026-06-17` | `data/daily/processed/macro_clean_20260617.csv` | 1 | Absent | 0 | Not requested | NA | SOX not represented / 0 | Five other closes present |
| `2026-06-18` | `data/daily/processed/macro_clean_20260618.csv` | 1 | Absent | 0 | Not requested | NA | SOX not represented / 0 | Five other closes present |
| `2026-06-19` | `data/daily/processed/macro_clean_20260619.csv` | 1 | Absent | 0 | Not requested | NA | SOX not represented / 0 | NASDAQ/S&P/VIX/WTI use `06-18`; USD/KRW uses `06-19` |
| `2026-06-20` | No snapshot | 0 | NA | 0 | NA | NA | NA | Saturday; no production target output exists |
| `2026-06-22` | `data/daily/processed/macro_clean_20260622.csv` | 1 | Absent | 0 | Not requested | NA | SOX not represented / 0 | All five registered sources use `06-22` |

No inspected snapshot contains duplicate dates. SOX was **not downloaded successfully by the daily production path** because it was not attempted. The `2026-06-16` value came from the legacy macro dataset, whose raw source ended on `2026-06-12` and whose clean series extended through `2026-06-16`.

### Registered Source Close Values

| Target date | NASDAQ | S&P 500 | VIX | WTI | USD/KRW |
|---|---:|---:|---:|---:|---:|
| `2026-06-16` | 25888.839844 | 7431.459961 | 17.680001 | 84.879997 | 1517.380005 |
| `2026-06-17` | 26021.656250 | 7420.100098 | 18.440001 | 75.209999 | 1510.959961 |
| `2026-06-18` | 26517.929688 | 7500.580078 | 16.400000 | 76.599998 | 1525.420044 |
| `2026-06-19` | 26517.929688 | 7500.580078 | 16.400000 | 76.599998 | 1537.560059 |
| `2026-06-22` | 26166.601562 | 7472.790039 | 17.280001 | 74.290001 | 1531.329956 |

## 3. yfinance Behavior and SOX Extraction

### Current Production Path

`src/pipeline/macro_download.py` uses:

| Source | yfinance ticker | Normalized close column |
|---|---|---|
| NASDAQ | `^IXIC` | `nasdaq_close` |
| S&P 500 | `^GSPC` | `sp500_close` |
| VIX | `^VIX` | `vix_close` |
| WTI | `CL=F` | `wti_close` |
| USD/KRW | `KRW=X` | `usdkrw` |
| SOX | **not registered** | **not registered** |

For registered sources, the downloader requests a date window with `yf.download()`, normalizes yfinance output including MultiIndex columns, extracts `Close`, coerces it numeric, rejects null or non-positive values, and selects the latest valid row on or before the target date within tolerance.

For SOX specifically:

- Intended historical ticker: `^SOX`, confirmed in `scripts/download_real_market_data.py` and `scripts/build_full_universe_real_dataset.py`.
- Expected source field: yfinance `Close`.
- Expected normalized column: `sox_close`.
- Actual production columns returned: **not applicable; production made no SOX request**.
- MultiIndex normalization: **not reached for SOX**.
- Rename failure: **none; no SOX column entered the rename path**.
- Close extraction failure: **none; no SOX frame entered Close extraction**.

Because production does not persist raw yfinance frames, the exact raw columns returned for the five requested tickers cannot be reconstructed from saved artifacts. This limitation does not obscure the SOX cause: the static registry proves no SOX API call could occur.

### Legacy Path

The historical download scripts map `sox_close` to `^SOX`, extract `data["Close"]`, reduce a DataFrame result to its first column where necessary, and rename the series to `sox_close`. The legacy raw and clean Parquet schemas confirm that this path produced SOX successfully.

## 4. First Failure Point and Classification

### Primary Classification: A. Download Failure

More precisely, this is a **source omission/configuration failure at the download stage**, rather than an upstream provider outage. SOX is not present in `MACRO_TICKERS` or `MACRO_CLOSE_COLUMNS` in `src/pipeline/macro_download.py`. The first production clean row on `2026-06-17` therefore lacks `sox_close`.

### Secondary Classification: F. Completeness-Check Logic Failure

`src/pipeline/feature_source_completeness.py` omits SOX from:

- `SOURCE_COLUMN_CANDIDATES`
- `REQUIRED_SOURCES`
- `US_MACRO_SOURCES`

Validation consequently checks only KRX and the five registered macros and reports success when all are present. It does not issue a SOX warning and cannot block on SOX.

### Propagation Mechanism

`src/features/macro_features.py` defines `sox_return_1d -> sox_close`, but skips a feature when its source column is absent. The daily feature CSV therefore contains 54 feature columns, not a 55th all-null column. `src/pipeline/feature_update.py` counts missing cells in that already-reduced frame. The store append aligns schemas and turns the absent SOX field into nulls later.

There is no evidence of classifications B, C, D, or E as the initiating fault. Parsing, merging, calculation, and Parquet storage behaved consistently with the incomplete input/schema.

## 5. SOX Compared With Other Macro Features

Latest prediction feature date: `2026-06-22`; 348 prediction rows.

| Feature | Source ticker | Selected source date | Source Close | Feature value | Source absent/missing | Prediction missing |
|---|---|---|---:|---:|---:|---:|
| `nasdaq_return_1d` | `^IXIC` | `2026-06-22` | 26166.601562 | 0.000000 | 0% | 0% |
| `sp500_return_1d` | `^GSPC` | `2026-06-22` | 7472.790039 | 0.000000 | 0% | 0% |
| `vix_change_1d` | `^VIX` | `2026-06-22` | 17.280001 | 0.000000 | 0% | 0% |
| `usdkrw_return_1d` | `KRW=X` | `2026-06-22` | 1531.329956 | 0.007958 | 0% | 0% |
| `wti_return_1d` | `CL=F` | `2026-06-22` | 74.290001 | 0.000000 | 0% | 0% |
| `sox_return_1d` | expected `^SOX` | None | NA | NA | 100% | 100% |

The macro feature formula uses lagged source closes (`shift(1) / shift(2) - 1`) to preserve T-1 behavior. The zero values above reflect the available lagged production macro series; they are valid numeric model inputs, not missing values.

Only SOX remains persistently absent because it alone is missing from the production source registry. On `2026-06-17` and `2026-06-18`, the other five daily macro returns were temporarily null because the newly initialized production clean series lacked enough lag history for the two-shift formula. By `2026-06-19`, all five had numeric values. SOX could not recover as additional dates accumulated because its close column never existed.

## 6. Historical Missingness Timeline

The full feature store contains eight dates with SOX missingness above 50%; every one is exactly 100% missing.

| Feature date | Missing / rows | Missing % | Interpretation |
|---|---:|---:|---|
| `2023-06-15` | 333 / 333 | 100% | Expected formula warm-up |
| `2023-06-16` | 332 / 332 | 100% | Expected formula warm-up |
| `2025-04-21` | 345 / 345 | 100% | Earlier intermittent historical outage |
| `2025-04-22` | 345 / 345 | 100% | Earlier intermittent historical outage |
| `2026-06-17` | 348 / 348 | 100% | Start of current production source omission |
| `2026-06-18` | 348 / 348 | 100% | Current outage |
| `2026-06-19` | 348 / 348 | 100% | Current outage |
| `2026-06-22` | 348 / 348 | 100% | Current outage/latest prediction features |

SOX was valid for all 348 rows on `2026-06-16`, with `sox_return_1d = 0.015187`. The current uninterrupted failure begins on `2026-06-17`, exactly when the new five-source production clean macro dataset takes over.

This is both:

- a **long-standing intermittent data-quality issue**, because two non-warm-up all-null dates exist in April 2025; and
- a **new continuous production regression**, because every feature date from June 17 onward is all-null due to a different, directly identifiable registry omission.

Within the latest 350-date rolling artifact, five dates are entirely missing SOX: `2025-04-21`, `2025-04-22`, `2026-06-17`, `2026-06-18`, and `2026-06-19`. Eight additional dates contain one missing stock row each. Total training-artifact missingness is 1,742 of 121,492 rows (1.4338%).

## 7. Why Production Did Not Stop

| Question | Answer |
|---|---|
| Was SOX included in source completeness validation? | No |
| Was SOX explicitly excluded from required sources? | Effectively yes; it is absent from all validation registries |
| Did validation incorrectly pass? | Yes relative to the 55-feature model contract, although it behaved exactly as its incomplete source list specifies |
| Did a warning exist but fail to block? | No SOX warning was generated; SOX was invisible to the validator and macro console |
| Was model schema checked against the daily feature frame? | No blocking 55-column/non-null contract was enforced before prediction |
| Why could missing count be zero? | The absent SOX column was not part of the frame whose null cells were counted |

Production status evidence:

| Prediction date | Feature date | Feature columns | Missing cells reported | Completeness passed | Result |
|---|---|---:|---:|---|---|
| `2026-06-18` | `2026-06-17` | 54 | 1,740 | true | Prediction published |
| `2026-06-19` | `2026-06-18` | 54 | 1,740 | true | Prediction published |
| `2026-06-22` | `2026-06-19` | 54 | 0 | true | Prediction published |
| `2026-06-23` | `2026-06-22` | 54 | 0 | true | Prediction published |

The relevant daily logs mark macro update and the overall pipeline successful and print PASS only for NASDAQ, S&P 500, VIX, WTI, and USD/KRW. They contain no SOX check, warning, or error.

## 8. Impact Assessment

### Model Dependence

| Measure | Latest ranking model result |
|---|---:|
| SOX feature index | 50 (zero-based), 51st of 55 |
| Split-count rank | #1 of 55 |
| Split count | 738 of 6,000 |
| Percentage of splits | 12.30% |
| Gain rank | #1 of 55 |
| Gain importance | 125.3637 of 697.6970 |
| Percentage of gain | 17.97% |

The model has explicit NaN routing at all 738 SOX split nodes, and some NaNs existed during training. That prevents a runtime error; it does not make a change from 1.43% training missingness to 100% prediction missingness acceptable.

### Affected Outputs

| Prediction date | Feature date | Rows | SOX state |
|---|---|---:|---|
| `2026-06-18` | `2026-06-17` | 348 | All null |
| `2026-06-19` | `2026-06-18` | 348 | All null |
| `2026-06-22` | `2026-06-19` | 348 | All null |
| `2026-06-23` | `2026-06-22` | 348 | All null |

At least **four saved prediction days and 1,392 prediction rows** are affected by the current continuous outage. The `2026-06-17` prediction used the valid `2026-06-16` feature row and is not part of this count.

### Severity: Critical

This is Critical because a required feature is wholly absent, the feature is #1 by both split and gain importance, four production rankings were published, and all configured controls passed silently. The impact is potentially material but not exactly quantifiable without a counterfactual prediction, which this audit did not perform.

## 9. Corrective Actions

### Immediate

| Recommendation | Benefit | Risk | Complexity |
|---|---|---|---|
| Add `^SOX -> sox_close` to the production macro registry and persisted close schema | Restores the model-required source | yfinance availability/calendar behavior must be validated | Low |
| Add SOX to completeness source candidates, required sources, US macro policy, status dates, diagnostics, and console output | Stops production when SOX is unavailable | May block a run during a real provider outage, which is appropriate absent an approved fallback | Low |
| Before prediction, require exact saved-model feature names/order and reject any absent or entirely-null required feature | Closes the generic 54-vs-55 contract hole | Could expose other existing data-quality defects and stop runs | Medium |
| After repair, rebuild affected feature dates and regenerate affected predictions under controlled validation | Removes known invalid inputs from current outputs | Reissued rankings may differ; archives must retain provenance | Medium |

### Short Term

| Recommendation | Benefit | Risk | Complexity |
|---|---|---|---|
| Persist bounded raw yfinance responses and normalization metadata per source/run | Makes provider, columns, dates, and parsing auditable | Additional storage and retention policy | Medium |
| Test every model macro feature through downloader, clean store, formula, daily snapshot, and prediction schema | Detects registry drift before deployment | Test fixtures require maintenance | Medium |
| Report expected, present, non-null, and all-null feature counts separately | Prevents absent columns from masquerading as zero missing cells | Status schema change | Low |
| Define and test an explicit SOX fallback policy, or fail closed | Gives deterministic outage behavior | A fallback can introduce bias unless independently validated | Medium to High |
| Re-audit/recompute metrics covering `2025-04-21`, `2025-04-22`, and the June 2026 outage | Establishes evaluation integrity | Historical results may change | Medium |

### Long Term

| Recommendation | Benefit | Risk | Complexity |
|---|---|---|---|
| Use one typed macro-source registry to generate downloader mappings, formulas, completeness rules, status fields, and tests | Eliminates duplicated lists and source drift | Requires coordinated refactor across pipeline boundaries | High |
| Version a model input data contract containing names, order, types, missingness limits, freshness, and source lineage | Makes training/prediction compatibility enforceable and reproducible | Migration/version management overhead | High |
| Add production drift monitoring for null rate, freshness, support range, and cross-sectional constancy, weighted by model dependence | Detects silent distribution shifts early | Threshold tuning and alert fatigue | High |

## 10. Final Conclusions

### A. What is the root cause?

The daily production macro downloader omitted SOX from its source registry. It never called yfinance for `^SOX`, never produced `sox_close`, and therefore caused feature generation to omit `sox_return_1d`. Schema alignment in the long-lived feature store converted that omitted field into `NaN`. A second root cause is the incomplete validation contract, which also omits SOX.

### B. On what exact date did it begin?

The current continuous production issue began on feature date **`2026-06-17`**. `2026-06-16` is the last valid SOX feature date. Earlier isolated all-null dates exist on `2025-04-21` and `2025-04-22`, but they are not the start of the current registry-driven outage.

### C. Why was production allowed to continue?

SOX was not a required source in completeness validation, the macro console did not list it, feature generation silently skipped an absent source, and missing-cell counting did not count an absent column. No model-schema/all-null gate blocked prediction.

### D. Which files/code sections are responsible?

- `src/pipeline/macro_download.py`: `MACRO_TICKERS` and `MACRO_CLOSE_COLUMNS` omit SOX.
- `src/pipeline/feature_source_completeness.py`: source candidates, required sources, and US macro sources omit SOX.
- `src/features/macro_features.py`: maps SOX correctly but silently skips it when `sox_close` is absent.
- `src/pipeline/feature_update.py`: reports null cells and column count but does not enforce the model's expected feature schema.

The feature formula itself is not wrong; the unsafe behavior is silent omission plus missing contract enforcement.

### E. How many prediction days may be affected?

At least **four saved production prediction days** are affected: `2026-06-18`, `2026-06-19`, `2026-06-22`, and `2026-06-23`, totaling **1,392 prediction rows**.

### F. Should the 2026-06-23 prediction be considered trustworthy?

**Not as a fully validated production ranking.** The output is mechanically reproducible and LightGBM handled NaNs as trained, but the model's highest-dependence feature was missing for the entire universe under a distribution far outside normal training prevalence. The result should be quarantined or clearly marked invalid until SOX is restored and the affected date is reassessed. This does not prove every rank is wrong; it means production controls cannot support trusting the published ordering.

### G. Should historical evaluations be re-audited?

**Yes.** Re-audit any evaluation or training period containing `2025-04-21`, `2025-04-22`, or feature dates from `2026-06-17` onward. Confirm whether missing-value rows influenced model fitting, validation metrics, Top10 results, and archived comparisons. Warm-up dates `2023-06-15` and `2023-06-16` should be documented separately rather than treated as unexpected outages.

## Bottom Line

SOX did not fail inside yfinance parsing. Production stopped requesting it. The same omission existed in validation, so the pipeline declared an incomplete 54-feature frame complete and published predictions against a 55-feature model. The registry handoff on `2026-06-17` is the exact start of the continuous failure, and the `2026-06-23` ranking should not be treated as production-valid until the missing source and contract checks are corrected.
