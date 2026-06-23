# Ranking Model and Top10 Scoring Audit

Generated: 2026-06-18

Scope: inspect-only audit of how `target_ranking`, `ranking_score`, `AI Score`, `AI Rank`, `AI Percentile`, and Top10 selection are implemented.

No code, configuration, feature formulas, target formulas, model logic, pipeline behavior, archive logic, production policy, portfolio, backtest, or execution logic was modified.

========================================
Ranking Model Architecture
========================================

## Executive Summary

| Item | Implementation |
|---|---|
| `target_ranking` | Next-trading-day close return from `prev_close` to target close |
| Ranking model class | `lightgbm.LGBMRegressor` |
| Ranking objective | Regression, not LambdaRank |
| Ranking metric | RMSE in config |
| `ranking_score` | Raw LightGBM regression prediction |
| `ranking_score` range | Unbounded, model/data dependent |
| Daily `AI Score` | 0-100 min-max normalization of `ranking_score` within the daily prediction universe |
| Daily `AI Rank` | Sequential rank after sorting `ranking_score` descending |
| Daily `AI Percentile` | `Top round(rank / universe_count * 100)%`, clipped to at least `Top 1%` |
| Daily Top10 rule | Sort by `ranking_score` descending and take first 10 |

The current ranking architecture is a return-regression model. It is not an `LGBMRanker`, does not use `lambdarank`, and does not optimize a ranking-specific LightGBM objective.

## Files Inspected

| Area | Files |
|---|---|
| Target generation | `src/pipeline/training_update.py`, `scripts/create_real_training_dataset.py`, `scripts/build_full_universe_real_dataset.py` |
| Model implementation | `src/models/ranking_model.py`, `src/models/model_factory.py`, `src/models/trainer.py`, `src/models/predictor.py`, `src/pipeline/daily_model.py`, `src/pipeline/daily_prediction.py` |
| Daily Top10 report | `src/pipeline/daily_report.py`, `scripts/run_daily_update_pipeline.py` |
| Validation/readable reports | `scripts/create_full_universe_validation_readable_report.py`, `scripts/create_validation_predictions_readable_report.py` |
| Walk-forward scripts | `scripts/run_full_universe_rolling_walk_forward.py`, `scripts/run_real_rolling_walk_forward.py` |
| Configuration | `configs/model.yaml`, `configs/daily_update.yaml`, `configs/backtest.yaml` |
| Tests | `tests/test_daily_update_pipeline.py`, `tests/test_model_training.py`, `tests/test_prediction.py`, `tests/test_data_leakage.py`, `tests/test_walk_forward.py` |

## Search Terms Used

The repository was searched for:

`target_ranking`, `ranking_score`, `AI Score`, `AI Rank`, `AI Percentile`, `expected_return`, `LightGBM`, `LGBMRanker`, `objective`, `lambdarank`, `rank_xendcg`, `rank`, `predict`, `score`, `percentile`, `argsort`, `rankdata`, `sort_values`.

## Target Definition

### Main Production Daily Target

| Item | Detail |
|---|---|
| Source file | `src/pipeline/training_update.py` |
| Function | `build_target_available_rows(features, clean_ohlcv, update_date)` |
| Target column | `target_ranking` |

Exact implementation pattern:

```python
price_data["feature_date"] = price_data["date"]
price_data["target_date"] = price_data.groupby("ticker")["date"].shift(-1)
price_data["prev_close"] = price_data["close"]
price_data["target_open"] = price_data.groupby("ticker")["open"].shift(-1)
price_data["target_close"] = price_data.groupby("ticker")["close"].shift(-1)

merged["target_ranking"] = merged["target_close"] / merged["prev_close"] - 1.0
merged["target_gap"] = merged["target_open"] / merged["prev_close"] - 1.0
merged["target_intraday"] = merged["target_close"] / merged["target_open"] - 1.0
merged["prediction_horizon"] = 1
merged["date"] = merged["target_date"]
```

Mathematical definition:

```text
target_ranking(ticker, feature_date)
= target_close(ticker, next_trading_date) / close(ticker, feature_date) - 1
```

Meaning:

```text
higher future return
=> higher numeric target_ranking
=> more desirable ranking target
```

`target_ranking` is not an ordinal rank number. It is a raw next-trading-day return.

Leakage guard:

```python
if not (merged["feature_date"] < merged["target_date"]).all():
    raise ValueError("Feature date must be before target date.")
```

### Full Universe / Real Dataset Target Builders

The same economic target appears in standalone dataset scripts:

| Source file | Function | Implementation |
|---|---|---|
| `scripts/create_real_training_dataset.py` | `build_target_frame(ohlcv)` | `target_ranking = close / previous_close - 1` |
| `scripts/build_full_universe_real_dataset.py` | `build_target_frame(clean_ohlcv_df)` | `target_ranking = close / prev_close - 1` |

These scripts express the target from the target-day row by shifting `feature_date` and `prev_close` backward one trading day. The mathematical meaning remains:

```text
feature_date close -> next trading day's close return
```

### 5-Stock Target Example

Assume all five stocks have feature-date close = 100.

| Stock | Target-date close | Future return | `target_ranking` |
|---|---:|---:|---:|
| A | 110 | +10.00% | 0.1000 |
| B | 105 | +5.00% | 0.0500 |
| C | 100 | 0.00% | 0.0000 |
| D | 98 | -2.00% | -0.0200 |
| E | 90 | -10.00% | -0.1000 |

Direction:

```text
A has the highest future return and the highest target_ranking.
E has the lowest future return and the lowest target_ranking.
```

## Ranking Model

### Source Files

| Source file | Class/function | Role |
|---|---|---|
| `src/models/ranking_model.py` | `train_ranking_model` | Thin wrapper around generic model training |
| `src/models/ranking_model.py` | `predict_ranking_score` | Thin wrapper around generic prediction |
| `src/models/model_factory.py` | `create_lightgbm_regressor` | Creates LightGBM regressor |
| `src/models/model_factory.py` | `train_model` | Fits model |
| `src/models/model_factory.py` | `predict_model` | Calls model `predict` |
| `src/pipeline/daily_model.py` | `train_daily_models` | Daily rolling model training |
| `src/pipeline/daily_model.py` | `build_daily_model_spec` | Daily target alias and estimator cap |

### LightGBM Class

The model factory uses:

```python
from lightgbm import LGBMRegressor
return LGBMRegressor(**constructor_params)
```

The ranking model is therefore:

```text
LGBMRegressor
```

It is not:

```text
LGBMRanker
```

### Objective and Metric

From `configs/model.yaml`:

```yaml
ranking_model:
  model_type: "regressor"
  algorithm: "lightgbm"
  target: "target_rank_return"
  prediction_column: "ranking_score"
  objective: "regression"
  metric:
    - rmse
    - l1
  lightgbm_params:
    objective: "regression"
    metric: "rmse"
```

Important finding:

```yaml
future_extensions:
  lambdarank: false
```

No active implementation of `lambdarank`, `rank_xendcg`, or `LGBMRanker` was found in the production model path.

### Important Hyperparameters

Configured ranking model parameters:

| Parameter | Value |
|---|---:|
| `objective` | `regression` |
| `metric` | `rmse` |
| `learning_rate` | `0.03` |
| `num_leaves` | `31` |
| `max_depth` | `-1` |
| `min_data_in_leaf` | `100` |
| `feature_fraction` | `0.8` |
| `bagging_fraction` | `0.8` |
| `bagging_freq` | `1` |
| `lambda_l1` | `0.0` |
| `lambda_l2` | `1.0` |
| `n_estimators` | `2000` in model config |
| `early_stopping_rounds` | `100` in config, removed in no-validation daily fitting |
| `random_state` | `42` |
| `verbosity` | `-1` |

Daily production training applies a cap from `configs/daily_update.yaml`:

```yaml
model_n_estimators_cap: 200
```

In `src/pipeline/daily_model.py`, `n_estimators` is capped to this value for daily training.

### Target Alias in Daily Training

The config still names the ranking target as:

```text
target_rank_return
```

Daily production training uses this bridge:

```python
if model_key == "ranking_model" and target not in train_df.columns and "target_ranking" in train_df.columns:
    target = "target_ranking"
```

Therefore the daily production ranking model trains on `target_ranking` when the daily training dataset contains that column.

## Prediction Output: `ranking_score`

### Where `predict()` Is Called

| Source file | Function | Code path |
|---|---|---|
| `src/models/model_factory.py` | `predict_model` | `trained_model.model.predict(x)` |
| `src/pipeline/daily_prediction.py` | `build_daily_prediction_frame` | assigns result to `ranking_score` |
| `scripts/run_full_universe_rolling_walk_forward.py` | walk-forward prediction path | assigns result to `ranking_score` |
| `scripts/run_real_rolling_walk_forward.py` | walk-forward prediction path | assigns result to `ranking_score` |

Core implementation:

```python
predictions = trained_model.model.predict(x)
return pd.Series(np.asarray(predictions, dtype=float), index=df.index)
```

Daily prediction assignment:

```python
result["ranking_score"] = predict_model(model_bundle.ranking_model, prediction_input)
```

### Meaning of `ranking_score`

`ranking_score` is:

```text
raw LightGBM regression output
```

It is not:

```text
probability
normalized score
percentile
ordinal rank
bounded confidence
```

Because the model is an `LGBMRegressor` trained on next-day return, `ranking_score` is best interpreted as the model's raw predicted ranking-target return signal.

### Range

The range is:

```text
unbounded
model/data dependent
```

LightGBM regression can output negative or positive values outside the observed target range.

### Example

Example raw outputs:

| Stock | `ranking_score` |
|---|---:|
| A | 0.0274 |
| B | 0.0112 |
| C | 0.0040 |
| D | -0.0035 |
| E | -0.0201 |

These are raw regression predictions. They are not percentages until separately formatted or transformed.

## Daily Prediction Frame

Source file: `src/pipeline/daily_prediction.py`

Function: `build_daily_prediction_frame(...)`

Daily prediction output includes:

```python
result["ranking_score"] = predict_model(model_bundle.ranking_model, prediction_input)
result["pred_gap"] = predict_model(model_bundle.gap_model, prediction_input)
result["pred_intraday"] = predict_model(model_bundle.intraday_model, prediction_input)
result["expected_return"] = result["pred_gap"] + result["pred_intraday"]
result["pred_open_price"] = result["prev_close"] * (1.0 + result["pred_gap"])
result["pred_close_price"] = result["pred_open_price"] * (1.0 + result["pred_intraday"])
```

Important distinction:

```text
Daily production expected_return = pred_gap + pred_intraday
```

Some historical validation scripts compute expected return as:

```text
(1 + pred_gap) * (1 + pred_intraday) - 1
```

That difference does not affect daily Top10 selection, because daily Top10 selection is based on `ranking_score`, not `expected_return`.

## AI Score

### Daily Production AI Score

| Item | Detail |
|---|---|
| Source file | `src/pipeline/daily_report.py` |
| Function | `enrich_predictions_for_report(predictions)` |

Exact calculation:

```python
min_score = float(data["ranking_score"].min())
max_score = float(data["ranking_score"].max())
if max_score == min_score:
    data["AI Score"] = 50.0
else:
    data["AI Score"] = ((data["ranking_score"] - min_score) / (max_score - min_score) * 100.0).round(1)
```

Mathematical formula:

```text
AI Score
= 100 * (ranking_score - min(ranking_score)) / (max(ranking_score) - min(ranking_score))
```

Where min and max are computed within the current daily prediction universe.

If all `ranking_score` values are equal:

```text
AI Score = 50.0 for all rows
```

### Interpretation

`AI Score` is a display normalization of `ranking_score`.

It is:

```text
0-100 normalized ranking preference within the current report universe
```

It is not:

```text
expected return
probability of profit
model confidence calibrated across days
```

### Example

| Stock | `ranking_score` | AI Score |
|---|---:|---:|
| A | 0.50 | 100.0 |
| B | 0.25 | 75.0 |
| C | 0.00 | 50.0 |
| D | -0.25 | 25.0 |
| E | -0.50 | 0.0 |

### Validation Report AI Score

In `scripts/create_full_universe_validation_readable_report.py`, AI Score is also min-max normalized:

```python
base["AI Score"] = ((base["ranking_score"] - min_score) / (max_score - min_score) * 100).round(1)
```

Difference:

```text
Daily report: min/max over one prediction universe.
Full-universe validation readable report: min/max over the entire multi-date prediction dataset.
```

## AI Rank

### Daily Production AI Rank

| Item | Detail |
|---|---|
| Source file | `src/pipeline/daily_report.py` |
| Function | `enrich_predictions_for_report(predictions)` |

Exact implementation:

```python
data = predictions.copy()
data = data.sort_values("ranking_score", ascending=False).reset_index(drop=True)
universe_count = len(data)
ranks = pd.Series(range(1, universe_count + 1), index=data.index)
data["AI Rank"] = ranks.astype(str) + " / " + str(universe_count)
```

Sorting direction:

```text
ranking_score descending
```

Rank meaning:

```text
1 / N = highest ranking_score
N / N = lowest ranking_score
```

Tie handling:

```text
No explicit secondary tie-breaker is defined.
Rows are first sorted by ranking_score descending, then sequential ranks are assigned.
For exact ties, ordering follows pandas sort behavior and original row order as retained by that operation; no ticker/date tie-breaker is specified.
```

### Validation Report AI Rank

In `scripts/create_full_universe_validation_readable_report.py`:

```python
base = base.sort_values(["date", "ranking_score"], ascending=[True, False]).reset_index(drop=True)
rank = base.groupby("date")["ranking_score"].rank(method="first", ascending=False).astype(int)
base["AI Rank"] = rank.astype(str) + " / " + count.astype(str)
```

Validation readable reports rank independently by date. Ties use `method="first"`.

## AI Percentile

### Daily Production AI Percentile

| Item | Detail |
|---|---|
| Source file | `src/pipeline/daily_report.py` |
| Function | `enrich_predictions_for_report(predictions)` |

Exact implementation:

```python
percentile = (ranks / universe_count * 100.0).round().clip(lower=1, upper=100).astype(int)
data["AI Percentile"] = "Top " + percentile.astype(str) + "%"
```

Formula:

```text
AI Percentile
= "Top " + round(AI_rank_number / universe_count * 100) + "%"
```

with clipping:

```text
minimum displayed percentile = Top 1%
maximum displayed percentile = Top 100%
```

Range:

```text
Top 1% through Top 100%
```

The value is stored as display text, not numeric.

### 10-Stock Example

For 10 stocks sorted by `ranking_score` descending:

| Stock | `ranking_score` | AI Rank | AI Percentile |
|---|---:|---:|---|
| A | 2.00 | 1 / 10 | Top 10% |
| B | 1.80 | 2 / 10 | Top 20% |
| C | 1.60 | 3 / 10 | Top 30% |
| D | 1.40 | 4 / 10 | Top 40% |
| E | 1.20 | 5 / 10 | Top 50% |
| F | 1.00 | 6 / 10 | Top 60% |
| G | 0.80 | 7 / 10 | Top 70% |
| H | 0.60 | 8 / 10 | Top 80% |
| I | 0.40 | 9 / 10 | Top 90% |
| J | 0.20 | 10 / 10 | Top 100% |

For a larger universe, such as 348 stocks:

```text
rank 1 / 348 -> round(0.287%) -> clipped to Top 1%
rank 2 / 348 -> round(0.575%) -> Top 1%
rank 10 / 348 -> round(2.874%) -> Top 3%
```

### Validation Report AI Percentile

In `scripts/create_full_universe_validation_readable_report.py`, AI Percentile is computed per date:

```python
percentile = (rank / count * 100).round().clip(lower=1, upper=100).astype(int)
base["AI Percentile"] = "Top " + percentile.astype(str) + "%"
```

This matches the daily formula, except validation groups by each prediction date.

## Top10 Selection Rule

### Daily Production Top10

| Item | Detail |
|---|---|
| Source file | `src/pipeline/daily_report.py` |
| Function | `generate_daily_top10_report(...)` |

Code path:

```python
enriched, warnings = enrich_predictions_for_report(predictions)
top10 = enriched.head(10).loc[:, TOP10_COLUMNS].copy()
```

Because `enrich_predictions_for_report` begins with:

```python
data = data.sort_values("ranking_score", ascending=False).reset_index(drop=True)
```

The final Top10 selection key is:

```text
ranking_score descending
```

Top10 is not selected by:

```text
AI Score
expected_return
pred_gap
pred_intraday
combination score
```

However, because daily `AI Score` is a monotonic min-max transform of `ranking_score`, sorting by `AI Score` would produce the same order for non-degenerate data.

### Daily Top10 Output Columns

`TOP10_COLUMNS`:

```python
[
    "prediction_date",
    "AI Rank",
    "AI Percentile",
    "AI Score",
    "ticker",
    "ticker_name",
    "Expected Return(%)",
    "Gap Forecast(%)",
    "Intraday Forecast(%)",
    "prev_close",
    "pred_open_price",
    "pred_close_price",
]
```

Actual-return evaluation columns are intentionally not included in live daily Top10 because actual results are unknown before market close.

### Validation Top10 Reports

In `scripts/create_full_universe_validation_readable_report.py`, validation Top10 is:

```python
top10 = base[base["_rank_number"] <= 10].copy()
```

where `_rank_number` is created from `ranking_score` descending within each date.

So validation Top10 is also selected by:

```text
ranking_score descending per date
```

## End-to-End Example

Assume the model predicts five stocks for one daily prediction universe.

### Model Output

| Stock | Feature row | `ranking_score` |
|---|---|---:|
| A | features at T-1 | 0.030 |
| B | features at T-1 | 0.015 |
| C | features at T-1 | 0.000 |
| D | features at T-1 | -0.005 |
| E | features at T-1 | -0.020 |

### AI Score

Min score = -0.020. Max score = 0.030.

```text
AI Score = 100 * (ranking_score - -0.020) / (0.030 - -0.020)
```

| Stock | `ranking_score` | AI Score |
|---|---:|---:|
| A | 0.030 | 100.0 |
| B | 0.015 | 70.0 |
| C | 0.000 | 40.0 |
| D | -0.005 | 30.0 |
| E | -0.020 | 0.0 |

### AI Rank and AI Percentile

| Stock | AI Rank | AI Percentile |
|---|---|---|
| A | 1 / 5 | Top 20% |
| B | 2 / 5 | Top 40% |
| C | 3 / 5 | Top 60% |
| D | 4 / 5 | Top 80% |
| E | 5 / 5 | Top 100% |

### Top10

For five stocks, all five would be selected. For a normal full universe, the first 10 rows after sorting `ranking_score` descending are selected.

Flow:

```text
Feature row
-> LGBMRegressor raw prediction
-> ranking_score
-> min-max display normalization
-> AI Score
-> descending ranking_score order
-> AI Rank
-> rank / universe_count display
-> AI Percentile
-> first 10 rows
-> Top10 report
```

========================================
Final Audit Conclusions
========================================

## Target Definition

`target_ranking` is a next-trading-day close return:

```text
target_close / prev_close - 1
```

Higher future return produces a higher numeric target.

## Model

The Ranking Model is a LightGBM regression model:

```text
LGBMRegressor(objective="regression", metric="rmse")
```

It is not a ranking-objective model.

## Prediction Output

`ranking_score` is raw LightGBM regression output.

It is:

```text
unbounded
not normalized
not probability
not percentile
```

## AI Score Formula

Daily production:

```text
AI Score = 100 * (ranking_score - min_score) / (max_score - min_score)
```

computed over the current daily prediction universe and rounded to one decimal.

If all scores are equal:

```text
AI Score = 50.0
```

## AI Rank Formula

Daily production:

```text
sort ranking_score descending
AI Rank = row_position / universe_count
```

## AI Percentile Formula

Daily production:

```text
AI Percentile = "Top " + round(rank / universe_count * 100) + "%"
```

with display clipped between `Top 1%` and `Top 100%`.

## Top10 Selection Rule

The final daily Top10 is selected by:

```text
ranking_score descending
head(10)
```

`expected_return`, `pred_gap`, and `pred_intraday` are displayed in the report but are not the daily Top10 selection key.

## Notes and Caveats

1. The config name `target_rank_return` is legacy relative to the daily dataset column `target_ranking`. Daily training bridges this by using `target_ranking` when the config target is missing.
2. Tie handling in daily Top10 has no explicit secondary sort key.
3. Daily `AI Score` is normalized within the daily prediction universe, while full-universe validation readable reports normalize across the full multi-date validation dataset.
4. Daily production `expected_return` uses `pred_gap + pred_intraday`; some validation scripts use compounded expected return. This does not change Top10 selection because Top10 uses `ranking_score`.

========================================
Console Summary
========================================

```text
========================================
Ranking Model Audit

target_ranking ......... OK
LightGBM objective ...... OK
ranking_score ........... OK
AI Score ................ OK
AI Rank ................. OK
AI Percentile ........... OK
Top10 rule .............. OK

Report generated
reports/daily/ranking_model_audit.md
========================================
```
