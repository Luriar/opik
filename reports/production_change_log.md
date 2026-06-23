# Production Change

## Previous

Rolling Train Window:

250 Trading Days

## New

Rolling Train Window:

350 Trading Days

## Reason

Window Comparison Study

The completed rolling window comparison study showed that the 350 Trading Day
rolling window outperformed the 150D and 250D alternatives on the main
production portfolio criteria:

- Portfolio CAGR
- Portfolio Sharpe
- Top10 Average Return

The 350D window also maintained acceptable drawdown in the latest 90-trading-day
walk-forward comparison.

## Results

- 150D: fastest training window, weaker portfolio performance.
- 250D: balanced baseline, best Top10 hit rate in the comparison.
- 350D: strongest portfolio CAGR, Sharpe, and Top10 Average Return.

Recommended Production Window:

350 Trading Days

## Files Modified

- `configs/daily_update.yaml`
- `src/pipeline/archive.py`
- `tests/test_daily_update_pipeline.py`
- `reports/production_change_log.md`

## Validation

- Dry-run command: `.\run_daily_update_dry_run.bat`
- Test command: `python -m pytest tests/test_daily_update_pipeline.py tests/test_model_training.py tests/test_prediction.py`

Validation results:

- Dry run: PASSED
- Dry-run status preview showed `rolling_train_days: 350`
- Dry-run status preview showed `rolling_train_unique_dates: 350`
- Tests: PASSED, 155 passed

## Rollback

Set:

```yaml
rolling_train_days: 250
```

in:

```text
configs/daily_update.yaml
```

No feature formulas, target formulas, model architecture, prediction logic,
Top10 logic, archive logic, macro download logic, or feature source
completeness logic were changed.
