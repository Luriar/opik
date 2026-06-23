# Macro Latest Valid Row Policy

## Scope

This policy applies uniformly to NASDAQ, S&P500, VIX, WTI, and USD/KRW data downloaded from yfinance.

## Selection

For each source, the daily pipeline:

1. Downloads a bounded window ending after `target_update_date`.
2. Normalizes ordinary and MultiIndex yfinance columns.
3. Keeps rows whose date is on or before `target_update_date` and whose `Close` is numeric and greater than zero.
4. Selects the latest remaining row.
5. Passes an exact target-date row immediately.
6. Passes a prior row when its calendar age is within `us_macro_max_age_calendar_days`.
7. Fails with the source, expected date, and tolerance reason when no valid row is available.

An invalid target-date row does not block a valid prior row. A missing or non-positive target-date `Close` is retained as a warning and status diagnostic.

## Diagnostics

Daily status JSON records individual actual dates, `macro_source_actual_dates`, `macro_source_expected_dates`, `sources_using_prior_trading_day`, `macro_invalid_target_date_rows`, `us_market_holiday_detected`, and `macro_date_policy`.

Using a prior row because the target-date row is invalid is not classified as a market holiday. Prior rows without an invalid target-date observation may indicate a holiday or non-trading day.

## VIX Example

For target date `2026-06-19`, a VIX row with missing `Close` is skipped. If `2026-06-18` has a positive `Close`, validation passes with actual date `2026-06-18` and reason `prior valid row used; target-date Close missing`.
