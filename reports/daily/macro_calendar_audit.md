# Production Macro Calendar Audit

Audit date: 2026-06-22  
Diagnostic target date: 2026-06-19 (Juneteenth)  
Scope: NASDAQ, S&P500, VIX, WTI, and USD/KRW production macro inputs.

## Executive Summary

The production entry point uses `src.pipeline.macro_download`, not the legacy
`src.pipeline.macro_update` helper. The active downloader requests a five-calendar-day
lookback from Yahoo Finance through `yfinance`, keeps rows dated on or before the
target, sorts by date, and chooses the newest dated row.

That is not yet equivalent to choosing the latest **valid** row. Validation selects
the newest dated row before checking `Close`. A newer row with a missing Close stops
the pipeline even when an older valid row is inside tolerance. The live Juneteenth
diagnostic reproduced this for VIX: Yahoo returned a 2026-06-19 row with `Close=NaN`,
so production validation failed instead of selecting the valid 2026-06-18 close.

The five sources span three calendar families. A single five-calendar-day tolerance
is holiday-tolerant, but it is not calendar-aware:

- NASDAQ and S&P500 follow US equity trading dates.
- VIX is a Cboe index and generally follows the US options/equity holiday calendar.
- WTI `CL=F` follows the CME/NYMEX energy futures calendar and special holiday hours.
- USD/KRW `KRW=X` follows Yahoo's global FX weekday feed, not the US equity calendar.

## Source Inventory

All five sources are downloaded serially with `yfinance.download`, using
`auto_adjust=False`, `threads=False`, and a 30-second per-source timeout. The active
configuration allows a maximum age of five calendar days.

| Feature | Source | Ticker | Exchange / Calendar | Market Type | Current Logic | Expected Logic | Risk |
|---|---|---|---|---|---|---|---|
| `nasdaq_return_1d` | Yahoo Finance, NASDAQ Composite index | `^IXIC` | Yahoo exchange `NIM`, New York; US Equity Calendar | Equity index | Latest dated row `<= target`, then validate Close; max age 5 calendar days | Latest row with valid Close on the latest expected NASDAQ session `<= target` | Medium |
| `sp500_return_1d` | Yahoo Finance, S&P 500 index | `^GSPC` | Yahoo exchange `SNP`, New York; US Equity Calendar | Equity index | Latest dated row `<= target`, then validate Close; max age 5 calendar days | Latest row with valid Close on the latest expected US equity session `<= target` | Medium |
| `vix_change_1d` | Yahoo Finance, Cboe VIX index | `^VIX` | Yahoo exchange `CXI`, Chicago; Cboe index/options calendar, classified as US Equity Calendar | Volatility index | Latest dated row `<= target`, then validate Close; NaN on newest row fails | Filter to finite positive Close first, then choose latest valid Cboe session `<= target` | **Critical** |
| `wti_return_1d` | Yahoo Finance, front-month NYMEX WTI futures | `CL=F` | Yahoo exchange `NYM`, New York; US Futures Calendar | Energy future | Latest dated row `<= target`, then validate Close; same 5-day rule as equities | Use CME/NYMEX product calendar and latest valid settlement/Close available by cutoff | High |
| `usdkrw_return_1d` | Yahoo Finance currency feed | `KRW=X` | Yahoo exchange `CCY`, London timezone; FX Calendar | Spot FX quote | Latest dated row `<= target`, then validate Close; same 5-day rule as equities | Latest valid FX weekday observation available by cutoff, with FX-specific freshness rule | High |

Yahoo metadata observed during the audit identified instrument types as `INDEX`,
`INDEX`, `INDEX`, `FUTURE`, and `CURRENCY`, respectively.

## Calendar And Availability Behavior

### NASDAQ (`^IXIC`)

- **Calendar:** US Equity Calendar (NASDAQ trading holidays).
- **Holiday:** No row is normally published for a full exchange holiday. Juneteenth
  2026 correctly fell back to 2026-06-18.
- **Weekend:** A Saturday/Sunday target selects the prior Friday if it is inside the
  five-calendar-day window.
- **Missing Close:** A missing Close on the newest dated row fails immediately;
  older valid rows are not considered.
- **Selection:** Newest dated row on or before target, not exact-date matching.

### S&P500 (`^GSPC`)

- **Calendar:** US Equity Calendar (index values follow the US cash-equity session).
- **Holiday:** No normal close on a full US equity holiday. Juneteenth selected
  2026-06-18.
- **Weekend:** Selects the prior published session within tolerance.
- **Missing Close:** Newest-row NaN fails instead of falling back.
- **Selection:** Newest dated row on or before target, not exact-date matching.

### VIX (`^VIX`)

- **Calendar:** Cboe volatility index/options calendar; operationally aligned with
  US equity holidays for this policy, but not a NASDAQ/NYSE instrument.
- **Holiday:** Yahoo may return a holiday-dated placeholder. On 2026-06-19 the live
  response contained a dated row with `Close=NaN`.
- **Weekend:** Normally falls back to the prior Cboe session if no placeholder row
  is returned.
- **Missing Close:** **Incorrect current behavior.** The placeholder is selected
  first and causes `MacroDataUnavailableError`; valid 2026-06-18 data is ignored.
- **Selection:** Latest dated row, not latest valid-close row.

### WTI (`CL=F`)

- **Calendar:** CME/NYMEX Energy Futures Calendar.
- **Holiday:** Futures use product-specific closures and shortened sessions. They
  must not inherit the cash-equity calendar. Yahoo published no 2026-06-19 row in
  the audit response, so current logic selected 2026-06-18.
- **Weekend:** Normally selects the latest futures session. Sunday evening trading
  and Yahoo's session-date labeling make naive calendar assumptions risky.
- **Missing Close:** Newest-row NaN fails instead of falling back.
- **Selection:** Newest dated row on or before target, without validating that the
  date is the expected NYMEX product session.

### USD/KRW (`KRW=X`)

- **Calendar:** FX Calendar / Yahoo global currency feed, generally 24x5.
- **Holiday:** A US equity holiday does not imply the FX feed is closed. Yahoo
  provided a valid 2026-06-19 USD/KRW close.
- **Weekend:** Normally selects Friday for Saturday/Sunday targets. Global and local
  banking holidays can reduce liquidity without eliminating all quotes.
- **Missing Close:** Newest-row NaN fails instead of falling back.
- **Selection:** Newest dated row on or before target; no independent FX session or
  publication-cutoff validation.

## Juneteenth Diagnostic

Read-only command path: `yfinance_download` followed by
`validate_macro_source_frame`, target `2026-06-19`, tolerance 5 calendar days.

| Feature | Dates Returned Through Target | Actual Selected Date | Result |
|---|---|---|---|
| NASDAQ | 2026-06-15 through 2026-06-18 | **2026-06-18** | PASS |
| S&P500 | 2026-06-15 through 2026-06-18 | **2026-06-18** | PASS |
| VIX | 2026-06-15 through 2026-06-19; 06-19 Close was NaN | **No date selected**; should be 2026-06-18 | FAIL |
| WTI | 2026-06-15 through 2026-06-18 | **2026-06-18** | PASS |
| USD/KRW | 2026-06-15 through 2026-06-19 | **2026-06-19** | PASS |

The persisted `macro_clean_latest.parquet` currently ends at 2026-06-18 and contains
only the value columns, with no `actual_*_date` or `expected_*_date` provenance
columns. It therefore cannot independently demonstrate the source dates that a new
June 19 production download would choose.

## Implementation Verification

### Active production path

`scripts/run_daily_update_pipeline.py` aliases
`run_production_macro_download` as `run_macro_update`. The active sequence is:

1. Download `[target - tolerance, target + 1 day)` from Yahoo Finance.
2. Normalize timestamps to dates and discard dates after target.
3. Sort remaining rows and select the last row.
4. Enforce age, non-null Close, and positive Close.
5. Store the value in a macro row keyed by the KRX target date.

Therefore the active path no longer requires exact source date equality. However,
step 3 occurs before Close validation, so its docstring's "latest valid Close" claim
is not satisfied.

### Feature completeness path

The completeness checker allows each required US macro source when its latest source
date is on/before target and no more than five calendar days old. KRX remains exact.
If provenance columns are absent, it treats the aligned macro row's `date` as the
actual source date. This legacy fallback can hide forward-filled or differently
calendarized source dates.

### Legacy helper

`src/pipeline/macro_update.py` still supports whole-row prior-date forward fill. It
is exercised by tests and remains callable, but it is not imported by the production
daily script. Invoking it from another entry point would collapse all source calendars
into one prior macro row and lose per-source provenance.

### Feature timing

Macro values are aligned to the KRX row date. Macro returns are then calculated with
`shift(1) / shift(2) - 1`, preserving the T-1 leakage rule. Provenance columns are
removed before feature construction. This audit found no exact-date requirement in
the macro feature formulas themselves.

## Remaining Incorrect-Stop And Integrity Risks

1. **VIX holiday placeholder (critical):** newest dated NaN stops the entire pipeline
   even when an older valid close is available within tolerance.
2. **Same defect for every source:** any newest-row NaN/non-numeric Close prevents
   fallback to an older valid row.
3. **One failure aborts all sources:** download, timeout, schema, or value failure for
   any single ticker terminates macro download and all downstream production steps.
4. **Transient Yahoo failures:** network errors, throttling, crumb/cookie failures,
   or yfinance cache/database errors have no retry or alternate provider and stop
   production. A cache database error was observed in the sandboxed diagnostic.
5. **Uniform calendar policy:** five calendar days is applied to equity indices,
   VIX, energy futures, and FX despite different expected sessions.
6. **No official-calendar check:** a row inside tolerance passes even if the source
   should have published a newer session. A two-to-five-day provider lag can be
   misclassified as a holiday.
7. **Holiday reason overstatement:** any valid prior-day US source is reported as
   "US market holiday"; delayed publication or a source outage produces the same flag.
8. **Legacy provenance fallback:** files without `actual_<source>_date` use the aligned
   macro row date, which may falsely claim same-day availability after forward fill.
9. **Existing target row is not refreshed without force:** the downloader runs, but
   `append_latest_macro` retains an existing target row when `force=False`. A stale,
   malformed, or legacy-aligned row can survive despite a successful fresh download.
10. **Backdated validation reads global latest source date:** completeness takes the
    maximum source date in the file without first restricting aligned rows to the
    requested target. A backdated rerun can fail on later stored provenance rather
    than select the latest source observation on/before that target.
11. **Partial historical nulls are not a direct stop:** `macro_missing_after_update`
    records null counts but the pipeline does not stop on that field. Completeness
    checks only the latest non-null date, so historical holes may reach return
    calculation and create missing features.
12. **Serial timeout amplification:** five sequential sources can consume up to about
    150 seconds before failure, and a timed-out daemon thread continues in process.
13. **Timezone/date-label risk:** timezone information is stripped before date
    normalization. This is usually safe for Yahoo daily bars but is not an explicit
    exchange-session-date conversion, especially for futures and FX.
14. **No cross-source coherence rule:** NASDAQ, S&P500, and VIX may resolve to
    different dates without a warning, even though inconsistent US risk-session data
    may indicate partial publication rather than a holiday.

## Recommended Final Production Policy

### US Equity Features

Apply to NASDAQ, S&P500, and VIX, with VIX using the Cboe session calendar:

- Resolve the expected session from an explicit US equity/Cboe trading calendar.
- Filter rows to date `<= target`, finite numeric Close, and Close `> 0` **before**
  selecting the latest row.
- Require the selected date to equal the expected session. Keep a calendar-day
  tolerance only as a secondary corruption guard, not as holiday detection.
- Require NASDAQ and S&P500 dates to match; require VIX to match its Cboe expected
  session or emit a specific Cboe publication warning.
- Report `holiday`, `provider delay`, and `invalid latest row` as distinct reasons.

### Energy Features

Apply to WTI:

- Use the CME/NYMEX energy product calendar and holiday schedule, including shortened
  sessions and Yahoo's session-date convention.
- Define the value contract explicitly: official settlement is preferable; if Yahoo
  `Close` remains the source, document it as a continuous front-month proxy rather
  than an official same-session spot price.
- Select the latest finite positive Close on/before target, then compare its date to
  the expected published energy session.
- Use an energy-specific freshness threshold and distinguish scheduled closure from
  missing settlement/provider failure.

### FX Features

Apply to USD/KRW:

- Use an FX weekday calendar and an explicit observation cutoff/timezone. Do not
  treat US equity holidays as FX holidays.
- Select the latest finite positive quote on/before the cutoff.
- Expect same-weekday availability except defined provider/global banking exceptions;
  use a short FX-specific tolerance that covers weekends but detects weekday feed lag.
- Record quote timestamp/provider status when available, because a daily FX "Close"
  is provider-defined rather than an exchange settlement.

### Shared Controls

- Persist actual date, expected calendar date, aligned KRX date, age, validation
  result, and reason separately for every source.
- Validate the newly downloaded row even when the aligned target row already exists;
  replace only through an explicit deterministic idempotency policy.
- Add bounded retries and a documented secondary provider or controlled prior-value
  fallback for provider outages. Never label an outage as a holiday.
- Keep KRX alignment and T-1 feature shifts unchanged; calendar resolution belongs
  in source ingestion and completeness validation, not feature formulas.

## Audit Verdict

The exact-date bug is removed for the active production downloader, but production
is not fully calendar-aware. The VIX Juneteenth diagnostic demonstrates that the
current implementation can still stop incorrectly. Risk remains **critical** until
selection filters for a valid Close before choosing the latest row and each source
family uses its own expected-session calendar.
