# Real Market Data Quality Report

## Inputs
- Korean OHLCV: `C:/Users/Dell3571/Desktop/PROJECTS/LLM_mini_PJT/AI_Trading_System/data/raw/kr_stock/ohlcv_20230615_20260614.parquet`
- Macro: `C:/Users/Dell3571/Desktop/PROJECTS/LLM_mini_PJT/AI_Trading_System/data/raw/macro/macro_20230615_20260614.parquet`

## Korean OHLCV Summary
- Existing validator: FAIL
- Validator error: DataValidationError: open must be positive
- Row count: 73670
- Unique ticker count: 102
- Unique trading dates: 728
- Min date: 2023-06-15
- Max date: 2026-06-12
- Ticker 005930 exists: True

### OHLCV Checks
| Check | Result | Detail |
| --- | --- | --- |
| required_columns | PASS | `[]` |
| date_dtype | PASS | `datetime64[us]` |
| ticker_dtype | PASS | `str` |
| duplicate_date_ticker_rows | PASS | `0` |
| positive_open_high_low_close | FAIL | `None` |
| non_negative_volume | PASS | `None` |
| high_greater_equal_low | PASS | `None` |
| high_greater_equal_open_close | FAIL | `None` |
| low_less_equal_open_close | PASS | `None` |
| non_negative_trading_value | PASS | `None` |
| ticker_005930_exists | PASS | `None` |

### OHLCV Missing Values
| Column | Missing Count |
| --- | ---: |
| date | 0 |
| ticker | 0 |
| open | 0 |
| high | 0 |
| low | 0 |
| close | 0 |
| volume | 0 |
| trading_value | 0 |

## Macro Summary
- Existing validator: FAIL
- Validator error: DataValidationError: wti_close must be positive
- Row count: 778
- Unique dates: 778
- Min date: 2023-06-15
- Max date: 2026-06-12

### Macro Checks
| Check | Result | Detail |
| --- | --- | --- |
| required_columns | PASS | `[]` |
| date_dtype | PASS | `datetime64[ms]` |
| non_missing_macro_values_positive | PASS | `{'nasdaq_close': True, 'sox_close': True, 'sp500_close': True, 'usdkrw': True, 'vix_close': True, 'wti_close': True}` |

### Macro Missing Values
| Column | Missing Count |
| --- | ---: |
| date | 0 |
| nasdaq_close | 27 |
| sox_close | 27 |
| sp500_close | 27 |
| vix_close | 26 |
| usdkrw | 1 |
| wti_close | 25 |

## Safe Macro Fill Policy Proposal
- Policy: Sort by date, forward-fill macro columns using only previous available observations, then optionally backward-fill only initial leading gaps from first available historical observations before the validation period.
- Leakage guard: Do not use future same-period values to fill earlier dates inside validation/test windows; fit any imputer policy on train only when used in model pipelines.
- Applied now: False

## Caveats
- Raw data files were not modified.
- Macro strict validation fails because existing validator requires all macro values to be positive and non-missing; missing values are reported above and should be filled downstream with a leakage-safe policy before model use.
- Data max date is 2026-06-12 because 2026-06-14 is not a trading day / market observation date in the downloaded data.

## Concrete Violation Counts

### Korean OHLCV
| Violation | Count |
| --- | ---: |
| open_nonpositive | 140 |
| high_nonpositive | 140 |
| low_nonpositive | 140 |
| close_nonpositive | 0 |
| volume_negative | 0 |
| trading_value_negative | 0 |
| high_less_than_low | 0 |
| high_less_than_open | 0 |
| high_less_than_close | 147 |
| low_greater_than_open | 0 |
| low_greater_than_close | 0 |

Zero OHLC rows: 140 rows across 7 tickers.

Tickers with zero OHLC rows: `['025900', '058470', '086520', '091990', '182400', '214370', '348370']`

### Macro Positivity Excluding Missing Values
| Column | Positive Non-missing Values |
| --- | --- |
| nasdaq_close | True |
| sox_close | True |
| sp500_close | True |
| vix_close | True |
| usdkrw | True |
| wti_close | True |
