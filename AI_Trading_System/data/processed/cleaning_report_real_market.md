# Real Market Data Cleaning Report

## OHLCV
- Raw row count: 73670
- Cleaned row count: 73523
- Removed row count: 147
- Removed tickers: `['025900', '058470', '085660', '086520', '091990', '182400', '214370', '348370']`
- 005930 exists after cleaning: True
- Cleaned date range: 2023-06-15 to 2026-06-12
- Cleaned unique ticker count: 102
- Cleaned unique trading date count: 728
- Existing OHLCV validator: {'passed': True, 'error': None}

## Macro
- Raw row count: 778
- Cleaned row count: 778
- Existing macro validator: {'passed': True, 'error': None}

### Macro Missing Values Before Fill
| Column | Missing Count |
| --- | ---: |
| nasdaq_close | 27 |
| sox_close | 27 |
| sp500_close | 27 |
| vix_close | 26 |
| usdkrw | 1 |
| wti_close | 25 |

### Macro Missing Values After Forward Fill
| Column | Missing Count |
| --- | ---: |
| nasdaq_close | 0 |
| sox_close | 0 |
| sp500_close | 0 |
| vix_close | 0 |
| usdkrw | 0 |
| wti_close | 0 |

### Remaining Leading NaN
| Column | Leading NaN Count |
| --- | ---: |
| nasdaq_close | 0 |
| sox_close | 0 |
| sp500_close | 0 |
| vix_close | 0 |
| usdkrw | 0 |
| wti_close | 0 |

## Outputs
- `C:/Users/Dell3571/Desktop/PROJECTS/LLM_mini_PJT/AI_Trading_System/data/processed/kr_stock/ohlcv_clean_20230615_20260614.parquet`
- `C:/Users/Dell3571/Desktop/PROJECTS/LLM_mini_PJT/AI_Trading_System/data/processed/kr_stock/ohlcv_clean_20230615_20260614.csv`
- `C:/Users/Dell3571/Desktop/PROJECTS/LLM_mini_PJT/AI_Trading_System/data/processed/macro/macro_clean_20230615_20260614.parquet`
- `C:/Users/Dell3571/Desktop/PROJECTS/LLM_mini_PJT/AI_Trading_System/data/processed/macro/macro_clean_20230615_20260614.csv`

Raw data files were not modified.
