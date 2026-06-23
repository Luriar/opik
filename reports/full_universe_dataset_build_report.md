# Full Universe Dataset Build Report

- Created at: `2026-06-16T00:20:43.336125+00:00`
- Universe count: `350`
- Raw OHLCV rows: `249541`
- Cleaned OHLCV rows: `249365`
- Invalid OHLCV rows removed: `176`
- Feature shape: `(249365, 57)`
- Feature count: `55`
- Training dataset shape: `(249017, 64)`
- Training feature count: `55`
- Target count: `3`
- Removed target rows: `348`
- 005930 exists: `True`
- Leakage check passed: `True`
- Leakage violation count: `0`
- prev_close is model feature: `False`
- Target columns as model features: `[]`

## Removed Features
- `market_cap_group`
- `market_type`
- `momentum_20d`
- `momentum_5d`
- `sector`
- `momentum_20d_rank_pct`
- `momentum_rank_pct`
- `relative_return_5d_rank_pct`
- `sector_relative_rank_pct`

## Output Files
- `C:\Users\Dell3571\Desktop\PROJECTS\LLM_mini_PJT\AI_Trading_System\data\metadata\ticker_names.csv`
- `C:\Users\Dell3571\Desktop\PROJECTS\LLM_mini_PJT\AI_Trading_System\data\metadata\full_universe_260616.csv`
- `C:\Users\Dell3571\Desktop\PROJECTS\LLM_mini_PJT\AI_Trading_System\data\raw\kr_stock\ohlcv_full_universe_20230615_20260614.parquet`
- `C:\Users\Dell3571\Desktop\PROJECTS\LLM_mini_PJT\AI_Trading_System\data\raw\kr_stock\ohlcv_full_universe_20230615_20260614.csv`
- `C:\Users\Dell3571\Desktop\PROJECTS\LLM_mini_PJT\AI_Trading_System\data\processed\kr_stock\ohlcv_full_universe_clean_20230615_20260614.parquet`
- `C:\Users\Dell3571\Desktop\PROJECTS\LLM_mini_PJT\AI_Trading_System\data\processed\kr_stock\ohlcv_full_universe_clean_20230615_20260614.csv`
- `C:\Users\Dell3571\Desktop\PROJECTS\LLM_mini_PJT\AI_Trading_System\data\processed\macro\macro_clean_20230615_20260614.parquet`
- `C:\Users\Dell3571\Desktop\PROJECTS\LLM_mini_PJT\AI_Trading_System\data\features\full_universe_features_optimized.parquet`
- `C:\Users\Dell3571\Desktop\PROJECTS\LLM_mini_PJT\AI_Trading_System\data\features\full_universe_features_optimized.csv`
- `C:\Users\Dell3571\Desktop\PROJECTS\LLM_mini_PJT\AI_Trading_System\data\processed\full_universe_training_dataset.parquet`
- `C:\Users\Dell3571\Desktop\PROJECTS\LLM_mini_PJT\AI_Trading_System\data\processed\full_universe_training_dataset.csv`
- `C:\Users\Dell3571\Desktop\PROJECTS\LLM_mini_PJT\AI_Trading_System\data\processed\full_universe_training_metadata.json`
- `C:\Users\Dell3571\Desktop\PROJECTS\LLM_mini_PJT\AI_Trading_System\reports\full_universe_dataset_build_report.md`
