# GitHub Large Files Audit

Audit date: 2026-06-22

Thresholds use binary MiB: large `>10 MiB`, very large `>50 MiB`, blocker `>100 MiB`. GitHub blocks ordinary Git files above 100 MB. No file was changed.

## Working Tree

| Size | Class | Tracked | Path | Recommendation |
|---:|---|---|---|---|
| 276.08 MiB | Blocker | Yes | `data/processed/full_universe_training_dataset.csv` | Remove from index and history; regenerate locally/object storage. |
| 253.54 MiB | Blocker | Yes | `data/features/full_universe_features_optimized.csv` | Remove from index and history; regenerate locally/object storage. |
| 136.50 MiB | Blocker | No | `outputs/archive/20260619/training/rolling_train_20260619.csv` | Keep local/external archive; ignore. |
| 136.46 MiB | Blocker | No | `outputs/archive/20260622/training/rolling_train_20260622.csv` | Keep local/external archive; ignore. |
| 136.46 MiB | Blocker | No | `outputs/archive/latest/training/rolling_train_20260622.csv` | Ignore regenerated duplicate. |
| 97.85 MiB | Very large | No | `outputs/archive/20260618/training/rolling_train_20260618.csv` | Keep local/external archive. |
| 81.85 MiB | Very large | Yes | `data/processed/real_training_dataset.csv` | Remove from Git; regenerate locally. |
| 81.00 MiB | Very large | Yes | `data/features/real_features_20230615_20260614.csv` | Remove from Git; regenerate locally. |
| 75.33 MiB | Very large | Yes | `data/features/real_features_optimized.csv` | Remove from Git; regenerate locally. |
| 71.66 MiB | Very large | No | `data/processed/full_universe_training_dataset.parquet` | Keep local; already covered by Parquet ignore. |
| 67.96 MiB | Very large | No | `data/features/full_universe_features_optimized.parquet` | Keep local; already covered by Parquet ignore. |
| 60.20 MiB | Very large | No | `outputs/legacy/data__features__model_training_real/gap_model_X_train.csv` | External archive only. |
| 60.20 MiB | Very large | No | `outputs/legacy/data__features__model_training_real/intraday_model_X_train.csv` | External archive only. |
| 60.20 MiB | Very large | No | `outputs/legacy/data__features__model_training_real/ranking_model_X_train.csv` | External archive only. |
| 38.30 MiB | Large | No | `outputs/archive/20260619/training/rolling_train_20260619.parquet` | External archive only. |
| 38.29 MiB | Large | No | `outputs/archive/20260622/training/rolling_train_20260622.parquet` | External archive only. |
| 38.29 MiB | Large | No | `outputs/archive/latest/training/rolling_train_20260622.parquet` | Ignore regenerated duplicate. |
| 27.86 MiB | Large | No | `outputs/archive/20260618/training/rolling_train_20260618.parquet` | External archive only. |
| 23.39 MiB | Large | No | `data/processed/real_training_dataset.parquet` | Keep local; ignore. |
| 23.09 MiB | Large | No | `data/features/real_features_20230615_20260614.parquet` | Keep local; ignore. |
| 21.92 MiB | Large | No | `data/features/real_features_optimized.parquet` | Keep local; ignore. |
| 14.77 MiB | Large | Yes | `data/raw/kr_stock/ohlcv_full_universe_20230615_20260614.csv` | Remove from Git; raw market data should remain local. |
| 14.76 MiB | Large | Yes | `data/processed/kr_stock/ohlcv_full_universe_clean_20230615_20260614.csv` | Remove from Git; regenerate locally. |

## Git History

The loose object database is 645.27 MiB. These historical blobs exceed 10 MiB:

| Size | Class | Object | Historical path |
|---:|---|---|---|
| 275.85 MiB | Blocker | `e47d1c6e...` | `data/processed/full_universe_training_dataset.csv` |
| 253.30 MiB | Blocker | `e84fee47...` | `data/features/full_universe_features_optimized.csv` |
| 81.78 MiB | Very large | `e72d68ec...` | `data/processed/real_training_dataset.csv` |
| 80.93 MiB | Very large | `b14b3e93...` | `data/features/real_features_20230615_20260614.csv` |
| 75.26 MiB | Very large | `175a7811...` | `data/features/real_features_optimized.csv` |
| 60.15 MiB | Very large | `522bb43f...` | `data/features/model_training_real/gap_model_X_train.csv` |
| 14.53 MiB | Large | `15438697...` | `data/raw/kr_stock/ohlcv_full_universe_20230615_20260614.csv` |
| 14.52 MiB | Large | `bfcbf583...` | `data/processed/kr_stock/ohlcv_full_universe_clean_20230615_20260614.csv` |

## Required Remediation Before Publication

1. Back up the repository and runtime data outside the working tree.
2. Add ignore rules before staging further files.
3. Stop tracking generated data with a reviewed `git rm --cached` operation; do not delete local copies.
4. Choose either a clean public-history repository or a reviewed `git filter-repo` history rewrite.
5. Re-scan every ref after rewriting; deleting only the current files is insufficient.
6. Consider release assets, object storage, or Git LFS only for intentionally published artifacts with clear licensing. Generated training matrices should normally not use LFS.
