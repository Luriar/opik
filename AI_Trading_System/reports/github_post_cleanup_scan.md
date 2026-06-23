# GitHub Post-Cleanup Scan

Scan date: 2026-06-22

## Result

| Check | Result | Evidence |
|---|---|---|
| No tracked file over 100 MiB | PASS | No tracked file exceeds 10 MiB. |
| No tracked file over 50 MiB unless justified | PASS | No current tracked file exceeds 10 MiB. |
| No reachable history blob over 100 MiB | PASS | Largest reachable blob is 60.15 MiB. |
| Requested paths absent from all history | PASS | All eight `git log --all -- <path>` checks returned no commits. |
| `.gitignore` runtime rules work | PASS | All requested data, output, cache, and extension probes are ignored. |
| Report/document Markdown allowed | PASS | `reports/**/*.md` and `docs/**/*.md` probes are not ignored. |
| Local generated data preserved | PASS | All seven working-tree target files exist and are untracked/ignored. |
| Git object cleanup | PASS | 0 loose objects, 0 garbage objects, one 64.15 MiB pack. |
| Validation tests | PASS | 168 passed. |
| Working-tree state expected | PASS | Restored pre-existing edits plus untracked Markdown reports remain. |

## Repository Metrics

- `.git` directory: 64.22 MiB
- Reachable packed objects: 64.15 MiB
- Tracked file count: 322
- Tracked-file total: 107.94 MiB
- Current tracked files over 10 MiB: none

## Remaining Historical Large Blob

| Size | Object | Historical path | Assessment |
|---:|---|---|---|
| 60.15 MiB | `522bb43f38c8a11d7e303280b68a0c58362f8bbd` | `data/features/model_training_real/intraday_model_X_train.csv` | Below GitHub's 100 MB limit; not currently tracked; outside requested removal list. |

## Requested Paths

The following paths are absent from all reachable history and the current index:

- `data/processed/full_universe_training_dataset.csv`
- `data/features/full_universe_features_optimized.csv`
- `data/processed/real_training_dataset.csv`
- `data/features/real_features_20230615_20260614.csv`
- `data/features/real_features_optimized.csv`
- `data/features/model_training_real/gap_model_X_train.csv`
- `data/raw/kr_stock/ohlcv_full_universe_20230615_20260614.csv`
- `data/processed/kr_stock/ohlcv_full_universe_clean_20230615_20260614.csv`

Seven paths with current local data remain on disk. The historical-only `gap_model_X_train.csv` path is absent locally.

## Status Assessment

GitHub hard-size blockers are removed. The repository is technically pushable after reviewing and committing the expected working-tree changes. A history rewrite requires a force push if replacing an existing remote history; no remote is currently configured and no push was performed.
