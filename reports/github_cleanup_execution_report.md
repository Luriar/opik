# GitHub Cleanup Execution Report

Execution date: 2026-06-22

## Status

Cleanup completed successfully. GitHub's 100 MB file blocker is removed from tracked files and all reachable Git history.

## Safety Preparation

- Backup branch created before destructive operations: `backup-before-github-cleanup`
- Original pre-cleanup commit: `f635f996b850f33d92ccea97cf6b2b0efe4c5679`
- Rewritten backup branch commit: `410f242fb5574501bf11a9944a2349d351a2cba7`
- Final `master` commit after rewrite: `7636331d6ef4c29eba735469c6f4df6402eec4f3`
- The seven local target files were copied to an external temporary safety directory before rewriting and verified by byte size.
- Existing production-code/config/test changes were isolated in a named stash before rewriting and restored afterward without conflict.
- All seven local generated data files still exist on disk and are ignored/untracked.

Because `git-filter-repo` rewrites every ordinary branch, the backup branch was also sanitized. It restores the pre-cleanup source tree but no longer contains the removed large blobs. The external data safety copy is the recovery source for generated data.

## Files Removed From Tracking

- `data/processed/full_universe_training_dataset.csv`
- `data/features/full_universe_features_optimized.csv`
- `data/processed/real_training_dataset.csv`
- `data/features/real_features_20230615_20260614.csv`
- `data/features/real_features_optimized.csv`
- `data/raw/kr_stock/ohlcv_full_universe_20230615_20260614.csv`
- `data/processed/kr_stock/ohlcv_full_universe_clean_20230615_20260614.csv`

Each file is absent from the index but remains present locally.

## Paths Removed From History

History was rewritten with `git-filter-repo` version `a40bce548d2c` using `--invert-paths` for:

- `data/processed/full_universe_training_dataset.csv`
- `data/features/full_universe_features_optimized.csv`
- `data/processed/real_training_dataset.csv`
- `data/features/real_features_20230615_20260614.csv`
- `data/features/real_features_optimized.csv`
- `data/features/model_training_real/gap_model_X_train.csv`
- `data/raw/kr_stock/ohlcv_full_universe_20230615_20260614.csv`
- `data/processed/kr_stock/ohlcv_full_universe_clean_20230615_20260614.csv`

A generated Codex capture ref skipped by `git-filter-repo` retained old trees. That internal capture ref was deleted, then reflogs were expired and garbage collection was repeated. No project branch or release tag was deleted.

## `.gitignore` Changes

Added or confirmed rules for:

- `.venv/`, Python caches, pytest cache, and `.cache/`
- `logs/`, `outputs/`, `outputs/archive/`, and `outputs/legacy/`
- `data/raw/`, `data/processed/`, `data/features/`, and `data/daily/`
- Parquet, CSV, XLSX, PKL, Joblib, and log files
- Markdown exceptions under `reports/` and `docs/`

`git check-ignore` verified runtime/data/file-extension rules. Markdown exception probes under both `reports/` and `docs/` returned not ignored.

## Post-Cleanup Size

- `.git` directory: 64.22 MiB
- Packed reachable objects: 64.15 MiB
- Loose objects: 0
- Garbage objects: 0
- Tracked files: 322
- Current tracked-file total: 107.94 MiB
- Before cleanup, the Git object database was approximately 645.27 MiB.

## Remaining Large Files

- Current tracked files over 10 MiB: none
- Current tracked files over 50 MiB: none
- Reachable history blobs over 100 MiB: none
- One reachable historical blob remains over 50 MiB: `data/features/model_training_real/intraday_model_X_train.csv`, 60.15 MiB

The remaining historical blob is below GitHub's 100 MB hard limit and is not present in the current tracked tree. It was outside the requested removal path list.

## Validation

Command:

```powershell
python -m pytest tests/test_daily_update_pipeline.py tests/test_model_training.py tests/test_prediction.py
```

Result: **168 passed**, with two non-failing local environment warnings concerning CPU detection and CP949 subprocess decoding.

## Working Tree

The working tree is intentionally not clean. Only expected pre-existing production/config/test modifications and untracked Markdown reports remain. Generated daily CSV/XLSX/data artifacts are now ignored.

## Rollback

Requested source-tree rollback command:

```powershell
git checkout backup-before-github-cleanup
```

This changes branches and may conflict with current uncommitted edits. Commit or separately preserve those edits first. Since the backup branch was sanitized by the all-history rewrite, use the external safety copy to recover removed generated data rather than expecting those blobs from Git.

## Push Readiness

The repository satisfies the requested GitHub size checks. It should not be pushed until the remaining intended source changes and Markdown reports are reviewed and committed, and the force-push implications of rewritten history are accepted. No remote push was performed.
