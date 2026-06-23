# GitHub Cleanup Plan

This is a recommendation only. No deletion, move, index change, ignore update, or history rewrite was performed.

## Safe To Remove

After making a verified backup, these are reproducible local artifacts that are safe candidates for manual removal:

- `.pytest_cache/`
- All `__pycache__/` directories and `*.pyc` files
- `.venv/` after confirming `requirements.txt` or `pyproject.toml` recreates the environment
- `logs/`
- `outputs/archive/latest/`, because it duplicates the newest dated archive
- Generated `data/daily/` snapshots when no operational retention rule requires them
- Generated `reports/daily/` CSV/XLSX and repeated daily summaries after retaining any required audit evidence

Do not remove them until local recovery/retention needs are confirmed.

## Move To Archive

Move these to external cold storage, object storage, or a private artifact repository rather than public Git:

- `outputs/archive/20260618/`
- `outputs/archive/20260619/`
- `outputs/archive/20260622/`
- `outputs/legacy/`
- Full row-level walk-forward outputs under `outputs/walk_forward_*`
- Full per-window result directories under `reports/window_comparison/window_*/`
- Large historical evaluation CSV/XLSX files needed for compliance or research reproducibility

Store checksums, run IDs, configuration versions, and retention metadata with external archives.

## Keep For GitHub

- `src/`
- `tests/`
- `configs/`, after checking secrets and documenting local paths
- `scripts/`
- `docs/`
- `README.md`, after UTF-8/rendering review
- `AGENTS.md`, coding standards, implementation guide, and useful project status documentation
- `pyproject.toml`, `requirements.txt`, `.env.example`, and `.env.sample`
- Curated evaluation Markdown, JSON summaries, and selected PNG charts
- The four GitHub preparation reports
- Small `data/metadata/` reference files only after redistribution review; stable sample filenames are preferable

Add before publication:

- A license selected by the owner
- `SECURITY.md`
- `CONTRIBUTING.md`
- Optional `CODE_OF_CONDUCT.md`
- CI workflow running tests and linting on a clean clone
- Data provenance/licensing and financial-risk disclaimers

## Keep Local Only

- `data/raw/`
- `data/processed/`
- `data/features/`
- `data/daily/`
- `outputs/`
- `logs/`
- `.venv/`
- Model binaries and full training matrices
- Daily predictions, Top10 spreadsheets, statuses, and execution artifacts
- Any API credentials in `.env`

## Ordered Publication Plan

1. Freeze work and create a full backup, including `.git` and external data.
2. Verify ownership and redistribution rights for market data, ticker names, charts, and derived datasets.
3. Repair document encoding and review README claims, setup steps, and risk disclaimers.
4. Add the reviewed `.gitignore`; explicitly whitelist only approved small fixtures/reference files.
5. Remove generated files from the Git index without deleting local copies.
6. Decide history strategy:
   - **Preferred for a first public release:** create a new clean public repository from the curated tree.
   - **Alternative:** use `git filter-repo` to purge data/output paths and oversized blobs from every ref.
7. Add governance files and CI.
8. Clone the candidate repository into a separate empty directory.
9. Install dependencies, run all tests, and execute the documented dry-run/sample workflow.
10. Re-run secret scanning and large-file scanning across the complete Git history.
11. Review `git status`, tracked-file inventory, and repository size before pushing.

## Acceptance Criteria

- No Git object or tracked file exceeds 100 MB.
- No unintended tracked file exceeds 10 MB.
- Runtime directories and generated tabular exports are ignored.
- No secrets or personal paths are committed.
- Tests pass from a clean clone without production data.
- Public data has documented provenance and redistribution permission.
- README renders correctly and contains setup, architecture, limitations, financial-risk disclaimer, and data requirements.
- Repository license and security/contact policy are explicit.
