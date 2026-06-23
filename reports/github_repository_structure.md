# Recommended GitHub Repository Structure

## Production Code

```text
AI_Trading_System/
|-- README.md
|-- LICENSE
|-- SECURITY.md
|-- CONTRIBUTING.md
|-- AGENTS.md
|-- pyproject.toml
|-- requirements.txt
|-- .env.example
|-- .gitignore
|-- configs/
|-- scripts/
|-- src/
|   |-- data/
|   |-- features/
|   |-- models/
|   |-- validation/
|   |-- portfolio/
|   |-- execution/
|   |-- backtest/
|   |-- pipeline/
|   `-- utils/
`-- tests/
```

Keep root batch launchers only if they are documented and sanitized. Prefer one cross-platform CLI entry point plus optional Windows wrappers.

## Research

```text
research/
|-- notebooks/          # cleared, curated notebooks only
|-- prompts/            # optional development provenance
`-- experiments/        # lightweight experiment definitions, no result matrices
```

The current `notebooks/` directory is empty. `prompts/` is optional and can remain at root if preserving existing paths is preferable.

## Documentation

```text
docs/
|-- 01_system_architecture.md
|-- 02_universe.md
|-- 03_targets.md
|-- 04_models.md
|-- 05_feature_library.md
|-- 06_data_leakage_rules.md
|-- 07_walk_forward_validation.md
|-- 08_backtest.md
|-- 09_portfolio.md
|-- 10_execution.md
|-- operations/
|-- audits/
`-- examples/
```

Move durable policy/audit Markdown from `reports/` into `docs/audits/`. Keep generated daily summaries out of documentation. Review all text as UTF-8 and repair visible mojibake before publication.

## Evaluation Reports

```text
reports/
|-- README.md
|-- model_evaluation_90d/
|   |-- evaluation_report.md
|   |-- metrics_summary.json
|   `-- charts/
|-- model_evaluation_150d/
|   |-- evaluation_report.md
|   |-- metrics_summary.json
|   `-- charts/
`-- window_comparison/
    |-- window_comparison_report.md
    |-- rolling_window_comparison_150_250_350.md
    `-- charts_150_250_350/
```

Publish compact summaries and selected PNG charts. Do not publish row-level predictions, portfolio returns, CSV/XLSX duplicates, or repeated per-window datasets in normal Git history.

## Runtime Data

```text
data/                     # local, ignored
|-- raw/
|-- processed/
|-- features/
|-- daily/
`-- metadata/             # reviewed public reference files or local files

outputs/                  # local, ignored
|-- models/
|-- predictions/
|-- portfolio/
|-- execution/
|-- metrics/
|-- status/
`-- archive/

logs/                     # local, ignored
archive/                  # external/local cold storage, ignored
```

Add placeholder `.gitkeep` files only when empty directory creation is operationally necessary. Prefer code that creates runtime directories automatically.

## Fresh-Clone Contract

A public clone should be able to:

1. Install dependencies without local `.venv` contents.
2. Run unit tests without private market datasets.
3. Run a documented sample or dry-run path using synthetic/small redistributable fixtures.
4. Explain where production data must be placed and its required schema.
5. Fail clearly when production-only data or credentials are absent.

The current production config references local Parquet datasets that will be ignored. Publish a sample config or data manifest rather than weakening the ignore policy.
