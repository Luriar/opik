"""Daily macro data update helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from src.model.pipeline.config import DailyUpdateConfig
from src.model.pipeline.daily_context import DailyRunContext


@dataclass(frozen=True)
class MacroUpdateResult:
    """Summary of one daily macro update/check."""

    macro_update_mode: str
    macro_source_date: str | None
    macro_rows_added: int
    macro_missing_after_update: dict[str, int]
    daily_macro_snapshot_path: str | None = None
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def run_macro_update(
    config: DailyUpdateConfig,
    context: DailyRunContext,
    dry_run: bool,
    force: bool,
) -> MacroUpdateResult:
    """Run the Part 2B macro update/check step."""
    macro_path = config.resolve_path("macro_file")
    update_date = pd.Timestamp(context.update_date).normalize()
    macro = read_macro_file(macro_path)
    macro_row, mode, source_date, warnings = build_macro_update_row(macro, update_date)

    rows_added = 0
    snapshot_path: str | None = None
    if dry_run:
        warnings.append("macro_update_skipped_in_dry_run")
        updated = preview_macro_append(macro, macro_row, update_date, force)
    else:
        updated, rows_added = safe_append_macro(macro, macro_row, update_date, force)
        write_macro_file(macro_path, updated)
        snapshot_path = write_daily_macro_snapshot(config, update_date, macro_row)

    missing_after = {
        column: int(value)
        for column, value in updated.isna().sum().items()
        if int(value) > 0
    }
    if mode == "existing" and not force:
        warnings.append("macro_update_date_already_existed")
    return MacroUpdateResult(
        macro_update_mode=mode,
        macro_source_date=source_date.date().isoformat() if source_date is not None else None,
        macro_rows_added=rows_added,
        macro_missing_after_update=missing_after,
        daily_macro_snapshot_path=snapshot_path,
        warnings=warnings,
    )


def read_macro_file(path: Path) -> pd.DataFrame:
    """Read and normalize macro parquet."""
    if not path.exists():
        raise FileNotFoundError(f"Missing macro file: {path}")
    data = pd.read_parquet(path)
    if "date" not in data.columns:
        raise ValueError(f"Macro file missing date column: {path}")
    data = data.copy()
    data["date"] = pd.to_datetime(data["date"]).dt.normalize()
    return data.sort_values("date").reset_index(drop=True)


def build_macro_update_row(
    macro: pd.DataFrame,
    update_date: pd.Timestamp,
) -> tuple[pd.DataFrame, str, pd.Timestamp | None, list[str]]:
    """Return one update_date macro row from existing data or prior-only ffill."""
    data = macro.sort_values("date").reset_index(drop=True)
    existing = data[data["date"].eq(update_date)].tail(1)
    if not existing.empty:
        return existing.copy(), "existing", update_date, []

    prior = data[data["date"].lt(update_date)]
    if prior.empty:
        raise ValueError(f"No prior macro row exists before {update_date.date()}")
    source = prior.tail(1).copy()
    source_date = pd.Timestamp(source["date"].iloc[0])
    row = source.copy()
    row.loc[:, "date"] = update_date
    warnings = [f"macro_forward_filled_from_prior_date_{source_date.date().isoformat()}"]
    return row, "forward_fill_prior", source_date, warnings


def safe_append_macro(
    macro: pd.DataFrame,
    macro_row: pd.DataFrame,
    update_date: pd.Timestamp,
    force: bool,
) -> tuple[pd.DataFrame, int]:
    """Append or replace a macro update row without duplicate dates."""
    base = macro.copy()
    row = macro_row.copy()
    row["date"] = pd.to_datetime(row["date"]).dt.normalize()
    if len(row) != 1:
        raise ValueError("macro_row must contain exactly one row")
    exists = base["date"].eq(update_date).any()
    if exists and not force:
        updated = base.copy()
        rows_added = 0
    else:
        base = base[~base["date"].eq(update_date)].copy()
        updated = pd.concat([base, row], ignore_index=True)
        rows_added = 1
    updated = updated.sort_values("date").reset_index(drop=True)
    if updated.duplicated(subset=["date"]).any():
        raise ValueError("Duplicate macro date rows after append")
    return updated, rows_added


def preview_macro_append(
    macro: pd.DataFrame,
    macro_row: pd.DataFrame,
    update_date: pd.Timestamp,
    force: bool,
) -> pd.DataFrame:
    """Return what macro data would look like without writing."""
    updated, _ = safe_append_macro(macro, macro_row, update_date, force)
    return updated


def write_macro_file(path: Path, macro: pd.DataFrame) -> None:
    """Write updated macro parquet."""
    path.parent.mkdir(parents=True, exist_ok=True)
    macro.sort_values("date").to_parquet(path, index=False)


def write_daily_macro_snapshot(
    config: DailyUpdateConfig,
    update_date: pd.Timestamp,
    macro_row: pd.DataFrame,
) -> str:
    """Write daily macro CSV snapshot."""
    compact = update_date.strftime("%Y%m%d")
    path = config.resolve_path("daily_processed_dir") / f"macro_clean_{compact}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    macro_row.sort_values("date").to_csv(path, index=False, encoding="utf-8-sig")
    return str(path)
