"""Date context logic for the daily update pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date
from pathlib import Path

import pandas as pd

from src.pipeline.config import DailyUpdateConfig


@dataclass(frozen=True)
class DailyRunContext:
    """Resolved date context for one daily pipeline run."""

    run_date: str
    as_of_date: str
    latest_clean_data_date: str | None
    target_update_date: str
    update_date: str
    prediction_date: str
    dry_run: bool
    skip_download: bool
    force: bool
    warnings: list[str] = field(default_factory=list)


def build_daily_run_context(
    config: DailyUpdateConfig,
    as_of_date: str | None,
    dry_run: bool,
    skip_download: bool,
    force: bool,
) -> DailyRunContext:
    """Build date context for a daily pipeline run."""
    run_date = date.today().isoformat()
    reference_date = pd.Timestamp(as_of_date or run_date).normalize()
    warnings: list[str] = []

    latest_clean = latest_clean_data_date(config.resolve_path("clean_ohlcv_file"))
    if latest_clean is None:
        warnings.append("clean_ohlcv_file_missing_or_empty_using_business_day_calendar")
    target_update = target_update_date(reference_date)
    prediction = next_business_day(target_update)

    return DailyRunContext(
        run_date=run_date,
        as_of_date=reference_date.date().isoformat(),
        latest_clean_data_date=latest_clean.date().isoformat() if latest_clean is not None else None,
        target_update_date=target_update.date().isoformat(),
        update_date=target_update.date().isoformat(),
        prediction_date=prediction.date().isoformat(),
        dry_run=dry_run,
        skip_download=skip_download,
        force=force,
        warnings=warnings,
    )


def fallback_to_latest_clean_context(context: DailyRunContext) -> DailyRunContext:
    """Return a context that uses latest clean data as an old-data fallback."""
    if context.latest_clean_data_date is None:
        return context
    update = pd.Timestamp(context.latest_clean_data_date).normalize()
    prediction = next_business_day(update)
    warnings = [*context.warnings, "OLD_DATA: latest market data unavailable; using latest existing clean data"]
    return replace(
        context,
        update_date=update.date().isoformat(),
        prediction_date=prediction.date().isoformat(),
        warnings=warnings,
    )


def latest_clean_data_date(path: Path) -> pd.Timestamp | None:
    """Return max date from clean OHLCV if the file exists."""
    if not path.exists():
        return None
    data = pd.read_parquet(path, columns=["date"])
    if data.empty:
        return None
    return pd.to_datetime(data["date"]).max().normalize()


def load_trading_dates(path: Path) -> list[pd.Timestamp]:
    """Load available trading dates from clean OHLCV."""
    if not path.exists():
        return []
    data = pd.read_parquet(path, columns=["date"])
    if data.empty:
        return []
    dates = pd.to_datetime(data["date"]).drop_duplicates().sort_values()
    return [pd.Timestamp(item).normalize() for item in dates]


def previous_business_day(reference_date: pd.Timestamp) -> pd.Timestamp:
    """Return latest pandas business day not after the reference date."""
    if reference_date.dayofweek < 5:
        return reference_date
    return reference_date - pd.offsets.BDay(1)


def target_update_date(reference_date: pd.Timestamp) -> pd.Timestamp:
    """Return the previous business day to update before market open."""
    return previous_business_day(reference_date - pd.Timedelta(days=1))


def next_business_day(update_date: pd.Timestamp) -> pd.Timestamp:
    """Return next pandas business day after update_date."""
    return pd.Timestamp(update_date + pd.offsets.BDay(1)).normalize()
