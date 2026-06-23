"""Feature source completeness checks for production daily updates."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.model.pipeline.config import DailyUpdateConfig


SOURCE_COLUMN_CANDIDATES: dict[str, tuple[str, ...]] = {
    "nasdaq": ("nasdaq_close", "nasdaq"),
    "sp500": ("sp500_close", "sp500"),
    "vix": ("vix_close", "vix"),
    "wti": ("wti_close", "wti"),
    "usdkrw": ("usdkrw", "usdkrw_close"),
    "us10y": ("us10y", "us10y_close", "us10y_yield"),
    "gold": ("gold", "gold_close"),
    "dxy": ("dxy", "dxy_close"),
}
REQUIRED_SOURCES: tuple[str, ...] = ("krx", "nasdaq", "sp500", "vix", "wti", "usdkrw")
OPTIONAL_SOURCES: tuple[str, ...] = ("us10y", "gold", "dxy")


@dataclass(frozen=True)
class FeatureSourceCompletenessChecker:
    """Check whether all configured feature sources are current."""

    config: DailyUpdateConfig
    expected_date: str | pd.Timestamp

    def check(self) -> dict[str, object]:
        """Return source completeness status for expected_date."""
        expected = pd.Timestamp(self.expected_date).normalize()
        source_dates = {
            "krx": latest_date_from_file(self.config.resolve_path("clean_ohlcv_file")),
            **latest_macro_source_dates(self.config.resolve_path("macro_file")),
        }
        enabled_sources = set(REQUIRED_SOURCES)
        if bool(self.config.values.get("enable_us10y_check", False)):
            enabled_sources.add("us10y")
        if bool(self.config.values.get("enable_gold_check", False)):
            enabled_sources.add("gold")
        if bool(self.config.values.get("enable_dxy_check", False)):
            enabled_sources.add("dxy")

        result: dict[str, object] = {"expected_date": expected.date().isoformat()}
        failures: list[str] = []
        failed_sources: list[str] = []
        for source in (*REQUIRED_SOURCES, *OPTIONAL_SOURCES):
            source_date = source_dates.get(source)
            result[f"{source}_check_enabled"] = source in enabled_sources
            available = source_date == expected if source in enabled_sources else True
            result[f"{source}_available"] = bool(available)
            result[f"actual_{source}_date"] = source_date.date().isoformat() if source_date is not None else None
            if source not in enabled_sources:
                result[f"{source}_skipped_reason"] = "disabled in config"
            if source in enabled_sources and not available:
                actual = source_date.date().isoformat() if source_date is not None else "missing"
                failures.append(f"{source}: expected {expected.date().isoformat()}, actual {actual}")
                failed_sources.append(source)

        result["all_available"] = not failures
        result["failure_reason"] = "; ".join(failures)
        result["failed_sources"] = failed_sources
        return result


def latest_date_from_file(path: Path) -> pd.Timestamp | None:
    """Return latest date from a parquet/csv file with a date column."""
    if not path.exists():
        return None
    data = read_table(path)
    if data.empty or "date" not in data.columns:
        return None
    dates = pd.to_datetime(data["date"], errors="coerce").dropna()
    if dates.empty:
        return None
    return pd.Timestamp(dates.max()).normalize()


def latest_macro_source_dates(path: Path) -> dict[str, pd.Timestamp | None]:
    """Return latest non-null date for each macro source."""
    result = {source: None for source in SOURCE_COLUMN_CANDIDATES}
    if not path.exists():
        return result
    data = read_table(path)
    if data.empty or "date" not in data.columns:
        return result
    data = data.copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.normalize()
    for source, candidates in SOURCE_COLUMN_CANDIDATES.items():
        column = next((candidate for candidate in candidates if candidate in data.columns), None)
        if column is None:
            continue
        valid_dates = data.loc[data[column].notna(), "date"].dropna()
        if not valid_dates.empty:
            result[source] = pd.Timestamp(valid_dates.max()).normalize()
    return result


def read_table(path: Path) -> pd.DataFrame:
    """Read parquet or CSV by extension."""
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)
