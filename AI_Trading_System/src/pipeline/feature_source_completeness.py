"""Feature source completeness checks for production daily updates."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.pipeline.config import DailyUpdateConfig


SOURCE_COLUMN_CANDIDATES: dict[str, tuple[str, ...]] = {
    "nasdaq": ("nasdaq_close", "nasdaq"),
    "sox": ("sox_close", "sox"),
    "sp500": ("sp500_close", "sp500"),
    "vix": ("vix_close", "vix"),
    "wti": ("wti_close", "wti"),
    "usdkrw": ("usdkrw", "usdkrw_close"),
    "us10y": ("us10y", "us10y_close", "us10y_yield"),
    "gold": ("gold", "gold_close"),
    "dxy": ("dxy", "dxy_close"),
}
REQUIRED_SOURCES: tuple[str, ...] = ("krx", "nasdaq", "sox", "sp500", "vix", "wti", "usdkrw")
OPTIONAL_SOURCES: tuple[str, ...] = ("us10y", "gold", "dxy")
US_MACRO_SOURCES: tuple[str, ...] = ("nasdaq", "sox", "sp500", "vix", "wti", "usdkrw")


@dataclass(frozen=True)
class FeatureSourceCompletenessChecker:
    """Check whether all configured feature sources are current."""

    config: DailyUpdateConfig
    expected_date: str | pd.Timestamp

    def check(self) -> dict[str, object]:
        """Return source completeness status for expected_date."""
        expected = pd.Timestamp(self.expected_date).normalize()
        macro_path = self.config.resolve_path("macro_file")
        sox_feature = sox_feature_diagnostics(self.config.resolve_path("feature_file"), expected)
        source_dates = {
            "krx": latest_date_from_file(self.config.resolve_path("clean_ohlcv_file")),
            **latest_macro_source_dates(macro_path),
        }
        enabled_sources = set(REQUIRED_SOURCES)
        if bool(self.config.values.get("enable_us10y_check", False)):
            enabled_sources.add("us10y")
        if bool(self.config.values.get("enable_gold_check", False)):
            enabled_sources.add("gold")
        if bool(self.config.values.get("enable_dxy_check", False)):
            enabled_sources.add("dxy")

        max_age_days = int(self.config.values.get("us_macro_max_age_calendar_days", 5))
        result: dict[str, object] = {
            "expected_date": expected.date().isoformat(),
            "macro_date_policy": (
                f"KRX must equal target_update_date; US macro latest date on/before target "
                f"within {max_age_days} calendar days"
            ),
            "sox_close_present": source_column_present(macro_path, SOURCE_COLUMN_CANDIDATES["sox"]),
            **sox_feature,
        }
        failures: list[str] = []
        failed_sources: list[str] = []
        for source in (*REQUIRED_SOURCES, *OPTIONAL_SOURCES):
            source_date = source_dates.get(source)
            result[f"{source}_check_enabled"] = source in enabled_sources
            if source not in enabled_sources:
                available = True
            elif source == "krx":
                available = source_date == expected
            elif source in US_MACRO_SOURCES:
                available = source_date is not None and 0 <= int((expected - source_date).days) <= max_age_days
            else:
                available = source_date == expected
            source_failure_reason: str | None = None
            if source == "sox":
                if not bool(result["sox_close_present"]):
                    available = False
                    source_failure_reason = "sox_close missing"
                elif not available:
                    actual = source_date.date().isoformat() if source_date is not None else "missing"
                    source_failure_reason = f"expected {expected.date().isoformat()}, actual {actual}"
                elif bool(result["sox_feature_date_present"]) and not bool(result["sox_return_present"]):
                    available = False
                    source_failure_reason = "sox_return_1d missing"
                elif bool(result["sox_feature_date_present"]) and int(result["sox_return_non_null_count"]) == 0:
                    available = False
                    source_failure_reason = (
                        "SOX close exists but sox_return_1d cannot be computed yet because "
                        "prior SOX history is missing."
                    )
                result["sox_failure_reason"] = source_failure_reason
            result[f"{source}_available"] = bool(available)
            result[f"actual_{source}_date"] = source_date.date().isoformat() if source_date is not None else None
            result[f"expected_{source}_date"] = expected.date().isoformat()
            if source not in enabled_sources:
                result[f"{source}_skipped_reason"] = "disabled in config"
            if source in enabled_sources and not available:
                actual = source_date.date().isoformat() if source_date is not None else "missing"
                failure_detail = source_failure_reason or f"expected {expected.date().isoformat()}, actual {actual}"
                failures.append(f"{source}: {failure_detail}")
                failed_sources.append(source)

        result["all_available"] = not failures
        result["failure_reason"] = "; ".join(failures)
        result["failed_sources"] = failed_sources
        prior_day_sources = [
            source
            for source in US_MACRO_SOURCES
            if bool(result.get(f"{source}_available"))
            and source_dates.get(source) is not None
            and source_dates[source] < expected
        ]
        result["us_market_holiday_detected"] = bool(prior_day_sources)
        result["us_market_holiday_reason"] = "US market holiday or non-trading day" if prior_day_sources else None
        result["sources_using_prior_trading_day"] = prior_day_sources
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
    date_column = resolve_column(data, ("date",))
    if date_column is None:
        return result
    data[date_column] = pd.to_datetime(data[date_column], errors="coerce").dt.normalize()
    for source, candidates in SOURCE_COLUMN_CANDIDATES.items():
        column = resolve_column(data, candidates)
        if column is None:
            continue
        actual_date_column = resolve_column(data, (f"actual_{source}_date",))
        source_date_column = actual_date_column or date_column
        source_dates = pd.to_datetime(data[source_date_column], errors="coerce").dt.normalize()
        valid_dates = source_dates.loc[data[column].notna()].dropna()
        if not valid_dates.empty:
            result[source] = pd.Timestamp(valid_dates.max()).normalize()
    return result


def source_column_present(path: Path, candidates: tuple[str, ...]) -> bool:
    """Return whether a source column exists, independent of external casing."""
    if not path.exists():
        return False
    return resolve_column(read_table(path), candidates) is not None


def sox_feature_diagnostics(path: Path, expected: pd.Timestamp) -> dict[str, object]:
    """Describe SOX return availability for an already-built expected-date feature slice."""
    result: dict[str, object] = {
        "sox_feature_date_present": False,
        "sox_return_present": False,
        "sox_return_non_null_count": 0,
        "sox_failure_reason": None,
    }
    if not path.exists():
        return result
    data = read_table(path)
    date_column = resolve_column(data, ("date",))
    if date_column is None:
        return result
    dates = pd.to_datetime(data[date_column], errors="coerce").dt.normalize()
    expected_rows = data.loc[dates.eq(expected)]
    if expected_rows.empty:
        return result
    result["sox_feature_date_present"] = True
    return_column = resolve_column(expected_rows, ("sox_return_1d",))
    result["sox_return_present"] = return_column is not None
    if return_column is not None:
        result["sox_return_non_null_count"] = int(
            pd.to_numeric(expected_rows[return_column], errors="coerce").notna().sum()
        )
    return result


def resolve_column(data: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    """Resolve an external column name case-insensitively to a canonical candidate."""
    columns_by_lower = {str(column).lower(): str(column) for column in data.columns}
    return next((columns_by_lower[candidate.lower()] for candidate in candidates if candidate.lower() in columns_by_lower), None)


def read_table(path: Path) -> pd.DataFrame:
    """Read parquet or CSV by extension."""
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)
