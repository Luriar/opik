"""Production daily macro download helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from queue import Queue
from typing import Any, Callable
import threading

import pandas as pd

from src.pipeline.config import DailyUpdateConfig
from src.pipeline.daily_context import DailyRunContext


MACRO_TICKERS: dict[str, dict[str, str]] = {
    "nasdaq": {"ticker": "^IXIC", "column": "nasdaq_close", "label": "NASDAQ"},
    "sox": {"ticker": "^SOX", "column": "sox_close", "label": "SOX"},
    "sp500": {"ticker": "^GSPC", "column": "sp500_close", "label": "S&P500"},
    "vix": {"ticker": "^VIX", "column": "vix_close", "label": "VIX"},
    "wti": {"ticker": "CL=F", "column": "wti_close", "label": "WTI"},
    "usdkrw": {"ticker": "KRW=X", "column": "usdkrw", "label": "USD/KRW"},
}
MACRO_CLOSE_COLUMNS = ["nasdaq_close", "sox_close", "sp500_close", "vix_close", "wti_close", "usdkrw"]
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_US_MACRO_MAX_AGE_DAYS = 5


class MacroDataUnavailableError(RuntimeError):
    """Raised when a required production macro source is unavailable."""

    def __init__(self, feature: str, expected_date: str, reason: str):
        self.feature = feature
        self.expected_date = expected_date
        self.reason = reason
        super().__init__(f"{feature}: expected {expected_date}; {reason}")


@dataclass(frozen=True)
class MacroDownloadResult:
    """Summary of a production macro download/update."""

    macro_update_mode: str
    macro_source_date: str | None
    macro_rows_added: int
    macro_missing_after_update: dict[str, int]
    daily_macro_snapshot_path: str | None = None
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    macro_download_method: str = "yfinance"
    macro_download_passed: bool = True
    macro_downloaded_date: str | None = None
    macro_download_failed_sources: list[str] = field(default_factory=list)
    macro_download_error: str | None = None
    macro_rows_downloaded: int = 0
    macro_file_path: str | None = None
    actual_source_dates: dict[str, str] = field(default_factory=dict)
    expected_source_dates: dict[str, str] = field(default_factory=dict)
    macro_invalid_target_date_rows: dict[str, str] = field(default_factory=dict)
    sources_using_prior_trading_day: list[str] = field(default_factory=list)


def run_production_macro_download(
    config: DailyUpdateConfig,
    context: DailyRunContext,
    dry_run: bool,
    force: bool,
    downloader: Callable[[str, str, pd.Timestamp], pd.DataFrame] | None = None,
) -> MacroDownloadResult:
    """Download required macro data for the target update date and update latest macro parquet."""
    target_date = pd.Timestamp(context.target_update_date).normalize()
    if dry_run:
        return MacroDownloadResult(
            macro_update_mode="dry_run",
            macro_source_date=target_date.date().isoformat(),
            macro_rows_added=0,
            macro_missing_after_update={},
            warnings=["macro_download_skipped_in_dry_run"],
            macro_downloaded_date=target_date.date().isoformat(),
            macro_rows_downloaded=0,
            macro_file_path=str(latest_macro_path(config)),
        )
    timeout_seconds = int(config.values.get("yfinance_macro_timeout_seconds", DEFAULT_TIMEOUT_SECONDS))
    max_age_days = int(config.values.get("us_macro_max_age_calendar_days", DEFAULT_US_MACRO_MAX_AGE_DAYS))
    macro_row = download_required_macro_row(
        target_date,
        downloader=downloader,
        timeout_seconds=timeout_seconds,
        max_age_days=max_age_days,
    )
    output_path = latest_macro_path(config)
    rows_added = 0
    snapshot_path: str | None = None
    mode = "dry_run"
    updated = preview_append_latest_macro(output_path, macro_row, target_date, force)
    updated, rows_added, mode = append_latest_macro(output_path, macro_row, target_date, force)
    write_macro_file(output_path, updated)
    snapshot_path = write_daily_macro_snapshot(config, target_date, macro_row)
    invalid_target_rows = dict(macro_row.attrs.get("macro_invalid_target_date_rows", {}))
    actual_source_dates = {
        source: pd.Timestamp(macro_row[f"actual_{source}_date"].iloc[0]).date().isoformat()
        for source in MACRO_TICKERS
    }
    expected_source_dates = {
        source: target_date.date().isoformat()
        for source in MACRO_TICKERS
    }
    prior_sources = [
        source for source, actual_date in actual_source_dates.items()
        if actual_date < target_date.date().isoformat()
    ]
    warnings = [f"{source}: {reason}" for source, reason in invalid_target_rows.items()]

    missing_after = {column: int(value) for column, value in updated.isna().sum().items() if int(value) > 0}
    return MacroDownloadResult(
        macro_update_mode=mode,
        macro_source_date=target_date.date().isoformat(),
        macro_rows_added=rows_added,
        macro_missing_after_update=missing_after,
        daily_macro_snapshot_path=snapshot_path,
        warnings=warnings,
        macro_downloaded_date=target_date.date().isoformat(),
        macro_rows_downloaded=int(len(macro_row)),
        macro_file_path=str(output_path),
        actual_source_dates=actual_source_dates,
        expected_source_dates=expected_source_dates,
        macro_invalid_target_date_rows=invalid_target_rows,
        sources_using_prior_trading_day=prior_sources,
    )


def latest_macro_path(config: DailyUpdateConfig) -> Path:
    """Return production latest macro dataset path."""
    return config.project_root / "data" / "processed" / "macro" / "macro_clean_latest.parquet"


def download_required_macro_row(
    target_date: pd.Timestamp,
    downloader: Callable[[str, str, pd.Timestamp], pd.DataFrame] | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_age_days: int = DEFAULT_US_MACRO_MAX_AGE_DAYS,
) -> pd.DataFrame:
    """Download all required macro closes for one target date."""
    target_date = target_date.normalize()
    row: dict[str, Any] = {"date": target_date}
    invalid_target_rows: dict[str, str] = {}
    for source, info in MACRO_TICKERS.items():
        label = info["label"]
        ticker = info["ticker"]
        try:
            data = download_one_macro_source(label, ticker, target_date, downloader, timeout_seconds, max_age_days)
            close, actual_date, invalid_reason = _select_latest_valid_macro_row(
                data, label, target_date, max_age_days
            )
        except MacroDataUnavailableError:
            raise
        except TimeoutError as exc:
            raise MacroDataUnavailableError(label, target_date.date().isoformat(), "download timeout") from exc
        except Exception as exc:
            raise MacroDataUnavailableError(label, target_date.date().isoformat(), str(exc)) from exc
        row[info["column"]] = close
        row[f"actual_{source}_date"] = actual_date
        row[f"expected_{source}_date"] = target_date
        if invalid_reason is not None:
            invalid_target_rows[source] = invalid_reason
    result = pd.DataFrame([row])
    result.attrs["macro_invalid_target_date_rows"] = invalid_target_rows
    return result


def download_one_macro_source(
    feature: str,
    ticker: str,
    target_date: pd.Timestamp,
    downloader: Callable[[str, str, pd.Timestamp], pd.DataFrame] | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_age_days: int = DEFAULT_US_MACRO_MAX_AGE_DAYS,
) -> pd.DataFrame:
    """Download one yfinance macro source with a hard timeout."""
    download = downloader or (
        lambda source, symbol, date: yfinance_download(source, symbol, date, max_age_days=max_age_days)
    )
    return call_macro_downloader_with_timeout(download, feature, ticker, target_date, timeout_seconds)


def yfinance_download(
    feature: str,
    ticker: str,
    target_date: pd.Timestamp,
    max_age_days: int = DEFAULT_US_MACRO_MAX_AGE_DAYS,
) -> pd.DataFrame:
    """Download a bounded lookback ending on the target date from yfinance."""
    import yfinance as yf

    start = (target_date - timedelta(days=max_age_days)).date().isoformat()
    end = (target_date + timedelta(days=1)).date().isoformat()
    return yf.download(
        ticker,
        start=start,
        end=end,
        progress=False,
        auto_adjust=False,
        threads=False,
    )


def call_macro_downloader_with_timeout(
    downloader: Callable[[str, str, pd.Timestamp], pd.DataFrame],
    feature: str,
    ticker: str,
    target_date: pd.Timestamp,
    timeout_seconds: int,
) -> pd.DataFrame:
    """Call a macro downloader in a daemon thread with a hard timeout."""
    queue: Queue = Queue(maxsize=1)

    def worker() -> None:
        try:
            queue.put(("ok", downloader(feature, ticker, target_date)))
        except Exception as exc:  # pragma: no cover - surfaced to caller
            queue.put(("error", exc))

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    thread.join(timeout_seconds)
    if thread.is_alive():
        raise TimeoutError(f"{feature} download timed out after {timeout_seconds}s")
    status, payload = queue.get_nowait()
    if status == "error":
        raise payload
    return payload


def validate_macro_source_frame(
    data: pd.DataFrame,
    feature: str,
    target_date: pd.Timestamp,
    max_age_days: int = DEFAULT_US_MACRO_MAX_AGE_DAYS,
) -> tuple[float, pd.Timestamp]:
    """Return the latest valid Close on/before target within the allowed age."""
    close, actual_date, _invalid_reason = _select_latest_valid_macro_row(
        data, feature, target_date, max_age_days
    )
    return close, actual_date


def _select_latest_valid_macro_row(
    data: pd.DataFrame,
    feature: str,
    target_date: pd.Timestamp,
    max_age_days: int,
) -> tuple[float, pd.Timestamp, str | None]:
    """Select a valid macro row and describe an invalid target-date row, if present."""
    expected = target_date.normalize()
    if data is None or data.empty:
        raise MacroDataUnavailableError(feature, expected.date().isoformat(), "No data returned")
    frame = data.reset_index().copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = [str(column[0]) if isinstance(column, tuple) else str(column) for column in frame.columns]
    if "Date" in frame.columns:
        frame = frame.rename(columns={"Date": "date"})
    elif "Datetime" in frame.columns:
        frame = frame.rename(columns={"Datetime": "date"})
    if "date" not in frame.columns:
        raise MacroDataUnavailableError(feature, expected.date().isoformat(), "Date column missing")
    if "Close" not in frame.columns:
        raise MacroDataUnavailableError(feature, expected.date().isoformat(), "Close missing")
    close_values = frame.loc[:, "Close"]
    if isinstance(close_values, pd.DataFrame):
        close_values = close_values.iloc[:, 0]
    normalized = pd.DataFrame(
        {
            "date": pd.to_datetime(frame["date"], errors="coerce").dt.tz_localize(None).dt.normalize(),
            "close": pd.to_numeric(close_values, errors="coerce"),
        }
    )
    eligible = normalized.loc[normalized["date"].notna() & normalized["date"].le(expected)].copy()
    if eligible.empty:
        raise MacroDataUnavailableError(feature, expected.date().isoformat(), "No data available on or before target date")
    target_rows = eligible.loc[eligible["date"].eq(expected)]
    invalid_reason: str | None = None
    if not target_rows.empty and not bool((target_rows["close"].notna() & target_rows["close"].gt(0)).any()):
        invalid_reason = (
            "target-date Close missing"
            if bool(target_rows["close"].isna().all())
            else "target-date Close non-positive"
        )
    valid = eligible.loc[eligible["close"].notna() & eligible["close"].gt(0)].sort_values("date")
    within_tolerance = valid.loc[(expected - valid["date"]).dt.days.le(max_age_days)]
    if within_tolerance.empty:
        if invalid_reason is None and not valid.empty:
            latest_valid_date = pd.Timestamp(valid.iloc[-1]["date"]).date().isoformat()
            raise MacroDataUnavailableError(
                feature,
                expected.date().isoformat(),
                f"Latest source date {latest_valid_date} is older than {max_age_days} calendar days",
            )
        raise MacroDataUnavailableError(
            feature,
            expected.date().isoformat(),
            f"No valid Close within {max_age_days} calendar days on or before target date",
        )
    latest = within_tolerance.iloc[-1]
    actual_date = pd.Timestamp(latest["date"]).normalize()
    return float(latest["close"]), actual_date, invalid_reason


def preview_append_latest_macro(path: Path, macro_row: pd.DataFrame, target_date: pd.Timestamp, force: bool) -> pd.DataFrame:
    """Return updated latest macro data without writing."""
    updated, _rows_added, _mode = append_latest_macro(path, macro_row, target_date, force)
    return updated


def append_latest_macro(
    path: Path,
    macro_row: pd.DataFrame,
    target_date: pd.Timestamp,
    force: bool,
) -> tuple[pd.DataFrame, int, str]:
    """Append or replace one macro row without duplicate dates."""
    existing = read_existing_latest_macro(path)
    row = macro_row.copy()
    row["date"] = pd.to_datetime(row["date"]).dt.normalize()
    if len(row) != 1:
        raise ValueError("macro_row must contain exactly one row")
    target = target_date.normalize()
    exists = not existing.empty and existing["date"].eq(target).any()
    if exists and not force:
        updated = existing.copy()
        target_index = updated.index[updated["date"].eq(target)]
        enriched = False
        for column in row.columns:
            if column == "date":
                continue
            incoming = row[column].iloc[0]
            if column not in updated.columns:
                updated[column] = pd.NA
            missing = updated.loc[target_index, column].isna()
            if bool(missing.any()) and pd.notna(incoming):
                updated.loc[target_index[missing], column] = incoming
                enriched = True
        mode = "existing_download_enriched" if enriched else "existing_download_verified"
        rows_added = 0
    else:
        base = existing[~existing["date"].eq(target)].copy() if not existing.empty else existing
        updated = pd.concat([base, row], ignore_index=True)
        rows_added = 1
        mode = "replaced" if exists else "downloaded"
    updated = updated.sort_values("date").reset_index(drop=True)
    if updated.duplicated(subset=["date"]).any():
        raise ValueError("Duplicate macro date rows after append")
    return updated, rows_added, mode


def read_existing_latest_macro(path: Path) -> pd.DataFrame:
    """Read existing latest macro dataset if present."""
    if not path.exists():
        return pd.DataFrame(columns=["date", *MACRO_CLOSE_COLUMNS])
    data = pd.read_parquet(path)
    if "date" not in data.columns:
        raise ValueError(f"Macro file missing date column: {path}")
    data = data.copy()
    data["date"] = pd.to_datetime(data["date"]).dt.normalize()
    return data.sort_values("date").reset_index(drop=True)


def write_macro_file(path: Path, macro: pd.DataFrame) -> None:
    """Write production latest macro parquet."""
    path.parent.mkdir(parents=True, exist_ok=True)
    macro.sort_values("date").to_parquet(path, index=False)


def write_daily_macro_snapshot(config: DailyUpdateConfig, update_date: pd.Timestamp, macro_row: pd.DataFrame) -> str:
    """Write daily macro download snapshot."""
    compact = update_date.strftime("%Y%m%d")
    path = config.resolve_path("daily_processed_dir") / f"macro_clean_{compact}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    macro_row.sort_values("date").to_csv(path, index=False, encoding="utf-8-sig")
    return str(path)


def print_macro_download_success() -> None:
    """Print production macro success block."""
    print("========================================", flush=True)
    print("Macro Download", flush=True)
    print("NASDAQ ...... PASS", flush=True)
    print("SOX ......... PASS", flush=True)
    print("S&P500 ...... PASS", flush=True)
    print("VIX ......... PASS", flush=True)
    print("WTI ......... PASS", flush=True)
    print("USD/KRW ..... PASS", flush=True)
    print("Macro Update SUCCESS", flush=True)
    print("========================================", flush=True)


def print_macro_download_failure(error: MacroDataUnavailableError) -> None:
    """Print production macro failure block."""
    print("========================================", flush=True)
    print("MACRO DOWNLOAD FAILED", flush=True)
    if error.feature == "SOX":
        print("SOX ......... FAIL", flush=True)
    print("", flush=True)
    print("Feature", flush=True)
    print(error.feature, flush=True)
    print("", flush=True)
    print("Expected Date", flush=True)
    print(error.expected_date, flush=True)
    print("", flush=True)
    print("Reason", flush=True)
    print(error.reason, flush=True)
    print("", flush=True)
    print("Pipeline terminated.", flush=True)
    print("========================================", flush=True)
