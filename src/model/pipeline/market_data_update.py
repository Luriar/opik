"""Daily Korean OHLCV update helpers."""

from __future__ import annotations

import time
import threading
from contextlib import redirect_stderr, redirect_stdout
from concurrent.futures import TimeoutError
from dataclasses import dataclass, field
from io import StringIO
from queue import Queue
from pathlib import Path
from typing import Callable

import pandas as pd

from src.model.pipeline.config import DailyUpdateConfig
from src.model.pipeline.daily_context import DailyRunContext


OHLCV_COLUMNS: list[str] = [
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trading_value",
]
NUMERIC_COLUMNS: list[str] = ["open", "high", "low", "close", "volume", "trading_value"]
DOWNLOAD_TIMEOUT_SECONDS = 30
PER_TICKER_TIMEOUT_SECONDS = 10
TOTAL_DOWNLOAD_TIMEOUT_SECONDS = 900
DOWNLOAD_MAX_RETRIES = 2
DOWNLOAD_RETRY_SLEEP_SECONDS = 5
MIN_SUCCESS_RATIO = 0.95


@dataclass(frozen=True)
class OhlcvUpdateResult:
    """Summary of one daily OHLCV update/check."""

    universe_count: int
    raw_rows_downloaded_or_found: int
    raw_rows_added: int
    cleaned_rows_added: int
    invalid_ohlcv_rows: int
    invalid_ohlcv_tickers: list[str] = field(default_factory=list)
    missing_005930: bool = False
    daily_ohlcv_snapshot_paths: list[str] = field(default_factory=list)
    ohlcv_download_mode: str = "not_attempted"
    ohlcv_download_attempts: int = 0
    ohlcv_download_timed_out: bool = False
    ohlcv_download_failed: bool = False
    ohlcv_download_error: str | None = None
    used_existing_data_fallback: bool = False
    pykrx_rows_returned: int = 0
    pykrx_empty_response: bool = False
    pykrx_missing_columns: bool = False
    pykrx_data_unavailable: bool = False
    pykrx_download_method: str = "per_ticker"
    pykrx_tickers_requested: int = 0
    pykrx_tickers_downloaded: int = 0
    pykrx_tickers_failed: int = 0
    pykrx_failed_tickers_sample: list[str] = field(default_factory=list)
    pykrx_success_ratio: float = 0.0
    trading_value_estimated_count: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


Downloader = Callable[[list[str], pd.Timestamp], pd.DataFrame]


def run_ohlcv_update(
    config: DailyUpdateConfig,
    context: DailyRunContext,
    dry_run: bool,
    skip_download: bool,
    force: bool,
    downloader: Downloader | None = None,
    timeout_seconds: int = DOWNLOAD_TIMEOUT_SECONDS,
    max_retries: int = DOWNLOAD_MAX_RETRIES,
    retry_sleep_seconds: int = DOWNLOAD_RETRY_SLEEP_SECONDS,
) -> OhlcvUpdateResult:
    """Run the Part 2A daily OHLCV update/check."""
    warnings: list[str] = []
    errors: list[str] = []
    update_date = pd.Timestamp(context.update_date).normalize()
    universe = load_universe(config.resolve_path("universe_file"))
    raw_path = config.resolve_path("raw_ohlcv_file")
    clean_path = config.resolve_path("clean_ohlcv_file")

    download_mode = "not_attempted"
    download_attempts = 0
    download_timed_out = False
    download_failed = False
    download_error: str | None = None
    used_existing_data_fallback = False
    pykrx_empty_response = False
    pykrx_missing_columns = False
    pykrx_data_unavailable = False
    pykrx_tickers_requested = int(universe["ticker"].nunique())
    pykrx_tickers_downloaded = 0
    pykrx_tickers_failed = 0
    pykrx_failed_tickers_sample: list[str] = []
    pykrx_success_ratio = 0.0
    trading_value_estimated_count = 0
    min_success_ratio = float(config.values.get("min_pykrx_success_ratio", MIN_SUCCESS_RATIO))
    per_ticker_timeout = int(config.values.get("pykrx_per_ticker_timeout_seconds", PER_TICKER_TIMEOUT_SECONDS))
    total_download_timeout = int(config.values.get("pykrx_total_download_timeout_seconds", TOTAL_DOWNLOAD_TIMEOUT_SECONDS))
    strict_production = bool(config.values.get("production_mode", False)) and bool(config.values.get("strict_feature_source_check", False))

    if skip_download or dry_run:
        daily_raw = find_existing_rows(raw_path, update_date)
        download_mode = "dry_run" if dry_run else "no_download"
        if daily_raw.empty:
            warning_prefix = "dry_run_enabled" if dry_run else "skip_download_enabled"
            warnings.append(f"{warning_prefix}_no_ohlcv_rows_found_for_{update_date.date()}")
            used_existing_data_fallback = True
    else:
        download = downloader or (
            lambda tickers, date_value: download_daily_ohlcv(
                tickers,
                date_value,
                timeout_seconds=per_ticker_timeout,
                max_retries=max_retries,
                retry_sleep_seconds=retry_sleep_seconds,
            )
        )
        outcome = download_with_retries(
            download,
            universe["ticker"].tolist(),
            update_date,
            timeout_seconds=total_download_timeout if downloader is None else timeout_seconds,
            max_retries=1 if downloader is None else max_retries,
            retry_sleep_seconds=retry_sleep_seconds,
        )
        daily_raw = outcome.data
        pykrx_tickers_requested = int(outcome.tickers_requested or pykrx_tickers_requested)
        pykrx_tickers_downloaded = int(outcome.tickers_downloaded)
        pykrx_tickers_failed = int(outcome.tickers_failed)
        pykrx_failed_tickers_sample = outcome.failed_tickers_sample
        pykrx_success_ratio = float(outcome.success_ratio)
        trading_value_estimated_count = int(outcome.trading_value_estimated_count)
        if outcome.trading_value_estimated_count:
            warnings.append("trading_value_estimated")
        download_attempts = outcome.attempts
        download_timed_out = outcome.timed_out
        download_failed = outcome.failed
        download_error = outcome.error
        download_mode = outcome.mode
        pykrx_empty_response = outcome.empty_response
        pykrx_missing_columns = outcome.missing_columns
        pykrx_data_unavailable = outcome.data_unavailable
        if outcome.enforce_success_threshold and pykrx_success_ratio < min_success_ratio:
            download_failed = True
            pykrx_data_unavailable = True
            download_error = (
                f"pykrx_success_ratio_below_threshold:"
                f"{pykrx_success_ratio:.4f}<{min_success_ratio:.4f}"
            )
            warnings.append("pykrx_success_ratio_below_threshold")
            if strict_production:
                daily_raw = pd.DataFrame(columns=OHLCV_COLUMNS)
        if daily_raw.empty:
            used_existing_data_fallback = True
            pykrx_data_unavailable = True
            warnings.append(f"ohlcv_download_fallback_using_existing_clean_data_for_{update_date.date()}")
            if download_error:
                warnings.append("ohlcv_download_error_suppressed")

    daily_raw = normalize_ohlcv(daily_raw)
    raw_rows = int(len(daily_raw))
    if raw_rows and daily_raw["ticker"].eq("005930").sum() == 0:
        warnings.append("ticker_005930_missing_from_daily_ohlcv")
    missing_005930 = bool(raw_rows == 0 or daily_raw["ticker"].eq("005930").sum() == 0)

    valid_daily, invalid_daily = split_valid_invalid_ohlcv(daily_raw)
    invalid_tickers = sorted(invalid_daily["ticker"].dropna().astype(str).unique().tolist())

    raw_rows_added = 0
    clean_rows_added = 0
    snapshot_paths: list[str] = []
    if not dry_run and raw_rows:
        raw_combined, raw_rows_added = safe_append_ohlcv(raw_path, daily_raw, update_date, force)
        clean_combined, clean_rows_added = safe_append_ohlcv(clean_path, valid_daily, update_date, force)
        write_ohlcv_file(raw_path, raw_combined)
        write_ohlcv_file(clean_path, clean_combined)
        snapshot_paths = write_daily_snapshots(config, update_date, daily_raw, valid_daily)
    elif dry_run:
        raw_rows_added = preview_append_count(raw_path, daily_raw, update_date, force)
        clean_rows_added = preview_append_count(clean_path, valid_daily, update_date, force)

    return OhlcvUpdateResult(
        universe_count=int(universe["ticker"].nunique()),
        raw_rows_downloaded_or_found=raw_rows,
        raw_rows_added=raw_rows_added,
        cleaned_rows_added=clean_rows_added,
        invalid_ohlcv_rows=int(len(invalid_daily)),
        invalid_ohlcv_tickers=invalid_tickers,
        missing_005930=missing_005930,
        daily_ohlcv_snapshot_paths=snapshot_paths,
        ohlcv_download_mode=download_mode,
        ohlcv_download_attempts=download_attempts,
        ohlcv_download_timed_out=download_timed_out,
        ohlcv_download_failed=download_failed,
        ohlcv_download_error=download_error,
        used_existing_data_fallback=used_existing_data_fallback,
        pykrx_rows_returned=raw_rows,
        pykrx_empty_response=pykrx_empty_response or raw_rows == 0,
        pykrx_missing_columns=pykrx_missing_columns,
        pykrx_data_unavailable=pykrx_data_unavailable or raw_rows == 0,
        pykrx_download_method="per_ticker",
        pykrx_tickers_requested=pykrx_tickers_requested,
        pykrx_tickers_downloaded=pykrx_tickers_downloaded or raw_rows,
        pykrx_tickers_failed=pykrx_tickers_failed,
        pykrx_failed_tickers_sample=pykrx_failed_tickers_sample,
        pykrx_success_ratio=pykrx_success_ratio if pykrx_tickers_requested else 0.0,
        trading_value_estimated_count=trading_value_estimated_count,
        warnings=warnings,
        errors=errors,
    )


@dataclass(frozen=True)
class DownloadOutcome:
    """Safe download result."""

    data: pd.DataFrame
    attempts: int
    timed_out: bool
    failed: bool
    error: str | None
    mode: str
    empty_response: bool = False
    missing_columns: bool = False
    data_unavailable: bool = False
    tickers_requested: int = 0
    tickers_downloaded: int = 0
    tickers_failed: int = 0
    failed_tickers_sample: list[str] = field(default_factory=list)
    success_ratio: float = 0.0
    trading_value_estimated_count: int = 0
    enforce_success_threshold: bool = False


def download_with_retries(
    downloader: Downloader,
    tickers: list[str],
    update_date: pd.Timestamp,
    timeout_seconds: int = DOWNLOAD_TIMEOUT_SECONDS,
    max_retries: int = DOWNLOAD_MAX_RETRIES,
    retry_sleep_seconds: int = DOWNLOAD_RETRY_SLEEP_SECONDS,
) -> DownloadOutcome:
    """Run a downloader with bounded retries and timeout."""
    attempts = 0
    timed_out = False
    empty_response = False
    missing_columns = False
    last_error: str | None = None
    for attempt in range(1, max_retries + 1):
        attempts = attempt
        try:
            data = call_with_timeout(downloader, tickers, update_date, timeout_seconds)
            if data is None or data.empty:
                empty_response = True
                last_error = "download_returned_no_data"
                continue
            return DownloadOutcome(
                data=data,
                attempts=attempts,
                timed_out=timed_out,
                failed=False,
                error=None,
                mode="downloaded",
                tickers_requested=int(data.attrs.get("tickers_requested", len(tickers))),
                tickers_downloaded=int(data.attrs.get("tickers_downloaded", len(data))),
                tickers_failed=int(data.attrs.get("tickers_failed", max(0, len(tickers) - len(data)))),
                failed_tickers_sample=list(data.attrs.get("failed_tickers_sample", [])),
                success_ratio=float(data.attrs.get("success_ratio", len(data) / len(tickers) if tickers else 0.0)),
                trading_value_estimated_count=int(data.attrs.get("trading_value_estimated_count", 0)),
                enforce_success_threshold=bool(data.attrs.get("enforce_success_threshold", False)),
            )
        except TimeoutError:
            timed_out = True
            last_error = f"download_timed_out_after_{timeout_seconds}s"
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if "pykrx_missing_columns" in last_error:
                missing_columns = True
                break
        if attempt < max_retries:
            time.sleep(retry_sleep_seconds)
    return DownloadOutcome(
        data=pd.DataFrame(columns=OHLCV_COLUMNS),
        attempts=attempts,
        timed_out=timed_out,
        failed=True,
        error=last_error,
        mode="fallback_existing",
        empty_response=empty_response,
        missing_columns=missing_columns,
        data_unavailable=True,
        tickers_requested=len(tickers),
        tickers_downloaded=0,
        tickers_failed=len(tickers),
        failed_tickers_sample=[str(ticker).zfill(6) for ticker in tickers[:20]],
        success_ratio=0.0,
        trading_value_estimated_count=0,
    )


def call_with_timeout(
    downloader: Downloader,
    tickers: list[str],
    update_date: pd.Timestamp,
    timeout_seconds: int,
) -> pd.DataFrame:
    """Call a downloader with a hard timeout boundary."""
    return _call_in_daemon_thread(
        _silent_downloader_call,
        (downloader, tickers, update_date),
        timeout_seconds,
    )


def _silent_downloader_call(downloader: Downloader, tickers: list[str], update_date: pd.Timestamp) -> pd.DataFrame:
    """Call downloader while suppressing library stdout/stderr."""
    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        return downloader(tickers, update_date)


def load_universe(path: Path) -> pd.DataFrame:
    """Load ticker universe for daily updates."""
    data = pd.read_csv(path, dtype={"ticker": str}, encoding="utf-8-sig")
    if "ticker" not in data.columns:
        raise ValueError(f"Universe file missing ticker column: {path}")
    if "ticker_name" not in data.columns:
        data["ticker_name"] = ""
    data["ticker"] = data["ticker"].astype(str).str.extract(r"(\d+)")[0].str.zfill(6)
    return data.dropna(subset=["ticker"]).drop_duplicates("ticker")


def download_daily_ohlcv(
    tickers: list[str],
    update_date: pd.Timestamp,
    timeout_seconds: int = PER_TICKER_TIMEOUT_SECONDS,
    max_retries: int = DOWNLOAD_MAX_RETRIES,
    retry_sleep_seconds: int = DOWNLOAD_RETRY_SLEEP_SECONDS,
    stock_module=None,
) -> pd.DataFrame:
    """Download target-date Korean OHLCV via pykrx per-ticker endpoint."""
    if stock_module is None:
        from pykrx import stock
    else:
        stock = stock_module

    compact = update_date.strftime("%Y%m%d")
    frames: list[pd.DataFrame] = []
    failed_tickers: list[str] = []
    estimated_count = 0
    normalized_tickers = [str(ticker).zfill(6) for ticker in tickers]
    total = len(normalized_tickers)
    for index, ticker in enumerate(normalized_tickers, start=1):
        if index == 1 or index % 25 == 0 or index == total:
            print(f"downloaded {len(frames)} / {total}", flush=True)
        data = pd.DataFrame()
        for attempt in range(1, max_retries + 1):
            try:
                data = call_pykrx_ticker_with_timeout(stock, compact, ticker, timeout_seconds)
                break
            except Exception as exc:
                if "pykrx_missing_columns" in str(exc):
                    raise
                if attempt < max_retries:
                    time.sleep(retry_sleep_seconds)
                continue
        if data.empty:
            failed_tickers.append(ticker)
            continue
        normalized = normalize_pykrx_ohlcv(data, ticker, update_date)
        estimated_count += int(normalized.attrs.get("trading_value_estimated_count", 0))
        frames.append(normalized)
    if not frames:
        result = pd.DataFrame(columns=OHLCV_COLUMNS)
    else:
        result = pd.concat(frames, ignore_index=True)
    downloaded = int(result["ticker"].nunique()) if not result.empty else 0
    result.attrs["tickers_requested"] = total
    result.attrs["tickers_downloaded"] = downloaded
    result.attrs["tickers_failed"] = max(0, total - downloaded)
    result.attrs["failed_tickers_sample"] = failed_tickers[:20]
    result.attrs["success_ratio"] = downloaded / total if total else 0.0
    result.attrs["trading_value_estimated_count"] = estimated_count
    result.attrs["enforce_success_threshold"] = True
    return result


def call_pykrx_ticker_with_timeout(stock_module, compact_date: str, ticker: str, timeout_seconds: int) -> pd.DataFrame:
    """Call one pykrx ticker request with a hard timeout."""
    return _call_in_daemon_thread(
        _silent_pykrx_call,
        (stock_module, compact_date, ticker),
        timeout_seconds,
    )


def _call_in_daemon_thread(func: Callable, args: tuple, timeout_seconds: int) -> pd.DataFrame:
    """Run a blocking call in a daemon thread so timeouts cannot keep Python alive."""
    queue: Queue = Queue(maxsize=1)

    def worker() -> None:
        try:
            queue.put(("ok", func(*args)))
        except Exception as exc:  # pragma: no cover - surfaced to caller
            queue.put(("error", exc))

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    thread.join(timeout_seconds)
    if thread.is_alive():
        raise TimeoutError()
    status, payload = queue.get_nowait()
    if status == "error":
        raise payload
    return payload


def _silent_pykrx_call(stock_module, compact_date: str, ticker: str) -> pd.DataFrame:
    """Call pykrx while suppressing library stdout/stderr."""
    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        return stock_module.get_market_ohlcv_by_date(compact_date, compact_date, ticker)


def normalize_pykrx_ohlcv(data: pd.DataFrame, ticker: str, update_date: pd.Timestamp | None = None) -> pd.DataFrame:
    """Normalize pykrx OHLCV output."""
    result = data.reset_index().copy()
    result = result.rename(
        columns={
            "Date": "date",
            "날짜": "date",
            "시가": "open",
            "고가": "high",
            "저가": "low",
            "종가": "close",
            "거래량": "volume",
            "거래대금": "trading_value",
        }
    )
    if "date" not in result.columns and update_date is not None:
        result["date"] = pd.Timestamp(update_date).normalize()
    estimated_count = 0
    if "trading_value" not in result.columns and {"close", "volume"} <= set(result.columns):
        result["trading_value"] = result["close"] * result["volume"]
        estimated_count = len(result)
    result["ticker"] = str(ticker).zfill(6)
    missing = set(OHLCV_COLUMNS) - set(result.columns)
    if missing:
        raise ValueError(f"pykrx_missing_columns:{sorted(missing)}")
    normalized = result.loc[:, OHLCV_COLUMNS]
    normalized.attrs["trading_value_estimated_count"] = estimated_count
    return normalized


def find_existing_rows(path: Path, update_date: pd.Timestamp) -> pd.DataFrame:
    """Find existing raw rows for update_date."""
    if not path.exists():
        return pd.DataFrame(columns=OHLCV_COLUMNS)
    data = pd.read_parquet(path)
    data = normalize_ohlcv(data)
    return data[data["date"].eq(update_date)].copy()


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize OHLCV columns and dtypes."""
    if df.empty:
        return pd.DataFrame(columns=OHLCV_COLUMNS)
    result = df.copy()
    missing = set(OHLCV_COLUMNS) - set(result.columns)
    if missing:
        raise ValueError(f"OHLCV data missing columns: {sorted(missing)}")
    result["date"] = pd.to_datetime(result["date"]).dt.normalize()
    result["ticker"] = result["ticker"].astype(str).str.extract(r"(\d+)")[0].str.zfill(6)
    for column in NUMERIC_COLUMNS:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    return result.loc[:, OHLCV_COLUMNS].sort_values(["date", "ticker"]).reset_index(drop=True)


def split_valid_invalid_ohlcv(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split OHLCV rows into valid and invalid sets."""
    data = normalize_ohlcv(df)
    if data.empty:
        return data.copy(), data.copy()
    duplicate_mask = data.duplicated(subset=["date", "ticker"], keep=False)
    valid_mask = (
        ~duplicate_mask
        & data["open"].gt(0)
        & data["high"].gt(0)
        & data["low"].gt(0)
        & data["close"].gt(0)
        & data["volume"].ge(0)
        & data["trading_value"].ge(0)
        & data["high"].ge(data["low"])
        & data["high"].ge(data["open"])
        & data["high"].ge(data["close"])
        & data["low"].le(data["open"])
        & data["low"].le(data["close"])
    )
    return data.loc[valid_mask].copy(), data.loc[~valid_mask].copy()


def safe_append_ohlcv(
    path: Path,
    daily_rows: pd.DataFrame,
    update_date: pd.Timestamp,
    force: bool = False,
) -> tuple[pd.DataFrame, int]:
    """Append or replace daily rows without date/ticker duplicates."""
    daily = normalize_ohlcv(daily_rows)
    existing = read_ohlcv_file(path)
    if force:
        base = existing[~existing["date"].eq(update_date)].copy()
        rows_added = len(daily)
    else:
        if daily.empty:
            return existing, 0
        existing_keys = set(zip(existing["date"], existing["ticker"], strict=False))
        keep_mask = [
            (row.date, row.ticker) not in existing_keys
            for row in daily.itertuples(index=False)
        ]
        daily = daily.loc[keep_mask].copy()
        base = existing
        rows_added = len(daily)
    combined = pd.concat([base, daily], ignore_index=True)
    combined = normalize_ohlcv(combined)
    if combined.duplicated(subset=["date", "ticker"]).any():
        raise ValueError(f"Duplicate date/ticker rows after append: {path}")
    return combined, int(rows_added)


def preview_append_count(path: Path, daily_rows: pd.DataFrame, update_date: pd.Timestamp, force: bool) -> int:
    """Return rows that would be appended/replaced."""
    combined, rows_added = safe_append_ohlcv(path, daily_rows, update_date, force)
    _ = combined
    return rows_added


def read_ohlcv_file(path: Path) -> pd.DataFrame:
    """Read existing OHLCV parquet or return an empty frame."""
    if not path.exists():
        return pd.DataFrame(columns=OHLCV_COLUMNS)
    return normalize_ohlcv(pd.read_parquet(path))


def write_ohlcv_file(path: Path, df: pd.DataFrame) -> None:
    """Write OHLCV to parquet preserving existing file type contract."""
    path.parent.mkdir(parents=True, exist_ok=True)
    normalize_ohlcv(df).to_parquet(path, index=False)


def write_daily_snapshots(
    config: DailyUpdateConfig,
    update_date: pd.Timestamp,
    raw_rows: pd.DataFrame,
    clean_rows: pd.DataFrame,
) -> list[str]:
    """Write daily raw and clean CSV snapshots."""
    compact = update_date.strftime("%Y%m%d")
    raw_path = config.resolve_path("daily_raw_dir") / f"ohlcv_{compact}.csv"
    clean_path = config.resolve_path("daily_processed_dir") / f"ohlcv_clean_{compact}.csv"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    normalize_ohlcv(raw_rows).to_csv(raw_path, index=False, encoding="utf-8-sig")
    normalize_ohlcv(clean_rows).to_csv(clean_path, index=False, encoding="utf-8-sig")
    return [str(raw_path), str(clean_path)]
