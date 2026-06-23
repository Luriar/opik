"""Diagnose pykrx daily download endpoints without requiring KRX login."""

from __future__ import annotations

import argparse
import platform
import sys
import threading
import time
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from datetime import date, datetime
from io import StringIO
from pathlib import Path
from queue import Queue
from typing import Any, Callable

import pandas as pd
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline.config import load_daily_update_config  # noqa: E402
from src.pipeline.daily_context import build_daily_run_context  # noqa: E402


TIMEOUT_SECONDS = 30
BASELINE_DATES: tuple[str, ...] = ("2024-06-14", "2025-06-13", "2026-06-12")
OHLCV_EXPECTED_ANY: tuple[str, ...] = (
    "close",
    "open",
    "high",
    "low",
    "volume",
    "종가",
    "시가",
    "고가",
    "저가",
    "거래량",
)


@dataclass(frozen=True)
class DiagnosticResult:
    """One pykrx endpoint diagnostic result."""

    endpoint: str
    elapsed_seconds: float
    success: bool
    rows_returned: int
    exception_message: str
    date_input: str = ""
    payload_type: str = ""
    shape: str = ""
    columns: list[str] = field(default_factory=list)
    preview_rows: list[dict[str, str]] = field(default_factory=list)
    exception_type: str = ""


@dataclass(frozen=True)
class NetworkCheckResult:
    """Network sanity check result."""

    elapsed_seconds: float
    success: bool
    status_code: int | None
    content_type: str
    preview: str
    exception_type: str
    exception_message: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse diagnostic CLI arguments."""
    parser = argparse.ArgumentParser(description="Diagnose pykrx daily download endpoints.")
    parser.add_argument("--date", default=None, help="Target update date, YYYY-MM-DD.")
    parser.add_argument("--sample-size", type=int, default=10, help="Reserved for compatibility.")
    parser.add_argument("--config", default="configs/daily_update.yaml", help="Daily update config path.")
    parser.add_argument("--timeout", type=int, default=TIMEOUT_SECONDS, help="Endpoint timeout seconds.")
    return parser.parse_args(argv)


def resolve_default_date(config_path: str | Path) -> str:
    """Resolve default target update date from daily pipeline context."""
    config = load_daily_update_config(config_path)
    context = build_daily_run_context(
        config=config,
        as_of_date=date.today().isoformat(),
        dry_run=True,
        skip_download=True,
        force=False,
    )
    return context.target_update_date


def diagnostic_dates(requested_date: str) -> list[str]:
    """Return baseline diagnostic dates plus requested date without duplicates."""
    dates: list[str] = []
    for value in [*BASELINE_DATES, pd.Timestamp(requested_date).date().isoformat()]:
        if value not in dates:
            dates.append(value)
    return dates


def date_inputs(diagnostic_date: str) -> list[str]:
    """Return pykrx date input variants for one date."""
    timestamp = pd.Timestamp(diagnostic_date)
    return [timestamp.strftime("%Y%m%d"), timestamp.date().isoformat()]


def collect_environment_info() -> dict[str, str]:
    """Collect local environment information relevant to pykrx diagnostics."""
    pykrx_version = "UNKNOWN"
    try:
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            import pykrx

        pykrx_version = str(getattr(pykrx, "__version__", "UNKNOWN"))
    except Exception as exc:  # pragma: no cover - surfaced in report only
        pykrx_version = f"ERROR: {type(exc).__name__}: {exc}"

    return {
        "python_version": sys.version.replace("\n", " "),
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "pykrx_version": pykrx_version,
        "pandas_version": pd.__version__,
        "requests_version": requests.__version__,
        "current_working_directory": str(Path.cwd()),
        "system_time": datetime.now().isoformat(timespec="seconds"),
    }


def run_diagnostics(
    target_date: str,
    sample_size: int = 10,
    timeout_seconds: int = TIMEOUT_SECONDS,
    stock_module: Any | None = None,
) -> list[DiagnosticResult]:
    """Run pykrx endpoint diagnostics across dates and date formats."""
    del sample_size
    stock = stock_module or import_pykrx_stock()
    results: list[DiagnosticResult] = []

    for diagnostic_date in diagnostic_dates(target_date):
        for date_input in date_inputs(diagnostic_date):
            results.extend(run_date_format_diagnostics(stock, date_input, timeout_seconds))

    return results


def run_date_format_diagnostics(stock: Any, date_input: str, timeout_seconds: int) -> list[DiagnosticResult]:
    """Run all required endpoint probes for one date-input string."""
    return [
        diagnose_endpoint(
            "stock.get_market_ticker_list KOSPI",
            date_input,
            lambda: stock.get_market_ticker_list(date_input, market="KOSPI"),
            timeout_seconds,
        ),
        diagnose_endpoint(
            "stock.get_market_ticker_list KOSDAQ",
            date_input,
            lambda: stock.get_market_ticker_list(date_input, market="KOSDAQ"),
            timeout_seconds,
        ),
        diagnose_endpoint(
            "stock.get_market_ohlcv_by_ticker KOSPI",
            date_input,
            lambda: stock.get_market_ohlcv_by_ticker(date_input, market="KOSPI"),
            timeout_seconds,
            expected_any_columns=OHLCV_EXPECTED_ANY,
        ),
        diagnose_endpoint(
            "stock.get_market_ohlcv_by_ticker KOSDAQ",
            date_input,
            lambda: stock.get_market_ohlcv_by_ticker(date_input, market="KOSDAQ"),
            timeout_seconds,
            expected_any_columns=OHLCV_EXPECTED_ANY,
        ),
        diagnose_endpoint(
            "stock.get_market_ohlcv_by_date 005930",
            date_input,
            lambda: stock.get_market_ohlcv_by_date(date_input, date_input, "005930"),
            timeout_seconds,
            expected_any_columns=OHLCV_EXPECTED_ANY,
        ),
    ]


def diagnose_ticker_list(
    stock: Any,
    compact: str,
    market: str,
    timeout_seconds: int,
) -> tuple[DiagnosticResult, list[str]]:
    """Diagnose ticker-list endpoint and return ticker values on success."""
    payload: dict[str, Any] = {}

    def call() -> list[str]:
        tickers = stock.get_market_ticker_list(compact, market=market)
        payload["tickers"] = [str(ticker).zfill(6) for ticker in tickers]
        return tickers

    result = diagnose_endpoint(f"stock.get_market_ticker_list {market}", compact, call, timeout_seconds)
    return result, payload.get("tickers", []) if result.success else []


def diagnose_endpoint(
    endpoint: str,
    date_input_or_call: str | Callable[[], Any],
    call_or_timeout: Callable[[], Any] | int | None = None,
    timeout_seconds: int | None = None,
    expected_any_columns: tuple[str, ...] = (),
) -> DiagnosticResult:
    """Run one endpoint with a hard timeout.

    The second calling convention is preserved for older tests:
    diagnose_endpoint(endpoint, call, timeout_seconds).
    """
    if callable(date_input_or_call):
        date_input = ""
        call = date_input_or_call
        timeout = int(timeout_seconds if timeout_seconds is not None else call_or_timeout or TIMEOUT_SECONDS)
    else:
        date_input = str(date_input_or_call)
        call = call_or_timeout
        timeout = int(timeout_seconds or TIMEOUT_SECONDS)
    if not callable(call):
        raise TypeError("diagnose_endpoint requires a callable endpoint")

    started = time.perf_counter()
    try:
        payload = call_with_timeout(call, timeout)
        elapsed = time.perf_counter() - started
        metadata = describe_payload(payload)
        rows = metadata["rows_returned"]
        if rows == 0:
            return DiagnosticResult(
                endpoint,
                elapsed,
                False,
                0,
                "endpoint returned empty; likely market data unavailable for date",
                date_input=date_input,
                **metadata_for_result(metadata),
            )
        if expected_any_columns and not has_any_expected_column(payload, expected_any_columns):
            return DiagnosticResult(
                endpoint,
                elapsed,
                False,
                rows,
                "endpoint missing expected columns; likely market data unavailable for date",
                date_input=date_input,
                **metadata_for_result(metadata),
            )
        return DiagnosticResult(
            endpoint=endpoint,
            elapsed_seconds=elapsed,
            success=True,
            rows_returned=rows,
            exception_message="",
            date_input=date_input,
            **metadata_for_result(metadata),
        )
    except TimeoutError:
        elapsed = time.perf_counter() - started
        return DiagnosticResult(endpoint, elapsed, False, 0, "TIMEOUT", date_input=date_input, exception_type="TimeoutError")
    except Exception as exc:
        elapsed = time.perf_counter() - started
        return DiagnosticResult(
            endpoint,
            elapsed,
            False,
            0,
            str(exc),
            date_input=date_input,
            exception_type=type(exc).__name__,
        )


def call_with_timeout(call: Callable[[], Any], timeout_seconds: int) -> Any:
    """Run call in a daemon thread with a bounded join."""
    queue: Queue = Queue(maxsize=1)

    def worker() -> None:
        try:
            queue.put(("ok", call()))
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


def describe_payload(payload: Any) -> dict[str, Any]:
    """Return type, shape, row-count, columns, and preview metadata."""
    payload_type = type(payload).__name__
    rows = count_rows(payload)
    shape = ""
    columns: list[str] = []
    preview_rows: list[dict[str, str]] = []
    if isinstance(payload, pd.DataFrame):
        shape = str(tuple(payload.shape))
        columns = [str(column) for column in payload.columns]
        preview_rows = [
            {str(key): sanitize_text(value) for key, value in row.items()}
            for row in payload.head(3).astype(str).to_dict(orient="records")
        ]
    elif isinstance(payload, (list, tuple)):
        shape = f"({len(payload)},)"
        preview_rows = [{"value": sanitize_text(value)} for value in payload[:3]]
    return {
        "payload_type": payload_type,
        "rows_returned": rows,
        "shape": shape,
        "columns": columns,
        "preview_rows": preview_rows,
    }


def metadata_for_result(metadata: dict[str, Any]) -> dict[str, Any]:
    """Convert payload metadata to DiagnosticResult keyword arguments."""
    return {
        "payload_type": metadata["payload_type"],
        "shape": metadata["shape"],
        "columns": metadata["columns"],
        "preview_rows": metadata["preview_rows"],
    }


def count_rows(payload: Any) -> int:
    """Return row count for common pykrx payloads."""
    if payload is None:
        return 0
    if isinstance(payload, pd.DataFrame):
        return int(len(payload))
    try:
        return int(len(payload))
    except TypeError:
        return 1


def has_any_expected_column(payload: Any, expected_any_columns: tuple[str, ...]) -> bool:
    """Return whether payload exposes at least one expected OHLCV-like column."""
    if not isinstance(payload, pd.DataFrame):
        return True
    columns = {str(column).lower() for column in payload.columns}
    return any(column.lower() in columns for column in expected_any_columns)


def run_network_sanity_check(timeout_seconds: int = 10) -> NetworkCheckResult:
    """Check whether KRX public site is reachable."""
    started = time.perf_counter()
    try:
        response = call_with_timeout(lambda: requests.get("https://data.krx.co.kr", timeout=timeout_seconds), timeout_seconds + 2)
        elapsed = time.perf_counter() - started
        return NetworkCheckResult(
            elapsed_seconds=elapsed,
            success=True,
            status_code=int(response.status_code),
            content_type=str(response.headers.get("content-type", "")),
            preview=sanitize_text(response.text[:200]),
            exception_type="",
            exception_message="",
        )
    except Exception as exc:
        elapsed = time.perf_counter() - started
        return NetworkCheckResult(
            elapsed_seconds=elapsed,
            success=False,
            status_code=None,
            content_type="",
            preview="",
            exception_type=type(exc).__name__,
            exception_message=str(exc),
        )


def sanitize_text(value: Any) -> str:
    """Return a single-line printable preview string."""
    return str(value).replace("\r", " ").replace("\n", " ").strip()


def print_environment_info(environment: dict[str, str]) -> None:
    """Print diagnostic environment information."""
    print("Environment", flush=True)
    for key, value in environment.items():
        print(f"{key}: {value}", flush=True)
    print("", flush=True)


def print_network_check(network: NetworkCheckResult) -> None:
    """Print network sanity check result."""
    print("Network sanity check", flush=True)
    print(f"endpoint: https://data.krx.co.kr", flush=True)
    print(f"elapsed seconds: {network.elapsed_seconds:.2f}", flush=True)
    print(f"success/fail: {'success' if network.success else 'fail'}", flush=True)
    print(f"status_code: {network.status_code}", flush=True)
    print(f"content-type: {network.content_type}", flush=True)
    print(f"first 200 chars sanitized: {network.preview}", flush=True)
    if network.exception_message:
        print(f"exception: {network.exception_type}: {network.exception_message}", flush=True)
    print("", flush=True)


def print_results(results: list[DiagnosticResult]) -> None:
    """Print diagnostic results."""
    for result in results:
        status = "success" if result.success else "fail"
        print(f"endpoint: {result.endpoint}", flush=True)
        print(f"date input: {result.date_input}", flush=True)
        print(f"elapsed seconds: {result.elapsed_seconds:.2f}", flush=True)
        print(f"success/fail: {status}", flush=True)
        print(f"type: {result.payload_type}", flush=True)
        print(f"shape/row count: {result.shape or result.rows_returned}", flush=True)
        print(f"columns: {result.columns}", flush=True)
        print(f"first 3 rows: {result.preview_rows}", flush=True)
        print(f"exception type/message: {result.exception_type}: {result.exception_message}", flush=True)
        print("", flush=True)
    if any(not result.success for result in results):
        print("likely market data unavailable for date or endpoint/environment issue", flush=True)
    recommendation = final_recommendation(results)
    if recommendation:
        print(f"recommendation: {recommendation}", flush=True)


def write_report(
    requested_date: str,
    environment: dict[str, str],
    network: NetworkCheckResult,
    results: list[DiagnosticResult],
    report_dir: Path | None = None,
) -> Path:
    """Write markdown diagnostic report."""
    output_dir = report_dir or PROJECT_ROOT / "reports" / "daily"
    output_dir.mkdir(parents=True, exist_ok=True)
    compact = pd.Timestamp(requested_date).strftime("%Y%m%d")
    path = output_dir / f"pykrx_diagnostic_{compact}.md"
    lines = render_report(requested_date, environment, network, results)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def render_report(
    requested_date: str,
    environment: dict[str, str],
    network: NetworkCheckResult,
    results: list[DiagnosticResult],
) -> list[str]:
    """Render markdown diagnostic report lines."""
    lines = [
        "# pykrx Daily Download Diagnostic",
        "",
        f"Requested date: {requested_date}",
        "",
        "## Environment",
        "",
    ]
    lines.extend([f"- {key}: {value}" for key, value in environment.items()])
    lines.extend(
        [
            "",
            "## Network Sanity Check",
            "",
            f"- success: {network.success}",
            f"- status_code: {network.status_code}",
            f"- content_type: {network.content_type}",
            f"- elapsed_seconds: {network.elapsed_seconds:.2f}",
            f"- first_200_chars_sanitized: {network.preview}",
            f"- exception: {network.exception_type}: {network.exception_message}",
            "",
            "## Endpoint Matrix",
            "",
        ]
    )
    for result in results:
        lines.extend(
            [
                f"### {result.endpoint} / {result.date_input}",
                "",
                f"- elapsed_seconds: {result.elapsed_seconds:.2f}",
                f"- success: {result.success}",
                f"- type: {result.payload_type}",
                f"- shape_or_row_count: {result.shape or result.rows_returned}",
                f"- columns: {result.columns}",
                f"- first_3_rows: {result.preview_rows}",
                f"- exception: {result.exception_type}: {result.exception_message}",
                "",
            ]
        )
    failures = [result for result in results if not result.success]
    recommendation = final_recommendation(results)
    lines.extend(
        [
            "## Summary",
            "",
            f"- total_checks: {len(results)}",
            f"- failed_checks: {len(failures)}",
            f"- likely_market_data_unavailable_or_endpoint_issue: {bool(failures)}",
            f"- recommendation: {recommendation}",
            "",
        ]
    )
    return lines


def final_recommendation(results: list[DiagnosticResult]) -> str:
    """Return endpoint recommendation from diagnostic outcomes."""
    per_ticker_success = any(
        result.success and result.endpoint == "stock.get_market_ohlcv_by_date 005930"
        for result in results
    )
    market_wide_failed = any(
        (not result.success)
        and result.endpoint
        in {
            "stock.get_market_ticker_list KOSPI",
            "stock.get_market_ticker_list KOSDAQ",
            "stock.get_market_ohlcv_by_ticker KOSPI",
            "stock.get_market_ohlcv_by_ticker KOSDAQ",
        }
        for result in results
    )
    if per_ticker_success and market_wide_failed:
        return "Use per-ticker endpoint"
    if all(not result.success for result in results):
        return "Network or endpoint unavailable in this environment"
    return "No endpoint preference detected"


def import_pykrx_stock():
    """Import pykrx stock module lazily without printing import-time noise."""
    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        from pykrx import stock

    return stock


def main(argv: list[str] | None = None) -> int:
    """Diagnostic CLI main."""
    args = parse_args(argv)
    target_date = args.date or resolve_default_date(args.config)
    print(f"Diagnosing pykrx daily download for {target_date}", flush=True)
    environment = collect_environment_info()
    network = run_network_sanity_check()
    results = run_diagnostics(target_date, args.sample_size, args.timeout)
    print_environment_info(environment)
    print_network_check(network)
    print_results(results)
    report_path = write_report(target_date, environment, network, results)
    print(f"Diagnostic report written: {report_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
