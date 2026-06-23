"""Daily update pipeline entrypoint.

Implemented parts cover scaffolding, date context, OHLCV update/checking,
macro update/checking, optimized daily feature-store updates, training dataset
updates, rolling-window selection, daily model training, daily predictions, and
Top10 report generation.
"""

from __future__ import annotations

import argparse
import builtins
import time
import sys
import threading
from contextlib import contextmanager
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline.config import ensure_daily_directories, load_daily_update_config  # noqa: E402
from src.pipeline.archive import create_daily_archive, copy_status_into_archive, refresh_latest_archive  # noqa: E402
from src.pipeline.daily_context import build_daily_run_context, fallback_to_latest_clean_context  # noqa: E402
from src.pipeline.daily_model import train_daily_models  # noqa: E402
from src.pipeline.daily_prediction import generate_daily_predictions  # noqa: E402
from src.pipeline.daily_report import generate_daily_top10_report, write_feature_source_failure_summary  # noqa: E402
from src.pipeline.env import load_project_env  # noqa: E402
from src.pipeline.feature_update import run_feature_update  # noqa: E402
from src.pipeline.feature_source_completeness import FeatureSourceCompletenessChecker  # noqa: E402
from src.pipeline.macro_download import (  # noqa: E402
    MacroDataUnavailableError,
    print_macro_download_failure,
    print_macro_download_success,
    run_production_macro_download as run_macro_update,
)
from src.pipeline.market_data_update import run_ohlcv_update  # noqa: E402
from src.pipeline.status import DailyUpdateStatus, status_path, write_status  # noqa: E402
from src.pipeline.training_update import run_training_update  # noqa: E402
from src.utils.logger import get_logger  # noqa: E402


TOTAL_STEPS = 12
CURRENT_STATUS: DailyUpdateStatus | None = None
CURRENT_CONFIG = None


def print(*args, **kwargs) -> None:  # noqa: A001
    """Print and flush immediately for morning operation visibility."""
    kwargs.setdefault("flush", True)
    try:
        builtins.print(*args, **kwargs)
    except UnicodeEncodeError:
        safe_args = tuple(str(arg).replace("█", "#").replace("░", "-") for arg in args)
        builtins.print(*safe_args, **kwargs)


SOURCE_DISPLAY_NAMES: dict[str, str] = {
    "krx": "KRX",
    "nasdaq": "NASDAQ",
    "sox": "SOX",
    "sp500": "S&P500",
    "vix": "VIX",
    "wti": "WTI",
    "usdkrw": "USD/KRW",
    "us10y": "US10Y",
    "gold": "Gold",
    "dxy": "DXY",
}

SOURCE_STATUS_ORDER: tuple[str, ...] = (
    "krx",
    "nasdaq",
    "sox",
    "sp500",
    "vix",
    "wti",
    "usdkrw",
    "us10y",
    "gold",
    "dxy",
)


def progress_bar(step_number: int, total_steps: int = TOTAL_STEPS, width: int = 13) -> str:
    """Return a simple text progress bar."""
    filled = max(0, min(width, round(width * step_number / total_steps)))
    return "█" * filled + "░" * (width - filled)


def emit_step_progress(step_number: int, step_name: str, status: str, elapsed: float) -> None:
    """Print one progress update block."""
    percent = round(step_number / TOTAL_STEPS * 100)
    print(f"[{step_number:02d}/{TOTAL_STEPS}] {step_name}")
    print(f"Progress: {progress_bar(step_number)} {percent}%")
    print(f"Status: {status}")
    print(f"Elapsed: {elapsed:.1f}s")
    print("")


@contextmanager
def pipeline_step(step_number: int, step_name: str, logger=None):
    """Print and log START/SUCCESS/FAILED for a pipeline step."""
    started = time.monotonic()
    emit_step_progress(step_number, step_name, "START", 0.0)
    if logger is not None:
        logger.info("START", extra={"step": step_name, "status": "START"})
    try:
        yield
    except Exception:
        elapsed = time.monotonic() - started
        emit_step_progress(step_number, step_name, "FAILED", elapsed)
        if logger is not None:
            logger.exception(f"FAILED elapsed={elapsed:.2f}s", extra={"step": step_name, "status": "FAILED"})
        raise
    else:
        elapsed = time.monotonic() - started
        emit_step_progress(step_number, step_name, "SUCCESS", elapsed)
        if logger is not None:
            logger.info(f"SUCCESS elapsed={elapsed:.2f}s", extra={"step": step_name, "status": "SUCCESS"})


def emit_heartbeat(step_name: str, elapsed: float) -> None:
    """Print a long-running step heartbeat."""
    print("Still running...")
    print(f"Current step: {step_name}")
    print(f"Elapsed: {elapsed:.1f}s")
    print("Press Ctrl+C to stop safely.")
    print("")


def print_production_stop(status: DailyUpdateStatus, source_check: dict[str, object]) -> None:
    """Print a clear strict-production stop block."""
    target_date = status.target_update_date or "UNKNOWN"
    latest_date = status.latest_available_market_date or status.latest_clean_data_date or "UNKNOWN"
    print("============================================================")
    print("AI Trading Daily Update Pipeline - STOPPED")
    print("============================================================")
    print("Reason:")
    print("Latest required market data is unavailable.")
    print("")
    print("Target update date:")
    print(target_date)
    print("")
    print("Latest available data:")
    print(latest_date)
    print("")
    print("Failed sources:")
    for source in status.failed_feature_sources:
        actual = source_check.get(f"actual_{source}_date") or "missing"
        print(f"- {SOURCE_DISPLAY_NAMES.get(source, source.upper())}: expected {status.expected_feature_date}, actual {actual}")
    print("")
    print("Top10:")
    print("NOT GENERATED")
    print("")
    print("Prediction:")
    print("NOT EXECUTED")
    print("")
    print("Exit code:")
    print("1")
    print("")
    print("============================================================")
    print("")
    print("Downloaded market date")
    print("")
    print(latest_date)
    print("")
    print("Target update date")
    print("")
    print(target_date)
    sys.stdout.flush()
    sys.stderr.flush()


def print_pipeline_finished(exit_code: int) -> None:
    """Print final pipeline exit code block."""
    print("")
    print("============================================================")
    print("")
    print("Pipeline finished.")
    print("")
    print(f"Exit code: {exit_code}")
    print("")
    print("============================================================")


def write_best_effort_failure_status(reason: str) -> None:
    """Write the current status JSON if pipeline context exists."""
    if CURRENT_STATUS is None or CURRENT_CONFIG is None:
        return
    CURRENT_STATUS.errors.append(reason)
    if CURRENT_STATUS.pipeline_stop_reason is None:
        CURRENT_STATUS.pipeline_stop_reason = reason
    try:
        output_status_path = status_path(CURRENT_CONFIG.resolve_path("daily_status_dir"), CURRENT_STATUS.as_of_date)
        write_status(CURRENT_STATUS, output_status_path)
        print(f"Status JSON written: {output_status_path}")
    except Exception as exc:  # pragma: no cover - best effort only
        print(f"Could not write status JSON: {exc}")
    try:
        report_date = CURRENT_STATUS.prediction_date or CURRENT_STATUS.as_of_date
        compact = str(report_date).replace("-", "")
        summary_path = CURRENT_CONFIG.resolve_path("daily_report_dir") / f"daily_update_summary_{compact}.md"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            "\n".join(
                [
                    "# Daily Update Summary",
                    "",
                    "Status: FAILED",
                    f"Reason: {reason}",
                    "",
                    "Top10: NOT GENERATED",
                    "Prediction: NOT EXECUTED",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        CURRENT_STATUS.daily_summary_report = str(summary_path)
        print(f"Failure summary written: {summary_path}")
        output_status_path = status_path(CURRENT_CONFIG.resolve_path("daily_status_dir"), CURRENT_STATUS.as_of_date)
        write_status(CURRENT_STATUS, output_status_path)
    except Exception as exc:  # pragma: no cover - best effort only
        print(f"Could not write failure summary: {exc}")


@contextmanager
def heartbeat(step_name: str, interval_seconds: float = 30.0):
    """Emit periodic heartbeat messages while a long step is running."""
    stop_event = threading.Event()
    started = time.monotonic()

    def worker() -> None:
        while not stop_event.wait(interval_seconds):
            emit_heartbeat(step_name, time.monotonic() - started)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop_event.set()
        thread.join(timeout=2.0)


def archive_inputs_exist(model_result, prediction_result, report_result) -> bool:
    """Return whether all production archive input files exist."""
    model_paths = getattr(model_result, "model_paths", {}) or {}
    required_model_keys = {"ranking_model", "gap_model", "intraday_model"}
    if not required_model_keys <= set(model_paths):
        return False
    paths = [
        *[model_paths[key] for key in sorted(required_model_keys)],
        getattr(prediction_result, "prediction_output_csv", None),
        getattr(prediction_result, "prediction_output_parquet", None),
        getattr(report_result, "top10_report_csv", None),
        getattr(report_result, "top10_report_xlsx", None),
        getattr(report_result, "daily_summary_report", None),
    ]
    return all(path is not None and Path(path).exists() for path in paths)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse daily update CLI arguments."""
    parser = argparse.ArgumentParser(description="Run the AI Trading System daily update pipeline.")
    parser.add_argument("--as-of-date", default=date.today().isoformat(), help="Pipeline as-of date, YYYY-MM-DD.")
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without creating files.")
    parser.add_argument("--skip-download", action="store_true", help="Skip download step in future pipeline parts.")
    parser.add_argument("--no-download", action="store_true", help="Skip KRX/pykrx download and use existing data.")
    parser.add_argument("--no-login", action="store_true", help="Compatibility no-op; daily updates use pykrx only.")
    parser.add_argument("--check-sources-only", action="store_true", help="Run source completeness checks and exit.")
    parser.add_argument("--diagnose-download", action="store_true", help="Run pykrx download diagnostics only.")
    parser.add_argument("--force", action="store_true", help="Overwrite future daily outputs if present.")
    parser.add_argument("--config", default="configs/daily_update.yaml", help="Path to daily update YAML config.")
    return parser.parse_args(argv)


def run_pipeline(args: argparse.Namespace) -> DailyUpdateStatus:
    """Run the implemented daily pipeline steps."""
    global CURRENT_CONFIG, CURRENT_STATUS
    with pipeline_step(1, "Config"):
        config = load_daily_update_config(args.config)
        CURRENT_CONFIG = config
        env_warnings = load_project_env(config.project_root)
        print(f"Selected config path: {config.config_path}")
        print(f"Project root: {config.project_root}")
    with pipeline_step(2, "Context"):
        context = build_daily_run_context(
            config=config,
            as_of_date=args.as_of_date,
            dry_run=bool(args.dry_run),
            skip_download=bool(args.skip_download or args.no_download),
            force=bool(args.force),
        )
        print("Selected dates:")
        print(f"- run_date: {context.run_date}")
        print(f"- as_of_date: {context.as_of_date}")
        print(f"- latest_clean_data_date: {context.latest_clean_data_date}")
        print(f"- target_update_date: {context.target_update_date}")
        print(f"- update_date: {context.update_date}")
        print(f"- prediction_date: {context.prediction_date}")

    log_path = config.resolve_path("log_dir") / f"daily_update_{context.as_of_date.replace('-', '')}.log"
    logger = get_logger(
        "daily_update_pipeline",
        run_id=f"daily_update_{context.as_of_date.replace('-', '')}",
        log_file=None if args.dry_run else log_path,
    )

    status = DailyUpdateStatus.create(
        as_of_date=context.as_of_date,
        dry_run=bool(args.dry_run),
        skip_download=bool(args.skip_download or args.no_download),
        force=bool(args.force),
        rolling_train_days=config.rolling_train_days,
        run_date=context.run_date,
        latest_clean_data_date=context.latest_clean_data_date,
        target_update_date=context.target_update_date,
        update_date=context.update_date,
        prediction_date=context.prediction_date,
    )
    CURRENT_STATUS = status
    status.production_mode = bool(config.values.get("production_mode", False))
    status.latest_available_market_date = context.latest_clean_data_date
    status.warnings.extend(env_warnings)
    status.warnings.extend(context.warnings)
    status.completed_steps.append("config_loaded")
    status.completed_steps.append("context_built")

    directories = ensure_daily_directories(config, dry_run=args.dry_run)
    status.completed_steps.append("directories_checked" if args.dry_run else "directories_created")

    if args.diagnose_download:
        from scripts.diagnose_pykrx_daily_download import print_results, run_diagnostics

        with pipeline_step(3, "KRX Download Diagnostics", logger), heartbeat("KRX Download Diagnostics"):
            diagnostic_results = run_diagnostics(context.target_update_date)
            print_results(diagnostic_results)
        status.pipeline_exit_code = 0
        status.pipeline_exit_message = "Download diagnostics completed"
        status.completed_steps.append("download_diagnostics_completed")
        with pipeline_step(12, "Status Write", logger):
            output_status_path = status_path(config.resolve_path("daily_status_dir"), context.as_of_date)
            status.completed_steps.append("status_written")
            write_status(status, output_status_path)
        print(f"Status JSON written: {output_status_path}")
        print(status.to_json())
        return status

    should_attempt_download = not bool(args.dry_run or args.skip_download or args.no_download)

    with pipeline_step(3, "KRX Download", logger), heartbeat("KRX Download"):
        ohlcv_result = run_ohlcv_update(
            config=config,
            context=context,
            dry_run=bool(args.dry_run),
            skip_download=not should_attempt_download,
            force=bool(args.force),
        )
    status.universe_count = ohlcv_result.universe_count
    status.raw_rows_downloaded_or_found = ohlcv_result.raw_rows_downloaded_or_found
    status.raw_rows_added = ohlcv_result.raw_rows_added
    status.cleaned_rows_added = ohlcv_result.cleaned_rows_added
    status.invalid_ohlcv_rows = ohlcv_result.invalid_ohlcv_rows
    status.invalid_ohlcv_tickers = ohlcv_result.invalid_ohlcv_tickers
    status.missing_005930 = ohlcv_result.missing_005930
    status.daily_ohlcv_snapshot_paths = ohlcv_result.daily_ohlcv_snapshot_paths
    status.ohlcv_download_mode = ohlcv_result.ohlcv_download_mode
    status.ohlcv_download_attempts = ohlcv_result.ohlcv_download_attempts
    status.ohlcv_download_timed_out = ohlcv_result.ohlcv_download_timed_out
    status.ohlcv_download_failed = ohlcv_result.ohlcv_download_failed
    status.ohlcv_download_error = ohlcv_result.ohlcv_download_error
    status.used_existing_data_fallback = ohlcv_result.used_existing_data_fallback
    status.pykrx_rows_returned = ohlcv_result.pykrx_rows_returned
    status.pykrx_empty_response = ohlcv_result.pykrx_empty_response
    status.pykrx_missing_columns = ohlcv_result.pykrx_missing_columns
    status.pykrx_data_unavailable = ohlcv_result.pykrx_data_unavailable
    status.pykrx_download_method = ohlcv_result.pykrx_download_method
    status.pykrx_tickers_requested = ohlcv_result.pykrx_tickers_requested
    status.pykrx_tickers_downloaded = ohlcv_result.pykrx_tickers_downloaded
    status.pykrx_tickers_failed = ohlcv_result.pykrx_tickers_failed
    status.pykrx_failed_tickers_sample = ohlcv_result.pykrx_failed_tickers_sample
    status.pykrx_success_ratio = ohlcv_result.pykrx_success_ratio
    status.trading_value_estimated_count = ohlcv_result.trading_value_estimated_count
    if should_attempt_download:
        status.attempted_download_date = context.target_update_date
    status.warnings.extend(ohlcv_result.warnings)
    status.errors.extend(ohlcv_result.errors)
    status.completed_steps.append("ohlcv_update_checked")
    if should_attempt_download and not ohlcv_result.ohlcv_download_failed and ohlcv_result.raw_rows_downloaded_or_found > 0:
        status.downloaded_update_date = context.target_update_date
        status.latest_available_market_date = context.target_update_date

    effective_context = context
    if ohlcv_result.used_existing_data_fallback and context.latest_clean_data_date:
        effective_context = fallback_to_latest_clean_context(context)
        status.update_date = effective_context.update_date
        status.prediction_date = effective_context.prediction_date
        status.used_existing_data_fallback = True
        status.latest_available_market_date = effective_context.update_date
        status.old_data_warning = "OLD_DATA: latest market data unavailable; using latest existing clean data"
        status.warnings.append(status.old_data_warning)
        if status.pykrx_data_unavailable:
            print(f"KRX data for target date {context.target_update_date} is unavailable.")
            print("Production pipeline stopped." if status.production_mode else "Using fallback data.")

    strict_feature_source_check = bool(config.values.get("strict_feature_source_check", False))
    strict_production_mode = status.production_mode and strict_feature_source_check

    with pipeline_step(4, "Macro Update", logger):
        try:
            macro_result = run_macro_update(
                config=config,
                context=context,
                dry_run=bool(args.dry_run),
                force=bool(args.force),
            )
        except MacroDataUnavailableError as exc:
            status.macro_download_method = "yfinance"
            status.macro_download_passed = False
            status.macro_download_failed_sources = [exc.feature]
            status.macro_download_error = exc.reason
            status.pipeline_stop_reason = "Macro Download Failed"
            status.pipeline_exit_code = 1
            status.pipeline_exit_message = "Required macro data unavailable"
            status.completed_steps.append("macro_download_failed")
            print_macro_download_failure(exc)
            summary_path = config.resolve_path("daily_report_dir") / f"daily_update_summary_{context.target_update_date.replace('-', '')}.md"
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary_path.write_text(
                "\n".join(
                    [
                        "# Daily Update Summary",
                        "",
                        "Status: FAILED",
                        "Reason: Macro Download Failed",
                        f"Feature: {exc.feature}",
                        f"Expected Date: {exc.expected_date}",
                        f"Error: {exc.reason}",
                        "",
                        "Feature Update: NOT EXECUTED",
                        "Training Update: NOT EXECUTED",
                        "Prediction: NOT EXECUTED",
                        "Top10: NOT GENERATED",
                        "Archive: NOT CREATED",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            status.daily_summary_report = str(summary_path)
            with pipeline_step(12, "Status Write", logger):
                output_status_path = status_path(config.resolve_path("daily_status_dir"), context.as_of_date)
                status.completed_steps.append("status_written")
                write_status(status, output_status_path)
            print(f"Failure summary written: {summary_path}")
            print(f"Status JSON written: {output_status_path}")
            print(status.to_json())
            return status
    status.macro_update_mode = macro_result.macro_update_mode
    status.macro_source_date = macro_result.macro_source_date
    status.macro_rows_added = macro_result.macro_rows_added
    status.macro_missing_after_update = macro_result.macro_missing_after_update
    status.daily_macro_snapshot_path = macro_result.daily_macro_snapshot_path
    status.macro_download_method = getattr(macro_result, "macro_download_method", "yfinance")
    status.macro_download_passed = bool(getattr(macro_result, "macro_download_passed", True))
    status.macro_downloaded_date = getattr(macro_result, "macro_downloaded_date", status.macro_source_date)
    status.macro_download_failed_sources = list(getattr(macro_result, "macro_download_failed_sources", []))
    status.macro_download_error = getattr(macro_result, "macro_download_error", None)
    status.macro_rows_downloaded = int(getattr(macro_result, "macro_rows_downloaded", status.macro_rows_added))
    status.macro_invalid_target_date_rows = dict(getattr(macro_result, "macro_invalid_target_date_rows", {}))
    status.macro_source_actual_dates = dict(getattr(macro_result, "actual_source_dates", {}))
    status.macro_source_expected_dates = dict(getattr(macro_result, "expected_source_dates", {}))
    status.sources_using_prior_trading_day = list(
        getattr(macro_result, "sources_using_prior_trading_day", [])
    )
    macro_file_path = getattr(macro_result, "macro_file_path", None)
    if macro_file_path:
        config.values["macro_file"] = macro_file_path
    status.warnings.extend(macro_result.warnings)
    status.errors.extend(macro_result.errors)
    status.completed_steps.append("macro_update_checked")
    if status.macro_download_passed:
        print_macro_download_success()

    with pipeline_step(5, "Feature Source Check", logger):
        expected_source_date = context.target_update_date if strict_production_mode else effective_context.update_date
        source_check = FeatureSourceCompletenessChecker(config, expected_source_date).check()
        stale_data_blocked = bool(strict_production_mode and status.used_existing_data_fallback)
        if strict_production_mode and str(source_check["expected_date"]) != str(context.target_update_date):
            stale_data_blocked = True
        if stale_data_blocked:
            source_check["all_available"] = False
            status.stale_data_blocked_by_production_policy = True
            stale_failures = list(source_check.get("failed_sources", []))
            if not stale_failures:
                stale_failures = ["krx"]
            source_check["failed_sources"] = stale_failures
            stale_reason = (
                "stale data fallback is not allowed in production strict mode: "
                f"target_update_date={context.target_update_date}, latest_clean_data_date={context.latest_clean_data_date}"
            )
            existing_reason = str(source_check.get("failure_reason", ""))
            source_check["failure_reason"] = f"{existing_reason}; {stale_reason}" if existing_reason else stale_reason
        downloaded_actual_dates = dict(getattr(macro_result, "actual_source_dates", {}))
        if downloaded_actual_dates and not args.dry_run:
            for source, actual_date in downloaded_actual_dates.items():
                source_check[f"actual_{source}_date"] = actual_date
                source_check[f"expected_{source}_date"] = context.target_update_date
            source_check["sources_using_prior_trading_day"] = list(
                getattr(macro_result, "sources_using_prior_trading_day", [])
            )
        print("Macro Validation")
        for source in SOURCE_STATUS_ORDER:
            label = SOURCE_DISPLAY_NAMES[source]
            if not bool(source_check.get(f"{source}_check_enabled", True)):
                state = "SKIPPED (disabled in config)"
            else:
                state = "PASS" if bool(source_check.get(f"{source}_available", False)) else "FAIL"
            print(f"{label:.<12} {state}")
            if source in {"nasdaq", "sox", "sp500", "vix", "wti", "usdkrw"}:
                print(f"actual: {source_check.get(f'actual_{source}_date') or 'missing'}")
                if source == "sox" and source_check.get("sox_failure_reason"):
                    print(f"reason: {source_check['sox_failure_reason']}")
                if source in source_check.get("sources_using_prior_trading_day", []):
                    invalid_reason = status.macro_invalid_target_date_rows.get(source)
                    reason = f"prior valid row used; {invalid_reason}" if invalid_reason else "prior valid trading day used"
                    print(f"reason: {reason}")
    status.feature_source_completeness_passed = bool(source_check["all_available"])
    status.expected_feature_date = str(source_check["expected_date"])
    status.actual_krx_date = source_check.get("actual_krx_date")
    status.actual_nasdaq_date = source_check.get("actual_nasdaq_date")
    status.actual_sox_date = source_check.get("actual_sox_date")
    status.sox_check_enabled = bool(source_check.get("sox_check_enabled", False))
    status.sox_close_present = bool(source_check.get("sox_close_present", False))
    status.sox_return_present = bool(source_check.get("sox_return_present", False))
    status.sox_return_non_null_count = int(source_check.get("sox_return_non_null_count", 0))
    status.sox_failure_reason = source_check.get("sox_failure_reason")
    status.actual_sp500_date = source_check.get("actual_sp500_date")
    status.actual_vix_date = source_check.get("actual_vix_date")
    status.actual_wti_date = source_check.get("actual_wti_date")
    status.actual_usdkrw_date = source_check.get("actual_usdkrw_date")
    status.macro_date_policy = source_check.get("macro_date_policy")
    status.us_market_holiday_detected = bool(source_check.get("us_market_holiday_detected", False))
    status.us_market_holiday_reason = source_check.get("us_market_holiday_reason")
    status.sources_using_prior_trading_day = list(source_check.get("sources_using_prior_trading_day", []))
    status.macro_source_actual_dates = {
        source: source_check.get(f"actual_{source}_date")
        for source in ("nasdaq", "sox", "sp500", "vix", "wti", "usdkrw")
    }
    status.macro_source_expected_dates = {
        source: str(source_check.get(f"expected_{source}_date") or status.expected_feature_date)
        for source in ("nasdaq", "sox", "sp500", "vix", "wti", "usdkrw")
    }
    prior_without_invalid_target = [
        source for source in status.sources_using_prior_trading_day
        if source not in status.macro_invalid_target_date_rows
    ]
    status.us_market_holiday_detected = bool(prior_without_invalid_target)
    status.us_market_holiday_reason = (
        "US market holiday or non-trading day" if prior_without_invalid_target else None
    )
    status.actual_us10y_date = source_check.get("actual_us10y_date")
    status.us10y_check_enabled = bool(source_check.get("us10y_check_enabled", False))
    status.actual_gold_date = source_check.get("actual_gold_date")
    status.gold_check_enabled = bool(source_check.get("gold_check_enabled", False))
    status.actual_dxy_date = source_check.get("actual_dxy_date")
    status.dxy_check_enabled = bool(source_check.get("dxy_check_enabled", False))
    status.failed_feature_sources = list(source_check.get("failed_sources", []))
    if not status.feature_source_completeness_passed:
        status.warnings.append(f"feature_source_completeness_failed: {source_check['failure_reason']}")
    status.completed_steps.append("feature_source_completeness_checked")

    should_stop_for_sources = (
        strict_production_mode
        and not status.feature_source_completeness_passed
        and not args.dry_run
    )
    if should_stop_for_sources:
        status.pipeline_stop_reason = (
            "Feature Source Completeness Failed: stale data"
            if status.stale_data_blocked_by_production_policy
            else "Feature Source Completeness Failed"
        )
        status.pipeline_exit_code = 1
        status.pipeline_exit_message = "Latest required market data unavailable"
        actual_source_dates = {
            source: source_check.get(f"actual_{source}_date") for source in SOURCE_STATUS_ORDER
        }
        print_production_stop(status, source_check)
        print("STRICT_PRODUCTION_STOP_EXIT_CODE=1")
        sys.stdout.flush()
        sys.stderr.flush()
        summary_path = write_feature_source_failure_summary(
            config=config,
            report_date=status.expected_feature_date,
            expected_feature_date=str(status.expected_feature_date),
            failed_sources=status.failed_feature_sources,
            actual_source_dates=actual_source_dates,
            target_update_date=context.target_update_date,
            latest_clean_data_date=context.latest_clean_data_date,
            stale_data_blocked=status.stale_data_blocked_by_production_policy,
        )
        status.daily_summary_report = str(summary_path)
        status.completed_steps.append("production_stopped_feature_source_completeness")

        log_path = config.resolve_path("log_dir") / f"daily_update_{context.as_of_date.replace('-', '')}.log"
        logger = get_logger(
            "daily_update_pipeline",
            run_id=f"daily_update_{context.as_of_date.replace('-', '')}",
            log_file=log_path,
        )
        logger.info("pipeline_start", extra={"step": "start", "status": "started"})
        logger.error(
            str(source_check["failure_reason"]),
            extra={"step": "feature_source_completeness", "status": "failed"},
        )
        with pipeline_step(12, "Status Write", logger):
            output_status_path = status_path(config.resolve_path("daily_status_dir"), context.as_of_date)
            status.completed_steps.append("status_written")
            write_status(status, output_status_path)
        print(f"Failure summary written: {summary_path}")
        print(f"Status JSON written: {output_status_path}")
        print(status.to_json())
        return status

    if args.check_sources_only:
        status.pipeline_exit_code = 0
        status.pipeline_exit_message = "Source completeness check passed"
        with pipeline_step(12, "Status Write", logger):
            output_status_path = status_path(config.resolve_path("daily_status_dir"), context.as_of_date)
            status.completed_steps.append("status_written")
            write_status(status, output_status_path)
        print(f"Status JSON written: {output_status_path}")
        print(status.to_json())
        return status

    with pipeline_step(6, "Feature Update", logger), heartbeat("Feature Update"):
        feature_result = run_feature_update(
            config=config,
            context=effective_context,
            dry_run=bool(args.dry_run),
            force=bool(args.force),
        )
    status.feature_rows_added = feature_result.feature_rows_added
    status.feature_rows_replaced = feature_result.feature_rows_replaced
    status.daily_feature_snapshot_path = feature_result.daily_feature_snapshot_path
    status.feature_update_mode = feature_result.feature_update_mode
    status.feature_update_date = feature_result.feature_update_date
    status.feature_missing_count = feature_result.feature_missing_count
    status.feature_column_count = feature_result.feature_column_count
    status.feature_ticker_count = feature_result.feature_ticker_count
    status.warnings.extend(feature_result.warnings)
    status.errors.extend(feature_result.errors)
    status.feature_update_executed = True
    status.completed_steps.append("feature_update_checked")

    with pipeline_step(7, "Training Dataset Update", logger), heartbeat("Training Dataset Update"):
        training_result = run_training_update(
            config=config,
            context=effective_context,
            dry_run=bool(args.dry_run),
            force=bool(args.force),
        )
    status.training_rows_added = training_result.training_rows_added
    status.training_rows_replaced = training_result.training_rows_replaced
    status.daily_training_snapshot_path = training_result.daily_training_snapshot_path
    status.training_update_mode = training_result.training_update_mode
    status.target_feature_dates_added = training_result.target_feature_dates_added
    status.leakage_violations = training_result.leakage_violations
    status.forbidden_model_features_found = training_result.forbidden_model_features_found
    status.rolling_train_start_date = training_result.rolling_train_start_date
    status.rolling_train_end_date = training_result.rolling_train_end_date
    status.rolling_train_unique_dates = training_result.rolling_train_unique_dates
    status.rolling_train_rows = training_result.rolling_train_rows
    status.selected_feature_count = training_result.selected_feature_count
    status.warnings.extend(training_result.warnings)
    status.errors.extend(training_result.errors)
    status.training_update_executed = True
    status.completed_steps.append("training_dataset_update_checked")
    with pipeline_step(8, "Rolling Train Window", logger):
        status.rolling_train_executed = True
        status.completed_steps.append("rolling_train_window_selected")

    if args.dry_run:
        status.warnings.append("dry_run_skipped_model_training_and_prediction_writes")
    else:
        with pipeline_step(9, "Model Training", logger), heartbeat("Model Training"):
            model_result = train_daily_models(
                config=config,
                prediction_date=effective_context.prediction_date,
            )
        status.model_output_dir = model_result.model_output_dir
        status.model_feature_count = len(model_result.feature_columns)
        status.completed_steps.append("models_trained")

        with pipeline_step(10, "Prediction", logger), heartbeat("Prediction"):
            prediction_result = generate_daily_predictions(
                config=config,
                model_bundle=model_result.model_bundle,
                update_date=effective_context.update_date,
                prediction_date=effective_context.prediction_date,
                train_start_date=model_result.train_start_date,
                train_end_date=model_result.train_end_date,
                rolling_train_days=model_result.rolling_train_days,
            )
        status.prediction_output_csv = prediction_result.prediction_output_csv
        status.prediction_output_parquet = prediction_result.prediction_output_parquet
        status.prediction_rows = prediction_result.prediction_rows
        status.prediction_executed = True
        status.completed_steps.append("daily_predictions_generated")

        with pipeline_step(11, "Top10 Report", logger), heartbeat("Top10 Report"):
            report_result = generate_daily_top10_report(
                config=config,
                prediction_date=effective_context.prediction_date,
                prediction_csv=prediction_result.prediction_output_csv,
                prediction_parquet=prediction_result.prediction_output_parquet,
                old_data_warning=status.old_data_warning,
                us_market_holiday_detected=status.us_market_holiday_detected,
                holiday_date=context.target_update_date,
                sources_using_prior_trading_day=status.sources_using_prior_trading_day,
            )
        status.top10_report_csv = report_result.top10_report_csv
        status.top10_report_xlsx = report_result.top10_report_xlsx
        status.daily_summary_report = report_result.daily_summary_report
        status.top10_tickers = report_result.top10_tickers
        status.top10_ticker_names = report_result.top10_ticker_names
        status.top10_average_ai_score = report_result.top10_average_ai_score
        status.top10_average_expected_return = report_result.top10_average_expected_return
        status.warnings.extend(report_result.warnings)
        status.top10_generated = True
        status.completed_steps.append("top10_report_generated")
        if archive_inputs_exist(model_result, prediction_result, report_result):
            archive_result = create_daily_archive(
                config=config,
                prediction_date=effective_context.prediction_date,
                model_result=model_result,
                prediction_result=prediction_result,
                report_result=report_result,
                status=status,
            )
            status.archive_created = True
            status.archive_path = archive_result.archive_path
            status.rolling_training_dataset_path_csv = archive_result.rolling_training_dataset_path_csv
            status.rolling_training_dataset_path_parquet = archive_result.rolling_training_dataset_path_parquet
            status.archive_prediction_path = archive_result.archive_prediction_path
            status.archive_model_path = archive_result.archive_model_path
            status.archive_top10_path = archive_result.archive_top10_path
            status.archive_metadata_path = archive_result.metadata_path
            status.archive_latest_path = archive_result.latest_path
            status.archive_integrity_passed = archive_result.integrity_passed
            status.archive_sha256_generated = archive_result.sha256_generated
            status.archive_readme_path = archive_result.readme_path
            status.completed_steps.append("daily_archive_created")
        else:
            status.warnings.append("daily_archive_skipped_missing_mock_artifacts")

    log_path = config.resolve_path("log_dir") / f"daily_update_{context.as_of_date.replace('-', '')}.log"
    logger = get_logger(
        "daily_update_pipeline",
        run_id=f"daily_update_{context.as_of_date.replace('-', '')}",
        log_file=None if args.dry_run else log_path,
    )
    logger.info("pipeline_start", extra={"step": "start", "status": "started"})
    logger.info(
        (
            f"cli_options dry_run={args.dry_run} skip_download={args.skip_download} "
            f"no_download={args.no_download} force={args.force}"
        ),
        extra={"step": "cli", "status": "success"},
    )
    logger.info(str(config.config_path), extra={"step": "config", "status": "success"})
    logger.info(
        f"update_date={status.update_date} prediction_date={status.prediction_date}",
        extra={"step": "date_context", "status": "success"},
    )
    logger.info(
        (
            f"raw_rows={status.raw_rows_downloaded_or_found} raw_added={status.raw_rows_added} "
            f"clean_added={status.cleaned_rows_added} download_mode={status.ohlcv_download_mode}"
        ),
        extra={"step": "ohlcv_update", "status": "success"},
    )
    logger.info(
        f"macro_mode={status.macro_update_mode} macro_rows_added={status.macro_rows_added}",
        extra={"step": "macro_update", "status": "success"},
    )
    logger.info(
        (
            f"feature_mode={status.feature_update_mode} "
            f"feature_added={status.feature_rows_added} "
            f"feature_replaced={status.feature_rows_replaced}"
        ),
        extra={"step": "feature_update", "status": "success"},
    )
    logger.info(
        (
            f"training_mode={status.training_update_mode} "
            f"training_added={status.training_rows_added} "
            f"rolling_dates={status.rolling_train_unique_dates}"
        ),
        extra={"step": "training_update", "status": "success"},
    )
    logger.info(
        f"model_output_dir={status.model_output_dir} feature_count={status.model_feature_count}",
        extra={"step": "model_training", "status": "success"},
    )
    logger.info(
        f"prediction_rows={status.prediction_rows} prediction_csv={status.prediction_output_csv}",
        extra={"step": "daily_prediction", "status": "success"},
    )
    logger.info(
        f"top10_csv={status.top10_report_csv} top10_xlsx={status.top10_report_xlsx}",
        extra={"step": "daily_report", "status": "success"},
    )

    with pipeline_step(12, "Status Write", logger):
        status.pipeline_exit_code = 0
        status.pipeline_exit_message = "Success"
        if args.dry_run:
            status.warnings.append("dry_run_enabled_no_files_written")
            logger.info("dry_run_status_preview", extra={"step": "dry_run", "status": "success"})
            print("Dry-run mode: no directories or status files will be written.")
            print("Directories that would be created or verified:")
            for directory in directories:
                print(f"- {directory}")
            print("Status JSON preview:")
            print(status.to_json())
            logger.info(",".join(status.completed_steps), extra={"step": "completed_steps", "status": "success"})
            return status

        output_status_path = status_path(config.resolve_path("daily_status_dir"), context.as_of_date)
        status.completed_steps.append("status_written")
        write_status(status, output_status_path)
        if status.archive_created and status.archive_path:
            copy_status_into_archive(output_status_path, status.archive_path, context.as_of_date)
            refresh_latest_archive(status.archive_path)
            write_status(status, output_status_path)
            copy_status_into_archive(output_status_path, status.archive_path, context.as_of_date)
            refresh_latest_archive(status.archive_path)
    logger.info(",".join(status.completed_steps), extra={"step": "completed_steps", "status": "success"})
    print(f"Status JSON written: {output_status_path}")
    print(status.to_json())
    return status


def main(argv: list[str] | None = None) -> int:
    """CLI main."""
    try:
        args = parse_args(argv)
        status = run_pipeline(args)
        exit_code = int(status)
        status.pipeline_exit_code = exit_code
        if status.pipeline_exit_message is None:
            status.pipeline_exit_message = "Success" if exit_code == 0 else str(status.pipeline_stop_reason)
        print_pipeline_finished(exit_code)
        return exit_code
    except KeyboardInterrupt:
        print("Interrupted by user (Ctrl+C). Cleaning up...")
        write_best_effort_failure_status("Interrupted by user")
        print_pipeline_finished(130)
        return 130
    except Exception as exc:
        print("Pipeline FAILED")
        print(f"Failure reason: {exc}")
        write_best_effort_failure_status(str(exc))
        print_pipeline_finished(1)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
