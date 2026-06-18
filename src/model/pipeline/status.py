"""Status helpers for the daily update pipeline."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path


@dataclass
class DailyUpdateStatus:
    """Serializable status object for a daily pipeline run."""

    run_timestamp: str
    run_date: str | None
    as_of_date: str
    latest_clean_data_date: str | None
    target_update_date: str | None
    update_date: str | None
    prediction_date: str | None
    dry_run: bool
    skip_download: bool
    force: bool
    rolling_train_days: int
    production_mode: bool = False
    pipeline_stop_reason: str | None = None
    pipeline_exit_code: int | None = None
    pipeline_exit_message: str | None = None
    latest_available_market_date: str | None = None
    stale_data_blocked_by_production_policy: bool = False
    universe_count: int | None = None
    raw_rows_downloaded_or_found: int = 0
    raw_rows_added: int = 0
    cleaned_rows_added: int = 0
    invalid_ohlcv_rows: int = 0
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
    pykrx_download_method: str | None = None
    pykrx_tickers_requested: int = 0
    pykrx_tickers_downloaded: int = 0
    pykrx_tickers_failed: int = 0
    pykrx_failed_tickers_sample: list[str] = field(default_factory=list)
    pykrx_success_ratio: float = 0.0
    trading_value_estimated_count: int = 0
    attempted_download_date: str | None = None
    downloaded_update_date: str | None = None
    old_data_warning: str | None = None
    krx_login_attempted: bool = False
    krx_login_success: bool = False
    krx_login_timed_out: bool = False
    krx_login_failed: bool = False
    krx_login_error: str | None = None
    macro_update_mode: str | None = None
    macro_source_date: str | None = None
    macro_rows_added: int = 0
    macro_missing_after_update: dict[str, int] = field(default_factory=dict)
    daily_macro_snapshot_path: str | None = None
    macro_download_method: str | None = None
    macro_download_passed: bool = False
    macro_downloaded_date: str | None = None
    macro_download_failed_sources: list[str] = field(default_factory=list)
    macro_download_error: str | None = None
    macro_rows_downloaded: int = 0
    feature_rows_added: int = 0
    feature_rows_replaced: int = 0
    daily_feature_snapshot_path: str | None = None
    feature_update_mode: str | None = None
    feature_update_date: str | None = None
    feature_missing_count: int = 0
    feature_column_count: int = 0
    feature_ticker_count: int = 0
    feature_source_completeness_passed: bool = False
    expected_feature_date: str | None = None
    actual_krx_date: str | None = None
    actual_nasdaq_date: str | None = None
    actual_sp500_date: str | None = None
    actual_vix_date: str | None = None
    actual_wti_date: str | None = None
    actual_usdkrw_date: str | None = None
    actual_us10y_date: str | None = None
    us10y_check_enabled: bool = False
    actual_gold_date: str | None = None
    gold_check_enabled: bool = False
    actual_dxy_date: str | None = None
    dxy_check_enabled: bool = False
    failed_feature_sources: list[str] = field(default_factory=list)
    feature_update_executed: bool = False
    training_update_executed: bool = False
    rolling_train_executed: bool = False
    prediction_executed: bool = False
    top10_generated: bool = False
    training_rows_added: int = 0
    training_rows_replaced: int = 0
    daily_training_snapshot_path: str | None = None
    training_update_mode: str | None = None
    target_feature_dates_added: list[str] = field(default_factory=list)
    leakage_violations: int = 0
    forbidden_model_features_found: list[str] = field(default_factory=list)
    rolling_train_start_date: str | None = None
    rolling_train_end_date: str | None = None
    rolling_train_unique_dates: int = 0
    rolling_train_rows: int = 0
    selected_feature_count: int = 0
    model_output_dir: str | None = None
    prediction_output_csv: str | None = None
    prediction_output_parquet: str | None = None
    prediction_rows: int = 0
    model_feature_count: int = 0
    top10_report_csv: str | None = None
    top10_report_xlsx: str | None = None
    daily_summary_report: str | None = None
    top10_tickers: list[str] = field(default_factory=list)
    top10_ticker_names: list[str] = field(default_factory=list)
    top10_average_ai_score: float = 0.0
    top10_average_expected_return: float = 0.0
    archive_created: bool = False
    archive_path: str | None = None
    rolling_training_dataset_path_csv: str | None = None
    rolling_training_dataset_path_parquet: str | None = None
    archive_prediction_path: str | None = None
    archive_model_path: str | None = None
    archive_top10_path: str | None = None
    archive_metadata_path: str | None = None
    archive_latest_path: str | None = None
    archive_integrity_passed: bool = False
    archive_sha256_generated: bool = False
    archive_readme_path: str | None = None
    completed_steps: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @classmethod
    def create(
        cls,
        as_of_date: str,
        dry_run: bool,
        skip_download: bool,
        force: bool,
        rolling_train_days: int,
        run_date: str | None = None,
        latest_clean_data_date: str | None = None,
        target_update_date: str | None = None,
        update_date: str | None = None,
        prediction_date: str | None = None,
    ) -> "DailyUpdateStatus":
        """Create a new run status."""
        return cls(
            run_timestamp=datetime.now(UTC).isoformat(),
            run_date=run_date,
            as_of_date=as_of_date,
            latest_clean_data_date=latest_clean_data_date,
            target_update_date=target_update_date,
            update_date=update_date,
            prediction_date=prediction_date,
            dry_run=dry_run,
            skip_download=skip_download,
            force=force,
            rolling_train_days=rolling_train_days,
        )

    def to_dict(self) -> dict[str, object]:
        """Return JSON-serializable status data."""
        return asdict(self)

    def to_json(self) -> str:
        """Return pretty-printed status JSON."""
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)

    def __int__(self) -> int:
        """Return process exit code represented by this status."""
        if self.pipeline_exit_code is not None:
            return int(self.pipeline_exit_code)
        return 1 if self.pipeline_stop_reason else 0


def status_path(status_dir: Path, as_of_date: str) -> Path:
    """Return the status JSON path for a date."""
    compact_date = as_of_date.replace("-", "")
    return status_dir / f"daily_update_status_{compact_date}.json"


def write_status(status: DailyUpdateStatus, path: Path) -> Path:
    """Write status JSON to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(status.to_json(), encoding="utf-8")
    return path
