"""Configuration helpers for the daily update pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.model.utils.config_loader import load_yaml_config
from src.model.utils.paths import build_project_paths


DIRECTORY_KEYS: tuple[str, ...] = (
    "daily_raw_dir",
    "daily_processed_dir",
    "daily_feature_dir",
    "daily_training_dir",
    "daily_prediction_dir",
    "daily_model_dir",
    "daily_report_dir",
    "daily_status_dir",
    "log_dir",
)


@dataclass(frozen=True)
class DailyUpdateConfig:
    """Loaded daily update configuration with path helpers."""

    config_path: Path
    values: dict[str, Any]
    project_root: Path

    @property
    def rolling_train_days(self) -> int:
        """Return rolling train window size in trading days."""
        return int(self.values["rolling_train_days"])

    def resolve_path(self, key: str) -> Path:
        """Resolve one config path key against the project root."""
        value = self.values[key]
        path = Path(value)
        return path if path.is_absolute() else self.project_root / path

    def required_directories(self) -> dict[str, Path]:
        """Return configured runtime directories keyed by config name."""
        return {key: self.resolve_path(key) for key in DIRECTORY_KEYS}


def load_daily_update_config(
    config_path: str | Path = "configs/daily_update.yaml",
) -> DailyUpdateConfig:
    """Load daily update config from YAML."""
    project_root = build_project_paths().root
    path = Path(config_path)
    if not path.is_absolute():
        path = project_root / path
    data = load_yaml_config(path)
    values = data.get("daily_update", data)
    if not isinstance(values, dict):
        raise ValueError("daily_update config must be a mapping")
    _validate_config(values)
    return DailyUpdateConfig(config_path=path, values=values, project_root=project_root)


def ensure_daily_directories(config: DailyUpdateConfig, dry_run: bool = False) -> list[Path]:
    """Create configured daily pipeline directories unless dry-run is enabled."""
    directories = list(config.required_directories().values())
    if dry_run:
        return directories
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    return directories


def _validate_config(values: dict[str, Any]) -> None:
    required = {
        "universe_file",
        "ticker_name_file",
        "raw_ohlcv_file",
        "clean_ohlcv_file",
        "macro_file",
        "feature_file",
        "training_dataset_file",
        "rolling_train_days",
        "recommended_run_time",
        "market_open_time",
        "pipeline_start_time",
        "production_mode",
        "strict_feature_source_check",
        "enable_us10y_check",
        "enable_gold_check",
        "enable_dxy_check",
        "model_n_estimators_cap",
        *DIRECTORY_KEYS,
    }
    missing = sorted(required - set(values))
    if missing:
        raise ValueError(f"daily_update config missing keys: {missing}")
