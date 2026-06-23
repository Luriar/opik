"""Configuration loading utilities for the AI Trading System."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


REQUIRED_CONFIG_FILES: tuple[str, ...] = (
    "feature.yaml",
    "model.yaml",
    "validation.yaml",
    "portfolio.yaml",
    "backtest.yaml",
    "execution.yaml",
)


class ConfigError(ValueError):
    """Raised when a configuration file is missing or invalid."""


def load_yaml_config(path: str | Path) -> dict[str, Any]:
    """Load one YAML config file and return its mapping content."""
    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(f"Config file does not exist: {config_path}")

    try:
        with config_path.open("r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML config: {config_path}") from exc

    if not isinstance(data, dict):
        raise ConfigError(f"Config must contain a YAML mapping: {config_path}")

    return data


def validate_required_configs(
    config_dir: str | Path = "configs",
    required_files: tuple[str, ...] = REQUIRED_CONFIG_FILES,
) -> None:
    """Validate that all required config files exist."""
    base_dir = Path(config_dir)
    missing = [file_name for file_name in required_files if not (base_dir / file_name).exists()]
    if missing:
        raise ConfigError(f"Missing config files: {missing}")


def load_all_configs(
    config_dir: str | Path = "configs",
    required_files: tuple[str, ...] = REQUIRED_CONFIG_FILES,
) -> dict[str, dict[str, Any]]:
    """Load all required YAML configs keyed by config root name."""
    validate_required_configs(config_dir, required_files)

    configs: dict[str, dict[str, Any]] = {}
    base_dir = Path(config_dir)
    for file_name in required_files:
        root_key = file_name.removesuffix(".yaml")
        data = load_yaml_config(base_dir / file_name)
        if root_key not in data:
            raise ConfigError(f"{file_name} must define root key '{root_key}'")
        configs[root_key] = data[root_key]

    return configs
