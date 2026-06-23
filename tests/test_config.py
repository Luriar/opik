
"""
tests/test_config.py

Configuration tests for AI Trading System v1.0.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml


CONFIG_DIR = Path("configs")

REQUIRED_CONFIG_FILES = [
    "feature.yaml",
    "model.yaml",
    "validation.yaml",
    "portfolio.yaml",
    "backtest.yaml",
    "execution.yaml",
]


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file)

    assert isinstance(data, dict), f"{path} must contain a YAML mapping"
    return data


def test_config_directory_exists() -> None:
    assert CONFIG_DIR.exists()
    assert CONFIG_DIR.is_dir()


def test_required_config_files_exist() -> None:
    missing = [
        file_name
        for file_name in REQUIRED_CONFIG_FILES
        if not (CONFIG_DIR / file_name).exists()
    ]

    assert not missing, f"Missing config files: {missing}"


@pytest.mark.parametrize("file_name", REQUIRED_CONFIG_FILES)
def test_config_files_are_valid_yaml(file_name: str) -> None:
    path = CONFIG_DIR / file_name
    load_yaml(path)


@pytest.mark.parametrize("file_name", REQUIRED_CONFIG_FILES)
def test_config_has_version(file_name: str) -> None:
    data = load_yaml(CONFIG_DIR / file_name)

    root_key = file_name.replace(".yaml", "")
    assert root_key in data, f"{file_name} must have root key '{root_key}'"

    assert "version" in data[root_key], f"{file_name} must define version"


def test_feature_config_required_sections() -> None:
    data = load_yaml(CONFIG_DIR / "feature.yaml")
    feature = data["feature"]

    required = {
        "global_rules",
        "price",
        "momentum",
        "volume",
        "volatility",
        "technical",
        "cross_sectional",
        "macro",
        "identity",
        "model_feature_sets",
        "leakage_rules",
    }

    missing = required - set(feature.keys())
    assert not missing, f"feature.yaml missing sections: {missing}"


def test_model_config_required_models() -> None:
    data = load_yaml(CONFIG_DIR / "model.yaml")
    model = data["model"]

    required = {
        "ranking_model",
        "gap_model",
        "intraday_model",
        "training",
        "prediction",
    }

    missing = required - set(model.keys())
    assert not missing, f"model.yaml missing sections: {missing}"


def test_validation_config_walk_forward_enabled() -> None:
    data = load_yaml(CONFIG_DIR / "validation.yaml")
    validation = data["validation"]

    assert validation["method"] == "walk_forward"
    assert validation["walk_forward"]["enabled"] is True
    assert validation["split_rules"]["allow_random_split"] is False
    assert validation["split_rules"]["allow_shuffle"] is False


def test_portfolio_config_values_are_consistent() -> None:
    data = load_yaml(CONFIG_DIR / "portfolio.yaml")
    portfolio = data["portfolio"]

    assert portfolio["candidate_size"] >= portfolio["portfolio_size"]
    assert portfolio["portfolio_size"] > 0
    assert portfolio["weighting_method"] == "equal_weight"


def test_backtest_config_cost_and_slippage_exist() -> None:
    data = load_yaml(CONFIG_DIR / "backtest.yaml")
    backtest = data["backtest"]

    assert "transaction_cost" in backtest
    assert "slippage" in backtest

    assert backtest["transaction_cost"]["buy_cost"] >= 0
    assert backtest["transaction_cost"]["sell_cost"] >= 0
    assert backtest["slippage"]["buy_slippage"] >= 0
    assert backtest["slippage"]["sell_slippage"] >= 0


def test_execution_config_required_sections() -> None:
    data = load_yaml(CONFIG_DIR / "execution.yaml")
    execution = data["execution"]

    required = {
        "mode",
        "initial_capital",
        "order",
        "cost",
        "slippage",
        "risk_check",
        "failure_policy",
        "output",
    }

    missing = required - set(execution.keys())
    assert not missing, f"execution.yaml missing sections: {missing}"


def test_model_targets_are_not_features() -> None:
    feature_data = load_yaml(CONFIG_DIR / "feature.yaml")
    model_data = load_yaml(CONFIG_DIR / "model.yaml")

    excluded = set(feature_data["feature"]["excluded_columns"])

    targets = {
        model_data["model"]["ranking_model"]["target"],
        model_data["model"]["gap_model"]["target"],
        model_data["model"]["intraday_model"]["target"],
    }

    assert targets.issubset(excluded)


def test_common_categorical_features_match() -> None:
    feature_data = load_yaml(CONFIG_DIR / "feature.yaml")
    model_data = load_yaml(CONFIG_DIR / "model.yaml")

    feature_cats = set(feature_data["feature"]["categorical_features"])
    model_cats = set(model_data["model"]["common"]["categorical_features"])

    assert feature_cats == model_cats


def test_random_seed_exists() -> None:
    data = load_yaml(CONFIG_DIR / "model.yaml")
    assert data["model"]["common"]["random_seed"] == 42

