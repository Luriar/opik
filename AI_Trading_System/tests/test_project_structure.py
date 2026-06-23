"""
tests/test_project_structure.py

Project structure tests for AI Trading System v1.0.
"""

from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(".")


# ==========================================================
# Required Directories
# ==========================================================

REQUIRED_DIRECTORIES = [
    "src",
    "docs",
    "configs",
    "tests",
    "outputs",
]


# ==========================================================
# Required Source Directories
# ==========================================================

REQUIRED_SRC_DIRECTORIES = [
    "src/data",
    "src/features",
    "src/models",
    "src/validation",
    "src/portfolio",
    "src/backtest",
    "src/execution",
    "src/utils",
]


# ==========================================================
# Required Documents
# ==========================================================

REQUIRED_DOCUMENTS = [
    "README.md",
    "AGENTS.md",
    "IMPLEMENTATION_GUIDE.md",
    "PROJECT_TASKS.md",
    "PROJECT_STATUS.md",
    "CODING_STANDARDS.md",
]


# ==========================================================
# Required Docs
# ==========================================================

REQUIRED_DOCS = [
    "01_system_architecture.md",
    "02_universe.md",
    "03_targets.md",
    "04_models.md",
    "05_feature_library.md",
    "06_data_leakage_rules.md",
    "07_walk_forward_validation.md",
    "08_backtest.md",
    "09_portfolio.md",
    "10_execution.md",
]


# ==========================================================
# Required Configs
# ==========================================================

REQUIRED_CONFIGS = [
    "feature.yaml",
    "model.yaml",
    "validation.yaml",
    "portfolio.yaml",
    "backtest.yaml",
    "execution.yaml",
]


# ==========================================================
# Required Tests
# ==========================================================

REQUIRED_TESTS = [
    "test_config.py",
    "test_project_structure.py",
    "test_data_loader.py",
    "test_universe.py",
    "test_feature_generation.py",
    "test_data_leakage.py",
    "test_model_training.py",
    "test_prediction.py",
    "test_walk_forward.py",
    "test_portfolio.py",
    "test_backtest.py",
    "test_execution.py",
    "test_pipeline.py",
    "test_integration.py",
]


# ==========================================================
# Project Directories
# ==========================================================

def test_required_directories_exist() -> None:

    missing = []

    for directory in REQUIRED_DIRECTORIES:
        if not (PROJECT_ROOT / directory).exists():
            missing.append(directory)

    assert not missing, f"Missing directories: {missing}"


def test_required_src_directories_exist() -> None:

    missing = []

    for directory in REQUIRED_SRC_DIRECTORIES:
        if not (PROJECT_ROOT / directory).exists():
            missing.append(directory)

    assert not missing, f"Missing src directories: {missing}"


# ==========================================================
# Documents
# ==========================================================

def test_required_documents_exist() -> None:

    missing = []

    for document in REQUIRED_DOCUMENTS:
        if not (PROJECT_ROOT / document).exists():
            missing.append(document)

    assert not missing, f"Missing documents: {missing}"


def test_required_docs_exist() -> None:

    docs_dir = PROJECT_ROOT / "docs"

    missing = []

    for document in REQUIRED_DOCS:
        if not (docs_dir / document).exists():
            missing.append(document)

    assert not missing, f"Missing docs: {missing}"


# ==========================================================
# Configs
# ==========================================================

def test_required_configs_exist() -> None:

    config_dir = PROJECT_ROOT / "configs"

    missing = []

    for config in REQUIRED_CONFIGS:
        if not (config_dir / config).exists():
            missing.append(config)

    assert not missing, f"Missing configs: {missing}"


# ==========================================================
# Tests
# ==========================================================

def test_required_test_files_exist() -> None:

    tests_dir = PROJECT_ROOT / "tests"

    missing = []

    for test_file in REQUIRED_TESTS:
        if not (tests_dir / test_file).exists():
            missing.append(test_file)

    assert not missing, f"Missing tests: {missing}"


# ==========================================================
# Output Directory
# ==========================================================

def test_outputs_directory_exists() -> None:

    outputs = PROJECT_ROOT / "outputs"

    assert outputs.exists()
    assert outputs.is_dir()


# ==========================================================
# Source Package Structure
# ==========================================================

def test_src_has_init_files() -> None:

    packages = [
        "src",
        "src/data",
        "src/features",
        "src/models",
        "src/validation",
        "src/portfolio",
        "src/backtest",
        "src/execution",
        "src/utils",
    ]

    missing = []

    for package in packages:
        init_file = PROJECT_ROOT / package / "__init__.py"

        if not init_file.exists():
            missing.append(str(init_file))

    assert not missing, f"Missing __init__.py: {missing}"


# ==========================================================
# pyproject
# ==========================================================

def test_pyproject_exists() -> None:

    assert (PROJECT_ROOT / "pyproject.toml").exists()


def test_requirements_exists() -> None:

    assert (PROJECT_ROOT / "requirements.txt").exists()


# ==========================================================
# Environment
# ==========================================================

def test_env_example_exists() -> None:

    assert (PROJECT_ROOT / ".env.example").exists()


def test_gitignore_exists() -> None:

    assert (PROJECT_ROOT / ".gitignore").exists()