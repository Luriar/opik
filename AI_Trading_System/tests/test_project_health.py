
"""
tests/test_project_health.py

High-level project health checks for AI Trading System v1.0.

This test does not replace detailed unit tests.
It verifies that the project has the minimum structure required
to be considered implementation-ready.
"""

from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(".")


REQUIRED_ROOT_FILES = {
    "README.md",
    "AGENTS.md",
    "IMPLEMENTATION_GUIDE.md",
    "PROJECT_TASKS.md",
    "PROJECT_STATUS.md",
    "CODING_STANDARDS.md",
    "requirements.txt",
    "pyproject.toml",
    ".env.example",
    ".gitignore",
}


REQUIRED_DOCS = {
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
}


REQUIRED_CONFIGS = {
    "feature.yaml",
    "model.yaml",
    "validation.yaml",
    "portfolio.yaml",
    "backtest.yaml",
    "execution.yaml",
}


REQUIRED_TESTS = {
    "test_config.py",
    "test_project_structure.py",
    "test_project_health.py",
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
}


REQUIRED_SRC_DIRS = {
    "src/data",
    "src/features",
    "src/models",
    "src/validation",
    "src/portfolio",
    "src/backtest",
    "src/execution",
    "src/utils",
}


def test_root_files_exist() -> None:
    missing = [
        file_name
        for file_name in REQUIRED_ROOT_FILES
        if not (PROJECT_ROOT / file_name).exists()
    ]

    assert not missing, f"Missing root files: {missing}"


def test_docs_are_complete() -> None:
    docs_dir = PROJECT_ROOT / "docs"

    missing = [
        file_name
        for file_name in REQUIRED_DOCS
        if not (docs_dir / file_name).exists()
    ]

    assert not missing, f"Missing docs: {missing}"


def test_configs_are_complete() -> None:
    config_dir = PROJECT_ROOT / "configs"

    missing = [
        file_name
        for file_name in REQUIRED_CONFIGS
        if not (config_dir / file_name).exists()
    ]

    assert not missing, f"Missing configs: {missing}"


def test_tests_are_complete() -> None:
    tests_dir = PROJECT_ROOT / "tests"

    missing = [
        file_name
        for file_name in REQUIRED_TESTS
        if not (tests_dir / file_name).exists()
    ]

    assert not missing, f"Missing test files: {missing}"


def test_src_directories_are_complete() -> None:
    missing = [
        directory
        for directory in REQUIRED_SRC_DIRS
        if not (PROJECT_ROOT / directory).exists()
    ]

    assert not missing, f"Missing source directories: {missing}"


def test_src_packages_have_init_files() -> None:
    missing = []

    for directory in REQUIRED_SRC_DIRS:
        init_file = PROJECT_ROOT / directory / "__init__.py"
        if not init_file.exists():
            missing.append(str(init_file))

    assert not missing, f"Missing __init__.py files: {missing}"


def test_outputs_directory_exists() -> None:
    outputs = PROJECT_ROOT / "outputs"

    assert outputs.exists()
    assert outputs.is_dir()


def test_project_has_minimum_document_count() -> None:
    docs = list((PROJECT_ROOT / "docs").glob("*.md"))

    assert len(docs) >= 10


def test_project_has_minimum_config_count() -> None:
    configs = list((PROJECT_ROOT / "configs").glob("*.yaml"))

    assert len(configs) >= 6


def test_project_has_minimum_test_count() -> None:
    tests = list((PROJECT_ROOT / "tests").glob("test_*.py"))

    assert len(tests) >= 15


def test_no_empty_required_documents() -> None:
    files = [PROJECT_ROOT / file_name for file_name in REQUIRED_ROOT_FILES]

    files += [
        PROJECT_ROOT / "docs" / file_name
        for file_name in REQUIRED_DOCS
    ]

    empty_files = [
        str(path)
        for path in files
        if path.exists() and path.stat().st_size == 0
    ]

    assert not empty_files, f"Empty required files: {empty_files}"


def test_no_empty_config_files() -> None:
    files = [
        PROJECT_ROOT / "configs" / file_name
        for file_name in REQUIRED_CONFIGS
    ]

    empty_files = [
        str(path)
        for path in files
        if path.exists() and path.stat().st_size == 0
    ]

    assert not empty_files, f"Empty config files: {empty_files}"


def test_no_empty_test_files() -> None:
    files = [
        PROJECT_ROOT / "tests" / file_name
        for file_name in REQUIRED_TESTS
    ]

    empty_files = [
        str(path)
        for path in files
        if path.exists() and path.stat().st_size == 0
    ]

    assert not empty_files, f"Empty test files: {empty_files}"
