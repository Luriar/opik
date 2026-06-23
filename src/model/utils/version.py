"""Version metadata helpers."""

from __future__ import annotations

import subprocess
import tomllib
from pathlib import Path
from typing import Any

from src.model.utils.config_loader import ConfigError, load_yaml_config
from src.model.utils.paths import get_project_root


def get_project_version(root: str | Path | None = None) -> str:
    """Return the project version from pyproject.toml."""
    project_root = Path(root).resolve() if root is not None else get_project_root()
    pyproject_path = project_root / "pyproject.toml"
    with pyproject_path.open("rb") as file:
        data = tomllib.load(file)
    return str(data.get("project", {}).get("version", "unknown"))


def get_config_version(config: dict[str, Any], root_key: str) -> str:
    """Return a version value from a loaded config mapping."""
    return str(config.get(root_key, {}).get("version", "unknown"))


def get_feature_version(config_path: str | Path = "configs/feature.yaml") -> str:
    """Return feature config version."""
    config = load_yaml_config(config_path)
    return get_config_version(config, "feature")


def get_model_version(config_path: str | Path = "configs/model.yaml") -> str:
    """Return model config version."""
    config = load_yaml_config(config_path)
    return get_config_version(config, "model")


def get_git_commit(root: str | Path | None = None) -> str | None:
    """Return the current git commit hash when available."""
    project_root = Path(root).resolve() if root is not None else get_project_root()
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None

    return result.stdout.strip() or None


def collect_version_metadata(root: str | Path | None = None) -> dict[str, str | None]:
    """Collect project, config, feature, model, and optional git metadata."""
    project_root = Path(root).resolve() if root is not None else get_project_root()
    metadata: dict[str, str | None] = {
        "project_version": get_project_version(project_root),
        "git_commit": get_git_commit(project_root),
    }

    try:
        metadata["feature_version"] = get_feature_version(
            project_root / "configs" / "feature.yaml"
        )
        metadata["model_version"] = get_model_version(
            project_root / "configs" / "model.yaml"
        )
    except ConfigError:
        metadata["feature_version"] = None
        metadata["model_version"] = None

    return metadata
