"""Project path helpers for Phase 0 infrastructure."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


RUNTIME_DIRECTORIES: tuple[str, ...] = (
    "docs",
    "configs",
    "tests",
    "src",
    "outputs",
    "notebooks",
    "data",
    "logs",
)


@dataclass(frozen=True)
class ProjectPaths:
    """Common project paths used by the pipeline."""

    root: Path
    configs: Path
    data: Path
    outputs: Path
    logs: Path
    docs: Path
    tests: Path
    src: Path
    notebooks: Path


def get_project_root(start: str | Path | None = None) -> Path:
    """Return the project root by walking upward to pyproject.toml."""
    current = Path(start or Path.cwd()).resolve()
    if current.is_file():
        current = current.parent

    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate

    raise FileNotFoundError("Could not locate project root containing pyproject.toml")


def build_project_paths(root: str | Path | None = None) -> ProjectPaths:
    """Build the standard project path collection."""
    project_root = Path(root).resolve() if root is not None else get_project_root()
    return ProjectPaths(
        root=project_root,
        configs=project_root / "configs",
        data=project_root / "data",
        outputs=project_root / "outputs",
        logs=project_root / "logs",
        docs=project_root / "docs",
        tests=project_root / "tests",
        src=project_root / "src",
        notebooks=project_root / "notebooks",
    )


def ensure_runtime_directories(root: str | Path | None = None) -> ProjectPaths:
    """Create required Phase 0 runtime directories if they are missing."""
    paths = build_project_paths(root)
    for directory in RUNTIME_DIRECTORIES:
        (paths.root / directory).mkdir(parents=True, exist_ok=True)
    return paths
