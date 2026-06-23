"""Daily production archive helpers."""

from __future__ import annotations

import json
import hashlib
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.config import DailyUpdateConfig
from src.pipeline.daily_model import DailyModelTrainingResult
from src.pipeline.daily_prediction import DailyPredictionResult
from src.pipeline.daily_report import DailyReportResult
from src.pipeline.status import DailyUpdateStatus


@dataclass(frozen=True)
class DailyArchiveResult:
    """Production archive output paths."""

    archive_path: str
    rolling_training_dataset_path_csv: str
    rolling_training_dataset_path_parquet: str
    archive_prediction_path: str
    archive_model_path: str
    archive_top10_path: str
    metadata_path: str
    latest_path: str
    readme_path: str
    integrity_passed: bool
    sha256_generated: bool


def create_daily_archive(
    config: DailyUpdateConfig,
    prediction_date: str | pd.Timestamp,
    model_result: DailyModelTrainingResult,
    prediction_result: DailyPredictionResult,
    report_result: DailyReportResult,
    status: DailyUpdateStatus,
) -> DailyArchiveResult:
    """Create a complete daily production archive for a successful run."""
    if not status.feature_source_completeness_passed:
        raise ValueError("Archive requires passed feature source completeness")
    if not status.prediction_executed:
        raise ValueError("Archive requires successful prediction")
    if not status.top10_generated:
        raise ValueError("Archive requires successful Top10 report")

    compact = pd.Timestamp(prediction_date).strftime("%Y%m%d")
    archive_root = config.project_root / "outputs" / "archive" / compact
    top10_dir = archive_root / "top10"
    prediction_dir = archive_root / "predictions"
    model_dir = archive_root / "models"
    training_dir = archive_root / "training"
    status_dir = archive_root / "status"
    metadata_dir = archive_root / "metadata"
    for directory in [top10_dir, prediction_dir, model_dir, training_dir, status_dir, metadata_dir]:
        directory.mkdir(parents=True, exist_ok=True)

    top10_csv = copy_file(report_result.top10_report_csv, top10_dir / f"top10_{compact}.csv")
    top10_xlsx = copy_file(report_result.top10_report_xlsx, top10_dir / f"top10_{compact}.xlsx")
    prediction_csv = copy_file(prediction_result.prediction_output_csv, prediction_dir / f"predictions_{compact}.csv")
    prediction_parquet = copy_file(
        prediction_result.prediction_output_parquet,
        prediction_dir / f"predictions_{compact}.parquet",
    )
    model_paths = copy_model_files(model_result.model_paths, model_dir)

    rolling_train_csv = training_dir / f"rolling_train_{compact}.csv"
    rolling_train_parquet = training_dir / f"rolling_train_{compact}.parquet"
    rolling_train = model_result.train_df.copy()
    rolling_train.to_csv(rolling_train_csv, index=False, encoding="utf-8-sig")
    rolling_train.to_parquet(rolling_train_parquet, index=False)

    summary_path = copy_file(report_result.daily_summary_report, status_dir / f"daily_update_summary_{compact}.md")
    status_json_path = status_dir / f"daily_update_status_{compact}.json"
    status_json_path.write_text(status.to_json(), encoding="utf-8")
    readme_path = archive_root / "README.txt"
    write_archive_readme(
        readme_path=readme_path,
        run_date=status.as_of_date,
        prediction_date=pd.Timestamp(prediction_date).date().isoformat(),
        target_update_date=status.target_update_date,
        train_start_date=model_result.train_start_date,
        train_end_date=model_result.train_end_date,
        rolling_train_rows=len(rolling_train),
        model_feature_count=len(model_result.feature_columns),
        prediction_rows=prediction_result.prediction_rows,
    )

    hashes = build_archive_hashes(
        model_paths=model_paths,
        rolling_train_parquet=rolling_train_parquet,
        prediction_parquet=prediction_parquet,
        top10_xlsx=top10_xlsx,
    )
    metadata_path = metadata_dir / "archive_metadata.json"
    metadata = build_archive_metadata(
        status=status,
        prediction_date=pd.Timestamp(prediction_date).date().isoformat(),
        model_result=model_result,
        prediction_result=prediction_result,
        report_result=report_result,
        rolling_train=rolling_train,
        artifact_paths={
            "top10_csv": top10_csv,
            "top10_xlsx": top10_xlsx,
            "prediction_csv": prediction_csv,
            "prediction_parquet": prediction_parquet,
            "model_paths": model_paths,
            "rolling_train_csv": rolling_train_csv,
            "rolling_train_parquet": rolling_train_parquet,
            "summary_path": summary_path,
            "status_json_path": status_json_path,
            "readme_path": readme_path,
        },
        hashes=hashes,
    )
    metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")

    required_files = required_archive_files(archive_root, compact)
    integrity_passed = verify_archive_integrity(required_files)
    latest_path = update_latest_archive(archive_root, config.project_root / "outputs" / "archive" / "latest")

    print_archive_success(archive_root, len(rolling_train), prediction_result.prediction_rows)
    print_archive_verification_success()
    return DailyArchiveResult(
        archive_path=str(archive_root),
        rolling_training_dataset_path_csv=str(rolling_train_csv),
        rolling_training_dataset_path_parquet=str(rolling_train_parquet),
        archive_prediction_path=str(prediction_dir),
        archive_model_path=str(model_dir),
        archive_top10_path=str(top10_dir),
        metadata_path=str(metadata_path),
        latest_path=str(latest_path),
        readme_path=str(readme_path),
        integrity_passed=integrity_passed,
        sha256_generated=True,
    )


def copy_status_into_archive(status_json_path: str | Path, archive_path: str | Path, as_of_date: str) -> Path:
    """Copy final status JSON into an existing archive status directory."""
    compact = pd.Timestamp(as_of_date).strftime("%Y%m%d")
    destination = Path(archive_path) / "status" / f"daily_update_status_{compact}.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(status_json_path, destination)
    return destination


def refresh_latest_archive(archive_path: str | Path) -> Path:
    """Refresh the latest archive mirror after final status is written."""
    archive = Path(archive_path)
    latest = archive.parent / "latest"
    return update_latest_archive(archive, latest)


def copy_file(source: str | Path, destination: Path) -> Path:
    """Copy a required file to destination."""
    source_path = Path(source)
    if not source_path.exists():
        raise FileNotFoundError(f"Archive source file missing: {source_path}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, destination)
    return destination


def copy_model_files(model_paths: dict[str, str], model_dir: Path) -> dict[str, Path]:
    """Copy model text files using canonical names."""
    copied: dict[str, Path] = {}
    for model_key in ("ranking_model", "gap_model", "intraday_model"):
        source = model_paths.get(model_key)
        if source is None:
            raise FileNotFoundError(f"Missing model path for archive: {model_key}")
        copied[model_key] = copy_file(source, model_dir / f"{model_key}.txt")
    return copied


def build_archive_metadata(
    status: DailyUpdateStatus,
    prediction_date: str,
    model_result: DailyModelTrainingResult,
    prediction_result: DailyPredictionResult,
    report_result: DailyReportResult,
    rolling_train: pd.DataFrame,
    artifact_paths: dict[str, Any],
    hashes: dict[str, Any],
) -> dict[str, Any]:
    """Build reproducibility metadata for a daily archive."""
    return {
        "run_date": status.as_of_date,
        "prediction_date": prediction_date,
        "target_update_date": status.target_update_date,
        "rolling_train_start_date": model_result.train_start_date,
        "rolling_train_end_date": model_result.train_end_date,
        "rolling_train_unique_dates": int(model_result.rolling_train_days),
        "rolling_train_rows": int(len(rolling_train)),
        "feature_count": int(len(model_result.feature_columns)),
        "model_feature_count": int(len(model_result.feature_columns)),
        "prediction_rows": int(prediction_result.prediction_rows),
        "top10_average_ai_score": float(getattr(report_result, "top10_average_ai_score", 0.0)),
        "top10_average_expected_return": float(getattr(report_result, "top10_average_expected_return", 0.0)),
        "python_version": sys.version,
        "lightgbm_version": get_optional_package_version("lightgbm"),
        "pandas_version": pd.__version__,
        "pipeline_version": get_pipeline_version(),
        "git_commit_hash": get_git_commit_hash(),
        "creation_timestamp": datetime.now(UTC).isoformat(),
        "model_sha256": hashes["model_sha256"],
        "rolling_train_sha256": hashes["rolling_train_sha256"],
        "prediction_sha256": hashes["prediction_sha256"],
        "top10_sha256": hashes["top10_sha256"],
        "artifacts": stringify_artifact_paths(artifact_paths),
    }


def stringify_artifact_paths(value: Any) -> Any:
    """Convert nested artifact paths to strings for JSON output."""
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {key: stringify_artifact_paths(item) for key, item in value.items()}
    return value


def build_archive_hashes(
    model_paths: dict[str, Path],
    rolling_train_parquet: Path,
    prediction_parquet: Path,
    top10_xlsx: Path,
) -> dict[str, Any]:
    """Compute SHA256 audit hashes for immutable archive artifacts."""
    return {
        "model_sha256": {
            "ranking_model": sha256_file(model_paths["ranking_model"]),
            "gap_model": sha256_file(model_paths["gap_model"]),
            "intraday_model": sha256_file(model_paths["intraday_model"]),
        },
        "rolling_train_sha256": sha256_file(rolling_train_parquet),
        "prediction_sha256": sha256_file(prediction_parquet),
        "top10_sha256": sha256_file(top10_xlsx),
    }


def sha256_file(path: str | Path) -> str:
    """Return SHA256 hex digest for a file."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def required_archive_files(archive_root: Path, compact: str) -> list[Path]:
    """Return required archive files for integrity verification."""
    return [
        archive_root / "top10" / f"top10_{compact}.xlsx",
        archive_root / "top10" / f"top10_{compact}.csv",
        archive_root / "predictions" / f"predictions_{compact}.csv",
        archive_root / "predictions" / f"predictions_{compact}.parquet",
        archive_root / "models" / "ranking_model.txt",
        archive_root / "models" / "gap_model.txt",
        archive_root / "models" / "intraday_model.txt",
        archive_root / "training" / f"rolling_train_{compact}.csv",
        archive_root / "training" / f"rolling_train_{compact}.parquet",
        archive_root / "status" / f"daily_update_status_{compact}.json",
        archive_root / "status" / f"daily_update_summary_{compact}.md",
        archive_root / "metadata" / "archive_metadata.json",
        archive_root / "README.txt",
    ]


def verify_archive_integrity(required_files: list[Path]) -> bool:
    """Raise if a required archive file is missing."""
    missing = [str(path) for path in required_files if not path.exists()]
    if missing:
        raise FileNotFoundError("Archive integrity failed; missing files: " + ", ".join(missing))
    return True


def update_latest_archive(archive_root: Path, latest_root: Path) -> Path:
    """Mirror the newest successful archive into outputs/archive/latest."""
    if latest_root.exists():
        shutil.rmtree(latest_root)
    shutil.copytree(archive_root, latest_root)
    return latest_root


def write_archive_readme(
    readme_path: Path,
    run_date: str,
    prediction_date: str,
    target_update_date: str | None,
    train_start_date: str,
    train_end_date: str,
    rolling_train_rows: int,
    model_feature_count: int,
    prediction_rows: int,
) -> None:
    """Write a human-readable archive README."""
    text = f"""Daily Production Archive

Run Date: {run_date}
Prediction Date: {prediction_date}
Target Update Date: {target_update_date}
Rolling Train Start Date: {train_start_date}
Rolling Train End Date: {train_end_date}
Rolling Train Rows: {rolling_train_rows}
Model Feature Count: {model_feature_count}
Prediction Rows: {prediction_rows}

Archive Purpose:
This directory is an immutable audit artifact for reproducing the day's Top10,
prediction outputs, model files, and selected rolling-window training dataset.

How to reproduce this day's prediction:
1. Use the rolling training dataset in training/.
2. Use the model feature columns recorded in metadata/archive_metadata.json.
3. Use the model files in models/.
4. Compare generated predictions with predictions/.
5. Compare the final report with top10/.

Note:
Files in this archive are immutable audit artifacts. Do not edit them in place.
"""
    readme_path.write_text(text, encoding="utf-8")


def get_pipeline_version() -> str | None:
    """Return project version from pyproject if available."""
    pyproject = Path("pyproject.toml")
    if not pyproject.exists():
        return None
    try:
        import tomllib

        data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        return data.get("project", {}).get("version")
    except Exception:
        return None


def get_optional_package_version(package_name: str) -> str | None:
    """Return optional package version without failing archive creation."""
    try:
        module = __import__(package_name)
        return getattr(module, "__version__", None)
    except Exception:
        return None


def get_git_commit_hash() -> str | None:
    """Return current git commit hash if available."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return None
    if result.returncode != 0:
        return None
    commit = result.stdout.strip()
    return commit or None


def print_archive_success(archive_path: Path, rolling_rows: int, prediction_rows: int) -> None:
    """Print user-facing archive success block."""
    print("========================================", flush=True)
    print("", flush=True)
    print("Daily Archive", flush=True)
    print("", flush=True)
    print("SUCCESS", flush=True)
    print("", flush=True)
    print("Archive", flush=True)
    print("", flush=True)
    print(str(archive_path), flush=True)
    print("", flush=True)
    print("Rolling Train Rows", flush=True)
    print("", flush=True)
    print(str(rolling_rows), flush=True)
    print("", flush=True)
    print("Prediction Rows", flush=True)
    print("", flush=True)
    print(str(prediction_rows), flush=True)
    print("", flush=True)
    print("Models Saved", flush=True)
    print("", flush=True)
    print("YES", flush=True)
    print("", flush=True)
    print("Top10 Saved", flush=True)
    print("", flush=True)
    print("YES", flush=True)
    print("", flush=True)
    print("========================================", flush=True)


def print_archive_verification_success() -> None:
    """Print user-facing archive verification block."""
    print("========================================", flush=True)
    print("Archive Verification", flush=True)
    print("SUCCESS", flush=True)
    print("Metadata: OK", flush=True)
    print("SHA256: OK", flush=True)
    print("Latest Updated: YES", flush=True)
    print("Archive Complete: YES", flush=True)
    print("========================================", flush=True)
