"""Walk-forward fold generation and validation."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.model.utils.config_loader import load_yaml_config
from src.model.utils.paths import build_project_paths


@dataclass(frozen=True)
class WalkForwardFold:
    """One chronological walk-forward train/validation/test split."""

    fold_id: int
    train_start_date: pd.Timestamp
    train_end_date: pd.Timestamp
    valid_start_date: pd.Timestamp
    valid_end_date: pd.Timestamp
    test_start_date: pd.Timestamp
    test_end_date: pd.Timestamp

    def to_metadata(self) -> dict[str, str | int]:
        """Return serializable fold metadata."""
        return {
            "fold_id": self.fold_id,
            "train_start_date": self.train_start_date.date().isoformat(),
            "train_end_date": self.train_end_date.date().isoformat(),
            "valid_start_date": self.valid_start_date.date().isoformat(),
            "valid_end_date": self.valid_end_date.date().isoformat(),
            "test_start_date": self.test_start_date.date().isoformat(),
            "test_end_date": self.test_end_date.date().isoformat(),
        }


def load_validation_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Load validation.yaml and return the validation config root."""
    path = Path(config_path) if config_path is not None else build_project_paths().configs / "validation.yaml"
    data = load_yaml_config(path)
    if "validation" not in data:
        raise ValueError("validation.yaml must contain a 'validation' root key")
    return data["validation"]


def generate_walk_forward_folds(
    config: dict[str, Any] | None = None,
) -> list[WalkForwardFold]:
    """Generate expanding-window folds from validation config."""
    validation_config = config or load_validation_config()
    walk_forward = validation_config.get("walk_forward", {})
    configured_folds = walk_forward.get("folds", [])
    if configured_folds:
        folds = [_fold_from_mapping(item) for item in configured_folds]
    else:
        folds = _generate_folds_from_dates(walk_forward)

    validate_folds(folds)
    return folds


def validate_fold_order(fold: WalkForwardFold) -> None:
    """Validate chronological order and no leakage for one fold."""
    if not fold.train_start_date < fold.train_end_date:
        raise ValueError(f"Fold {fold.fold_id}: train_start_date must be before train_end_date")
    if not fold.train_end_date < fold.valid_start_date:
        raise ValueError(f"Fold {fold.fold_id}: train period must end before validation starts")
    if not fold.valid_start_date < fold.valid_end_date:
        raise ValueError(f"Fold {fold.fold_id}: valid_start_date must be before valid_end_date")
    if not fold.valid_end_date < fold.test_start_date:
        raise ValueError(f"Fold {fold.fold_id}: validation must end before test starts")
    if not fold.test_start_date < fold.test_end_date:
        raise ValueError(f"Fold {fold.fold_id}: test_start_date must be before test_end_date")


def validate_folds(folds: list[WalkForwardFold]) -> None:
    """Validate all folds and ensure test windows do not overlap."""
    if not folds:
        raise ValueError("At least one walk-forward fold is required")
    seen_fold_ids: set[int] = set()
    test_windows: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    previous_train_end: pd.Timestamp | None = None

    for fold in sorted(folds, key=lambda item: item.fold_id):
        if fold.fold_id in seen_fold_ids:
            raise ValueError(f"Duplicate fold_id: {fold.fold_id}")
        seen_fold_ids.add(fold.fold_id)
        validate_fold_order(fold)
        if previous_train_end is not None and fold.train_end_date <= previous_train_end:
            raise ValueError("Expanding window requires increasing train_end_date")
        previous_train_end = fold.train_end_date
        test_windows.append((fold.test_start_date, fold.test_end_date))

    for idx, (start_a, end_a) in enumerate(test_windows):
        for start_b, end_b in test_windows[idx + 1 :]:
            if start_a <= end_b and start_b <= end_a:
                raise ValueError("Walk-forward test windows must not overlap")


def split_by_fold(
    df: pd.DataFrame,
    fold: WalkForwardFold,
    date_column: str = "date",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split a DataFrame into train, validation, and test sets for one fold."""
    if date_column not in df.columns:
        raise ValueError(f"Missing date column: {date_column}")
    data = df.copy()
    data[date_column] = pd.to_datetime(data[date_column])
    train = data[
        (data[date_column] >= fold.train_start_date)
        & (data[date_column] <= fold.train_end_date)
    ].copy()
    valid = data[
        (data[date_column] >= fold.valid_start_date)
        & (data[date_column] <= fold.valid_end_date)
    ].copy()
    test = data[
        (data[date_column] >= fold.test_start_date)
        & (data[date_column] <= fold.test_end_date)
    ].copy()
    _validate_split_data(train, valid, test, date_column)
    return train, valid, test


def save_fold_metadata(
    folds: list[WalkForwardFold],
    output_path: str | Path | None = None,
) -> Path:
    """Save fold metadata to CSV and return the output path."""
    path = Path(output_path) if output_path is not None else _default_fold_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [fold.to_metadata() for fold in folds]
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return path


def _fold_from_mapping(data: dict[str, Any]) -> WalkForwardFold:
    return WalkForwardFold(
        fold_id=int(data["fold_id"]),
        train_start_date=pd.Timestamp(data["train_start_date"]),
        train_end_date=pd.Timestamp(data["train_end_date"]),
        valid_start_date=pd.Timestamp(data["valid_start_date"]),
        valid_end_date=pd.Timestamp(data["valid_end_date"]),
        test_start_date=pd.Timestamp(data["test_start_date"]),
        test_end_date=pd.Timestamp(data["test_end_date"]),
    )


def _generate_folds_from_dates(walk_forward: dict[str, Any]) -> list[WalkForwardFold]:
    train_start = pd.Timestamp(walk_forward["train_start_date"])
    end_date = pd.Timestamp(walk_forward["end_date"])
    min_train_years = int(walk_forward.get("min_train_years", 4))
    validation_years = int(walk_forward.get("validation_years", 1))
    test_years = int(walk_forward.get("test_years", 1))
    if walk_forward.get("train_window_type") != "expanding":
        raise ValueError("Phase 4 supports expanding train windows only")

    folds: list[WalkForwardFold] = []
    fold_id = 1
    train_end = train_start + pd.DateOffset(years=min_train_years) - pd.DateOffset(days=1)
    while True:
        valid_start = train_end + pd.DateOffset(days=1)
        valid_end = valid_start + pd.DateOffset(years=validation_years) - pd.DateOffset(days=1)
        test_start = valid_end + pd.DateOffset(days=1)
        test_end = test_start + pd.DateOffset(years=test_years) - pd.DateOffset(days=1)
        if test_end > end_date:
            break
        folds.append(
            WalkForwardFold(
                fold_id=fold_id,
                train_start_date=train_start,
                train_end_date=train_end,
                valid_start_date=valid_start,
                valid_end_date=valid_end,
                test_start_date=test_start,
                test_end_date=test_end,
            )
        )
        fold_id += 1
        train_end = train_end + pd.DateOffset(years=test_years)
    return folds


def _validate_split_data(
    train: pd.DataFrame,
    valid: pd.DataFrame,
    test: pd.DataFrame,
    date_column: str,
) -> None:
    if train.empty or valid.empty or test.empty:
        raise ValueError("Fold split produced an empty train, validation, or test set")
    if not train[date_column].max() < valid[date_column].min():
        raise ValueError("Train period must be before validation period")
    if not valid[date_column].max() < test[date_column].min():
        raise ValueError("Validation period must be before test period")


def _default_fold_file() -> Path:
    config = load_validation_config()
    return build_project_paths().root / config["output"]["fold_file"]


def folds_to_frame(folds: list[WalkForwardFold]) -> pd.DataFrame:
    """Convert fold metadata to a DataFrame."""
    return pd.DataFrame([asdict(fold) for fold in folds])

