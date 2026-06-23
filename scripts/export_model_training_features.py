"""Export exact model training matrices produced by the model preparation path."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
for path in (PROJECT_ROOT, SCRIPT_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from preview_features import create_sample_dataset  # noqa: E402
from src.features.feature_builder import build_features  # noqa: E402
from src.models.model_factory import MODEL_KEYS, build_model_spec, prepare_training_data  # noqa: E402


OUTPUT_DIR = PROJECT_ROOT / "data" / "features" / "model_training"
TARGET_COLUMNS = ("target_rank_return", "target_gap", "target_intraday")
FORBIDDEN_X_COLUMNS = {"date", "ticker", *TARGET_COLUMNS}


def build_sample_training_dataset() -> pd.DataFrame:
    """Create feature data plus synthetic targets for model preparation export."""
    raw_df = create_sample_dataset()
    feature_df = build_features(raw_df).features
    aligned = feature_df.sort_values(["ticker", "date"]).reset_index(drop=True).copy()
    raw_aligned = raw_df.sort_values(["ticker", "date"]).reset_index(drop=True)
    close_values = raw_aligned["close"]
    open_values = raw_aligned["open"]
    group_key = aligned["ticker"]

    aligned["target_rank_return"] = (
        close_values.groupby(group_key).shift(-1) / close_values - 1
    )
    aligned["target_gap"] = (
        open_values.groupby(group_key).shift(-1) / close_values - 1
    )
    aligned["target_intraday"] = (
        close_values.groupby(group_key).shift(-1)
        / open_values.groupby(group_key).shift(-1)
        - 1
    )
    return aligned.dropna(subset=list(TARGET_COLUMNS)).reset_index(drop=True)


def export_training_matrices(
    training_df: pd.DataFrame,
    output_dir: Path = OUTPUT_DIR,
    fold_id: int | None = None,
) -> list[dict[str, Any]]:
    """Export X/y matrices and metadata for each configured LightGBM model."""
    target_dir = output_dir / f"fold_{fold_id:03d}" if fold_id is not None else output_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    metadata: list[dict[str, Any]] = []
    audit_rows: list[pd.DataFrame] = []

    for model_key in MODEL_KEYS:
        spec = build_model_spec(model_key)
        x_train, y_train, feature_columns, categorical_features = prepare_training_data(
            training_df,
            spec,
        )
        _validate_x_train_columns(x_train)

        x_path = target_dir / f"{model_key}_X_train.csv"
        y_path = target_dir / f"{model_key}_y_train.csv"
        x_train.to_csv(x_path, index=False)
        y_train.rename(spec.target).to_frame().to_csv(y_path, index=False)

        date_series = pd.to_datetime(training_df.loc[y_train.index, "date"])
        metadata.append(
            {
                "model_name": spec.model_name,
                "target_name": spec.target,
                "x_train_file": str(x_path),
                "y_train_file": str(y_path),
                "feature_columns": feature_columns,
                "categorical_features": categorical_features,
                "train_start_date": date_series.min().date().isoformat(),
                "train_end_date": date_series.max().date().isoformat(),
                "row_count": int(x_train.shape[0]),
                "column_count": int(x_train.shape[1]),
                "fold_id": fold_id,
            }
        )

        audit = training_df.loc[y_train.index, ["date", "ticker"]].copy()
        audit["fold_id"] = fold_id
        audit["model_name"] = spec.model_name
        audit["target_name"] = spec.target
        audit_rows.append(audit)

        print(
            f"{model_key}: X_train shape={x_train.shape}, "
            f"feature_count={len(feature_columns)}, y_train shape={y_train.shape}"
        )

    _write_metadata(metadata, output_dir, fold_id)
    _write_audit(audit_rows, output_dir, fold_id)
    return metadata


def _validate_x_train_columns(x_train: pd.DataFrame) -> None:
    forbidden = FORBIDDEN_X_COLUMNS & set(x_train.columns)
    if forbidden:
        raise ValueError(f"Forbidden columns found in X_train: {sorted(forbidden)}")


def _write_metadata(
    metadata: list[dict[str, Any]],
    output_dir: Path,
    fold_id: int | None,
) -> None:
    if fold_id is None:
        metadata_path = output_dir / "feature_metadata.json"
    else:
        metadata_path = output_dir / f"fold_{fold_id:03d}" / "feature_metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")


def _write_audit(
    audit_rows: list[pd.DataFrame],
    output_dir: Path,
    fold_id: int | None,
) -> None:
    audit = pd.concat(audit_rows, ignore_index=True)
    if fold_id is None:
        audit_path = output_dir / "training_audit.csv"
    else:
        audit_path = output_dir / f"fold_{fold_id:03d}" / "training_audit.csv"
    audit.to_csv(audit_path, index=False)


def main() -> None:
    """Export model training features for inspection."""
    print("Creating sample feature data with synthetic targets...")
    training_df = build_sample_training_dataset()
    print(f"Training dataset shape before model preparation: {training_df.shape}")
    print("Exporting exact model training matrices...")
    export_training_matrices(training_df)
    export_training_matrices(training_df, fold_id=1)
    print(f"Saved model training feature exports under: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
