"""Export validation feature store files from the existing feature builder."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
for path in (PROJECT_ROOT, SCRIPT_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from preview_features import TARGET_COLUMNS, create_sample_dataset  # noqa: E402
from src.features.feature_builder import build_features  # noqa: E402


OUTPUT_DIR = PROJECT_ROOT / "data" / "features"
CSV_PATH = OUTPUT_DIR / "validation_features.csv"
PARQUET_PATH = OUTPUT_DIR / "validation_features.parquet"


def main() -> None:
    """Build and save the validation feature store."""
    print("Creating validation feature input dataset...")
    sample_df = create_sample_dataset()

    print("Generating validation features with existing Phase 2 FeatureBuilder...")
    feature_df = build_features(sample_df).features

    forbidden = TARGET_COLUMNS & set(feature_df.columns)
    if forbidden:
        raise ValueError(f"Target columns must not appear in validation features: {sorted(forbidden)}")
    if {"date", "ticker"} - set(feature_df.columns):
        raise ValueError("Validation features must preserve date and ticker columns")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    feature_df.to_csv(CSV_PATH, index=False)
    feature_df.to_parquet(PARQUET_PATH, index=False)

    feature_count = len([column for column in feature_df.columns if column not in {"date", "ticker"}])
    print("Validation feature store exported")
    print(f"Shape: {feature_df.shape}")
    print(f"Feature count: {feature_count}")
    print(f"CSV: {CSV_PATH}")
    print(f"Parquet: {PARQUET_PATH}")


if __name__ == "__main__":
    main()
