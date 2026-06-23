"""Build real feature store from cleaned market data."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.features.feature_builder import build_features  # noqa: E402


OHLCV_PATH = PROJECT_ROOT / "data" / "processed" / "kr_stock" / "ohlcv_clean_20230615_20260614.parquet"
MACRO_PATH = PROJECT_ROOT / "data" / "processed" / "macro" / "macro_clean_20230615_20260614.parquet"
OUTPUT_DIR = PROJECT_ROOT / "data" / "features"
FEATURE_PARQUET = OUTPUT_DIR / "real_features_20230615_20260614.parquet"
FEATURE_CSV = OUTPUT_DIR / "real_features_20230615_20260614.csv"
METADATA_JSON = OUTPUT_DIR / "real_feature_metadata.json"
QUALITY_MD = OUTPUT_DIR / "real_feature_quality_report.md"
TARGET_COLUMNS = {"target_rank_return", "target_gap", "target_intraday"}


def load_cleaned_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load cleaned OHLCV and macro datasets."""
    ohlcv = pd.read_parquet(OHLCV_PATH)
    macro = pd.read_parquet(MACRO_PATH)
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    ohlcv["ticker"] = ohlcv["ticker"].astype(str).str.zfill(6)
    macro["date"] = pd.to_datetime(macro["date"])
    return ohlcv, macro


def merge_ohlcv_macro(ohlcv: pd.DataFrame, macro: pd.DataFrame) -> pd.DataFrame:
    """Merge cleaned OHLCV with cleaned macro by date."""
    merged = ohlcv.merge(macro, on="date", how="left", validate="many_to_one")
    return merged.sort_values(["ticker", "date"]).reset_index(drop=True)


def build_report(
    feature_df: pd.DataFrame,
    metadata: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    """Build JSON metadata and Markdown quality report."""
    feature_columns = [column for column in feature_df.columns if column not in {"date", "ticker"}]
    nan_ratio = {
        column: float(value)
        for column, value in feature_df.isna().mean().sort_values(ascending=False).items()
    }
    min_date = pd.Timestamp(feature_df["date"].min()).date().isoformat()
    max_date = pd.Timestamp(feature_df["date"].max()).date().isoformat()
    target_excluded = TARGET_COLUMNS.isdisjoint(feature_df.columns)
    samsung_exists = bool((feature_df["ticker"] == "005930").any())

    report = {
        "input_files": {
            "ohlcv": str(OHLCV_PATH),
            "macro": str(MACRO_PATH),
        },
        "output_files": {
            "parquet": str(FEATURE_PARQUET),
            "csv": str(FEATURE_CSV),
        },
        "shape": list(feature_df.shape),
        "feature_count": len(feature_columns),
        "feature_columns": feature_columns,
        "first_10_columns": list(feature_df.columns[:10]),
        "unique_ticker_count": int(feature_df["ticker"].nunique()),
        "min_date": min_date,
        "max_date": max_date,
        "ticker_005930_exists": samsung_exists,
        "target_columns_excluded": target_excluded,
        "nan_ratio_by_column": nan_ratio,
        "builder_metadata": metadata,
    }

    lines = [
        "# Real Feature Store Quality Report",
        "",
        f"- Shape: `{tuple(feature_df.shape)}`",
        f"- Feature count: `{len(feature_columns)}`",
        f"- Unique ticker count: `{feature_df['ticker'].nunique()}`",
        f"- Date range: `{min_date}` to `{max_date}`",
        f"- Ticker 005930 exists: `{samsung_exists}`",
        f"- Target columns excluded: `{target_excluded}`",
        f"- First 10 columns: `{list(feature_df.columns[:10])}`",
        "",
        "## NaN Ratio By Column",
        "| Column | NaN Ratio |",
        "| --- | ---: |",
    ]
    for column, ratio in nan_ratio.items():
        lines.append(f"| {column} | {ratio:.6f} |")
    lines.extend(
        [
            "",
            "## Leakage Note",
            "Features were generated only through the existing Phase 2 FeatureBuilder.",
            "No feature formulas were changed in this script.",
        ]
    )
    return report, "\n".join(lines) + "\n"


def main() -> None:
    """Build and save real feature store."""
    print("Loading cleaned market data...")
    ohlcv, macro = load_cleaned_inputs()
    print(f"Cleaned OHLCV shape: {ohlcv.shape}")
    print(f"Cleaned macro shape: {macro.shape}")

    print("Merging OHLCV and macro by date...")
    merged = merge_ohlcv_macro(ohlcv, macro)
    print(f"Merged input shape: {merged.shape}")

    print("Generating features with existing Phase 2 FeatureBuilder...")
    result = build_features(merged)
    feature_df = result.features

    if not TARGET_COLUMNS.isdisjoint(feature_df.columns):
        raise ValueError("Target columns must not be present in real feature store")
    if {"date", "ticker"} - set(feature_df.columns):
        raise ValueError("Real feature store must preserve date and ticker")
    if not (feature_df["ticker"] == "005930").any():
        raise ValueError("Ticker 005930 must exist in real feature store")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    feature_df.to_parquet(FEATURE_PARQUET, index=False)
    feature_df.to_csv(FEATURE_CSV, index=False)
    report, markdown = build_report(feature_df, result.metadata)
    METADATA_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    QUALITY_MD.write_text(markdown, encoding="utf-8")

    print("Real feature store generated")
    print(f"Shape: {tuple(feature_df.shape)}")
    print(f"Feature count: {report['feature_count']}")
    print(f"Unique ticker count: {report['unique_ticker_count']}")
    print(f"Date range: {report['min_date']} to {report['max_date']}")
    print(f"Ticker 005930 exists: {report['ticker_005930_exists']}")
    print(f"Target columns excluded: {report['target_columns_excluded']}")
    print(f"First 10 columns: {report['first_10_columns']}")
    print(f"Saved parquet: {FEATURE_PARQUET}")
    print(f"Saved CSV: {FEATURE_CSV}")
    print(f"Saved metadata: {METADATA_JSON}")
    print(f"Saved quality report: {QUALITY_MD}")


if __name__ == "__main__":
    main()
