"""Generate a small synthetic feature preview dataset."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.features.feature_builder import build_features  # noqa: E402


TARGET_COLUMNS: set[str] = {"target_rank_return", "target_gap", "target_intraday"}
OUTPUT_DIR = PROJECT_ROOT / "data" / "features"
CSV_PATH = OUTPUT_DIR / "sample_features.csv"
PARQUET_PATH = OUTPUT_DIR / "sample_features.parquet"


def create_sample_dataset() -> pd.DataFrame:
    """Create synthetic OHLCV, identity, and macro input data."""
    dates = pd.date_range("2024-01-01", periods=90, freq="B")
    tickers = [
        ("AAA", "Semiconductor", "KOSPI", "Top50", 0.0),
        ("BBB", "Auto", "KOSPI", "Top100", 8.0),
        ("CCC", "Battery", "KOSDAQ", "MidCap", 16.0),
    ]
    rows: list[dict[str, object]] = []

    for ticker, sector, market_type, market_cap_group, offset in tickers:
        for idx, date in enumerate(dates):
            trend = idx * 0.45
            cycle = (idx % 7) * 0.12
            close = 100.0 + offset + trend + cycle
            open_price = close * (1.0 - 0.002 + (idx % 3) * 0.001)
            high = max(open_price, close) * 1.01
            low = min(open_price, close) * 0.99
            volume = 1_000_000 + idx * 8_000 + int(offset * 2_000)

            rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "sector": sector,
                    "market_type": market_type,
                    "market_cap_group": market_cap_group,
                    "nasdaq_close": 15_000.0 + idx * 12.0,
                    "sox_close": 4_000.0 + idx * 4.5,
                    "sp500_close": 5_000.0 + idx * 3.2,
                    "vix_close": 15.0 + (idx % 10) * 0.15,
                    "usdkrw": 1_300.0 + idx * 0.35,
                    "wti_close": 72.0 + idx * 0.04,
                }
            )

    return pd.DataFrame(rows)


def print_preview(feature_df: pd.DataFrame) -> None:
    """Print a compact feature inspection report."""
    feature_columns = [column for column in feature_df.columns if column not in {"date", "ticker"}]
    missing_ratio = feature_df.isna().mean().sort_values(ascending=False)

    print("Feature preview generated")
    print(f"Shape: {feature_df.shape}")
    print(f"Feature column count: {len(feature_columns)}")
    print("Feature columns:")
    for column in feature_columns:
        print(f"  - {column}")

    print("\nFirst 10 rows:")
    print(feature_df.head(10).to_string(index=False))

    print("\nMissing value ratio by column:")
    print(missing_ratio.to_string())


def main() -> None:
    """Build and save sample feature outputs."""
    print("Creating synthetic OHLCV + macro sample dataset...")
    sample_df = create_sample_dataset()

    print("Generating features with existing Phase 2 FeatureBuilder...")
    build_result = build_features(sample_df)
    feature_df = build_result.features

    forbidden = TARGET_COLUMNS & set(feature_df.columns)
    if forbidden:
        raise ValueError(f"Target columns must not appear in feature output: {sorted(forbidden)}")
    if {"date", "ticker"} - set(feature_df.columns):
        raise ValueError("Feature output must preserve date and ticker columns")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    feature_df.to_csv(CSV_PATH, index=False)
    feature_df.to_parquet(PARQUET_PATH, index=False)

    print_preview(feature_df)
    print("\nSaved outputs:")
    print(f"  CSV: {CSV_PATH}")
    print(f"  Parquet: {PARQUET_PATH}")


if __name__ == "__main__":
    main()
