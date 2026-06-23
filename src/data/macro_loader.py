"""Macro data loading helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.data.data_loader import DataFormat, load_macro_data


def load_macro(path: str | Path, file_format: DataFormat | None = None) -> pd.DataFrame:
    """Load the combined macro dataset required by v1.0."""
    return load_macro_data(path, file_format)


def calculate_macro_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with basic one-day macro return columns."""
    result = df.sort_values("date").copy()
    mappings = {
        "nasdaq_close": "nasdaq_return_1d",
        "sox_close": "sox_return_1d",
        "sp500_close": "sp500_return_1d",
        "vix_close": "vix_change_1d",
        "usdkrw": "usdkrw_return_1d",
        "wti_close": "wti_return_1d",
    }

    for source_column, output_column in mappings.items():
        if source_column in result.columns:
            result[output_column] = result[source_column] / result[source_column].shift(1) - 1

    return result
