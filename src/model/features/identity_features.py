"""Identity feature handling."""

from __future__ import annotations

import pandas as pd

from src.model.features._common import sort_by_ticker_date


IDENTITY_COLUMNS: tuple[str, ...] = ("sector", "market_type", "market_cap_group")


def add_identity_features(df: pd.DataFrame, fill_value: str = "unknown") -> pd.DataFrame:
    """Ensure configured categorical identity features are present and filled."""
    result = sort_by_ticker_date(df)
    for column in IDENTITY_COLUMNS:
        if column not in result.columns:
            result[column] = fill_value
        result[column] = result[column].fillna(fill_value).astype(str)
    return result

