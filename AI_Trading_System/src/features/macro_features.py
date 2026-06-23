"""Macro feature generation."""

from __future__ import annotations

import pandas as pd


MACRO_RETURN_COLUMNS: dict[str, str] = {
    "nasdaq_return_1d": "nasdaq_close",
    "sox_return_1d": "sox_close",
    "sp500_return_1d": "sp500_close",
    "vix_change_1d": "vix_close",
    "usdkrw_return_1d": "usdkrw",
    "wti_return_1d": "wti_close",
}


def add_macro_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add macro returns using macro values known by T-1."""
    if "date" not in df.columns:
        raise ValueError("Missing required column: date")
    result = df.copy()
    result["date"] = pd.to_datetime(result["date"])
    sort_columns = ["ticker", "date"] if "ticker" in result.columns else ["date"]
    result = result.sort_values(sort_columns).reset_index(drop=True)

    for output, source in MACRO_RETURN_COLUMNS.items():
        if source not in result.columns:
            continue
        if "ticker" in result.columns:
            result[output] = result.groupby("ticker", sort=False)[source].transform(
                lambda series: series.shift(1) / series.shift(2) - 1
            )
        else:
            result[output] = result[source].shift(1) / result[source].shift(2) - 1
    return result

