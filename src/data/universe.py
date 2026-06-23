"""Daily universe construction for KOSPI200 + KOSDAQ150."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.data.validator import validate_universe_input


SURVIVORSHIP_BIAS_WARNING = (
    "This backtest may contain survivorship bias because the current index "
    "constituents are applied to historical periods."
)

EXCLUDED_SECURITY_TYPES: set[str] = {
    "ETF",
    "ETN",
    "SPAC",
    "PREFERRED",
    "REIT",
}


@dataclass(frozen=True)
class UniverseConfig:
    """Configuration for daily universe filtering."""

    min_trading_value_ma20: float = 5_000_000_000
    max_universe_size: int = 350
    common_stock_type: str = "COMMON"
    apply_liquidity_filter: bool = True


def filter_common_stocks(
    df: pd.DataFrame,
    common_stock_type: str = "COMMON",
) -> pd.DataFrame:
    """Keep common stocks and remove non-common security types."""
    result = df.copy()
    security_type = result["security_type"].astype(str).str.upper()
    mask = (security_type == common_stock_type.upper()) & ~security_type.isin(
        EXCLUDED_SECURITY_TYPES
    )
    return result[mask].copy()


def remove_flagged_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """Remove trading halt and management issue rows when flags exist."""
    result = df.copy()
    if "trading_halt" in result.columns:
        result = result[result["trading_halt"] == False].copy()
    if "management_issue" in result.columns:
        result = result[result["management_issue"] == False].copy()
    return result


def apply_liquidity_filter(
    df: pd.DataFrame,
    min_trading_value_ma20: float,
) -> pd.DataFrame:
    """Filter rows by T-1 20-day average trading value."""
    return df[df["trading_value_ma20"] >= min_trading_value_ma20].copy()


def limit_universe_size(df: pd.DataFrame, max_size: int) -> pd.DataFrame:
    """Limit universe by highest trading value."""
    if max_size <= 0:
        raise ValueError("max_size must be positive")
    return (
        df.sort_values("trading_value_ma20", ascending=False)
        .head(max_size)
        .sort_values(["date", "ticker"])
        .reset_index(drop=True)
    )


def build_daily_universe(
    df: pd.DataFrame,
    config: UniverseConfig | None = None,
) -> pd.DataFrame:
    """Build the v1.0 daily universe from supplied T-1 metadata."""
    settings = config or UniverseConfig()
    validate_universe_input(df)

    universe = filter_common_stocks(df, settings.common_stock_type)
    universe = remove_flagged_stocks(universe)

    if settings.apply_liquidity_filter:
        universe = apply_liquidity_filter(
            universe,
            settings.min_trading_value_ma20,
        )

    universe = limit_universe_size(universe, settings.max_universe_size)
    validate_universe_input(universe)
    return universe


def add_survivorship_bias_warning(metadata: dict[str, str] | None = None) -> dict[str, str]:
    """Attach the required v1.0 survivorship-bias warning to metadata."""
    result = dict(metadata or {})
    result["survivorship_bias_warning"] = SURVIVORSHIP_BIAS_WARNING
    return result
