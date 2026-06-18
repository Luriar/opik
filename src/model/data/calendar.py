"""Trading calendar utilities."""

from __future__ import annotations

import pandas as pd


def sort_trading_dates(dates: pd.Series | list[pd.Timestamp | str]) -> list[pd.Timestamp]:
    """Return unique trading dates sorted ascending."""
    parsed = pd.to_datetime(pd.Series(dates)).dropna().drop_duplicates()
    return list(parsed.sort_values())


def get_previous_trading_date(
    dates: pd.Series | list[pd.Timestamp | str],
    current_date: pd.Timestamp | str,
) -> pd.Timestamp:
    """Return the trading date immediately before current_date."""
    sorted_dates = sort_trading_dates(dates)
    current = pd.Timestamp(current_date)
    previous = [date for date in sorted_dates if date < current]
    if not previous:
        raise ValueError(f"No previous trading date before {current}")
    return previous[-1]


def get_next_trading_date(
    dates: pd.Series | list[pd.Timestamp | str],
    current_date: pd.Timestamp | str,
) -> pd.Timestamp:
    """Return the trading date immediately after current_date."""
    sorted_dates = sort_trading_dates(dates)
    current = pd.Timestamp(current_date)
    following = [date for date in sorted_dates if date > current]
    if not following:
        raise ValueError(f"No next trading date after {current}")
    return following[0]


def validate_date_coverage(
    dates: pd.Series | list[pd.Timestamp | str],
    start_date: pd.Timestamp | str,
    end_date: pd.Timestamp | str,
) -> None:
    """Validate that trading dates cover the requested range."""
    sorted_dates = sort_trading_dates(dates)
    if not sorted_dates:
        raise ValueError("Trading calendar is empty")

    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    if sorted_dates[0] > start:
        raise ValueError(f"Calendar starts after requested start date: {start}")
    if sorted_dates[-1] < end:
        raise ValueError(f"Calendar ends before requested end date: {end}")
